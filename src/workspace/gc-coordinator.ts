import { spawn } from "node:child_process";
import { access } from "node:fs/promises";
import { dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { acquireRepositoryLock } from "../locks.js";
import { WorkspaceLifecycle, type GcReport } from "./lifecycle.js";
import {
  readJsonFile,
  removeFile,
  writeJsonAtomic,
  HUB_STATE_SUBDIR,
} from "./home.js";
import type { WorkspaceRecord } from "./records.js";

/**
 * The CLI is intentionally short-lived, but a handoff's retention clock is
 * not.  This state file is the durable wake-up record for a detached worker.
 * The worker re-runs the normal recovery/GC gates; it is only a scheduler, not
 * a second deletion implementation.
 */
const GC_STATE_SCHEMA = "agent-hub-gc-coordinator/v1" as const;
const GC_STATE_LOCK = "gc-state";
const GC_WORKER_LOCK = "gc-worker";
const GC_STATE_FILE = `${HUB_STATE_SUBDIR}/gc/coordinator.json`;
const WORKER_POLL_MS = 30_000;
const WORKER_LOCK_WAIT_MS = 5_000;
const RETRY_MS = 15 * 60 * 1000;
const MAX_TIMER_MS = 2_147_000_000;

interface GcCoordinatorState {
  schema: typeof GC_STATE_SCHEMA;
  home: string;
  repository_cwd: string;
  due_at: string;
  revision: number;
}

export interface GcCoordinatorOptions {
  /** Test seam; production always starts a detached worker process. */
  spawnWorker?: (home: string) => void;
}

export function gcCoordinatorStatePath(home: string): string {
  return join(home, GC_STATE_FILE);
}

function validState(value: unknown, home: string): value is GcCoordinatorState {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return false;
  const candidate = value as Record<string, unknown>;
  return (
    candidate.schema === GC_STATE_SCHEMA
    && candidate.home === home
    && typeof candidate.repository_cwd === "string"
    && isAbsolute(candidate.repository_cwd)
    && typeof candidate.due_at === "string"
    && Number.isFinite(Date.parse(candidate.due_at))
    && Number.isInteger(candidate.revision)
    && (candidate.revision as number) >= 0
  );
}

async function readState(home: string): Promise<GcCoordinatorState | null> {
  const value = await readJsonFile(gcCoordinatorStatePath(home));
  return validState(value, home) ? value : null;
}

async function withStateLock<T>(home: string, operation: () => Promise<T>): Promise<T> {
  const lock = await acquireRepositoryLock({
    commonDir: home,
    name: GC_STATE_LOCK,
    waitMs: 2_000,
  });
  try {
    return await operation();
  } finally {
    await lock.release();
  }
}

function retryable(code: GcReport["retained"][number]["code"]): boolean {
  return new Set([
    "retention-active",
    "runtime-attached",
    "runtime-live",
    "lease-live",
    "lease-uncertain",
    "lease-foreign",
    "hub-live",
    "locked-by-peer",
    "admin-locked",
    "repository-unreachable",
    "worktree-remove-failed",
  ]).has(code);
}

function nextRetentionDeadline(records: readonly WorkspaceRecord[], nowMs: number): number | null {
  let next: number | null = null;
  for (const record of records) {
    if (record.handoff === null || record.retention_until === null) continue;
    const at = Date.parse(record.retention_until);
    if (!Number.isFinite(at) || at <= nowMs) continue;
    next = next === null ? at : Math.min(next, at);
  }
  return next;
}

export function earliestGcWake(
  retentionDue: number | null,
  retryAt: number | null,
): number | null {
  if (retentionDue === null) return retryAt;
  if (retryAt === null) return retentionDue;
  return Math.min(retentionDue, retryAt);
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolveSleep) => setTimeout(resolveSleep, ms));
}

function workerCommand(): { command: string; args: string[] } {
  const source = fileURLToPath(import.meta.url);
  const isTypeScript = source.endsWith(".ts");
  const worker = `${dirname(source)}/../gc-worker${isTypeScript ? ".ts" : ".js"}`;
  return {
    command: process.execPath,
    args: isTypeScript ? ["--import", "tsx", worker] : [worker],
  };
}

/** Start a process whose lifetime is independent of the caller/CLI process. */
export function spawnDetachedGcWorker(home: string): void {
  const { command, args } = workerCommand();
  try {
    const child = spawn(command, [...args, "--home", home], {
      detached: true,
      stdio: "ignore",
      env: { ...process.env, AGENT_HUB_HOME: home },
    });
    child.once("error", () => undefined);
    child.unref();
  } catch {
    // The durable state remains. A later Agent Hub startup performs catch-up,
    // and a later handoff will try to start the worker again.
  }
}

/**
 * Arms one durable coordinator for a handoff. Multiple handoffs share the
 * earliest deadline; duplicate detached launches are harmless because the
 * worker lock makes the actual runner a singleton.
 */
export class GcCoordinator {
  private readonly home: string;
  private readonly spawnWorker: (home: string) => void;

  constructor(home: string, options: GcCoordinatorOptions = {}) {
    this.home = resolve(home);
    this.spawnWorker = options.spawnWorker ?? spawnDetachedGcWorker;
  }

  async arm(record: Pick<WorkspaceRecord, "repository_cwd" | "retention_until">): Promise<void> {
    if (record.retention_until === null) return;
    const dueAt = Date.parse(record.retention_until);
    if (!Number.isFinite(dueAt)) {
      throw new Error("cannot arm GC coordinator: retention_until is not a valid timestamp");
    }
    await withStateLock(this.home, async () => {
      const previous = await readState(this.home);
      const previousDue = previous === null ? Number.POSITIVE_INFINITY : Date.parse(previous.due_at);
      const useNewDeadline = dueAt < previousDue;
      const state: GcCoordinatorState = {
        schema: GC_STATE_SCHEMA,
        home: this.home,
        repository_cwd: useNewDeadline || previous === null
          ? record.repository_cwd
          : previous.repository_cwd,
        due_at: new Date(Math.min(dueAt, previousDue)).toISOString(),
        revision: (previous?.revision ?? 0) + 1,
      };
      await writeJsonAtomic(gcCoordinatorStatePath(this.home), state);
    });
    this.spawnWorker(this.home);
  }

  /**
   * Startup catch-up for a worker that died without deleting the durable
   * schedule. The worker lock makes this safe when another copy is already
   * alive; the state file makes it possible to recover without a new handoff.
   */
  async ensureWorker(): Promise<void> {
    const scheduled = await withStateLock(this.home, async () => (await readState(this.home)) !== null);
    if (scheduled) this.spawnWorker(this.home);
  }
}

async function runGcPass(home: string): Promise<{ report: GcReport; records: WorkspaceRecord[] }> {
  const lifecycle = new WorkspaceLifecycle({ home });
  await lifecycle.recover([]);
  const report = await lifecycle.gc([]);
  return { report, records: await lifecycle.list() };
}

async function updateAfterPass(
  home: string,
  observed: GcCoordinatorState,
  report: GcReport | null,
  records: readonly WorkspaceRecord[],
): Promise<boolean> {
  return withStateLock(home, async () => {
    const current = await readState(home);
    if (current === null) return false;
    // A handoff arrived while the pass was running. Keep its durable state;
    // the next loop reads the new deadline instead of overwriting it.
    if (current.revision !== observed.revision) return true;

    const nowMs = Date.now();
    const retentionDue = nextRetentionDeadline(records, nowMs);
    const needsRetry = report?.retained.some((entry) => retryable(entry.code)) ?? true;
    const retryAt = needsRetry ? nowMs + RETRY_MS : null;
    const nextDue = earliestGcWake(retentionDue, retryAt);
    if (nextDue === null) {
      await removeFile(gcCoordinatorStatePath(home));
      return false;
    }
    await writeJsonAtomic(gcCoordinatorStatePath(home), {
      ...current,
      due_at: new Date(nextDue).toISOString(),
      revision: current.revision + 1,
    } satisfies GcCoordinatorState);
    return true;
  });
}

/**
 * Detached worker entry point. It holds a recoverable singleton lease, uses a
 * durable deadline with bounded polling (so an earlier new handoff wakes it),
 * and delegates every deletion decision to WorkspaceLifecycle.gc().
 */
export async function runGcWorker(home: string): Promise<void> {
  const normalizedHome = resolve(home);
  let workerLock;
  try {
    workerLock = await acquireRepositoryLock({
      commonDir: normalizedHome,
      name: GC_WORKER_LOCK,
      // A handoff can launch a replacement while the previous worker is
      // finishing its last state update. Waiting closes that exit/wakeup race;
      // the durable state is then re-read after the lease is acquired.
      waitMs: WORKER_LOCK_WAIT_MS,
    });
  } catch {
    return;
  }

  try {
    for (;;) {
      const state = await readState(normalizedHome);
      if (state === null) return;
      const waitMs = Math.max(0, Date.parse(state.due_at) - Date.now());
      if (waitMs > 0) {
        await sleep(Math.min(waitMs, WORKER_POLL_MS, MAX_TIMER_MS));
        continue;
      }

      // GC's safety gates use each record's repository identity. Chdir to the
      // durable coordinator anchor when it still exists so the admin pass also
      // has a useful caller context after the original CLI has exited.
      try {
        await access(state.repository_cwd);
        process.chdir(state.repository_cwd);
      } catch {
        // A removed/unreachable checkout is retained by the normal GC report.
      }

      let report: GcReport | null = null;
      let records: WorkspaceRecord[] = [];
      try {
        ({ report, records } = await runGcPass(normalizedHome));
      } catch {
        // Keep the deadline durable and retry later. No deletion occurs outside
        // WorkspaceLifecycle.gc(), whose safe preconditions remain authoritative.
      }
      if (!(await updateAfterPass(normalizedHome, state, report, records))) return;
    }
  } finally {
    await workerLock.release().catch(() => undefined);
  }
}
