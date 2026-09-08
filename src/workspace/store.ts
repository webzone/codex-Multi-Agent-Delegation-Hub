import { mkdir, readdir, rm } from "node:fs/promises";
import { join } from "node:path";

import { AgentHubError } from "../errors.js";
import { acquireRepositoryLock } from "../locks.js";
import type { RepositoryLock } from "../locks.js";
import {
  isWorkspaceSessionId,
  PROCESSES_SUBDIR,
  readJsonFile,
  removeFile,
  resultPath,
  runtimeMirrorPath,
  WORKSPACES_SUBDIR,
  WORKTREES_SUBDIR,
  workspaceDir,
  workspacePendingPath,
  workspaceRecordPath,
  writeJsonAtomic,
} from "./home.js";
import { probeRef, casRef } from "./gitops.js";
import {
  parseResultRecord,
  parseRuntimeMirror,
  parseWorkspaceRecord,
  parseWorkspaceTransaction,
  WORKSPACE_RUNTIME_SCHEMA_VERSION,
} from "./records.js";
import type {
  WorkspaceRecord,
  WorkspaceResultRecord,
  WorkspaceRuntimeMirror,
  WorkspaceTransaction,
} from "./records.js";
import type { SessionRecord } from "../kernel/contracts.js";

/**
 * The custody store: hub-home persistence, per-workspace locking, and the
 * sidecar → ref-CAS → state-write → sidecar-removal transaction that keeps
 * the git ref and the durable record in exact agreement across crashes.
 *
 * The hub home plays the role of a git common dir for `src/locks.ts`: all
 * locks live under `<AGENT_HUB_HOME>/agent-hub/locks` and follow the same
 * token/owner/stale-dead discipline. Nothing in this module ever deletes a
 * workspace; deletion belongs to GC, which runs its own preconditions.
 */

export const WORKSPACE_ADMIN_LOCK = "workspace-admin";

export function workspaceLockName(sessionId: string): string {
  return `ws-${sessionId}`;
}

export interface StoreContext {
  home: string;
  repositoryCwd: string;
  now: () => Date;
  /** How long a custody lock may be waited for. 0 (default) = single attempt. */
  lockWaitMs?: number;
  lockRetryDelayMs?: number;
  probePid?: (pid: number) => "live" | "dead";
}

export type WorkspacePhase =
  | "sidecar-written"
  | "captured-written"
  | "ref-updated"
  | "record-committed";

export type PhaseObserver = (phase: WorkspacePhase, sessionId: string) => Promise<void> | void;

function workspaceError(code: string, message: string): AgentHubError {
  return new AgentHubError(code, message);
}

// ---------------------------------------------------------------------------
// Locks
// ---------------------------------------------------------------------------

async function acquireLock(
  ctx: StoreContext,
  name: string,
  waitMs: number,
): Promise<RepositoryLock> {
  return acquireRepositoryLock({
    commonDir: ctx.home,
    name,
    waitMs,
    retryDelayMs: ctx.lockRetryDelayMs ?? 25,
    ...(ctx.probePid ? { probePid: ctx.probePid } : {}),
    now: ctx.now,
  });
}

/** Run `operation` while holding the workspace's exclusive custody lock. */
export async function withWorkspaceLock<T>(
  ctx: StoreContext,
  sessionId: string,
  operation: () => Promise<T>,
): Promise<T> {
  const lock = await acquireLock(ctx, workspaceLockName(sessionId), ctx.lockWaitMs ?? 0);
  try {
    return await operation();
  } finally {
    await lock.release();
  }
}

/** Admin lock for worktree administration (add/prune) and GC passes. */
export async function withAdminLock<T>(
  ctx: StoreContext,
  operation: () => Promise<T>,
  opts: { waitMs?: number } = {},
): Promise<T> {
  const lock = await acquireLock(ctx, WORKSPACE_ADMIN_LOCK, opts.waitMs ?? ctx.lockWaitMs ?? 0);
  try {
    return await operation();
  } finally {
    await lock.release();
  }
}

// ---------------------------------------------------------------------------
// Record / result / runtime reads
// ---------------------------------------------------------------------------

export type RecordRead =
  | { status: "absent" }
  | { status: "corrupt" }
  | { status: "present"; record: WorkspaceRecord };

export async function readRecordRaw(home: string, sessionId: string): Promise<RecordRead> {
  const raw = await readJsonFile(workspaceRecordPath(home, sessionId));
  if (raw === undefined) {
    return { status: "absent" };
  }
  if (raw === null) {
    return { status: "corrupt" };
  }
  try {
    return { status: "present", record: parseWorkspaceRecord(raw) };
  } catch {
    return { status: "corrupt" };
  }
}

export async function writeRecordAtomic(home: string, record: WorkspaceRecord): Promise<void> {
  // Rebuild through the parser: only a record the durable contract accepts
  // may hit disk.
  await writeJsonAtomic(
    workspaceRecordPath(home, record.session_id),
    parseWorkspaceRecord(record),
  );
}

export async function readResult(
  home: string,
  sessionId: string,
  seq: number,
): Promise<WorkspaceResultRecord | "absent" | "corrupt"> {
  const raw = await readJsonFile(resultPath(home, sessionId, seq));
  if (raw === undefined) {
    return "absent";
  }
  if (raw === null) {
    return "corrupt";
  }
  try {
    return parseResultRecord(raw);
  } catch {
    return "corrupt";
  }
}

export async function writeResultAtomic(
  home: string,
  result: WorkspaceResultRecord,
): Promise<void> {
  await writeJsonAtomic(
    resultPath(home, result.session_id, result.seq),
    parseResultRecord(result),
  );
}

export type RuntimeRead =
  | { status: "absent" }
  | { status: "corrupt" }
  | { status: "present"; mirror: WorkspaceRuntimeMirror };

export async function readRuntimeMirror(home: string, sessionId: string): Promise<RuntimeRead> {
  const raw = await readJsonFile(runtimeMirrorPath(home, sessionId));
  if (raw === undefined) {
    return { status: "absent" };
  }
  if (raw === null) {
    return { status: "corrupt" };
  }
  try {
    return { status: "present", mirror: parseRuntimeMirror(raw) };
  } catch {
    return { status: "corrupt" };
  }
}

/**
 * Last-writer-wins mirror write. Custody records and runtime mirrors are
 * deliberately separate files so kernel mirror traffic never contends with
 * custody transitions; the mirror is the kernel's committed truth, and
 * recovery is the only writer allowed to rewrite a status it proves dead.
 */
export async function writeRuntimeMirrorAtomic(
  home: string,
  sessionId: string,
  record: SessionRecord,
  mirroredAt: string,
  rewrittenByRecovery: boolean,
): Promise<void> {
  const mirror: WorkspaceRuntimeMirror = {
    schema: WORKSPACE_RUNTIME_SCHEMA_VERSION,
    mirrored_at: mirroredAt,
    rewritten_by_recovery: rewrittenByRecovery,
    record,
  };
  await writeJsonAtomic(runtimeMirrorPath(home, sessionId), mirror);
}

// ---------------------------------------------------------------------------
// Loading with sidecar recovery + ref agreement
// ---------------------------------------------------------------------------

export type RecoveryOutcome =
  | { outcome: "none" }
  | { outcome: "landed"; detail: string }
  | { outcome: "abandoned"; detail: string }
  | { outcome: "inconsistent"; detail: string };

export type LoadedWorkspace =
  | { status: "absent" }
  | { status: "present"; record: WorkspaceRecord; recovery: RecoveryOutcome }
  | { status: "inconsistent"; detail: string };

async function readPending(home: string, sessionId: string): Promise<
  | { status: "absent" }
  | { status: "corrupt" }
  | { status: "present"; tx: WorkspaceTransaction }
> {
  const raw = await readJsonFile(workspacePendingPath(home, sessionId));
  if (raw === undefined) {
    return { status: "absent" };
  }
  if (raw === null) {
    return { status: "corrupt" };
  }
  try {
    return { status: "present", tx: parseWorkspaceTransaction(raw) };
  } catch {
    return { status: "corrupt" };
  }
}

/**
 * Replay a surviving sidecar. The ref proves either that the transition
 * landed (sidecar replayed onto the record, result file ensured) or that it
 * never landed (sidecar removed, record untouched). Anything else is an
 * explicit inconsistency left on disk for audit — never guessed away.
 * Must run under the workspace lock.
 */
export async function recoverPendingTransaction(
  ctx: StoreContext,
  sessionId: string,
): Promise<RecoveryOutcome> {
  const home = ctx.home;
  const pending = await readPending(home, sessionId);
  if (pending.status === "absent") {
    return { outcome: "none" };
  }
  if (pending.status === "corrupt") {
    return {
      outcome: "inconsistent",
      detail: "a transaction sidecar exists but is corrupt or invalid; the pending transition can be neither landed nor abandoned on this evidence",
    };
  }
  const tx = pending.tx;
  const ref = await probeRef(ctx.repositoryCwd, tx.ref);
  const record = await readRecordRaw(home, sessionId);

  // Landed if the ref proves the move OR the post-transition record is
  // already in place (transitions that never touch the ref are proven by the
  // record's revision alone — the sidecar is then a stale completion marker).
  const postInPlace =
    tx.capture_phase === "captured"
    && tx.next_record !== null
    && record.status === "present"
    && record.record.revision === tx.next_record.revision;
  const landed =
    tx.capture_phase === "captured" && tx.new_commit !== null && (ref === tx.new_commit || postInPlace);
  if (landed) {
    const next = tx.next_record!;
    const needsWrite = record.status !== "present" || record.record.revision !== next.revision;
    if (needsWrite) {
      await writeRecordAtomic(home, next);
    }
    if (tx.result !== null) {
      const existing = await readResult(home, sessionId, tx.result.seq);
      if (
        existing === "absent"
        || existing === "corrupt"
        || JSON.stringify(existing) !== JSON.stringify(tx.result)
      ) {
        await writeResultAtomic(home, tx.result);
      }
    }
    await removeFile(workspacePendingPath(home, sessionId));
    return {
      outcome: "landed",
      detail: needsWrite
        ? `sidecar for ${tx.kind} ${tx.seq ?? "(close)"} had landed; record and result replayed from it`
        : `sidecar for ${tx.kind} ${tx.seq ?? "(close)"} had landed; stale sidecar removed`,
    };
  }

  const refMatchesOld = ref === tx.expected_ref;
  if (refMatchesOld) {
    if (record.status === "corrupt") {
      return {
        outcome: "inconsistent",
        detail: "the transaction never touched the ref, but the custody record is corrupt; nothing may be assumed about the pre-transition state",
      };
    }
    if (record.status === "present" && record.record.revision !== tx.expected_revision) {
      return {
        outcome: "inconsistent",
        detail: `sidecar expected revision ${tx.expected_revision} but the record sits at ${record.record.revision}`,
      };
    }
    await removeFile(workspacePendingPath(home, sessionId));
    return {
      outcome: "abandoned",
      detail: `the ref proves ${tx.kind} ${tx.seq ?? "(close)"} never moved it; sidecar abandoned, any unreachable capture commit is reclaimed by git itself`,
    };
  }

  return {
    outcome: "inconsistent",
    detail: `sidecar expects ref ${tx.expected_ref ?? "(absent)"} -> ${tx.new_commit ?? "(uncaptured)"}, but the ref points at ${ref ?? "(absent)"}`,
  };
}

/**
 * Load the authoritative custody record: replay any pending transaction,
 * then prove the ref and the record agree. Ref-absent is consistent exactly
 * while the head is still the base (no capture has advanced the chain).
 */
export async function loadWorkspace(
  ctx: StoreContext,
  sessionId: string,
): Promise<LoadedWorkspace> {
  const recovery = await recoverPendingTransaction(ctx, sessionId);
  if (recovery.outcome === "inconsistent") {
    const record = await readRecordRaw(ctx.home, sessionId);
    return {
      status: "inconsistent",
      detail: `${recovery.detail}${record.status === "absent" ? " (and the custody record is absent)" : ""}`,
    };
  }
  const record = await readRecordRaw(ctx.home, sessionId);
  if (record.status === "corrupt") {
    return { status: "inconsistent", detail: "custody record exists but is corrupt" };
  }
  if (record.status === "absent") {
    return { status: "absent" };
  }
  const ref = await probeRef(ctx.repositoryCwd, record.record.ref);
  // Single invariant: the custody ref exists exactly when the head has
  // advanced past base, and then points at the head.
  const agrees =
    ref === null
      ? record.record.head_commit === record.record.base_commit
      : ref === record.record.head_commit;
  if (!agrees) {
    return {
      status: "inconsistent",
      detail: `custody ref points at ${ref ?? "(absent)"} but the record's head is ${record.record.head_commit}`,
    };
  }
  return {
    status: "present",
    record: record.record,
    recovery: recovery.outcome === "none" ? { outcome: "none" } : recovery,
  };
}

// ---------------------------------------------------------------------------
// The transactional transition (ref-visible kinds only)
// ---------------------------------------------------------------------------

/**
 * The git-visible transition, run under the workspace lock AFTER the caller
 * has loaded the current record. Order, fixed:
 *
 *   sidecar(intent) → capture → sidecar(captured) → ref CAS →
 *   result write → record write → sidecar removal
 *
 * Every crash point between these steps is resolvable by
 * `recoverPendingTransaction`. A rejected CAS removes only the sidecar this
 * pass wrote and reports `WORKSPACE_REF_DIVERGED`; the divergence itself
 * stays on disk for audit.
 */
export interface RefTransition {
  sessionId: string;
  tx: WorkspaceTransaction;
  observe?: PhaseObserver;
}

export async function writeIntentSidecar(ctx: StoreContext, tx: WorkspaceTransaction): Promise<void> {
  const existing = await readPending(ctx.home, tx.session_id);
  if (existing.status !== "absent") {
    throw workspaceError(
      "WORKSPACE_PENDING_TRANSACTION",
      `a pending transaction already exists for session "${tx.session_id}"`,
    );
  }
  await writeJsonAtomic(workspacePendingPath(ctx.home, tx.session_id), tx);
}

export async function applyRefTransitionCaptured(
  ctx: StoreContext,
  step: RefTransition,
): Promise<void> {
  const { tx, sessionId } = step;
  if (tx.capture_phase !== "captured" || tx.new_commit === null || tx.next_record === null) {
    throw workspaceError("WORKSPACE_TRANSITION_INVALID", "captured transition carries no commit or next record");
  }
  const record = await readRecordRaw(ctx.home, sessionId);
  if (record.status !== "present" || record.record.revision !== tx.expected_revision) {
    throw workspaceError(
      "WORKSPACE_TRANSITION_INVALID",
      `captured transition expected revision ${tx.expected_revision}, found ${record.status === "present" ? record.record.revision : record.status}`,
    );
  }
  await writeJsonAtomic(workspacePendingPath(ctx.home, sessionId), tx);
  await step.observe?.("captured-written", sessionId);

  // The ref moves only when the lineage genuinely advances: a capture whose
  // commit is still the base has nothing to pin (a ref at base would create
  // a state GC must later un-create), so base-pinned transitions are
  // record-and-result only.
  if (tx.new_commit !== tx.expected_ref && tx.new_commit !== tx.next_record.base_commit) {
    try {
      await casRef(ctx.repositoryCwd, tx.ref, tx.expected_ref, tx.new_commit);
    } catch {
      await removeFile(workspacePendingPath(ctx.home, sessionId));
      throw workspaceError(
        "WORKSPACE_REF_DIVERGED",
        `custody ref ${tx.ref} moved concurrently; refused to advance it and left the divergence on disk`,
      );
    }
    await step.observe?.("ref-updated", sessionId);
  }

  if (tx.result !== null) {
    await writeResultAtomic(ctx.home, tx.result);
  }
  await writeRecordAtomic(ctx.home, tx.next_record);
  await removeFile(workspacePendingPath(ctx.home, sessionId));
  await step.observe?.("record-committed", sessionId);
}

// ---------------------------------------------------------------------------
// Plain CAS (filesystem-only custody transitions: handoff, closures, marks)
// ---------------------------------------------------------------------------

/**
 * Read-modify-write the record under the lock with revision discipline:
 * the mutation only lands if the record still sits at `expectedRevision`.
 * Single-file atomic rename; no git side, so no sidecar is required.
 */
export async function updateRecordCas(
  ctx: StoreContext,
  sessionId: string,
  expectedRevision: number,
  mutate: (record: WorkspaceRecord) => WorkspaceRecord,
): Promise<WorkspaceRecord> {
  const record = await readRecordRaw(ctx.home, sessionId);
  if (record.status === "absent") {
    throw workspaceError("WORKSPACE_NOT_FOUND", `no custody record for session "${sessionId}"`);
  }
  if (record.status === "corrupt") {
    throw workspaceError("WORKSPACE_STATE_INCONSISTENT", `custody record for "${sessionId}" is corrupt`);
  }
  if (record.record.revision !== expectedRevision) {
    throw workspaceError(
      "WORKSPACE_CAS_CONFLICT",
      `custody record moved to revision ${record.record.revision}; this writer expected ${expectedRevision}`,
    );
  }
  const next = mutate(structuredClone(record.record));
  if (next.revision !== expectedRevision + 1) {
    throw workspaceError("WORKSPACE_TRANSITION_INVALID", "mutation must advance the revision by exactly one");
  }
  await writeRecordAtomic(ctx.home, next);
  return next;
}

// ---------------------------------------------------------------------------
// Home scanning (recovery and GC inventory)
// ---------------------------------------------------------------------------
export interface HubInventory {
  /** Directory names under workspaces/ (custody present or partial). */
  custodyIds: string[];
  /** Lease file session ids under processes/. */
  leaseIds: string[];
  /** Directory names under worktrees/. */
  worktreeIds: string[];
  /**
   * Full paths of namespace entries that name no hub session (junk dirs,
   * non-JSON lease files): retained and reported, never acted upon.
   */
  unknownSegments: string[];
}

async function listSegments(dir: string): Promise<string[]> {
  try {
    const entries = await readdir(dir, { withFileTypes: true });
    return entries.map((e) => e.name);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return [];
    }
    throw error;
  }
}

function idFromSegment(segment: string): string | null {
  const base = segment.replace(/\.json$/, "");
  return isWorkspaceSessionId(base) ? base : null;
}

export async function scanHubHome(home: string): Promise<HubInventory> {
  const [custody, leases, worktrees] = await Promise.all([
    listSegments(join(home, WORKSPACES_SUBDIR)),
    listSegments(join(home, PROCESSES_SUBDIR)),
    listSegments(join(home, WORKTREES_SUBDIR)),
  ]);
  const unknownSegments: string[] = [];
  const custodyIds: string[] = [];
  for (const name of custody) {
    const id = idFromSegment(name);
    if (id === null || name.includes(".")) {
      unknownSegments.push(join(home, WORKSPACES_SUBDIR, name));
    } else {
      custodyIds.push(id);
    }
  }
  const leaseIds: string[] = [];
  for (const name of leases) {
    const id = name.endsWith(".json") ? idFromSegment(name) : null;
    if (id === null) {
      unknownSegments.push(join(home, PROCESSES_SUBDIR, name));
    } else {
      leaseIds.push(id);
    }
  }
  const worktreeIds: string[] = [];
  for (const name of worktrees) {
    const id = idFromSegment(name);
    if (id === null) {
      unknownSegments.push(join(home, WORKTREES_SUBDIR, name));
    } else {
      worktreeIds.push(id);
    }
  }
  return { custodyIds, leaseIds, worktreeIds, unknownSegments };
}

/** Remove every custody artifact for a session (GC's final step; call only with proof). */
export async function removeCustodyTree(home: string, sessionId: string): Promise<void> {
  await rm(workspaceDir(home, sessionId), { recursive: true, force: true });
}

export async function ensureLayout(home: string): Promise<void> {
  await mkdir(join(home, WORKSPACES_SUBDIR), { recursive: true });
  await mkdir(join(home, WORKTREES_SUBDIR), { recursive: true });
  await mkdir(join(home, PROCESSES_SUBDIR), { recursive: true });
}
