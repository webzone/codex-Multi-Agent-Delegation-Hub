import { randomUUID } from "node:crypto";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { AgentHubError } from "../src/errors.js";
import { InteractionKernel } from "../src/kernel/index.js";
import type { ProcessFacts, TurnResult } from "../src/kernel/contracts.js";
import { FakeFactory } from "./kernel-fakes.js";
import type { FakeTransport } from "./kernel-fakes.js";
import { createGitRepository, removeDirectory } from "./helpers.js";
import { WorkspaceLifecycle } from "../src/workspace/index.js";
import type { LeaseProbes } from "../src/workspace/index.js";
import type { WorkspaceRecord } from "../src/workspace/records.js";
import type { WorkspacePhase } from "../src/workspace/store.js";

/**
 * Shared fixtures for P2 WorkspaceLifecycle tests. Every fixture owns its
 * own repository and hub-home temp directories; tests clean up explicitly.
 */

export function fixedClock(startISO = "2026-09-07T00:00:00.000Z") {
  let ms = Date.parse(startISO);
  return {
    now: (): Date => new Date(ms),
    advance: (deltaMs: number): void => {
      ms += deltaMs;
    },
    at: (iso: string): void => {
      ms = Date.parse(iso);
    },
    currentMs: (): number => ms,
  };
}

export type Clock = ReturnType<typeof fixedClock>;

/**
 * Lease probes with scripted fates. `hub` matters only when the lease's
 * hub_pid differs from this process (leases written by other processes);
 * leases written inside the test run always see `hub_pid === process.pid`.
 */
export function fakeProbes(fate: {
  pid: "live" | "dead";
  group: "alive" | "gone" | "uncertain";
  token?: string | null;
}): LeaseProbes {
  return {
    probePid: () => fate.pid,
    probeGroup: () => fate.group,
    async startToken() {
      return fate.token === undefined ? "fake-start-token" : fate.token;
    },
    killGroup: () => true,
  };
}

/** A total, delivered TurnResult; override what the test is about. */
export function fakeTurn(sessionId: string, overrides: Partial<TurnResult> = {}): TurnResult {
  return {
    session_id: sessionId,
    command_id: randomUUID(),
    kind: "prompt",
    outcome: "succeeded",
    final_text: { text: "turn finished", truncated: false },
    usage: null,
    started_at: "2026-09-07T00:00:00.000Z",
    finished_at: "2026-09-07T00:00:01.000Z",
    duration_ms: 1000,
    error: null,
    ...overrides,
  };
}

export interface FixtureOptions {
  retentionMs?: number;
  probes?: LeaseProbes;
  /** Throw once (simulated hub crash) when the given publish phase is reached. */
  failOncePhase?: WorkspacePhase;
  lockWaitMs?: number;
  spawnFacts?: ProcessFacts | null;
  clock?: Clock;
}

export interface OpenedSession {
  id: string;
  workspace: WorkspaceRecord;
  transport: FakeTransport;
}

export interface Fixture {
  repo: string;
  home: string;
  clock: Clock;
  lc: WorkspaceLifecycle;
  kernel: InteractionKernel;
  factory: FakeFactory;
  /** Provision + kernel.start a fresh agent session. */
  openSession(input?: { agent?: string; base?: string; session_id?: string }): Promise<OpenedSession>;
  /** A lifecycle bound to the same home/repo without a kernel. */
  bareLifecycle(overrides?: FixtureOptions): WorkspaceLifecycle;
  fileInWorktree(worktree: string, name: string, content: string): Promise<string>;
  cleanup(): Promise<void>;
}

const CRASH_CODE = "SIMULATED_CRASH";

export async function makeFixture(options: FixtureOptions = {}): Promise<Fixture> {
  const repo = await createGitRepository();
  const home = await mkdtemp(join(tmpdir(), "agent-hub-ws-test-home-"));
  const clock = options.clock ?? fixedClock();
  const probes = options.probes ?? fakeProbes({ pid: "dead", group: "gone" });

  let armedPhase = options.failOncePhase ?? null;
  const observePhase = (phase: WorkspacePhase): void => {
    if (armedPhase !== null && phase === armedPhase) {
      armedPhase = null;
      throw new AgentHubError(CRASH_CODE, `simulated hub crash at ${phase}`);
    }
  };

  const lc = new WorkspaceLifecycle({
    home,
    env: {},
    retentionMs: options.retentionMs,
    now: clock.now,
    probes,
    lockWaitMs: options.lockWaitMs ?? 0,
    observePhase,
  });

  const factory = new FakeFactory("fake-rpc", "fake");
  factory.configureTransport = (t) => {
    t.spawnFacts = options.spawnFacts === null ? null : (options.spawnFacts ?? { pid: 411111, pgid: 411111 });
  };
  const kernel = new InteractionKernel({
    transportFactories: [factory],
    durable: lc.mirror,
    onProviderSpawn: lc.onProviderSpawn,
    now: clock.now,
  });

  return {
    repo,
    home,
    clock,
    lc,
    kernel,
    factory,
    async openSession(input = {}) {
      const { workspace, start } = await lc.startWorkspace(kernel, {
        session_id: input.session_id ?? randomUUID(),
        repository_cwd: repo,
        agent: input.agent,
        base: input.base,
        provider: "fake",
      });
      const transport = factory.created[factory.created.length - 1]!;
      return { id: start.session_id, workspace, transport };
    },
    bareLifecycle(overrides = {}) {
      return new WorkspaceLifecycle({
        home,
        env: {},
        retentionMs: overrides.retentionMs ?? options.retentionMs,
        now: overrides.clock?.now ?? clock.now,
        probes: overrides.probes ?? options.probes ?? probes,
        lockWaitMs: overrides.lockWaitMs ?? options.lockWaitMs ?? 0,
      });
    },
    async fileInWorktree(worktree, name, content) {
      const path = join(worktree, name);
      await writeFile(path, content);
      return path;
    },
    async cleanup() {
      await kernel.closeAll().catch(() => undefined);
      await removeDirectory(repo);
      await removeDirectory(home);
    },
  };
}

export const CRASH = CRASH_CODE;

export async function expectCode(
  run: Promise<unknown> | (() => Promise<unknown>),
  code: string,
): Promise<AgentHubError> {
  try {
    await (typeof run === "function" ? run() : run);
  } catch (error) {
    if (error instanceof AgentHubError) {
      if (error.code !== code) {
        throw new Error(`expected ${code}, got ${error.code}: ${error.message}`);
      }
      return error;
    }
    throw error;
  }
  throw new Error(`expected ${code} to be thrown, nothing was`);
}
