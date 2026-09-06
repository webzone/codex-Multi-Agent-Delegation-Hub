import { Buffer } from "node:buffer";
import { access, chmod, mkdtemp, readdir, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { AgentHubError } from "../src/errors.js";
import { resolveRepositoryIdentity } from "../src/git.js";
import { encodeJsonlFrame, LiveJsonlFramer } from "../src/live/jsonl.js";
import { launchLiveChild, SUPPORTS_GROUP_SIGNALS } from "../src/live/child-process.js";
import { AgyStreamJsonTransport } from "../src/live/transports/agy-stream-json.js";
import { liveStatePath } from "../src/live/state.js";
import {
  LiveSessionManager,
  LIVE_COMMON_DIR_SESSION_QUOTA,
  LIVE_DEFAULT_MAX_TEXT_BYTES,
  LIVE_FOLLOW_UP_MAX_MESSAGE_BYTES,
  LIVE_FOLLOW_UP_QUEUE_MAX_BYTES,
  LIVE_FOLLOW_UP_QUEUE_MAX_MESSAGES,
  LIVE_PROCESS_SESSION_QUOTA,
  type LiveManagerOptions,
  type LiveManagerPhase,
} from "../src/live/manager.js";
import {
  liveLeasePath,
  readLiveLease,
  type LiveLeaseProbes,
} from "../src/live/lease.js";
import { acquireRepositoryLock } from "../src/locks.js";
import { LIVE_ADMIN_LOCK_NAME } from "../src/live/state.js";
import type {
  LiveCapabilities,
  LiveCommand,
  LiveEvent,
  LiveEventBody,
  LiveLaunchRequest,
  LiveLaunchReport,
  LivePermissionDecision,
  LiveProbeResult,
  LiveProviderFactory,
  LiveStopMode,
  LiveStopReport,
  LiveTransport,
  LiveTransportFactory,
  LiveTurnResult,
} from "../src/live/types.js";
import {
  BLOCKED_WRITE_IS_MEANINGFUL,
  createGitRepository,
  removeDirectory,
  resolveRef,
  runGit,
} from "./helpers.js";
import {
  realProcessAlive,
  runLeaderFirstSurvivorScenario,
} from "./live-stop-authority.js";

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

function fullCapabilities(overrides: Partial<LiveCapabilities> = {}): LiveCapabilities {
  return {
    prompt: { support: "native", evidence: "fake ready handshake" },
    follow_up: { support: "native", evidence: "fake ready handshake" },
    steer: { support: "native", evidence: "fake mid-turn channel" },
    cancel: { support: "native", evidence: "fake cancel message" },
    status: { support: "derived", evidence: "hub stream evidence" },
    permission_response: { support: "native", evidence: "fake rpc accepted" },
    resume: { support: "native", evidence: "fake round trip" },
    checkpoint: { support: "derived", evidence: "hub worktree capture" },
    usage_reporting: { support: "unsupported", evidence: null },
    ...overrides,
  };
}

interface KillRecord {
  target: number;
  signal: string;
}

class FakeTransport implements LiveTransport {
  readonly id = "omp-rpc" as const;
  readonly provider = "omp" as const;
  readonly commands: LiveCommand[] = [];
  launch: LiveLaunchRequest | null = null;
  readonly stopCalls: LiveStopMode[] = [];
  stopResults: LiveStopReport[] = [];
  /** Overlap detector: stop() calls must never nest under the manager. */
  activeStops = 0;
  maxActiveStops = 0;
  private queue: LiveEventBody[] = [];
  private wake: (() => void) | null = null;
  private ended = false;

  constructor(
    private readonly caps: LiveCapabilities = fullCapabilities(),
    private readonly opts: {
      pid?: number | null;
      pgid?: number;
      providerSessionId?: string | null;
      /** Echo the launch resume hint back as a transport-verified resume state. */
      resumeState?: "echo";
      onSend?: (command: LiveCommand, transport: FakeTransport) => void;
      /** Gates every stop() until resolved; proves pipelines never overlap. */
      stopHold?: Promise<void>;
      /**
       * Mirror hermes-acp: `describe()` reports the permission_response
       * claim honest to the CURRENT launch's policy — deny auto-denies and
       * advertises no usable answer path, interactive answers natively.
       */
      permissionResponseFollowsPolicy?: boolean;
    } = {},
  ) {}

  async describe(): Promise<{ transport: "omp-rpc"; provider: "omp"; capabilities: LiveCapabilities }> {
    const capabilities = this.opts.permissionResponseFollowsPolicy
      ? {
          ...this.caps,
          permission_response:
            this.launch?.permission_policy === "interactive"
              ? { support: "native" as const, evidence: "fake policy-scoped answer path" }
              : { support: "unsupported" as const, evidence: null },
        }
      : this.caps;
    return { transport: this.id, provider: this.provider, capabilities };
  }

  async open(request: LiveLaunchRequest): Promise<LiveLaunchReport> {
    this.launch = request;
    const pid = this.opts.pid === undefined ? 424_242 : this.opts.pid;
    if (pid !== null) {
      // Real transports hand the spawn facts to the hub the moment the
      // process exists; the lease's provider evidence — and every recovery
      // verdict built on it — depends on this call landing.
      await request.report_process?.({ pid, pgid: this.opts.pgid ?? pid });
    }
    const resumed =
      this.opts.resumeState === "echo" && request.resume
        ? { ...request.resume, verified: true as const, verified_via: "transport-verified:open-echo" }
        : null;
    return {
      pid,
      provider_session_id: this.opts.providerSessionId ?? "prov-1",
      launched_at: "2026-09-05T00:00:00.000Z",
      ...(resumed !== null ? { resume_state: resumed } : {}),
    };
  }

  async send(command: LiveCommand): Promise<void> {
    this.commands.push(command);
    this.opts.onSend?.(command, this);
  }

  push(body: LiveEventBody): void {
    this.queue.push(body);
    this.wake?.();
  }

  endStream(): void {
    this.ended = true;
    this.wake?.();
  }

  async *events(): AsyncGenerator<LiveEvent> {
    for (;;) {
      while (this.queue.length > 0) {
        const body = this.queue.shift() as LiveEventBody;
        // The hub re-stamps every envelope; these placeholders are lies on
        // purpose so tests can prove which side owns seq/occurred_at.
        yield {
          live_session_id: "forged",
          seq: 999,
          transport: "hermes-acp",
          occurred_at: "forged",
          body,
        };
      }
      if (this.ended) {
        return;
      }
      await new Promise<void>((resolve) => {
        this.wake = resolve;
        if (this.queue.length > 0 || this.ended) {
          resolve();
        }
      });
    }
  }

  async stop(mode: LiveStopMode): Promise<LiveStopReport> {
    this.stopCalls.push(mode);
    this.activeStops += 1;
    this.maxActiveStops = Math.max(this.maxActiveStops, this.activeStops);
    try {
      if (this.opts.stopHold) {
        await this.opts.stopHold;
      }
      const report = this.stopResults.shift() ?? {
        status: "closed" as const,
        exit_code: 0,
        exit_signal: null,
        waited_ms: 1,
      };
      this.endStream();
      return report;
    } finally {
      this.activeStops -= 1;
    }
  }
}

class FakeTransportFactory implements LiveTransportFactory {
  readonly transport = "omp-rpc" as const;
  readonly provider = "omp" as const;
  readonly created: FakeTransport[] = [];
  probeResult: LiveProbeResult = { found: true, version: "fake-1", detail: null };

  constructor(private readonly make: () => FakeTransport) {}

  async probe(): Promise<LiveProbeResult> {
    return this.probeResult;
  }

  create(): LiveTransport {
    const transport = this.make();
    this.created.push(transport);
    return transport;
  }
}

const ompProviderFactory: LiveProviderFactory = {
  provider: "omp",
  transports: ["omp-rpc"],
  selectTransport: (factories) => factories[0] ?? null,
};

function fakeProbes(
  config: {
    alive?: (pid: number) => boolean;
    startToken?: (pid: number) => string;
    kills?: KillRecord[];
    /** Override the group probe; default derives from the pid liveness model. */
    groupState?: (pgid: number) => "alive" | "gone" | "uncertain";
  } = {},
): LiveLeaseProbes {
  const kills = config.kills ?? [];
  const alive = config.alive ?? ((p: number) => p === process.pid);
  return {
    probePid: (pid) => (alive(pid) ? "live" : "dead"),
    startToken: async (pid) =>
      (config.startToken ?? ((p) => (p === process.pid ? "hub-tok" : "prov-tok")))(pid),
    killGroup: (pgid, signal) => {
      kills.push({ target: pgid, signal });
      return true;
    },
    probeGroup: config.groupState ?? ((pgid) => (alive(pgid) ? "alive" : "gone")),
    now: () => new Date(),
  };
}

interface HarnessConfig {
  caps?: LiveCapabilities;
  transportOptions?: ConstructorParameters<typeof FakeTransport>[1];
  processQuota?: number;
  commonDirQuota?: number;
  leaseProbes?: LiveLeaseProbes;
  acquireLock?: LiveManagerOptions["acquireLock"];
  phases?: LiveManagerPhase[];
  /** When set, the durable ordering seam throws (once) at this phase. */
  failPhase?: LiveManagerPhase;
}

interface Harness {
  repository: string;
  commonDir: string;
  tmpRoot: string;
  manager: LiveSessionManager;
  factory: FakeTransportFactory;
}

async function harness(config: HarnessConfig = {}): Promise<Harness> {
  const repository = await createGitRepository();
  const identity = await resolveRepositoryIdentity(repository);
  const tmpRoot = await mkdtemp(join(tmpdir(), "agent-hub-live-core-"));
  const factory = new FakeTransportFactory(
    () => new FakeTransport(config.caps ?? fullCapabilities(), config.transportOptions),
  );
  const phases = config.phases;
  const seam = (): ((phase: LiveManagerPhase) => Promise<void>) => {
    let failed = false;
    return async (phase) => {
      if (!failed && phase === config.failPhase) {
        failed = true;
        throw new AgentHubError("INJECTED_DURABLE_FAILURE", `injected failure at ${phase}`);
      }
      phases?.push(phase);
    };
  };
  const manager = new LiveSessionManager({
    commonDir: identity.common_dir,
    repositoryCwd: repository,
    transportFactories: [factory],
    providerFactories: [ompProviderFactory],
    processQuota: config.processQuota,
    commonDirQuota: config.commonDirQuota,
    tmpRoot,
    leaseProbes: config.leaseProbes ?? fakeProbes(),
    acquireLock: config.acquireLock,
    observePhase: phases || config.failPhase ? seam() : undefined,
  });
  return { repository, commonDir: identity.common_dir, tmpRoot, manager, factory };
}

function secondHub(
  source: Harness,
  probes: LiveLeaseProbes,
  phases?: LiveManagerPhase[],
  acquireLock?: LiveManagerOptions["acquireLock"],
): LiveSessionManager {
  return new LiveSessionManager({
    commonDir: source.commonDir,
    repositoryCwd: source.repository,
    transportFactories: [new FakeTransportFactory(() => new FakeTransport())],
    providerFactories: [ompProviderFactory],
    leaseProbes: probes,
    acquireLock,
    observePhase: phases
      ? async (phase) => {
          phases.push(phase);
        }
      : undefined,
  });
}

const tick = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));

const captureRejection = (promise: Promise<unknown>): Promise<AgentHubError | null> =>
  promise.then(
    () => null,
    (error) => error as AgentHubError,
  );

async function expectCode(promise: Promise<unknown>, code: string): Promise<void> {
  await expect(promise).rejects.toMatchObject({ code });
}

async function leasesOf(commonDir: string): Promise<string[]> {
  try {
    return await readdir(join(commonDir, "agent-hub", "live", "leases"));
  } catch {
    return [];
  }
}

async function durableState(commonDir: string, liveSessionId: string): Promise<Record<string, never>> {
  return JSON.parse(await readFile(liveStatePath(commonDir, liveSessionId), "utf8")) as Record<string, never>;
}

async function startSession(manager: LiveSessionManager): Promise<{ live_session_id: string; workspace: string }> {
  const started = await manager.start({ provider: "omp" });
  return { live_session_id: started.live_session_id, workspace: started.workspace };
}

const writingTransportOptions = {
  onSend: (command: LiveCommand, transport: FakeTransport): void => {
    if (command.kind === "prompt" || command.kind === "follow_up") {
      void writeFile(join(transport.launch?.workspace ?? ".", `${command.command_id}.txt`), "work\n");
    }
  },
};

async function settle(harnessRef: Harness): Promise<void> {
  await harnessRef.manager.closeAll();
  await removeDirectory(harnessRef.repository);
  await removeDirectory(harnessRef.tmpRoot);
}

// ---------------------------------------------------------------------------
// Manager lifecycle
// ---------------------------------------------------------------------------

describe("live manager lifecycle", () => {
  it("starts on lease + worktree + durable state, and closes in strict order", async () => {
    const phases: LiveManagerPhase[] = [];
    const scope = await harness({ phases });
    const { live_session_id, workspace } = await startSession(scope.manager);

    expect(scope.manager.activeCount).toBe(1);
    const state = await scope.manager.view(live_session_id);
    expect(await resolveRef(scope.repository, `refs/agent-hub/live/${live_session_id}`)).toBe(state.base_commit);
    expect(await leasesOf(scope.commonDir)).toHaveLength(1);
    expect(scope.factory.created[0].launch?.max_text_bytes).toBe(LIVE_DEFAULT_MAX_TEXT_BYTES);
    expect(scope.factory.created[0].launch?.workspace).toBe(workspace);

    const closed = await scope.manager.close(live_session_id);
    expect(closed.state.status).toBe("closed");
    expect(closed.checkpoint_taken).toBe(false); // nothing changed to pin
    expect(scope.manager.activeCount).toBe(0);
    expect(await leasesOf(scope.commonDir)).toHaveLength(0);
    const durable = await durableState(scope.commonDir, live_session_id);
    expect(durable.status).toBe("closed");
    // No checkpoint was ever taken: the reason stays null, and the record
    // names the hub-owned worktree it owns.
    expect(durable.last_checkpoint_reason).toBeNull();
    expect(String(durable.worktree_path).startsWith(`${String(durable.worktree_parent)}/`)).toBe(true);

    // reap → checkpoint → state ordering, and no sidecars left behind.
    expect(phases).toEqual(["transport-stopped", "checkpoint-captured", "state-advanced"]);
    const sessionsDir = await readdir(join(scope.commonDir, "agent-hub", "live", "sessions"));
    expect(sessionsDir.filter((name) => name.endsWith(".pending.json"))).toEqual([]);

    await settle(scope);
  });

  it("runs a prompt turn: work pinned, final text assembled, events re-stamped by the hub", async () => {
    const scope = await harness({ transportOptions: writingTransportOptions });
    const { live_session_id } = await startSession(scope.manager);
    const t = scope.factory.created[0];

    const turn = scope.manager.prompt(live_session_id, "build the thing");
    t.push({ kind: "status", status: "running", note: null });
    t.push({ kind: "text", role: "assistant", stream_id: "s1", text: { text: "working", truncated: false }, final: false });
    t.push({ kind: "text", role: "assistant", stream_id: "s1", text: { text: "… done", truncated: false }, final: true });
    t.push({ kind: "usage", usage: { input_tokens: 10, output_tokens: 2, cached_tokens: null, cost_usd: null } });
    t.push({ kind: "status", status: "idle", note: null });

    const result = await turn;
    expect(result.outcome).toBe("succeeded");
    expect(result.final_text?.text).toBe("working… done");
    expect(result.usage?.cached_tokens).toBeNull();
    expect(result.checkpoint).not.toBeNull();
    expect(result.checkpoint?.reason).toBe("turn_end");

    const state = await scope.manager.view(live_session_id);
    expect(state.status).toBe("idle");
    expect(state.checkpoint_seq).toBe(1);
    expect(state.current_commit).not.toBe(state.base_commit);
    expect(await resolveRef(scope.repository, `refs/agent-hub/live/${live_session_id}`)).toBe(state.current_commit);

    // Hub re-stamped every envelope despite the forged transport metadata.
    const replay = scope.manager.eventsAfter(live_session_id, 0);
    expect(replay.events.map((event) => event.seq)).toEqual([1, 2, 3, 4, 5]);
    expect(replay.events.every((event) => event.live_session_id === live_session_id)).toBe(true);
    expect(replay.events.every((event) => event.transport === "omp-rpc")).toBe(true);
    expect(replay.events.every((event) => event.occurred_at !== "forged")).toBe(true);

    // The omp resume cursor rode the durable write.
    expect(state.resume?.provider === "omp" && state.resume.last_event_seq).toBe(5);
    // The task text reached the worktree and the wire — never durable state.
    // The new durable fields carry only hub-owned paths and enum reasons.
    const durable = await durableState(scope.commonDir, live_session_id);
    expect(JSON.stringify(durable)).not.toContain("build the thing");
    expect(durable.last_checkpoint_reason).toBe("turn_end");

    await settle(scope);
  });

  it("accepts the prompt exactly once and refuses work on dead sessions", async () => {
    const scope = await harness();
    const { live_session_id } = await startSession(scope.manager);
    const t = scope.factory.created[0];

    const turn = scope.manager.prompt(live_session_id, "first");
    t.push({ kind: "status", status: "idle", note: null });
    await turn;

    await expectCode(scope.manager.prompt(live_session_id, "second"), "LIVE_PROMPT_ALREADY_ACCEPTED");
    await scope.manager.close(live_session_id);
    await expectCode(scope.manager.prompt(live_session_id, "late"), "LIVE_SESSION_NOT_FOUND");

    await settle(scope);
  });

  it("marks the session error and reaps before pinning when the provider dies mid-turn", async () => {
    const phases: LiveManagerPhase[] = [];
    const scope = await harness({ phases });
    const { live_session_id } = await startSession(scope.manager);
    const t = scope.factory.created[0];

    const turn = scope.manager.prompt(live_session_id, "long job");
    t.push({ kind: "status", status: "running", note: null });
    t.push({ kind: "exit", intentional: false, exit_code: 137, exit_signal: null });

    const result = await turn;
    expect(result.outcome).toBe("failed");
    expect(result.exit_code).toBe(137);

    const durable = await durableState(scope.commonDir, live_session_id);
    expect(durable.status).toBe("error");
    expect(t.stopCalls).toEqual(["terminate"]);
    expect(phases).toEqual(["transport-stopped", "checkpoint-captured", "state-advanced"]);

    await settle(scope);
  });

  it("crash whose stop cannot prove death: failed turn, no checkpoint, resources retained", async () => {
    const phases: LiveManagerPhase[] = [];
    const scope = await harness({ phases, transportOptions: writingTransportOptions });
    const { live_session_id } = await startSession(scope.manager);
    const t = scope.factory.created[0];
    // The provider crashed AND the terminate stop cannot prove it gone.
    t.stopResults = [{ status: "orphaned", exit_code: null, exit_signal: null, waited_ms: 7 }];

    const turn = scope.manager.prompt(live_session_id, "hot work");
    t.push({ kind: "status", status: "running", note: null });
    t.push({ kind: "exit", intentional: false, exit_code: 137, exit_signal: null });

    const result = await turn;
    expect(result.outcome).toBe("failed");
    // The honest settlement: failed with NO checkpoint — the tree may still
    // be mutating, so nothing may be pinned on an unproven stop.
    expect(result.checkpoint).toBeNull();
    expect(result.error?.code).toBe("LIVE_PROVIDER_EXITED");

    // The turn resolves before the (git-backed) orphan rewrite lands; pump
    // the event loop until the durable path signals its final phase — the
    // awaited condition, not a guessed delay.
    for (let spin = 0; !phases.includes("state-advanced"); spin += 1) {
      if (spin > 50_000) {
        throw new Error("the crash-orphan rewrite never reached the durable record");
      }
      await tick();
    }
    const state = await scope.manager.view(live_session_id);
    expect(state.status).toBe("orphaned");
    expect(state.last_error?.code).toBe("LIVE_STOP_UNPROVEN");
    // Work exists in the worktree, yet the chain stayed untouched.
    expect(state.current_commit).toBe(state.base_commit);
    expect(state.checkpoint_seq).toBe(0);
    await access(state.worktree_path);
    expect(await leasesOf(scope.commonDir)).toHaveLength(1);
    // No `checkpoint-captured` phase may ever fire on this path.
    expect(phases).toEqual(["transport-stopped", "state-advanced"]);

    await settle(scope);
  });

  it("graceful close reports orphaned without silent escalation; terminate close then completes", async () => {
    const scope = await harness();
    const { live_session_id } = await startSession(scope.manager);
    const t = scope.factory.created[0];
    t.stopResults = [{ status: "orphaned", exit_code: null, exit_signal: null, waited_ms: 5 }];

    const orphaned = await scope.manager.close(live_session_id);
    expect(orphaned.state.status).toBe("orphaned");
    expect(orphaned.state.last_error?.code).toBe("LIVE_STOP_UNPROVEN");
    expect(orphaned.checkpoint_taken).toBe(false);
    // graceful never quietly escalates: exactly one stop, no terminate call.
    expect(t.stopCalls).toEqual(["graceful"]);
    // Lease and worktree stay for recovery; the durable record says orphaned.
    expect(await leasesOf(scope.commonDir)).toHaveLength(1);

    // An authorized terminate re-attempts shutdown with fresh authority —
    // not the old graceful report — and completes checkpoint + teardown.
    const closed = await scope.manager.close(live_session_id, "terminate");
    expect(closed.state.status).toBe("closed");
    expect(closed.stop?.status).toBe("closed");
    expect(t.stopCalls).toEqual(["graceful", "terminate"]);
    const durable = await durableState(scope.commonDir, live_session_id);
    expect(durable.status).toBe("closed");
    expect(await leasesOf(scope.commonDir)).toHaveLength(0);

    await settle(scope);
  });
});

// ---------------------------------------------------------------------------
// Close single-flight (manager terminal pipeline)
// ---------------------------------------------------------------------------

describe("close single-flight", () => {
  it("concurrent same-mode closes share one pipeline: one stop, one result, one teardown", async () => {
    const phases: LiveManagerPhase[] = [];
    let release!: () => void;
    const hold = new Promise<void>((resolve) => {
      release = resolve;
    });
    const scope = await harness({ phases, transportOptions: { stopHold: hold } });
    const { live_session_id } = await startSession(scope.manager);
    const t = scope.factory.created[0];

    const first = scope.manager.close(live_session_id);
    const second = scope.manager.close(live_session_id);
    release();
    const [a, b] = await Promise.all([first, second]);

    expect(a).toBe(b); // the same shared result, not a replayed pipeline
    expect(a.state.status).toBe("closed");
    expect(t.stopCalls).toEqual(["graceful"]);
    expect(t.maxActiveStops).toBe(1);
    expect(await leasesOf(scope.commonDir)).toHaveLength(0);
    // Exactly one shutdown, one durable write, one teardown.
    expect(phases).toEqual(["transport-stopped", "checkpoint-captured", "state-advanced"]);
    await settle(scope);
  });

  it("simultaneous graceful and terminate never overlap: terminate upgrades the settled orphan", async () => {
    const phases: LiveManagerPhase[] = [];
    let release!: () => void;
    const hold = new Promise<void>((resolve) => {
      release = resolve;
    });
    const scope = await harness({ phases, transportOptions: { stopHold: hold } });
    const { live_session_id } = await startSession(scope.manager);
    const t = scope.factory.created[0];
    t.stopResults = [
      { status: "orphaned", exit_code: 0, exit_signal: null, waited_ms: 3 },
      { status: "closed", exit_code: 0, exit_signal: "SIGKILL", waited_ms: 4 },
    ];

    const graceful = scope.manager.close(live_session_id);
    const upgrade = scope.manager.close(live_session_id, "terminate");
    // A third attempt queued behind both: it must observe the final record.
    const late = scope.manager.close(live_session_id);
    release();
    const [orphaned, closed, lateResult] = await Promise.all([graceful, upgrade, late]);

    expect(orphaned.state.status).toBe("orphaned");
    expect(orphaned.state.last_error?.code).toBe("LIVE_STOP_UNPROVEN");
    expect(orphaned.checkpoint_taken).toBe(false);
    expect(orphaned.stop).toMatchObject({ status: "orphaned" });

    expect(closed.state.status).toBe("closed");
    expect(closed.stop).toMatchObject({ status: "closed", exit_signal: "SIGKILL" });

    expect(lateResult.stop).toBeNull(); // terminal record, never a fourth pipeline

    // Serialized: exactly one stop per pipeline, and never two at once.
    expect(t.stopCalls).toEqual(["graceful", "terminate"]);
    expect(t.maxActiveStops).toBe(1);

    const durable = await durableState(scope.commonDir, live_session_id);
    expect(durable.status).toBe("closed");
    expect(await leasesOf(scope.commonDir)).toHaveLength(0);
    // One orphaned CAS, then one close chain: no duplicated checkpoint or CAS.
    expect(phases).toEqual([
      "transport-stopped",
      "state-advanced",
      "transport-stopped",
      "checkpoint-captured",
      "state-advanced",
    ]);
    await settle(scope);
  });

  it("a second graceful after a settled orphan gets the terminal record, not a second stop", async () => {
    const scope = await harness();
    const { live_session_id } = await startSession(scope.manager);
    const t = scope.factory.created[0];
    t.stopResults = [{ status: "orphaned", exit_code: 0, exit_signal: null, waited_ms: 2 }];

    const first = await scope.manager.close(live_session_id);
    expect(first.state.status).toBe("orphaned");
    expect(first.stop).toMatchObject({ status: "orphaned" });

    const second = await scope.manager.close(live_session_id);
    expect(second.stop).toBeNull();
    expect(second.state.status).toBe("orphaned");
    expect(t.stopCalls).toEqual(["graceful"]);
    expect(scope.manager.activeCount).toBe(1);
    expect(await leasesOf(scope.commonDir)).toHaveLength(1);
    await settle(scope);
  });
});

// ---------------------------------------------------------------------------
// Commands: capability gate, queue, permissions
// ---------------------------------------------------------------------------

describe("capability gate and command surface", () => {
  it("refuses undeliverable commands pre-dispatch", async () => {
    const scope = await harness({
      caps: fullCapabilities({
        steer: { support: "unsupported", evidence: null },
        follow_up: { support: "unsupported", evidence: null },
        status: { support: "unsupported", evidence: null },
      }),
    });
    const { live_session_id } = await startSession(scope.manager);

    const steer = await scope.manager.steer(live_session_id, "left");
    expect(steer.outcome).toBe("unsupported");
    expect(steer.error?.stage).toBe("capability");

    expect((await scope.manager.followUp(live_session_id, "more")).outcome).toBe("unsupported");
    expect((await scope.manager.requestStatus(live_session_id)).outcome).toBe("unsupported");

    expect(scope.factory.created[0].commands).toEqual([]);
    await settle(scope);
  });

  it("answers derived status locally but forwards native status", async () => {
    const scope = await harness();
    const { live_session_id } = await startSession(scope.manager);

    const answer = await scope.manager.requestStatus(live_session_id);
    expect(answer.outcome).toBe("succeeded");
    expect(JSON.parse(answer.final_text?.text ?? "{}")).toMatchObject({ status: "idle", turn_in_flight: false });
    expect(scope.factory.created[0].commands).toEqual([]);

    await settle(scope);
  });

  it("delivers permission responses only for open observed requests", async () => {
    const scope = await harness();
    const { live_session_id } = await startSession(scope.manager);
    const t = scope.factory.created[0];

    await expectCode(
      scope.manager.respondPermission(live_session_id, "req-x", "deny", null),
      "LIVE_PERMISSION_REQUEST_UNKNOWN",
    );

    t.push({
      kind: "permission_request",
      request_id: "req-1",
      tool: "write",
      summary: { text: "overwrite README", truncated: false },
    });
    await tick();
    // The v3 vocabulary is exactly {allow_once, deny}: anything else is a
    // caller error, and refusing it must not consume the open request.
    await expectCode(
      scope.manager.respondPermission(
        live_session_id,
        "req-1",
        "allow_session" as unknown as LivePermissionDecision,
        null,
      ),
      "LIVE_COMMAND_INVALID",
    );

    const answered = await scope.manager.respondPermission(live_session_id, "req-1", "allow_once", "ok");
    expect(answered.outcome).toBe("succeeded");
    expect(t.commands.some((c) => c.kind === "permission_response" && c.request_id === "req-1")).toBe(true);
    await expectCode(
      scope.manager.respondPermission(live_session_id, "req-1", "deny", null),
      "LIVE_PERMISSION_REQUEST_UNKNOWN",
    );

    await settle(scope);
  });

  it("cancels a running turn and pins the partial work as a cancel checkpoint", async () => {
    const scope = await harness({
      transportOptions: {
        onSend: (command, transport) => {
          if (command.kind === "prompt") {
            void writeFile(join(transport.launch?.workspace ?? ".", "partial.txt"), "partial\n");
          }
        },
      },
    });
    const { live_session_id } = await startSession(scope.manager);
    const t = scope.factory.created[0];

    const turn = scope.manager.prompt(live_session_id, "big job");
    t.push({ kind: "status", status: "running", note: null });
    const ack = await scope.manager.cancel(live_session_id, "user aborted");
    expect(ack.outcome).toBe("succeeded");
    expect(t.commands.some((c) => c.kind === "cancel")).toBe(true);

    t.push({ kind: "status", status: "idle", note: null });
    const result = await turn;
    expect(result.outcome).toBe("cancelled");
    expect(result.checkpoint?.reason).toBe("cancel");
    const subject = (await runGit(scope.repository, ["log", "-1", "--format=%s", result.checkpoint?.commit ?? ""])).trim();
    expect(subject).toContain("(cancel)");

    await settle(scope);
  });
});

// Queue bounds live on the hub queue. A `native` follow_up claim must go
// straight to the provider mid-run, so the bound tests pin the claim to
// `hub-queued` explicitly instead of inheriting the fake's native default.
const hubQueuedFollowUps = fullCapabilities({
  follow_up: { support: "hub-queued", evidence: "hub queues next-turn input" },
});

describe("follow-up queue", () => {
  it("hub-queues while running, drains in order, and enforces every bound", async () => {
    const scope = await harness({ caps: hubQueuedFollowUps, transportOptions: writingTransportOptions });
    const { live_session_id } = await startSession(scope.manager);
    const t = scope.factory.created[0];

    const first = scope.manager.prompt(live_session_id, "one");
    t.push({ kind: "status", status: "running", note: null });

    const second = scope.manager.followUp(live_session_id, "two");
    const third = scope.manager.followUp(live_session_id, "three");

    const oversized = await captureRejection(
      scope.manager.followUp(live_session_id, "x".repeat(LIVE_FOLLOW_UP_MAX_MESSAGE_BYTES + 1)),
    );
    expect(oversized?.code).toBe("LIVE_QUEUE_FULL");

    const fillers: Promise<LiveTurnResult>[] = [];
    for (let index = 0; index < LIVE_FOLLOW_UP_QUEUE_MAX_MESSAGES - 2; index += 1) {
      fillers.push(scope.manager.followUp(live_session_id, `filler-${index}`));
    }
    const countFull = await captureRejection(scope.manager.followUp(live_session_id, "33rd"));
    expect(countFull?.code).toBe("LIVE_QUEUE_FULL");

    // End turn 1 ⇒ the queue drains toward "two" immediately.
    t.push({ kind: "status", status: "idle", note: null });
    await first;
    expect(t.commands.filter((c) => c.kind === "follow_up")).toHaveLength(1);

    // End turn 2 ⇒ "three" goes out.
    t.push({ kind: "status", status: "idle", note: null });
    expect((await second).kind).toBe("follow_up");
    expect(t.commands.filter((c) => c.kind === "follow_up")).toHaveLength(2);

    // End turn 3 ⇒ the first filler goes out; then close fails the rest.
    t.push({ kind: "status", status: "idle", note: null });
    expect((await third).outcome).toBe("succeeded");
    const closed = await scope.manager.close(live_session_id);
    expect(closed.state.status).toBe("closed");

    const resolved = await Promise.all(fillers);
    // The queued remainder is failed by the close; the follow-up whose turn
    // was already in flight is cancelled by it. Both are honest ends.
    expect(
      resolved.every(
        (r) =>
          r.outcome === "cancelled" ||
          (r.outcome === "failed" && r.error?.code === "LIVE_SESSION_CLOSING"),
      ),
    ).toBe(true);
    expect(resolved.filter((r) => r.error?.code === "LIVE_SESSION_CLOSING")).toHaveLength(fillers.length - 1);

    await settle(scope);
  });

  it("delivers native follow-ups immediately and settles them at the next terminal boundary", async () => {
    const scope = await harness();
    const { live_session_id } = await startSession(scope.manager);
    const t = scope.factory.created[0];

    const first = scope.manager.prompt(live_session_id, "one");
    t.push({ kind: "status", status: "running", note: null });

    const second = scope.manager.followUp(live_session_id, "two");
    await tick();
    // A `native` claim promises provider-side queueing: delivery is now.
    expect(t.commands.filter((c) => c.kind === "follow_up")).toHaveLength(1);

    const third = scope.manager.followUp(live_session_id, "three");
    await tick();
    expect(t.commands.filter((c) => c.kind === "follow_up")).toHaveLength(2);

    // The per-message bound guards the native path too.
    const oversized = await captureRejection(
      scope.manager.followUp(live_session_id, "x".repeat(LIVE_FOLLOW_UP_MAX_MESSAGE_BYTES + 1)),
    );
    expect(oversized?.code).toBe("LIVE_QUEUE_FULL");

    // The terminal boundary adopts the already-delivered follow-up as the
    // tracked next turn — capability honesty forbids any re-send.
    t.push({ kind: "status", status: "idle", note: null });
    expect((await first).outcome).toBe("succeeded");
    expect(t.commands.filter((c) => c.kind === "follow_up")).toHaveLength(2);

    t.push({ kind: "status", status: "idle", note: null });
    expect((await second).kind).toBe("follow_up");

    t.push({ kind: "status", status: "idle", note: null });
    expect((await third).outcome).toBe("succeeded");
    expect(t.commands.filter((c) => c.kind === "follow_up")).toHaveLength(2);

    const closed = await scope.manager.close(live_session_id);
    expect(closed.state.status).toBe("closed");
    await settle(scope);
  });

  it("bounds the queue by total bytes", async () => {
    const scope = await harness({ caps: hubQueuedFollowUps });
    const { live_session_id } = await startSession(scope.manager);
    const t = scope.factory.created[0];

    const first = scope.manager.prompt(live_session_id, "one");
    t.push({ kind: "status", status: "running", note: null });

    const chunk = "y".repeat(100 * 1024);
    const held: Promise<LiveTurnResult>[] = [];
    for (let index = 0; index < Math.floor(LIVE_FOLLOW_UP_QUEUE_MAX_BYTES / (100 * 1024)); index += 1) {
      held.push(scope.manager.followUp(live_session_id, chunk));
    }
    const bytesFull = await captureRejection(scope.manager.followUp(live_session_id, chunk));
    expect(bytesFull?.code).toBe("LIVE_QUEUE_FULL");

    const settled = Promise.all(held);
    const closed = await scope.manager.close(live_session_id);
    expect(closed.state.status).toBe("closed");
    expect((await settled).every((r) => r.error?.code === "LIVE_SESSION_CLOSING")).toBe(true);
    void first;
    // The in-flight turn resolved as cancelled by the close above.
    expect((await first).outcome).toBe("cancelled");

    await settle(scope);
  });
});

// ---------------------------------------------------------------------------
// Quotas
// ---------------------------------------------------------------------------

describe("quotas", () => {
  it("enforces the per-process cap", async () => {
    const scope = await harness({ processQuota: 2 });
    await startSession(scope.manager);
    await startSession(scope.manager);
    const quota = await captureRejection(scope.manager.start({ provider: "omp" }));
    expect(quota?.code).toBe("LIVE_QUOTA_EXCEEDED");
    await settle(scope);
    expect(LIVE_PROCESS_SESSION_QUOTA).toBe(8);
  });

  it("enforces the per-common-dir cap across hub processes via the durable leases", async () => {
    const repository = await createGitRepository();
    const identity = await resolveRepositoryIdentity(repository);
    const tmpRoot = await mkdtemp(join(tmpdir(), "agent-hub-live-core-"));
    const options = {
      commonDir: identity.common_dir,
      repositoryCwd: repository,
      transportFactories: [new FakeTransportFactory(() => new FakeTransport())],
      providerFactories: [ompProviderFactory],
      commonDirQuota: 2,
      tmpRoot,
      leaseProbes: fakeProbes(),
    };
    const hubA = new LiveSessionManager(options);
    const hubB = new LiveSessionManager(options);

    await hubA.start({ provider: "omp" });
    await hubB.start({ provider: "omp" });
    const quota = await captureRejection(hubB.start({ provider: "omp" }));
    expect(quota?.code).toBe("LIVE_QUOTA_EXCEEDED");

    await hubA.closeAll();
    await hubB.closeAll();
    expect(LIVE_COMMON_DIR_SESSION_QUOTA).toBe(4);
    await removeDirectory(repository);
    await removeDirectory(tmpRoot);
  });

  it("launches nothing when the probe does not find the provider", async () => {
    const scope = await harness();
    scope.factory.probeResult = { found: false, version: null, detail: "not on PATH" };
    const quota = await captureRejection(scope.manager.start({ provider: "omp" }));
    expect(quota?.code).toBe("LIVE_TRANSPORT_UNAVAILABLE");
    expect(scope.factory.created).toHaveLength(0);
    expect(await leasesOf(scope.commonDir)).toEqual([]);
    await settle(scope);
  });
});

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

describe("recovery", () => {
  interface Stranded extends Harness {
    liveSessionId: string;
    second: LiveSessionManager;
  }

  async function stranded(
    probes: LiveLeaseProbes,
    phases?: LiveManagerPhase[],
    acquireLock?: LiveManagerOptions["acquireLock"],
  ): Promise<Stranded> {
    const scope = await harness();
    const started = await startSession(scope.manager);
    // Provider-side work exists in the worktree after "hub death".
    await writeFile(join(started.workspace, "provider-work.txt"), "survived\n");
    return {
      ...scope,
      liveSessionId: started.live_session_id,
      second: secondHub(scope, probes, phases, acquireLock),
    };
  }

  it("reaps a provably-owned orphan before pinning and rewriting status", async () => {
    const phases: LiveManagerPhase[] = [];
    const kills: KillRecord[] = [];
    let providerAlive = true;
    const probes: LiveLeaseProbes = {
      probePid: (pid) => (pid === 424_242 && providerAlive ? "live" : "dead"),
      startToken: async (pid) => (pid === process.pid ? "hub-tok" : "prov-tok"),
      killGroup: (pgid, signal) => {
        kills.push({ target: pgid, signal });
        providerAlive = false;
        return true;
      },
      probeGroup: (pgid) => (pgid === 424_242 && providerAlive ? "alive" : "gone"),
      now: () => new Date(),
    };
    const scope = await stranded(probes, phases);
    const report = await scope.second.recover();

    expect(report.sessions.map((s) => s.outcome)).toEqual(["recovered"]);
    expect(kills.map((k) => k.signal)).toEqual(["SIGTERM"]);
    expect(phases).toEqual(["provider-reaped", "checkpoint-captured", "state-advanced"]);

    const durable = await durableState(scope.commonDir, scope.liveSessionId);
    expect(durable.status).toBe("orphaned");
    expect(durable.checkpoint_seq).toBe(1); // provider-work.txt got pinned
    expect(await leasesOf(scope.commonDir)).toHaveLength(0);

    // The pinned crash_recovery commit is on the live ref with the reason
    // recorded in the hub-constant commit message.
    const head = await resolveRef(scope.repository, `refs/agent-hub/live/${scope.liveSessionId}`);
    const subject = (await runGit(scope.repository, ["log", "-1", "--format=%s", head ?? ""])).trim();
    expect(subject).toContain("(crash_recovery)");

    await removeDirectory(scope.repository);
    await removeDirectory(scope.tmpRoot);
  });

  it("never signals a provider whose identity cannot be matched to the lease", async () => {
    const kills: KillRecord[] = [];
    const probes = fakeProbes({
      kills,
      alive: (pid) => pid === 424_242,
      startToken: (pid) => (pid === process.pid ? "hub-tok" : `reused-${pid}`),
    });
    const scope = await stranded(probes);
    const report = await scope.second.recover();
    expect(report.sessions.map((s) => s.outcome)).toEqual(["manual"]);
    expect(kills).toEqual([]);
    expect(await leasesOf(scope.commonDir)).toHaveLength(1);
    await removeDirectory(scope.repository);
    await removeDirectory(scope.tmpRoot);
  });

  it("keeps sessions whose live hub identity matches", async () => {
    const probes = fakeProbes();
    const scope = await stranded(probes);
    const report = await scope.second.recover();
    expect(report.sessions.map((s) => s.outcome)).toEqual(["kept-live"]);
    expect(await leasesOf(scope.commonDir)).toHaveLength(1);
    await removeDirectory(scope.repository);
    await removeDirectory(scope.tmpRoot);
  });

  it("treats a live hub pid with a reused identity as gone, not alive", async () => {
    const probes = fakeProbes({
      alive: () => true,
      startToken: (pid) => (pid === process.pid ? "someone-elses-boot" : "prov-start-mismatch"),
    });
    const scope = await stranded(probes);
    const report = await scope.second.recover();
    // Provider reads alive but its start identity mismatches → uncertain →
    // manual, and the hub itself (reused pid) is treated as gone.
    expect(report.sessions.map((s) => s.outcome)).toEqual(["manual"]);
    expect(await leasesOf(scope.commonDir)).toHaveLength(1);
    await removeDirectory(scope.repository);
    await removeDirectory(scope.tmpRoot);
  });

  it("classifies an unreapable survivor as manual and leaves everything alone", async () => {
    const probes = fakeProbes({
      alive: (pid) => pid === 424_242, // hub pid dead, provider alive
    });
    const scope = await stranded(probes);
    const report = await scope.second.recover();
    expect(report.sessions.map((s) => s.outcome)).toEqual(["manual"]);
    const durable = await durableState(scope.commonDir, scope.liveSessionId);
    expect(durable.status).toBe("idle"); // untouched: idle is not rewritten
    expect(await leasesOf(scope.commonDir)).toHaveLength(1);
    await removeDirectory(scope.repository);
    await removeDirectory(scope.tmpRoot);
  }, 30_000);

  it("reaps only after hub loss when the provider is already dead: pin, rewrite, clean", async () => {
    const probes = fakeProbes({ alive: () => false });
    const scope = await stranded(probes);
    const report = await scope.second.recover();
    expect(report.sessions.map((s) => s.outcome)).toEqual(["recovered"]);
    const durable = await durableState(scope.commonDir, scope.liveSessionId);
    expect(durable.status).toBe("orphaned");
    expect(durable.checkpoint_seq).toBe(1);
    expect(await leasesOf(scope.commonDir)).toHaveLength(0);
    await removeDirectory(scope.repository);
    await removeDirectory(scope.tmpRoot);
  });

  it("treats leader death with a surviving process group as manual and retains everything", async () => {
    const kills: KillRecord[] = [];
    // Leader PID gone, but kill(-pgid, 0) still answers: helpers live.
    const probes = fakeProbes({
      kills,
      alive: () => false,
      groupState: (pgid) => (pgid === 424_242 ? "alive" : "gone"),
    });
    const scope = await stranded(probes);
    const report = await scope.second.recover();
    expect(report.sessions.map((s) => s.outcome)).toEqual(["manual"]);
    expect(kills).toEqual([]);
    const durable = await durableState(scope.commonDir, scope.liveSessionId);
    // No checkpoint, no rewrite: the tree may still be under mutation.
    expect(durable.status).toBe("idle");
    expect(durable.checkpoint_seq).toBe(0);
    expect(await leasesOf(scope.commonDir)).toHaveLength(1);
    const worktreePath = String(durable.worktree_path);
    await access(worktreePath);
    await removeDirectory(scope.repository);
    await removeDirectory(scope.tmpRoot);
  });

  it("routes a live provider whose lease lost the pgid to manual and never signals", async () => {
    const kills: KillRecord[] = [];
    const probes = fakeProbes({
      kills,
      alive: (pid) => pid === 424_242, // hub pid dead, provider alive
      startToken: (pid) => (pid === process.pid ? "hub-tok" : "prov-tok"),
    });
    const scope = await stranded(probes);
    // Same provider, but the lease lost its group identity: recovery may
    // NEVER reconstruct PGID == PID and signal the pid as a group.
    const lease = await readLiveLease(scope.commonDir, scope.liveSessionId);
    expect(lease).toBeDefined();
    await writeFile(
      liveLeasePath(scope.commonDir, scope.liveSessionId),
      JSON.stringify({ ...lease, provider_pgid: null }),
      "utf8",
    );

    const report = await scope.second.recover();
    expect(report.sessions.map((s) => s.outcome)).toEqual(["manual"]);
    expect(report.sessions[0]?.detail).toContain("process-group identity");
    expect(kills).toEqual([]);
    const durable = await durableState(scope.commonDir, scope.liveSessionId);
    expect(durable.status).toBe("idle"); // no checkpoint, no rewrite
    expect(await leasesOf(scope.commonDir)).toHaveLength(1);
    await removeDirectory(scope.repository);
    await removeDirectory(scope.tmpRoot);
  });

  it("routes a dead leader with a mismatched nonexistent group to manual, never probing it", async () => {
    const kills: KillRecord[] = [];
    const probedGroups: number[] = [];
    // Hub gone, leader gone; every group probe — were one made — answers ESRCH.
    const probes = fakeProbes({
      kills,
      alive: () => false,
      groupState: (pgid) => {
        probedGroups.push(pgid);
        return "gone";
      },
    });
    const scope = await stranded(probes);
    // The lease's group identity no longer names the group its leader
    // provably leads: it names a group that does not exist.
    const lease = await readLiveLease(scope.commonDir, scope.liveSessionId);
    expect(lease).toBeDefined();
    await writeFile(
      liveLeasePath(scope.commonDir, scope.liveSessionId),
      JSON.stringify({ ...lease, provider_pgid: 999_333 }),
      "utf8",
    );

    const report = await scope.second.recover();
    expect(report.sessions.map((s) => s.outcome)).toEqual(["manual"]);
    expect(report.sessions[0]?.detail).toContain("not the group");
    // A phantom group's ESRCH could fake provider death and license cleanup,
    // so it may not even be probed, let alone signalled.
    expect(probedGroups).toEqual([]);
    expect(kills).toEqual([]);
    // No cleanup: no checkpoint, no status rewrite; lease and worktree stay.
    const durable = await durableState(scope.commonDir, scope.liveSessionId);
    expect(durable.status).toBe("idle");
    expect(durable.checkpoint_seq).toBe(0);
    expect(await leasesOf(scope.commonDir)).toHaveLength(1);
    await access(String(durable.worktree_path));
    await removeDirectory(scope.repository);
    await removeDirectory(scope.tmpRoot);
  });

  it("escalates recovery to KILL and certifies only when the group answers gone", async () => {
    const phases: LiveManagerPhase[] = [];
    const kills: KillRecord[] = [];
    let leaderAlive = true;
    // A helper keeps the group alive through the SIGTERM window; only the
    // bounded SIGKILL sweep empties it. Recovery must not "reap" on leader
    // death alone.
    let groupGone = false;
    const probes: LiveLeaseProbes = {
      probePid: (pid) => (pid === 424_242 && leaderAlive ? "live" : "dead"),
      startToken: async (pid) => (pid === process.pid ? "hub-tok" : "prov-tok"),
      killGroup: (pgid, signal) => {
        kills.push({ target: pgid, signal });
        if (signal === "SIGTERM") {
          leaderAlive = false;
        }
        if (signal === "SIGKILL") {
          groupGone = true;
        }
        return true;
      },
      probeGroup: (pgid) => (pgid === 424_242 && !groupGone ? "alive" : "gone"),
      now: () => new Date(),
    };
    const scope = await stranded(probes, phases);
    const report = await scope.second.recover();
    expect(report.sessions.map((s) => s.outcome)).toEqual(["recovered"]);
    expect(kills.map((k) => k.signal)).toEqual(["SIGTERM", "SIGKILL"]);
    expect(phases).toEqual(["provider-reaped", "checkpoint-captured", "state-advanced"]);
    const durable = await durableState(scope.commonDir, scope.liveSessionId);
    expect(durable.status).toBe("orphaned");
    expect(durable.checkpoint_seq).toBe(1);
    expect(await leasesOf(scope.commonDir)).toHaveLength(0);
    await removeDirectory(scope.repository);
    await removeDirectory(scope.tmpRoot);
  }, 30_000);

  it("reports foreign-host leases without touching them", async () => {
    const scope = await stranded(fakeProbes());
    const leaseDir = join(scope.commonDir, "agent-hub", "live", "leases");
    const names = await leasesOf(scope.commonDir);
    const path = join(leaseDir, names[0]);
    const record = JSON.parse(await readFile(path, "utf8")) as Record<string, unknown>;
    record.hub_hostname = "another-machine";
    await writeFile(path, JSON.stringify(record), "utf8");

    const report = await scope.second.recover();
    expect(report.sessions.map((s) => s.outcome)).toEqual(["foreign"]);
    expect(await leasesOf(scope.commonDir)).toHaveLength(1);
    await removeDirectory(scope.repository);
    await removeDirectory(scope.tmpRoot);
  });
});

// ---------------------------------------------------------------------------
// Live-admin lock discipline
// ---------------------------------------------------------------------------

describe("live-admin lock discipline", () => {
  /** Hands out real locks until poisoned; then only `live-admin` refuses. */
  function adminLockPoison(state: { poisoned: boolean }): LiveManagerOptions["acquireLock"] {
    return async (options) => {
      if (state.poisoned && options.name === LIVE_ADMIN_LOCK_NAME) {
        throw new AgentHubError("LOCK_BUSY", "the live-admin lock is held by a stuck peer");
      }
      return await acquireRepositoryLock(options);
    };
  }

  it("launch is refused while the live-admin lock is unavailable, claiming nothing", async () => {
    const state = { poisoned: true };
    const scope = await harness({ acquireLock: adminLockPoison(state) });
    await expectCode(scope.manager.start({ provider: "omp" }), "LOCK_BUSY");
    expect(await leasesOf(scope.commonDir)).toEqual([]);
    expect(await readdir(scope.tmpRoot)).toEqual([]);
    await settle(scope);
  });

  it("teardown without the admin lock changes nothing and retains lease plus worktree", async () => {
    const state = { poisoned: false };
    const scope = await harness({ acquireLock: adminLockPoison(state) });
    const { live_session_id } = await startSession(scope.manager);
    const worktreePath = scope.manager.view(live_session_id).worktree_path;
    state.poisoned = true;

    const closed = await scope.manager.close(live_session_id);
    // The provider really is gone, so the close checkpoint is honest…
    expect(closed.state.status).toBe("closed");
    // …but no worktree mutation may run without the lock: structured
    // failure out, lease and worktree retained untouched.
    expect(closed.cleanup_errors.map((error) => error.code)).toContain("LIVE_ADMIN_LOCK_UNAVAILABLE");
    expect(await leasesOf(scope.commonDir)).toHaveLength(1);
    await access(worktreePath);

    state.poisoned = false;
    await settle(scope);
  });

  it("recovery without the admin lock continues, retaining lease and worktree", async () => {
    const state = { poisoned: false };
    const scope = await harness();
    const started = await startSession(scope.manager);
    await writeFile(join(started.workspace, "provider-work.txt"), "survived\n");
    const worktreePath = scope.manager.view(started.live_session_id).worktree_path;
    const second = secondHub(scope, fakeProbes({ alive: () => false }), undefined, adminLockPoison(state));
    state.poisoned = true;

    const report = await second.recover();
    const session = report.sessions[0];
    expect(session?.outcome).toBe("recovered");
    expect(session?.detail).toContain("worktree cleanup refused");
    expect(session?.detail).toContain("lease and worktree retained");
    // The durable rewrite happened; cleanup was refused, so the lease that
    // audits the surviving worktree must still exist.
    const durable = await durableState(scope.commonDir, started.live_session_id);
    expect(durable.status).toBe("orphaned");
    expect(await leasesOf(scope.commonDir)).toHaveLength(1);
    await access(worktreePath);

    state.poisoned = false;
    await removeDirectory(scope.repository);
    await removeDirectory(scope.tmpRoot);
  });
});

// ---------------------------------------------------------------------------
// Resume honesty
// ---------------------------------------------------------------------------

describe("resume state honesty", () => {
  it("marks verified only for an observed round trip through open()", async () => {
    const scope = await harness();
    const started = await scope.manager.start({
      provider: "omp",
      resume: {
        provider: "omp",
        provider_session_id: "prov-1",
        verified: false,
        verified_via: null,
        last_event_seq: 4,
      },
    });
    expect(started.state.resume).toMatchObject({
      provider: "omp",
      provider_session_id: "prov-1",
      verified: true,
      verified_via: "hub-restart:omp-rpc",
      last_event_seq: 4,
    });
    await settle(scope);

    const fresh = await harness();
    const unverified = await fresh.manager.start({ provider: "omp" });
    expect(unverified.state.resume).toMatchObject({ verified: false, verified_via: null });
    await settle(fresh);
  });

  it("resumeFromState refuses a live record and continues a closed one on the same ref", async () => {
    const scope = await harness();
    const started = await startSession(scope.manager);
    const resumable = new LiveSessionManager({
      commonDir: scope.commonDir,
      repositoryCwd: scope.repository,
      transportFactories: [
        new FakeTransportFactory(() => new FakeTransport(fullCapabilities(), { resumeState: "echo" })),
      ],
      providerFactories: [ompProviderFactory],
      tmpRoot: scope.tmpRoot,
      leaseProbes: fakeProbes(),
    });

    // A non-terminal record may not be resumed anywhere, in any hub.
    await expectCode(
      resumable.resumeFromState({ live_session_id: started.live_session_id }),
      "LIVE_SESSION_NOT_RESUMABLE",
    );

    const closed = await scope.manager.close(started.live_session_id);
    expect(closed.state.status).toBe("closed");

    const resumed = await resumable.resumeFromState({ live_session_id: started.live_session_id });
    expect(resumed.state.status).toBe("idle");
    // The existing chain advances by exactly one revision — the live ref is
    // continued, never branched per restart.
    expect(resumed.state.revision).toBe(closed.state.revision + 1);
    expect(await resolveRef(scope.repository, `refs/agent-hub/live/${started.live_session_id}`)).toBe(
      resumed.state.current_commit,
    );
    // The durable handle only lands verified when the transport itself
    // shows the post-handshake identity.
    expect(resumed.state.resume).toMatchObject({
      provider_session_id: "prov-1",
      verified: true,
      verified_via: "transport-verified:open-echo",
    });
    expect(await leasesOf(scope.commonDir)).toHaveLength(1);

    await resumable.closeAll();
    await settle(scope);
  });
});

// ---------------------------------------------------------------------------
// JSONL framing (core transport primitive)
// ---------------------------------------------------------------------------

describe("strict LF JSONL framing", () => {
  it("reassembles frames split across chunks at arbitrary byte offsets", () => {
    const frame = encodeJsonlFrame({ msg: "héllo 世界", seq: 1 });
    const framer = new LiveJsonlFramer();
    const results = [];
    for (let offset = 0; offset < frame.byteLength; offset += 3) {
      results.push(...framer.feed(frame.subarray(offset, offset + 3)));
    }
    results.push(...framer.end());
    expect(results).toEqual([
      { kind: "frame", value: { msg: "héllo 世界", seq: 1 }, bytes: frame.byteLength - 1 },
    ]);
  });

  it("recognizes only the LF byte as a terminator", () => {
    const framer = new LiveJsonlFramer();
    // CR alone never completes a frame: nothing comes out, bytes stay pending.
    expect(framer.feed(Buffer.from('{"a":1}\r', "utf8"))).toEqual([]);
    expect(framer.pendingBytes).toBe(8);
    // The LF terminates the frame; the CR is accounted inside the frame bytes
    // (JSON's own whitespace tolerance is JSON's business, not a boundary).
    const results = framer.feed(Buffer.from("\n", "utf8"));
    expect(results).toEqual([{ kind: "frame", value: { a: 1 }, bytes: 8 }]);

    // A CR run carrying two documents is one invalid frame — CR never split it.
    const framer2 = new LiveJsonlFramer();
    expect(framer2.feed(Buffer.from('{"a":1}\r{"b":2}', "utf8"))).toEqual([]);
    const invalid = framer2.feed(Buffer.from("\n", "utf8"));
    if (invalid[0].kind === "error") {
      expect(invalid[0].error.code).toBe("LIVE_JSONL_FRAME_INVALID_JSON");
      expect(invalid[0].error.frame_bytes).toBe(15); // {"a":1}\r{"b":2}
    } else {
      expect.unreachable("two documents are not one frame");
    }
  });
  it("reports oversized frames once, then resynchronizes after the newline", () => {
    const framer = new LiveJsonlFramer({ maxFrameBytes: 16 });
    const first = framer.feed(Buffer.from("aaaaaaaaaaaaaaaaaaaaaaaaaaa\n", "utf8"));
    expect(first).toHaveLength(1);
    if (first[0].kind === "error") {
      expect(first[0].error.code).toBe("LIVE_JSONL_FRAME_TOO_LARGE");
    } else {
      expect.unreachable("oversized run must error");
    }
    const second = framer.feed(Buffer.from('{"ok":true}\n', "utf8"));
    expect(second[0].kind).toBe("frame");
  });

  it("refuses to parse an unterminated tail", () => {
    const framer = new LiveJsonlFramer();
    framer.feed(Buffer.from('{"partial":tr', "utf8"));
    const results = framer.end();
    if (results[0].kind === "error") {
      expect(results[0].error.code).toBe("LIVE_JSONL_FRAME_UNTERMINATED");
    } else {
      expect.unreachable("tail must error");
    }
  });

  it("never echoes raw frame bytes into errors", () => {
    const framer = new LiveJsonlFramer({ maxFrameBytes: 12 });
    const results = framer.feed(Buffer.from("SECRET-CREDENTIAL-CHAIN-abc\n", "utf8"));
    expect(JSON.stringify(results)).not.toContain("SECRET");
    if (results[0].kind === "error") {
      expect(results[0].error.code).toBe("LIVE_JSONL_FRAME_TOO_LARGE");
    } else {
      expect.unreachable("must error");
    }
  });

  it("bounds the encoder", () => {
    let code = "";
    try {
      encodeJsonlFrame({ big: "z".repeat(500) }, 64);
    } catch (error) {
      code = (error as AgentHubError).code;
    }
    expect(code).toBe("LIVE_JSONL_FRAME_TOO_LARGE");
  });
});

// ---------------------------------------------------------------------------
// Child process ownership
// ---------------------------------------------------------------------------

describe("process-group child ownership", () => {
  it("owns stdin/stdout, observes the exit, and marks exits as hub-requested", async () => {
    const child = await launchLiveChild({
      command: "/bin/sh",
      args: ["-c", "cat"],
      cwd: "/",
      maxStderrBytes: 64,
    });
    const collected: string[] = [];
    child.onStdout((chunk) => collected.push(chunk.toString("utf8")));
    await child.writeStdin("ping\n");
    child.closeStdin();

    const exit = await child.exited();
    expect(exit.exit_code).toBe(0);
    expect(exit.intentional).toBe(false);
    expect(collected.join("")).toBe("ping\n");

    const stop = await child.stop("terminate");
    expect(stop.status).toBe("closed");
    expect(stop.exit_code).toBe(0);
  });

  it("terminates the whole group and reports the observed exit", async () => {
    const child = await launchLiveChild({
      command: "/bin/sh",
      args: ["-c", "sleep 60"],
      cwd: "/",
      maxStderrBytes: 64,
    });
    const stop = await child.stop("graceful", { graceMs: 3_000 });
    expect(stop.status).toBe("closed");
    expect(stop.exit_signal).toBe("SIGTERM");
    expect(child.exitInfo?.intentional).toBe(true);
  });

  it("escalates to SIGKILL for a group that ignores TERM, and reports survival honestly", async () => {
    const ignoring = await launchLiveChild({
      command: "/bin/sh",
      // Ignored-signal dispositions are inherited: the whole group shrugs TERM off.
      args: ["-c", "trap '' TERM; echo ready; sleep 60 & wait"],
      cwd: "/",
      maxStderrBytes: 64,
    });
    // Signal only once the trap is demonstrably installed: `ready` is echoed
    // after the trap runs, so this tests TERM resistance, not a startup race.
    await new Promise<void>((resolve) => {
      let seen = "";
      const fallback = setTimeout(resolve, 1_000);
      ignoring.onStdout((chunk) => {
        seen += chunk.toString("utf8");
        if (seen.includes("ready")) {
          clearTimeout(fallback);
          resolve();
        }
      });
    });
    const graceful = await ignoring.stop("graceful", { graceMs: 200 });
    expect(graceful.status).toBe("orphaned");
    const forced = await ignoring.stop("terminate", { graceMs: 0, killWaitMs: 5_000 });
    expect(forced.status).toBe("closed");
    expect(forced.exit_signal).toBe("SIGKILL");
  });

  it("kills a helper that outlives its leader: closed proves the whole group is gone", async () => {
    // The leader forks a `sleep` helper into the same group and exits FIRST,
    // leaving the helper behind. `closed` may only be reported after the
    // hub proves the group is gone — which requires the helper to die too.
    const child = await launchLiveChild({
      command: "/bin/sh",
      args: ["-c", "sleep 120 & echo helper:$!; exit 0"],
      cwd: "/",
      maxStderrBytes: 64,
    });
    const helperPid = await new Promise<number>((resolve, reject) => {
      let seen = "";
      const fallback = setTimeout(() => reject(new Error(`no helper pid line (${seen})`)), 5_000);
      child.onStdout((chunk) => {
        seen += chunk.toString("utf8");
        const match = /helper:(\d+)/.exec(seen);
        if (match) {
          clearTimeout(fallback);
          resolve(Number(match[1]));
        }
      });
      void child.exited().then(() => reject(new Error("leader exited before naming its helper")));
    });
    // The helper is alive and the group still exists with the leader gone.
    expect(() => process.kill(helperPid, 0)).not.toThrow();
    await child.exited();

    const stop = await child.stop("terminate", { graceMs: 200, killWaitMs: 5_000 });
    expect(stop.status).toBe("closed");
    expect(child.groupAlive()).toBe(false);
    // The helper the hub never spawned directly is dead because the whole
    // group was signaled — a leader-only kill would leave it behind.
    expect(() => process.kill(helperPid, 0)).toThrow();
  });

  it("graceful never signals a leaderless group; a later terminate escalates and closes", async () => {
    // Shared authorization scenario (test/live-stop-authority.ts) on the
    // primitive seam. The leader exits FIRST, having handed its `sleep`
    // helper an ignored disposition for TERM/INT (SIG_IGN survives exec),
    // so only a terminate-authorized SIGKILL can dissolve the group.
    const child = await launchLiveChild({
      command: "/bin/sh",
      args: ["-c", "trap '' TERM INT; sleep 120 & echo helper:$!; exit 0"],
      cwd: "/",
      maxStderrBytes: 64,
    });
    try {
      const helperPid = await new Promise<number>((resolve, reject) => {
        let seen = "";
        const fallback = setTimeout(() => reject(new Error(`no helper pid line (${seen})`)), 5_000);
        child.onStdout((chunk) => {
          seen += chunk.toString("utf8");
          const match = /helper:(\d+)/.exec(seen);
          if (match) {
            clearTimeout(fallback);
            resolve(Number(match[1]));
          }
        });
      });
      await child.exited();
      expect(realProcessAlive(helperPid)).toBe(true);

      await runLeaderFirstSurvivorScenario({
        stop: (mode) => child.stop(mode, { graceMs: 100, killWaitMs: 5_000 }),
        survivorAlive: () => realProcessAlive(helperPid),
      });
      expect(child.groupAlive()).toBe(false);
    } finally {
      await child.stop("terminate", { graceMs: 50, killWaitMs: 2_000 });
    }
  }, 20_000);

  it("capability-gates signal-only cancel to platforms with reliable group signals", async () => {
    const transport = new AgyStreamJsonTransport({ command: "definitely-not-a-real-binary-xyzzy" });
    const descriptor = await transport.describe();
    expect(descriptor.capabilities.cancel.support).toBe(
      SUPPORTS_GROUP_SIGNALS ? "signal" : "unsupported",
    );
    if (!SUPPORTS_GROUP_SIGNALS) {
      expect(descriptor.capabilities.cancel.evidence).toBeNull();
    }
  });

  it("bounds stderr observation without losing truncation honesty", async () => {
    const child = await launchLiveChild({
      command: "/bin/sh",
      args: ["-c", "head -c 2000 /dev/zero >&2; echo done"],
      cwd: "/",
      maxStderrBytes: 100,
    });
    const exit = await child.exited();
    expect(exit.exit_code).toBe(0);
    expect(child.stderrTruncated).toBe(true);
    expect(Buffer.byteLength(child.stderrText, "utf8")).toBe(100);
  });

  it("fails the launch, not the session, when the binary cannot start", async () => {
    const failure = await captureRejection(
      launchLiveChild({
        command: "definitely-not-a-real-binary-xyzzy",
        args: [],
        cwd: "/",
        maxStderrBytes: 64,
      }),
    );
    expect(failure?.code).toBe("LIVE_CHILD_SPAWN_FAILED");
  });

  it("owns provider stdin EPIPE as a structured LIVE_CHILD_STDIN_CLOSED, never an uncaught hub error", async () => {
    // A trap stands in for Node's default crash-on-uncaught behavior: with
    // the fix nothing lands in it; before the fix the unowned stream
    // 'error' emission lands here and the assertion fails.
    const uncaught: unknown[] = [];
    const trap = (error: unknown): void => {
      uncaught.push(error);
    };
    process.on("uncaughtException", trap);
    try {
      // A provider that closes its stdin and then lives until signaled —
      // exactly the shape of a real agent that stops reading input.
      // closeSync(0) provably closes the pipe's read end (a not-yet-started
      // stdin stream's destroy() leaves the fd open), so the hub's next
      // write is guaranteed to EPIPE.
      const child = await launchLiveChild({
        command: process.execPath,
        args: [
          "-e",
          "require(\"fs\").closeSync(0); console.log(\"ready\"); setInterval(() => {}, 1000);",
        ],
        cwd: "/",
        maxStderrBytes: 64,
      });
      try {
        await new Promise<void>((resolve, reject) => {
          let seen = "";
          const fallback = setTimeout(() => reject(new Error(`provider never announced readiness (${seen})`)), 5_000);
          child.onStdout((chunk) => {
            seen += chunk.toString("utf8");
            if (seen.includes("ready")) {
              clearTimeout(fallback);
              resolve();
            }
          });
        });
        // The pipe's read end is provably gone: this write must EPIPE.
        await expectCode(child.writeStdin("ping\n"), "LIVE_CHILD_STDIN_CLOSED");
        // Every later write is refused structured, without touching the stream.
        await expectCode(child.writeStdin("again\n"), "LIVE_CHILD_STDIN_CLOSED");

        // Observation pipes may fail too; the hub owns those errors as well.
        child.stdout?.destroy(new Error("injected stdout failure"));
        child.stderr?.destroy(new Error("injected stderr failure"));
        await tick();
        await tick();
        expect(uncaught).toEqual([]);

        // Shutdown and exit observation remain fully operational.
        const stop = await child.stop("terminate", { graceMs: 2_000, killWaitMs: 2_000 });
        expect(stop.status).toBe("closed");
        expect(child.exitInfo?.intentional).toBe(true);
      } finally {
        await child.stop("terminate", { graceMs: 50, killWaitMs: 2_000 });
      }
    } finally {
      process.off("uncaughtException", trap);
    }
  }, 20_000);
});

// ---------------------------------------------------------------------------
// Durable failure: every accepted turn settles exactly once
// ---------------------------------------------------------------------------

describe("durable failure settlement", () => {
  const sessionsDirOf = (commonDir: string): string =>
    join(commonDir, "agent-hub", "live", "sessions");

  async function spinUntil(condition: () => Promise<boolean>, message: string): Promise<void> {
    for (let spin = 0; !(await condition()); spin += 1) {
      if (spin > 50_000) {
        throw new Error(message);
      }
      await tick();
    }
  }

  const runningMirror = (scope: Harness, liveSessionId: string): Promise<void> =>
    spinUntil(
      async () => (await scope.manager.view(liveSessionId)).status === "running",
      "the running-status mirror write never landed",
    );

  // Each test `await`s the turn promises: a hang surfaces as the test timeout
  // (the pre-fix behavior), and an outcome other than `failed` fails the
  // assertion. Neither is allowed; success would be a lie.

  it("checkpoint/commit-phase failure: prompt fails, queued follow-up fails undispached, resources retained", async () => {
    const phases: LiveManagerPhase[] = [];
    const scope = await harness({
      caps: hubQueuedFollowUps,
      transportOptions: writingTransportOptions,
      phases,
      failPhase: "checkpoint-captured",
    });
    const { live_session_id, workspace } = await startSession(scope.manager);
    const t = scope.factory.created[0];

    const first = scope.manager.prompt(live_session_id, "one");
    t.push({ kind: "status", status: "running", note: null });
    await runningMirror(scope, live_session_id);
    const second = scope.manager.followUp(live_session_id, "two");

    // The terminal boundary now hits the injected durable failure.
    t.push({ kind: "status", status: "idle", note: null });

    const result = await first;
    expect(result.outcome).toBe("failed");
    expect(result.checkpoint).toBeNull();
    expect(result.error?.code).toBe("INJECTED_DURABLE_FAILURE");
    expect(result.error?.stage).toBe("state");

    const queued = await second;
    expect(queued.outcome).toBe("failed");
    expect(queued.error?.code).toBe("INJECTED_DURABLE_FAILURE");

    // A broken chain fails the queue, it never dispatches from it.
    expect(t.commands.filter((c) => c.kind === "follow_up")).toHaveLength(0);

    // The best-effort honest rewrite lands: the record says `error` with the
    // durable failure — and still carries NO checkpoint the chain never took.
    // `state-advanced` is pushed strictly after the state file is committed,
    // so waiting on it deterministically also guarantees the file is readable.
    await spinUntil(
      async () => phases.includes("state-advanced"),
      "the best-effort durable error rewrite never landed",
    );
    const durable = await durableState(scope.commonDir, live_session_id);
    expect(durable.last_error).toMatchObject({ code: "INJECTED_DURABLE_FAILURE", stage: "state" });
    expect(durable.checkpoint_seq).toBe(0);
    // Exactly the honest rewrite ran; no checkpoint phase was ever recorded.
    expect(phases).toEqual(["state-advanced"]);
    const names = await readdir(sessionsDirOf(scope.commonDir));
    expect(names.filter((name) => name.endsWith(".pending.json"))).toEqual([]);

    // Safety unproven ⇒ nothing released; the degraded session refuses work.
    expect(scope.manager.activeCount).toBe(1);
    expect(await leasesOf(scope.commonDir)).toHaveLength(1);
    await access(workspace);
    await expectCode(scope.manager.followUp(live_session_id, "three"), "LIVE_SESSION_NOT_LIVE");

    await settle(scope);
  });

  it("live-ref CAS divergence: the turn fails with LIVE_STATE_INCONSISTENT; the concurrent ref stands", async () => {
    const scope = await harness({
      caps: hubQueuedFollowUps,
      transportOptions: writingTransportOptions,
    });
    const { live_session_id } = await startSession(scope.manager);
    const t = scope.factory.created[0];

    const first = scope.manager.prompt(live_session_id, "one");
    t.push({ kind: "status", status: "running", note: null });
    await runningMirror(scope, live_session_id);
    const second = scope.manager.followUp(live_session_id, "two");

    // A concurrent mover pins the live ref somewhere else; the hub's CAS
    // must refuse it, never overwrite it.
    await writeFile(join(scope.repository, "external.txt"), "external\n");
    await runGit(scope.repository, ["add", "external.txt"]);
    await runGit(scope.repository, ["commit", "-qm", "external commit"]);
    const external = (await runGit(scope.repository, ["rev-parse", "HEAD"])).trim();
    await runGit(scope.repository, [
      "update-ref",
      `refs/agent-hub/live/${live_session_id}`,
      external,
    ]);

    t.push({ kind: "status", status: "idle", note: null });
    const result = await first;
    expect(result.outcome).toBe("failed");
    expect(result.error?.code).toBe("LIVE_STATE_INCONSISTENT");
    expect(result.checkpoint).toBeNull();

    const queued = await second;
    expect(queued.outcome).toBe("failed");
    expect(queued.error?.code).toBe("LIVE_STATE_INCONSISTENT");
    expect(t.commands.filter((c) => c.kind === "follow_up")).toHaveLength(0);

    // The externally-moved ref stays put; the record never moved; the lease
    // and worktree stay for recovery.
    expect(await resolveRef(scope.repository, `refs/agent-hub/live/${live_session_id}`)).toBe(
      external,
    );
    const durable = await durableState(scope.commonDir, live_session_id);
    expect(durable.status).toBe("running"); // last honestly committed mirror
    expect(durable.checkpoint_seq).toBe(0);
    expect(await leasesOf(scope.commonDir)).toHaveLength(1);
    await settle(scope);
  });

  it.skipIf(!BLOCKED_WRITE_IS_MEANINGFUL)("state-record write failure: turns fail, the record stays at its last committed truth", async () => {
    const scope = await harness({
      caps: hubQueuedFollowUps,
      transportOptions: writingTransportOptions,
    });
    const { live_session_id, workspace } = await startSession(scope.manager);
    const t = scope.factory.created[0];
    const sessionsDir = sessionsDirOf(scope.commonDir);

    const first = scope.manager.prompt(live_session_id, "one");
    t.push({ kind: "status", status: "running", note: null });
    await runningMirror(scope, live_session_id);
    const second = scope.manager.followUp(live_session_id, "two");

    // The durable state directory itself refuses writes (real EACCES).
    await chmod(sessionsDir, 0o500);
    try {
      t.push({ kind: "status", status: "idle", note: null });
      const result = await first;
      expect(result.outcome).toBe("failed");
      expect(result.error?.code).toBe("LIVE_STATE_WRITE_FAILED");
      expect(result.checkpoint).toBeNull();

      const queued = await second;
      expect(queued.outcome).toBe("failed");
      expect(queued.error?.code).toBe("LIVE_STATE_WRITE_FAILED");
      expect(t.commands.filter((c) => c.kind === "follow_up")).toHaveLength(0);

      expect(await leasesOf(scope.commonDir)).toHaveLength(1);
      await access(workspace);
    } finally {
      await chmod(sessionsDir, 0o755);
    }

    const durable = await durableState(scope.commonDir, live_session_id);
    expect(durable.status).toBe("running"); // last honest mirror
    const names = await readdir(sessionsDir);
    expect(names.filter((name) => name.endsWith(".pending.json"))).toEqual([]);
    await settle(scope);
  });

  it.skipIf(!BLOCKED_WRITE_IS_MEANINGFUL)("proven close over a broken state write: honest failure, retained resources, no closed lie", async () => {
    const scope = await harness();
    const { live_session_id, workspace } = await startSession(scope.manager);
    await chmod(sessionsDirOf(scope.commonDir), 0o500);
    try {
      const closed = await scope.manager.close(live_session_id);
      // The shutdown proof stands, but nothing was committed and nothing
      // was torn down: the close reports its own failure.
      expect(closed.stop?.status).toBe("closed");
      expect(closed.checkpoint_taken).toBe(false);
      expect(closed.cleanup_errors.map((error) => error.code)).toContain("LIVE_STATE_WRITE_FAILED");
      expect(await leasesOf(scope.commonDir)).toHaveLength(1);
      await access(workspace);
    } finally {
      await chmod(sessionsDirOf(scope.commonDir), 0o755);
    }
    const durable = await durableState(scope.commonDir, live_session_id);
    expect(durable.status).toBe("idle"); // the record never claimed closed
    await settle(scope);
  });

  it("crash whose ordering seam fails: the taken-over turn still settles once as failed, conservatively orphaned", async () => {
    const phases: LiveManagerPhase[] = [];
    const scope = await harness({
      transportOptions: writingTransportOptions,
      phases,
      failPhase: "transport-stopped",
    });
    const { live_session_id } = await startSession(scope.manager);
    const t = scope.factory.created[0];

    const first = scope.manager.prompt(live_session_id, "hot work");
    t.push({ kind: "status", status: "running", note: null });
    t.push({ kind: "exit", intentional: false, exit_code: 137, exit_signal: null });

    const result = await first;
    expect(result.outcome).toBe("failed");
    expect(result.exit_code).toBe(137);
    expect(t.stopCalls).toEqual(["terminate"]);

    // The seam aborted crash handling right after the stop: the session is
    // conservatively orphaned (the stop report is abandoned), pinned by the
    // best-effort honest rewrite, and nothing is torn down. `state-advanced`
    // is pushed strictly after the rewrite commits — await that, not the file.
    await spinUntil(
      async () => phases.includes("state-advanced"),
      "the conservative orphan rewrite never landed",
    );
    const durable = await durableState(scope.commonDir, live_session_id);
    expect(durable.last_error).toMatchObject({ code: "INJECTED_DURABLE_FAILURE" });
    expect(durable.checkpoint_seq).toBe(0);
    expect(phases).toEqual(["state-advanced"]);
    expect(await leasesOf(scope.commonDir)).toHaveLength(1);
    await settle(scope);
  });
});

// ---------------------------------------------------------------------------
// Caller-worktree allow_dirty contract
// ---------------------------------------------------------------------------

describe("caller worktree allow_dirty contract", () => {
  it("refuses untracked local changes by default, claiming nothing", async () => {
    const scope = await harness();
    await writeFile(join(scope.repository, "scratch.txt"), "mine\n");
    await expectCode(scope.manager.start({ provider: "omp" }), "DIRTY_WORKTREE");
    expect(await leasesOf(scope.commonDir)).toEqual([]);
    expect(await readdir(scope.tmpRoot)).toEqual([]);
    await settle(scope);
  });

  it("refuses tracked local changes by default, launching nothing", async () => {
    const scope = await harness();
    await writeFile(join(scope.repository, "README.md"), "uncommitted caller work\n");
    await expectCode(scope.manager.start({ provider: "omp" }), "DIRTY_WORKTREE");
    expect(scope.factory.created).toHaveLength(0);
    await settle(scope);
  });

  it("allow_dirty starts from committed HEAD and never copies caller changes", async () => {
    const scope = await harness();
    await writeFile(join(scope.repository, "scratch.txt"), "mine\n");
    await writeFile(join(scope.repository, "README.md"), "uncommitted caller work\n");

    const started = await scope.manager.start({ provider: "omp", allow_dirty: true });
    const identity = await resolveRepositoryIdentity(scope.repository);
    expect(started.state.base_commit).toBe(identity.head);
    expect((await runGit(started.workspace, ["rev-parse", "HEAD"])).trim()).toBe(identity.head);
    // Committed HEAD content only — the caller's uncommitted state never
    // crosses into the isolated live worktree.
    expect(await readFile(join(started.workspace, "README.md"), "utf8")).toBe("initial\n");
    await expect(access(join(started.workspace, "scratch.txt"))).rejects.toThrow();
    await settle(scope);
  });

  it("resumeFromState applies the same gate; allow_dirty resumes at the committed chain head", async () => {
    const scope = await harness();
    const started = await startSession(scope.manager);
    const closed = await scope.manager.close(started.live_session_id);
    expect(closed.state.status).toBe("closed");

    const resumable = new LiveSessionManager({
      commonDir: scope.commonDir,
      repositoryCwd: scope.repository,
      transportFactories: [
        new FakeTransportFactory(() => new FakeTransport(fullCapabilities(), { resumeState: "echo" })),
      ],
      providerFactories: [ompProviderFactory],
      tmpRoot: scope.tmpRoot,
      leaseProbes: fakeProbes(),
    });

    await writeFile(join(scope.repository, "scratch.txt"), "mine\n");
    await expectCode(
      resumable.resumeFromState({ live_session_id: started.live_session_id }),
      "DIRTY_WORKTREE",
    );
    expect(await leasesOf(scope.commonDir)).toHaveLength(0);

    const resumed = await resumable.resumeFromState({
      live_session_id: started.live_session_id,
      allow_dirty: true,
    });
    expect(resumed.state.status).toBe("idle");
    expect(await readFile(join(resumed.workspace, "README.md"), "utf8")).toBe("initial\n");
    await expect(access(join(resumed.workspace, "scratch.txt"))).rejects.toThrow();

    await resumable.closeAll();
    await settle(scope);
  });
});

// ---------------------------------------------------------------------------
// Permission policy contract
// ---------------------------------------------------------------------------

describe("permission policy contract", () => {
  it("defaults to deny and threads the explicit policy into the provider launch", async () => {
    const scope = await harness();
    await startSession(scope.manager);
    expect(scope.factory.created[0].launch?.permission_policy).toBe("deny");
    await settle(scope);

    const interactive = await harness();
    await interactive.manager.start({ provider: "omp", permission_policy: "interactive" });
    expect(interactive.factory.created[0].launch?.permission_policy).toBe("interactive");
    await settle(interactive);
  });

  it("resume launches with the requested policy, defaulting to deny", async () => {
    const scope = await harness();
    const started = await startSession(scope.manager);
    await scope.manager.close(started.live_session_id);

    const factory = new FakeTransportFactory(
      () => new FakeTransport(fullCapabilities(), { resumeState: "echo" }),
    );
    const resumable = new LiveSessionManager({
      commonDir: scope.commonDir,
      repositoryCwd: scope.repository,
      transportFactories: [factory],
      providerFactories: [ompProviderFactory],
      tmpRoot: scope.tmpRoot,
      leaseProbes: fakeProbes(),
    });
    await resumable.resumeFromState({
      live_session_id: started.live_session_id,
      permission_policy: "interactive",
    });
    expect(factory.created[0].launch?.permission_policy).toBe("interactive");

    // Close, then resume again with no policy: the default re-applies deny.
    const closedAgain = await resumable.closeAll();
    expect(closedAgain[0]?.state.status).toBe("closed");
    await resumable.resumeFromState({ live_session_id: started.live_session_id });
    expect(factory.created[1].launch?.permission_policy).toBe("deny");

    await resumable.closeAll();
    await settle(scope);
  });

  it("resume refreshes the capability snapshot: deny launch, interactive resume answers natively", async () => {
    const scope = await harness({
      transportOptions: { resumeState: "echo", permissionResponseFollowsPolicy: true },
    });
    // Deny launch: like hermes-acp under deny, the fake withholds an answer
    // path it would only auto-deny — the recorded claim is unsupported.
    const started = await scope.manager.start({ provider: "omp" });
    expect(started.capabilities.permission_response).toEqual({ support: "unsupported", evidence: null });
    await scope.manager.close(started.live_session_id);

    const resumed = await scope.manager.resumeFromState({
      live_session_id: started.live_session_id,
      permission_policy: "interactive",
    });
    // The resumed session describes what THIS launch actually does.
    expect(resumed.capabilities.permission_response).toEqual({
      support: "native",
      evidence: "fake policy-scoped answer path",
    });
    expect(resumed.state.capabilities.permission_response.support).toBe("native");
    const durable = await durableState(scope.commonDir, started.live_session_id);
    expect(durable.capabilities.permission_response.support).toBe("native");

    // The consequence the stale snapshot would have buried: the hub now
    // accepts and forwards answers instead of refusing a live path.
    const t = scope.factory.created[1];
    t.push({
      kind: "permission_request",
      request_id: "req-r1",
      tool: "write",
      summary: { text: "overwrite README", truncated: false },
    });
    await tick();
    const answered = await scope.manager.respondPermission(
      started.live_session_id,
      "req-r1",
      "allow_once",
      null,
    );
    expect(answered.outcome).toBe("succeeded");
    expect(
      t.commands.some((c) => c.kind === "permission_response" && c.decision === "allow_once"),
    ).toBe(true);
    await settle(scope);
  });

  it("resume refreshes the capability snapshot: interactive launch, deny resume refuses the dead path", async () => {
    const scope = await harness({
      transportOptions: { resumeState: "echo", permissionResponseFollowsPolicy: true },
    });
    const started = await scope.manager.start({ provider: "omp", permission_policy: "interactive" });
    expect(started.capabilities.permission_response.support).toBe("native");
    await scope.manager.close(started.live_session_id);

    // Resume with no policy: the default deny means no answer path exists.
    const resumed = await scope.manager.resumeFromState({
      live_session_id: started.live_session_id,
    });
    expect(resumed.capabilities.permission_response).toEqual({ support: "unsupported", evidence: null });
    expect(resumed.state.capabilities.permission_response.support).toBe("unsupported");
    const durable = await durableState(scope.commonDir, started.live_session_id);
    expect(durable.capabilities.permission_response.support).toBe("unsupported");

    // A hub still believing the native claim would answer into a session
    // that auto-denies; the refreshed snapshot refuses pre-dispatch.
    const refused = await scope.manager.respondPermission(
      started.live_session_id,
      "req-r2",
      "allow_once",
      null,
    );
    expect(refused.outcome).toBe("unsupported");
    expect(refused.error?.code).toBe("LIVE_CAPABILITY_UNSUPPORTED");
    expect(scope.factory.created[1].commands).toHaveLength(0);
    await settle(scope);
  });

  it("keeps capability truth representable under deny: claims pass through untouched", async () => {
    const scope = await harness();
    const started = await scope.manager.start({ provider: "omp", permission_policy: "deny" });
    // Deny mode may not rewrite the evidence-backed claim the transport made.
    expect(started.capabilities.permission_response).toEqual({
      support: "native",
      evidence: "fake rpc accepted",
    });
    const t = scope.factory.created[0];
    t.push({
      kind: "permission_request",
      request_id: "req-1",
      tool: "write",
      summary: { text: "overwrite README", truncated: false },
    });
    await tick();
    const answered = await scope.manager.respondPermission(
      started.live_session_id,
      "req-1",
      "deny",
      null,
    );
    expect(answered.outcome).toBe("succeeded");
    expect(
      t.commands.some((c) => c.kind === "permission_response" && c.decision === "deny"),
    ).toBe(true);
    await settle(scope);
  });
});
