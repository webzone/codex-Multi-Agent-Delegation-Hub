import { Buffer } from "node:buffer";
import { mkdtemp, readdir, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { AgentHubError } from "../src/errors.js";
import { resolveRepositoryIdentity } from "../src/git.js";
import { encodeJsonlFrame, LiveJsonlFramer } from "../src/live/jsonl.js";
import { launchLiveChild } from "../src/live/child-process.js";
import {
  LiveSessionManager,
  LIVE_COMMON_DIR_SESSION_QUOTA,
  LIVE_DEFAULT_MAX_TEXT_BYTES,
  LIVE_FOLLOW_UP_MAX_MESSAGE_BYTES,
  LIVE_FOLLOW_UP_QUEUE_MAX_BYTES,
  LIVE_FOLLOW_UP_QUEUE_MAX_MESSAGES,
  LIVE_PROCESS_SESSION_QUOTA,
  type LiveManagerPhase,
} from "../src/live/manager.js";
import type { LiveLeaseProbes } from "../src/live/lease.js";
import type {
  LiveCapabilities,
  LiveCommand,
  LiveEvent,
  LiveEventBody,
  LiveLaunchRequest,
  LiveLaunchReport,
  LiveProbeResult,
  LiveProviderFactory,
  LiveStopMode,
  LiveStopReport,
  LiveTransport,
  LiveTransportFactory,
  LiveTurnResult,
} from "../src/live/types.js";
import { createGitRepository, removeDirectory, resolveRef, runGit } from "./helpers.js";

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
  private queue: LiveEventBody[] = [];
  private wake: (() => void) | null = null;
  private ended = false;

  constructor(
    private readonly caps: LiveCapabilities = fullCapabilities(),
    private readonly opts: {
      pid?: number | null;
      providerSessionId?: string | null;
      onSend?: (command: LiveCommand, transport: FakeTransport) => void;
    } = {},
  ) {}

  async describe(): Promise<{ transport: "omp-rpc"; provider: "omp"; capabilities: LiveCapabilities }> {
    return { transport: this.id, provider: this.provider, capabilities: this.caps };
  }

  async open(request: LiveLaunchRequest): Promise<LiveLaunchReport> {
    this.launch = request;
    return {
      pid: this.opts.pid === undefined ? 424_242 : this.opts.pid,
      provider_session_id: this.opts.providerSessionId ?? "prov-1",
      launched_at: "2026-09-05T00:00:00.000Z",
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
    const report = this.stopResults.shift() ?? {
      status: "closed" as const,
      exit_code: 0,
      exit_signal: null,
      waited_ms: 1,
    };
    this.endStream();
    return report;
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

function fakeProbes(config: {
  alive?: (pid: number) => boolean;
  startToken?: (pid: number) => string;
  kills?: KillRecord[];
} = {}): LiveLeaseProbes {
  const kills = config.kills ?? [];
  return {
    probePid: (pid) => ((config.alive ?? ((p: number) => p === process.pid))(pid) ? "live" : "dead"),
    startToken: async (pid) =>
      (config.startToken ?? ((p) => (p === process.pid ? "hub-tok" : "prov-tok")))(pid),
    killGroup: (pgid, signal) => {
      kills.push({ target: pgid, signal });
      return true;
    },
    now: () => new Date(),
  };
}

interface HarnessConfig {
  caps?: LiveCapabilities;
  transportOptions?: ConstructorParameters<typeof FakeTransport>[1];
  processQuota?: number;
  commonDirQuota?: number;
  leaseProbes?: LiveLeaseProbes;
  phases?: LiveManagerPhase[];
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
  const manager = new LiveSessionManager({
    commonDir: identity.common_dir,
    repositoryCwd: repository,
    transportFactories: [factory],
    providerFactories: [ompProviderFactory],
    processQuota: config.processQuota,
    commonDirQuota: config.commonDirQuota,
    tmpRoot,
    leaseProbes: config.leaseProbes ?? fakeProbes(),
    observePhase: phases
      ? async (phase) => {
          phases.push(phase);
        }
      : undefined,
  });
  return { repository, commonDir: identity.common_dir, tmpRoot, manager, factory };
}

function secondHub(
  source: Harness,
  probes: LiveLeaseProbes,
  phases?: LiveManagerPhase[],
): LiveSessionManager {
  return new LiveSessionManager({
    commonDir: source.commonDir,
    repositoryCwd: source.repository,
    transportFactories: [new FakeTransportFactory(() => new FakeTransport())],
    providerFactories: [ompProviderFactory],
    leaseProbes: probes,
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
  return JSON.parse(
    await readFile(join(commonDir, "agent-hub", "live", `${liveSessionId}.json`), "utf8"),
  ) as Record<string, never>;
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
    expect((await durableState(scope.commonDir, live_session_id)).status).toBe("closed");

    // reap → checkpoint → state ordering, and no sidecars left behind.
    expect(phases).toEqual(["transport-stopped", "checkpoint-captured", "state-advanced"]);
    const liveDir = await readdir(join(scope.commonDir, "agent-hub", "live"));
    expect(liveDir.filter((name) => name.endsWith(".pending.json"))).toEqual([]);

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
    expect(JSON.stringify(await durableState(scope.commonDir, live_session_id))).not.toContain("build the thing");

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

  it("pins nothing and reports orphaned when shutdown cannot prove the exit", async () => {
    const scope = await harness();
    const { live_session_id } = await startSession(scope.manager);
    const t = scope.factory.created[0];
    t.stopResults = [
      { status: "orphaned", exit_code: null, exit_signal: null, waited_ms: 5 },
      { status: "orphaned", exit_code: null, exit_signal: null, waited_ms: 10 },
    ];

    const closed = await scope.manager.close(live_session_id);
    expect(closed.state.status).toBe("orphaned");
    expect(closed.state.last_error?.code).toBe("LIVE_STOP_UNPROVEN");
    expect(closed.checkpoint_taken).toBe(false);
    expect(t.stopCalls).toEqual(["graceful", "terminate"]);
    // Lease and worktree stay for recovery; the durable record says orphaned.
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

describe("follow-up queue", () => {
  it("queues while running, drains in order, and enforces every bound", async () => {
    const scope = await harness({ transportOptions: writingTransportOptions });
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

  it("bounds the queue by total bytes", async () => {
    const scope = await harness();
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

  async function stranded(probes: LiveLeaseProbes, phases?: LiveManagerPhase[]): Promise<Stranded> {
    const scope = await harness();
    const started = await startSession(scope.manager);
    // Provider-side work exists in the worktree after "hub death".
    await writeFile(join(started.workspace, "provider-work.txt"), "survived\n");
    return { ...scope, liveSessionId: started.live_session_id, second: secondHub(scope, probes, phases) };
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
});
