/**
 * Deterministic fake-wire tests for the OMP live RPC transport (verified
 * 18.1.13 RPC v2 dialect). The transport suites spawn no real provider
 * processes and use no wall-clock sleeps: vitest fake timers drive every
 * timeout, scripted frames arrive synchronously, and assertions wait on
 * observed conditions via `until`, never guessed delays. The probe suite at
 * the end runs only an injected fake version-reporting script through the
 * command seam — never a binary looked up from PATH.
 */
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { OmpRpcTransport } from "../src/live/transports/omp-rpc.js";
import { probeOmp } from "../src/live/probes/omp.js";
import { validateLiveCapabilities } from "../src/live/provider-registry.js";
import type { RpcWireExit, RpcWireHandle } from "../src/live/transports/rpc-base.js";
import type { LiveCommand, LiveEvent, LiveLaunchRequest } from "../src/live/types.js";
import { runLeaderFirstSurvivorScenario } from "./live-stop-authority.js";

// ---------------------------------------------------------------------------
// Fake wire
// ---------------------------------------------------------------------------

class FakeWire implements RpcWireHandle {
  readonly pid = 4242;
  readonly written: string[] = [];
  readonly signals: NodeJS.Signals[] = [];
  stdinEnded = false;

  private dataHandler: ((chunk: Buffer) => void) | null = null;
  private resolveExit!: (exit: RpcWireExit) => void;
  readonly exited = new Promise<RpcWireExit>((resolve) => {
    this.resolveExit = resolve;
  });

  constructor(private readonly diesOnSignal = true) {}

  onData(next: (chunk: Buffer) => void): void {
    this.dataHandler = next;
  }
  write(line: string): void {
    this.written.push(line);
  }
  endStdin(): void {
    this.stdinEnded = true;
  }
  signal(signal: NodeJS.Signals): void {
    this.signals.push(signal);
    if (this.diesOnSignal) {
      this.resolveExit({ exitCode: null, exitSignal: signal });
    }
  }
  exit(code: number, signalName: NodeJS.Signals | null = null): void {
    this.resolveExit({ exitCode: code, exitSignal: signalName });
  }

  frames(): Record<string, unknown>[] {
    return this.written.map((line) => JSON.parse(line) as Record<string, unknown>);
  }
  lastFrame(): Record<string, unknown> {
    const frames = this.written.map((line) => JSON.parse(line) as Record<string, unknown>);
    const frame = frames[frames.length - 1];
    return frame ?? {};
  }
  pushFrame(frame: Record<string, unknown>): void {
    this.pushBytes(Buffer.from(`${JSON.stringify(frame)}\n`, "utf8"));
  }
  pushBytes(bytes: Buffer): void {
    this.dataHandler?.(bytes);
  }
}

/**
 * Group-owned wire for the shared authorization scenario: the leader exits
 * via `exit()`, a helper keeps the owned PGID alive, and only SIGKILL takes
 * the group down (the helper shrugs TERM off, like a trapped process).
 */
class GroupSurvivorWire extends FakeWire {
  groupAlive = true;

  constructor() {
    super(false);
  }

  override signal(signal: NodeJS.Signals): void {
    this.signals.push(signal);
    if (signal === "SIGKILL") {
      this.groupAlive = false;
    }
  }

  async proveGroupGone(timeoutMs: number): Promise<boolean> {
    let left = Math.max(0, timeoutMs);
    for (;;) {
      if (!this.groupAlive) {
        return true;
      }
      if (left <= 0) {
        return false;
      }
      const step = Math.min(5, left);
      await new Promise<void>((resolve) => setTimeout(resolve, step));
      left -= step;
    }
  }
}

/** Consumes the transport's pump into an array; assertions poll that array. */
class Pump {
  readonly got: LiveEvent[] = [];
  ended = false;
  failure: unknown = null;

  constructor(transport: OmpRpcTransport) {
    void (async () => {
      try {
        for await (const event of transport.events()) {
          this.got.push(event);
        }
        this.ended = true;
      } catch (error) {
        this.failure = error;
      }
    })();
  }

  async have(count: number): Promise<void> {
    await until(() => {
      if (this.failure) {
        throw this.failure;
      }
      if (this.got.length < count) {
        throw new Error(`expected ≥${count} events, saw ${this.got.length}: ${this.got.map((e) => e.body.kind).join(",")}`);
      }
    });
  }

  async quiet(): Promise<void> {
    const before = this.got.length;
    await vi.advanceTimersByTimeAsync(300);
    expect(this.got).toHaveLength(before);
  }

  async closed(): Promise<void> {
    await until(() => {
      if (this.failure) {
        throw this.failure;
      }
      if (!this.ended) {
        throw new Error("pump not closed");
      }
    });
  }
}

// ---------------------------------------------------------------------------
// Deterministic time control
// ---------------------------------------------------------------------------

/** Flushes pending microtasks/timers; every frame handshake uses this. */
async function flush(): Promise<void> {
  await vi.advanceTimersByTimeAsync(0);
}

/** Advances fake time in small steps until the condition stops throwing. */
async function until(condition: () => void): Promise<void> {
  let lastError: unknown = new Error("condition never met");
  for (let step = 0; step < 60; step += 1) {
    await vi.advanceTimersByTimeAsync(10);
    try {
      condition();
      return;
    } catch (error) {
      lastError = error;
    }
  }
  throw lastError;
}

// ---------------------------------------------------------------------------
// Scripted startup for the verified 18.1.13 RPC v2 dialect
// ---------------------------------------------------------------------------

const READY_FRAME = {
  type: "ready",
  protocolVersion: 1,
  supportedProtocolVersions: [1, 2],
  maxFrameBytes: 1048576,
  maxReassembledFrameBytes: 67108864,
};

const FAST = {
  readyTimeoutMs: 60,
  handshakeTimeoutMs: 120,
  commandTimeoutMs: 500,
  shutdownGraceMs: 40,
};

function launch(overrides: Partial<LiveLaunchRequest> = {}): LiveLaunchRequest {
  return {
    live_session_id: "live-omp-1",
    workspace: "/tmp/hub-workspace",
    max_text_bytes: 256,
    resume: null,
    ...overrides,
  };
}

function command(kind: string, extra: Record<string, unknown> = {}): LiveCommand {
  return {
    command_id: `cmd-${kind}`,
    live_session_id: "live-omp-1",
    issued_at: new Date(0).toISOString(),
    kind,
    ...extra,
  } as LiveCommand;
}

function setup(...wireArgs: ConstructorParameters<typeof FakeWire>) {
  const wire = new FakeWire(...wireArgs);
  const calls: { argv: readonly string[]; cwd: string }[] = [];
  const transport = new OmpRpcTransport({
    ...FAST,
    spawner: async (argv, cwd) => {
      calls.push({ argv, cwd });
      return wire;
    },
  });
  return { transport, wire, calls };
}

/** Startup per the verified dialect: unsolicited frames, ready, v2, then get_state. */
async function openStarted(
  transport: OmpRpcTransport,
  wire: FakeWire,
  request = launch(),
  stateData: Record<string, unknown> = { sessionFile: "/sessions/a.jsonl", sessionId: "s-a" },
) {
  const opening = transport.open(request);
  await flush();
  wire.pushFrame({ type: "available_commands_update", commands: [{ name: "security" }] });
  wire.pushFrame(READY_FRAME);
  await completeV2(wire, stateData);
  return await opening;
}

async function completeV2(wire: FakeWire, stateData: Record<string, unknown>): Promise<void> {
  await until(() => {
    if (!wire.written.some((line) => line.includes('"type":"negotiate_protocol"'))) {
      throw new Error("negotiate_protocol frame not written yet");
    }
  });
  answer(wire, lastCommand(wire, "negotiate_protocol"), { protocolVersion: 2 });
  await until(() => {
    if (!wire.written.some((line) => line.includes('"type":"get_state"'))) {
      throw new Error("get_state frame not written yet");
    }
  });
  answer(wire, lastCommand(wire, "get_state"), stateData);
}

function answer(wire: FakeWire, frame: Record<string, unknown>, data?: Record<string, unknown>): void {
  const response: Record<string, unknown> = {
    id: frame.id,
    type: "response",
    command: frame.type,
    success: true,
  };
  if (data) {
    response.data = data;
  }
  wire.pushFrame(response);
}

function lastCommand(wire: FakeWire, type: string): Record<string, unknown> {
  const frames = wire.frames().filter((frame) => frame.type === type);
  const frame = frames[frames.length - 1];
  if (!frame) {
    throw new Error(`no ${type} command frame was written`);
  }
  return frame;
}

async function delivered(wire: FakeWire, type: string, sending: Promise<void>, data?: Record<string, unknown>): Promise<void> {
  await until(() => {
    if (!wire.written.some((line) => line.includes(`"type":"${type}"`))) {
      throw new Error(`${type} frame not written yet`);
    }
  });
  answer(wire, lastCommand(wire, type), data);
  await sending;
}

// ---------------------------------------------------------------------------

describe("omp-rpc transport (fake wire, OMP 18.1.13 RPC v2 dialect)", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("launches argv-array, negotiates RPC v2, and tolerates unsolicited startup frames", async () => {
    const { transport, wire, calls } = setup();
    const report = await openStarted(transport, wire);
    const pump = new Pump(transport);

    expect(calls).toHaveLength(1);
    expect(calls[0]?.argv).toEqual(["omp", "--mode", "rpc"]);
    expect(calls[0]?.argv).not.toContain("--no-session");
    expect(calls[0]?.cwd).toBe("/tmp/hub-workspace");
    expect(report.pid).toBe(4242);
    expect(report.provider_session_id).toBe("/sessions/a.jsonl");

    await pump.have(2);
    const [notice, idle] = pump.got;
    expect(notice?.body).toEqual({
      kind: "log",
      level: "info",
      text: { text: "provider_notice:available_commands_update", truncated: false },
    });
    expect(idle?.body).toMatchObject({ kind: "status", status: "idle" });
    expect([notice?.seq, idle?.seq]).toEqual([1, 2]);
    expect(idle?.transport).toBe("omp-rpc");
    expect(idle?.live_session_id).toBe("live-omp-1");
  });

  it("treats a ready frame outside the v2 bootstrap envelope as OMP_RPC_FRAME_INVALID", async () => {
    const { transport, wire } = setup();
    const opening = transport.open(launch());
    const rejected = expect(opening).rejects.toMatchObject({
      liveError: { code: "OMP_RPC_FRAME_INVALID", stage: "protocol", retryable: false },
    });
    await flush();
    wire.pushFrame({ type: "ready", protocolVersion: 2 });
    await rejected;
    const pump = new Pump(transport);
    await pump.have(2);
    expect(pump.got[0]?.body).toMatchObject({
      kind: "error",
      error: { code: "OMP_RPC_FRAME_INVALID", stage: "protocol" },
    });
    expect(pump.got[1]?.body).toMatchObject({ kind: "exit", intentional: true });
    await pump.closed();
    expect(wire.signals).toContain("SIGTERM");
  });

  it("fails closed when the ready frame does not advertise RPC v2", async () => {
    const { transport, wire } = setup();
    const opening = transport.open(launch());
    const rejected = expect(opening).rejects.toMatchObject({
      liveError: { code: "OMP_RPC_V2_REQUIRED", stage: "protocol", retryable: false },
    });
    await flush();
    wire.pushFrame({ ...READY_FRAME, supportedProtocolVersions: [1] });
    await rejected;
    expect(wire.frames().some((frame) => frame.type === "negotiate_protocol")).toBe(false);
  });

  it("fails closed when the provider rejects RPC v2 negotiation", async () => {
    const { transport, wire } = setup();
    const opening = transport.open(launch());
    const rejected = expect(opening).rejects.toMatchObject({
      liveError: { code: "OMP_RPC_V2_REQUIRED", stage: "protocol", retryable: false },
    });
    await flush();
    wire.pushFrame(READY_FRAME);
    await until(() => {
      if (!wire.written.some((line) => line.includes('"type":"negotiate_protocol"'))) {
        throw new Error("negotiate_protocol frame not written yet");
      }
    });
    const negotiation = lastCommand(wire, "negotiate_protocol");
    wire.pushFrame({ id: negotiation.id, type: "response", command: "negotiate_protocol", success: false, error: "unsupported" });
    await rejected;
  });

  it("times out the ready negotiation and proves the child gone before failing launch", async () => {
    const { transport, wire } = setup();
    const opening = transport.open(launch());
    const rejected = expect(opening).rejects.toMatchObject({
      liveError: { code: "LIVE_TRANSPORT_HANDSHAKE_TIMEOUT", stage: "launch", retryable: true },
    });
    await vi.advanceTimersByTimeAsync(80);
    await rejected;
    const pump = new Pump(transport);
    await pump.have(2);
    expect(pump.got[0]?.body).toMatchObject({ kind: "error", error: { code: "LIVE_TRANSPORT_HANDSHAKE_TIMEOUT" } });
    expect(pump.got[1]?.body).toMatchObject({ kind: "exit", intentional: true, exit_signal: "SIGTERM" });
    await pump.closed();
  });

  it("maps structurally unsupported stdout frames to PROVIDER_PROTOCOL_UNSUPPORTED and ends the pump", async () => {
    const { transport, wire } = setup();
    await openStarted(transport, wire);
    const pump = new Pump(transport);

    wire.pushBytes(Buffer.from("this line is not JSON\n", "utf8"));
    await pump.have(4);
    const [, , error, exit] = pump.got;
    expect(error?.body).toMatchObject({
      kind: "error",
      error: { code: "PROVIDER_PROTOCOL_UNSUPPORTED", stage: "protocol", provider: "omp", retryable: false },
    });
    expect(exit?.body).toMatchObject({ kind: "exit", intentional: true });
    await pump.closed();
    await expect(transport.send(command("prompt", { text: "x" }))).rejects.toMatchObject({
      liveError: { code: "LIVE_SESSION_CLOSED" },
    });
  });

  it("prompts, keeps running through isTerminal:false, and settles on terminal agent_end", async () => {
    const { transport, wire } = setup();
    await openStarted(transport, wire);
    const pump = new Pump(transport);

    const sending = transport.send(command("prompt", { text: "do the thing" }));
    await delivered(wire, "prompt", sending);
    const promptFrame = lastCommand(wire, "prompt");
    expect(promptFrame).toMatchObject({ type: "prompt", message: "do the thing" });
    expect(typeof promptFrame.id).toBe("string");

    wire.pushFrame({ type: "agent_start" });
    await pump.have(3);
    expect(pump.got.at(-1)?.body).toMatchObject({ kind: "status", status: "running" });

    const steer = transport.send(command("steer", { text: "left" }));
    await delivered(wire, "steer", steer);
    expect(lastCommand(wire, "steer")).toMatchObject({ type: "steer", message: "left" });

    // A non-terminal agent_end must not report idle.
    wire.pushFrame({ type: "agent_end", messages: [], isTerminal: false });
    await pump.quiet();

    // Mid-turn steer proves we stayed running.
    const steer2 = transport.send(command("steer", { text: "right" }));
    await delivered(wire, "steer", steer2);

    wire.pushFrame({ type: "agent_end", messages: [] });
    await pump.have(pump.got.length + 1);
    expect(pump.got.at(-1)?.body).toMatchObject({ kind: "status", status: "idle" });
  });

  it("surfaces a local-only prompt completion instead of waiting for lifecycle events", async () => {
    const { transport, wire } = setup();
    await openStarted(transport, wire);
    const pump = new Pump(transport);

    const sending = transport.send(command("prompt", { text: "/some-local-command" }));
    await delivered(wire, "prompt", sending, { agentInvoked: false });

    await pump.have(3);
    expect(pump.got.at(-1)?.body).toMatchObject({
      kind: "status",
      status: "idle",
      note: expect.stringContaining("agentInvoked:false"),
    });
  });

  it("rejects idle steer with LIVE_NOT_RUNNING without writing any frame", async () => {
    const { transport, wire } = setup();
    await openStarted(transport, wire);
    const framesBefore = wire.written.length;

    await expect(transport.send(command("steer", { text: "too early" }))).rejects.toMatchObject({
      liveError: { code: "LIVE_NOT_RUNNING", stage: "state" },
    });
    expect(wire.written).toHaveLength(framesBefore);
    expect(wire.frames().some((frame) => frame.type === "steer")).toBe(false);
  });

  it("queues follow_up with a same-id ack", async () => {
    const { transport, wire } = setup();
    await openStarted(transport, wire);

    const sending = transport.send(command("follow_up", { text: "next please" }));
    await delivered(wire, "follow_up", sending);
    expect(lastCommand(wire, "follow_up")).toMatchObject({ type: "follow_up", message: "next please" });
  });

  it("cancels via abort: cancelling status, then idle once the provider answered", async () => {
    const { transport, wire } = setup();
    await openStarted(transport, wire);
    const pump = new Pump(transport);

    wire.pushFrame({ type: "agent_start" });
    await pump.have(3);

    const cancelling = transport.send(command("cancel", { reason: "user esc" }));
    await until(() => {
      if (!wire.written.some((line) => line.includes('"type":"abort"'))) {
        throw new Error("abort not written yet");
      }
    });
    await pump.have(4);
    expect(pump.got.at(-1)?.body).toMatchObject({ kind: "status", status: "cancelling", note: "user esc" });

    answer(wire, lastCommand(wire, "abort"));
    await cancelling;
    await pump.have(5);
    expect(pump.got.at(-1)?.body).toMatchObject({ kind: "status", status: "idle" });
  });

  it("answers status commands from get_state", async () => {
    const { transport, wire } = setup();
    await openStarted(transport, wire);
    const pump = new Pump(transport);

    const asking = transport.send(command("status"));
    await delivered(wire, "get_state", asking, { isStreaming: true, isCompacting: false });
    await pump.have(3);
    expect(pump.got.at(-1)?.body).toMatchObject({ kind: "status", status: "running" });

    const askingIdle = transport.send(command("status"));
    await delivered(wire, "get_state", askingIdle, { isStreaming: false, isCompacting: false });
    await pump.have(4);
    expect(pump.got.at(-1)?.body).toMatchObject({ kind: "status", status: "idle" });
  });

  it("tolerates unknown semantic frames and provider notices without leaking payloads", async () => {
    const { transport, wire } = setup();
    await openStarted(transport, wire);
    const pump = new Pump(transport);

    wire.pushFrame({ type: "quantum_flux", secretPayload: "raw provider content" });
    wire.pushFrame({ type: "extension_ui_request", id: "u1", method: "setWidget", secretPayload: "raw" });
    wire.pushFrame({ type: "notice", text: "advisor stepped in" });
    await pump.have(5);
    const [unknown, ui, notice] = pump.got.slice(2);
    expect(unknown?.body).toMatchObject({ kind: "log", level: "info", text: { text: "provider_notice:quantum_flux" } });
    expect(ui?.body).toMatchObject({ kind: "log", level: "info", text: { text: "provider_notice:extension_ui_request setWidget" } });
    expect(notice?.body).toMatchObject({ kind: "log", level: "info", text: { text: "advisor stepped in" } });
    expect(JSON.stringify([unknown, ui, notice])).not.toContain("raw provider content");
  });

  it("maps message and tool frames into bounded normalized events with stable stream ids", async () => {
    const { transport, wire } = setup();
    await openStarted(transport, wire);
    const pump = new Pump(transport);

    wire.pushFrame({ type: "message_start", message: { role: "assistant", content: [] } });
    wire.pushFrame({ type: "message_update", assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "Hel" } });
    wire.pushFrame({ type: "message_update", assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "lo 🙂" } });
    wire.pushBytes(Buffer.from('{"type":"message_update","assistantMessageEvent":{"type":"text_end","contentIndex":0}}\r\n', "utf8"));
    wire.pushFrame({ type: "tool_execution_start", toolCallId: "call-1", toolName: "bash", args: { command: "ls -la" } });
    wire.pushFrame({
      type: "tool_execution_end",
      toolCallId: "call-1",
      toolName: "bash",
      isError: false,
      result: { content: [{ type: "text", text: "total 48" }] },
    });
    wire.pushFrame({ type: "message_end", message: { role: "assistant", content: [] } });

    await pump.have(7);
    const mapped = pump.got.slice(2);
    expect(mapped[0]?.body).toMatchObject({ kind: "text", role: "assistant", stream_id: "m1:t0", text: { text: "Hel" }, final: false });
    expect(mapped[1]?.body).toMatchObject({ kind: "text", stream_id: "m1:t0", text: { text: "lo 🙂" }, final: false });
    expect(mapped[2]?.body).toMatchObject({ kind: "text", stream_id: "m1:t0", final: true });
    expect(mapped[3]?.body).toMatchObject({
      kind: "tool_start",
      call_id: "call-1",
      tool: "bash",
      input_preview: { text: '{"command":"ls -la"}' },
    });
    expect(mapped[4]?.body).toMatchObject({ kind: "tool_end", call_id: "call-1", ok: true, output_preview: { text: "total 48" } });
    expect(new Set(mapped.map((event) => event.seq)).size).toBe(5);
  });

  it("truncates provider text to the session byte bound", async () => {
    const { transport, wire } = setup();
    await openStarted(transport, wire, launch({ max_text_bytes: 8 }));
    const pump = new Pump(transport);

    wire.pushFrame({ type: "message_start", message: { role: "assistant", content: [] } });
    wire.pushFrame({ type: "message_update", assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "0123456789" } });
    await pump.have(3);
    expect(pump.got.at(-1)?.body).toMatchObject({ kind: "text", text: { text: "01234567", truncated: true } });
  });

  it("verifies resume over switch_session plus a get_state locator echo", async () => {
    const { transport, wire } = setup();
    const opening = transport.open(
      launch({
        resume: {
          provider: "omp",
          provider_session_id: "/sessions/target.jsonl",
          verified: false,
          verified_via: null,
          last_event_seq: 7,
        },
      }),
    );
    await flush();
    wire.pushFrame(READY_FRAME);
    await completeV2(wire, { sessionFile: "/sessions/a.jsonl", sessionId: "s-a" });
    await until(() => {
      if (!wire.written.some((line) => line.includes('"type":"switch_session"'))) {
        throw new Error("switch_session not written yet");
      }
    });
    expect(lastCommand(wire, "switch_session")).toMatchObject({ type: "switch_session", sessionPath: "/sessions/target.jsonl" });
    answer(wire, lastCommand(wire, "switch_session"), { cancelled: false });
    await until(() => {
      const count = wire.frames().filter((frame) => frame.type === "get_state").length;
      if (count < 2) {
        throw new Error("resume-verify get_state not written yet");
      }
    });
    const verifyFrames = wire.frames().filter((frame) => frame.type === "get_state");
    answer(wire, verifyFrames[verifyFrames.length - 1], { sessionFile: "/sessions/target.jsonl", sessionId: "s-target" });

    const report = await opening;
    expect(report.provider_session_id).toBe("/sessions/target.jsonl");
    expect(transport.resumeVerification).toMatchObject({
      verified: true,
      verified_via: expect.stringContaining("switch_session"),
    });

    const pump = new Pump(transport);
    await pump.have(1);
    expect(pump.got[0]?.body).toMatchObject({ kind: "status", status: "idle" });
  });

  it("fails resume when switch_session succeeds but the state locator disagrees", async () => {
    const { transport, wire } = setup();
    const opening = transport.open(
      launch({
        resume: {
          provider: "omp",
          provider_session_id: "/sessions/target.jsonl",
          verified: false,
          verified_via: null,
          last_event_seq: 0,
        },
      }),
    );
    await flush();
    wire.pushFrame(READY_FRAME);
    await completeV2(wire, { sessionFile: "/sessions/a.jsonl" });
    await flush();
    answer(wire, lastCommand(wire, "switch_session"), { cancelled: false });
    await flush();
    const verifyFrames = wire.frames().filter((frame) => frame.type === "get_state");
    answer(wire, verifyFrames[verifyFrames.length - 1], { sessionFile: "/sessions/somebody-elses.jsonl" });

    await expect(opening).rejects.toMatchObject({ liveError: { code: "LIVE_RESUME_VERIFICATION_FAILED" } });
    expect(transport.resumeVerification).toBeNull();
    expect(wire.signals).toContain("SIGTERM");
  });

  it("refuses to silently start fresh when the resume hint carries no locator", async () => {
    const { transport, wire } = setup();
    const opening = transport.open(
      launch({
        resume: { provider: "omp", provider_session_id: null, verified: false, verified_via: null, last_event_seq: 3 },
      }),
    );
    await flush();
    wire.pushFrame(READY_FRAME);
    await completeV2(wire, { sessionFile: "/sessions/a.jsonl" });

    await expect(opening).rejects.toMatchObject({ liveError: { code: "LIVE_RESUME_VERIFICATION_FAILED", stage: "state" } });
  });

  it("rejects a resume hint belonging to another provider", async () => {
    const { transport, wire } = setup();
    const opening = transport.open(
      launch({
        resume: { provider: "pi", provider_session_id: "x", verified: false, verified_via: null, resume_token: "x" },
      }),
    );
    await flush();
    wire.pushFrame(READY_FRAME);
    await completeV2(wire, { sessionFile: "/sessions/a.jsonl" });

    await expect(opening).rejects.toMatchObject({ liveError: { code: "LIVE_RESUME_VERIFICATION_FAILED" } });
  });

  it("closes gracefully: stdin close proves the exit and the pump drains", async () => {
    const { transport, wire } = setup();
    await openStarted(transport, wire);
    const pump = new Pump(transport);

    const stopping = transport.stop("graceful");
    await flush();
    expect(wire.stdinEnded).toBe(true);
    wire.exit(0);
    const report = await stopping;
    expect(report.status).toBe("closed");
    expect(report.exit_code).toBe(0);

    await pump.have(4);
    expect(pump.got.at(-2)?.body).toMatchObject({ kind: "status", status: "closing" });
    expect(pump.got.at(-1)?.body).toMatchObject({ kind: "exit", intentional: true, exit_code: 0 });
    await pump.closed();
  });

  it("reports orphaned, never closed, when a graceful stop cannot prove the exit", async () => {
    const { transport, wire } = setup(false);
    await openStarted(transport, wire);
    const pump = new Pump(transport);

    const stopping = transport.stop("graceful");
    await vi.advanceTimersByTimeAsync(100);
    const report = await stopping;
    expect(report.status).toBe("orphaned");
    expect(report.exit_code).toBeNull();
    expect(wire.stdinEnded).toBe(true);
    expect(wire.signals).toContain("SIGTERM");
    // A graceful stop is never authorized to escalate to SIGKILL.
    expect(wire.signals).not.toContain("SIGKILL");
    // The pump stays open: an orphan is never reported as reaped.
    expect(pump.got.some((event) => event.body.kind === "exit")).toBe(false);
  });

  it("a terminate after an orphaned graceful escalates for real, never reusing the old report", async () => {
    const { transport, wire } = setup(false);
    await openStarted(transport, wire);
    new Pump(transport);

    const stopping = transport.stop("graceful");
    await vi.advanceTimersByTimeAsync(100);
    const first = await stopping;
    expect(first.status).toBe("orphaned");
    expect(wire.signals).not.toContain("SIGKILL");

    // The hub now holds a terminate authorization. Cached graceful authority
    // may not answer for it: the transport must actually run the escalation.
    const escalating = transport.stop("terminate");
    await vi.advanceTimersByTimeAsync(90);
    expect(wire.signals.filter((s) => s === "SIGKILL").length).toBeGreaterThanOrEqual(1);
    wire.exit(137);
    const second = await escalating;
    expect(second.status).toBe("closed");
    expect(second.exit_code).toBe(137);
  });

  it("a second graceful never re-runs shutdown behind the caller's back", async () => {
    const { transport, wire } = setup(false);
    await openStarted(transport, wire);
    new Pump(transport);

    const first = transport.stop("graceful");
    await vi.advanceTimersByTimeAsync(100);
    expect((await first).status).toBe("orphaned");
    const signalsAfterFirst = [...wire.signals];
    const second = await transport.stop("graceful");
    expect(second).toEqual({ status: "orphaned", exit_code: null, exit_signal: null, waited_ms: expect.any(Number) });
    expect(wire.signals).toEqual(signalsAfterFirst);
  });

  it("a terminate after a settled orphaned terminate re-probes instead of replaying", async () => {
    const { transport, wire } = setup(false);
    await openStarted(transport, wire);
    new Pump(transport);

    const first = transport.stop("terminate");
    await vi.advanceTimersByTimeAsync(200);
    expect((await first).status).toBe("orphaned");

    // The provider died on its own between attempts; a fresh terminate is
    // new authorization to observe that, not a replay of the old window.
    const retry = transport.stop("terminate");
    await vi.advanceTimersByTimeAsync(5);
    wire.exit(137);
    const second = await retry;
    expect(second.status).toBe("closed");
    expect(second.exit_code).toBe(137);
  });

  it("leader exits first, helper survives: graceful proves-only; terminate escalates the owned PGID", async () => {
    const wire = new GroupSurvivorWire();
    const transport = new OmpRpcTransport({
      ...FAST,
      spawner: async () => wire,
    });
    await openStarted(transport, wire);
    new Pump(transport);

    // The leader exits FIRST, before any shutdown is requested; the helper
    // keeps the owned group alive and ignores everything but KILL.
    wire.exit(0);
    await flush();

    await runLeaderFirstSurvivorScenario({
      stop: async (mode) => {
        const stopping = transport.stop(mode);
        await vi.advanceTimersByTimeAsync(500);
        return stopping;
      },
      survivorAlive: () => wire.groupAlive,
      signals: () => wire.signals,
    });
  });

  it("kills the session when a live response id answers a different command", async () => {
    const { transport, wire } = setup();
    await openStarted(transport, wire);
    const pump = new Pump(transport);
    await pump.have(2);

    const asking = transport.send(command("status"));
    const askingFate = expect(asking).rejects.toMatchObject({ liveError: { code: "LIVE_SESSION_CLOSED" } });
    await until(() => {
      if (!wire.written.some((line) => line.includes('"type":"get_state"'))) {
        throw new Error("get_state frame not written yet");
      }
    });
    const askingFrame = lastCommand(wire, "get_state");
    wire.pushFrame({ id: askingFrame.id, type: "response", command: "prompt", success: true });

    await pump.have(3);
    expect(pump.got[2]?.body).toMatchObject({
      kind: "error",
      error: { code: "PROVIDER_RESPONSE_MISCORRELATED", stage: "protocol" },
    });
    // The misdelivery resolves nothing: the status command fails closed when
    // the mandated shutdown takes the provider — never on the fake success.
    await vi.advanceTimersByTimeAsync(10);
    await askingFate;
    expect(wire.signals).toContain("SIGTERM");
    await pump.closed();
  });

  it("tolerates blank LF records as whitespace and fails closed on stray bytes", async () => {
    const { transport, wire } = setup();
    await openStarted(transport, wire);
    const pump = new Pump(transport);
    await pump.have(2);

    wire.pushBytes(Buffer.from("\n\n\r\n", "utf8"));
    await pump.quiet();

    // A whitespace-only record carries bytes claiming content: not JSON.
    wire.pushBytes(Buffer.from(" \n", "utf8"));
    await pump.have(3);
    expect(pump.got[2]?.body).toMatchObject({
      kind: "error",
      error: { code: "PROVIDER_PROTOCOL_UNSUPPORTED", stage: "protocol" },
    });
    await pump.closed();
  });

  it("refuses a ready frame advertising out-of-range v2 frame bounds", async () => {
    for (const bad of [
      { ...READY_FRAME, maxFrameBytes: 2097152 },
      { ...READY_FRAME, maxReassembledFrameBytes: 524288 },
      { ...READY_FRAME, maxFrameBytes: "1048576" },
    ]) {
      const { transport, wire } = setup();
      const opening = transport.open(launch());
      const rejected = expect(opening).rejects.toMatchObject({
        liveError: { code: "OMP_RPC_FRAME_INVALID", stage: "protocol" },
      });
      await flush();
      wire.pushFrame(bad);
      await rejected;
      // No negotiation may be attempted with an unproven bounds envelope.
      expect(wire.written.some((line) => line.includes("negotiate_protocol"))).toBe(false);
    }
  });

  it("enforces the advertised physical cap on provider records, partial or framed", async () => {
    const smallReady = { ...READY_FRAME, maxFrameBytes: 512, maxReassembledFrameBytes: 65536 };
    const { transport, wire } = setup();
    const opening = transport.open(launch());
    await flush();
    wire.pushFrame(smallReady);
    await completeV2(wire, { sessionFile: "/sessions/a.jsonl", sessionId: "s-a" });
    await opening;
    const pump = new Pump(transport);
    await pump.have(1);

    wire.pushBytes(Buffer.from(`${"x".repeat(600)}\n`, "utf8"));
    await pump.have(2);
    expect(pump.got[1]?.body).toMatchObject({
      kind: "error",
      error: { code: "PROVIDER_PROTOCOL_UNSUPPORTED", message: expect.stringContaining("frame size guard") },
    });
    await pump.closed();
  });

  it("splits an oversized outgoing command into ordered bounded rpc_chunks under the advertised cap", async () => {
    const smallReady = { ...READY_FRAME, maxFrameBytes: 512, maxReassembledFrameBytes: 65536 };
    const { transport, wire } = setup();
    const opening = transport.open(launch());
    await flush();
    wire.pushFrame(smallReady);
    await completeV2(wire, { sessionFile: "/sessions/a.jsonl", sessionId: "s-a" });
    await opening;

    const sending = transport.send(command("follow_up", { text: "x".repeat(2000) }));
    await until(() => {
      if (!wire.written.some((line) => line.includes('"type":"rpc_chunk"'))) {
        throw new Error("no rpc_chunk frames written yet");
      }
    });
    const chunkLines = wire.written.filter((line) => line.includes("rpc_chunk"));
    expect(chunkLines.length).toBeGreaterThan(2);
    for (const line of chunkLines) {
      expect(Buffer.byteLength(line, "utf8")).toBeLessThanOrEqual(512);
    }
    const chunks = chunkLines.map((line) => JSON.parse(line) as Record<string, unknown>);
    expect(chunks.map((c) => c.index)).toEqual(chunks.map((_, i) => i));
    expect(new Set(chunks.map((c) => c.chunkId)).size).toBe(1);
    const joined = Buffer.concat(chunks.map((c) => Buffer.from(c.data as string, "base64")));
    const reassembled = JSON.parse(joined.toString("utf8")) as Record<string, unknown>;
    expect(reassembled).toMatchObject({ type: "follow_up", message: "x".repeat(2000) });

    // The provider answers the LOGICAL frame; the hub correlates it by id.
    answer(wire, { id: reassembled.id, type: "follow_up" });
    await sending;
  });

  it("records process ownership before the handshake and refuses launch when recording fails", async () => {
    const facts: { pid: number; pgid: number }[] = [];
    const { transport, wire } = setup();
    const opening = transport.open(
      launch({
        report_process: async (f) => {
          facts.push(f);
        },
      }),
    );
    await flush();
    // Recorded after spawn, before any ready frame was even offered: an
    // ownership record can never be raced by a handshake failure.
    expect(facts).toEqual([{ pid: 4242, pgid: 4242 }]);
    wire.pushFrame(READY_FRAME);
    await completeV2(wire, { sessionFile: "/sessions/a.jsonl", sessionId: "s-a" });
    await opening;

    const failing = setup();
    const rejected = expect(
      failing.transport.open(
        launch({
          report_process: async () => {
            throw new Error("lease recording unavailable");
          },
        }),
      ),
    ).rejects.toMatchObject({
      liveError: { code: "LIVE_OWNERSHIP_RECORDING_FAILED", stage: "launch", retryable: false },
    });
    await vi.advanceTimersByTimeAsync(10);
    await rejected;
    // The spawn happened, so a rejected launch must prove the child dead.
    expect(failing.wire.stdinEnded).toBe(true);
    expect(failing.wire.signals).toContain("SIGTERM");
  });

  it("describes the full RPC v2 capability snapshot under the evidence rules", async () => {
    const { transport } = setup();
    const descriptor = await transport.describe();
    expect(descriptor.transport).toBe("omp-rpc");
    expect(descriptor.provider).toBe("omp");
    expect(Object.keys(descriptor.capabilities).sort()).toEqual([
      "cancel",
      "checkpoint",
      "follow_up",
      "permission_response",
      "prompt",
      "resume",
      "status",
      "steer",
      "usage_reporting",
    ]);
    for (const claim of Object.values(descriptor.capabilities)) {
      if (claim.support === "unsupported") {
        expect(claim.evidence).toBeNull();
      } else {
        expect(claim.evidence.trim().length).toBeGreaterThan(0);
        expect(claim.evidence).toContain("18.1.13");
      }
    }
    expect(descriptor.capabilities.permission_response).toEqual({ support: "unsupported", evidence: null });
    expect(descriptor.capabilities.checkpoint).toEqual({ support: "unsupported", evidence: null });
    expect(descriptor.capabilities.usage_reporting).toEqual({ support: "unsupported", evidence: null });
    expect(() => validateLiveCapabilities(descriptor.capabilities)).not.toThrow();
  });
});

describe("omp probe (hermetic command seam)", () => {
  it("reads the version from an injected command and reports absence honestly", async () => {
    const dir = await mkdtemp(join(tmpdir(), "agent-hub-omp-probe-"));
    try {
      const bin = join(dir, "omp");
      await writeFile(
        bin,
        '#!/usr/bin/env node\nif (process.argv.includes("--version")) { console.log("omp/18.1.13"); process.exit(0); }\nprocess.exit(1);\n',
      );
      await chmod(bin, 0o755);
      const found = await probeOmp({ command: bin, environment: process.env });
      expect(found).toEqual({
        found: true,
        version: "18.1.13",
        detail: "live RPC dialect verified against omp 18.1.13",
      });

      const missing = await probeOmp({ command: "definitely-not-installed-omp-xyz", environment: process.env });
      expect(missing.found).toBe(false);
      expect(missing.version).toBeNull();

      // The env knob resolves the same way the production factory sees it.
      const viaEnv = await probeOmp({ environment: { ...process.env, AGENT_HUB_OMP_BIN: bin } });
      expect(viaEnv.found).toBe(true);
      expect(viaEnv.version).toBe("18.1.13");
    } finally {
      await rm(dir, { recursive: true, force: true });
    }
  });
});
