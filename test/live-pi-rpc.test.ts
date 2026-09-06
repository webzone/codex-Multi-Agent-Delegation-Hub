/**
 * Deterministic fake-wire tests for the PI live RPC transport (verified
 * 0.85.0 dialect). No real processes and no wall-clock sleeps: vitest fake
 * timers drive every timeout, scripted frames arrive synchronously, and
 * assertions wait on observed conditions via `until`, never guessed delays.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { PiRpcTransport } from "../src/live/transports/pi-rpc.js";
import type { RpcWireExit, RpcWireHandle } from "../src/live/transports/rpc-base.js";
import type { LiveCommand, LiveEvent, LiveLaunchRequest } from "../src/live/types.js";
import { runLeaderFirstSurvivorScenario } from "./live-stop-authority.js";

// ---------------------------------------------------------------------------
// Fake wire (same seam as the OMP suite; each file owns its fake)
// ---------------------------------------------------------------------------

class FakeWire implements RpcWireHandle {
  readonly pid = 5150;
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
    const frames = this.frames();
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

class Pump {
  readonly got: LiveEvent[] = [];
  ended = false;
  failure: unknown = null;

  constructor(transport: PiRpcTransport) {
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
// Scripted startup: the 0.85.0 dialect has NO ready frame — the first written
// frame must be the optimistic correlated get_state, and the first stdout
// frame we choose to send back is its response.
// ---------------------------------------------------------------------------

const FAST = {
  handshakeTimeoutMs: 120,
  commandTimeoutMs: 500,
  shutdownGraceMs: 40,
};

function launch(overrides: Partial<LiveLaunchRequest> = {}): LiveLaunchRequest {
  return {
    live_session_id: "live-pi-1",
    workspace: "/tmp/hub-workspace",
    max_text_bytes: 256,
    resume: null,
    ...overrides,
  };
}

function command(kind: string, extra: Record<string, unknown> = {}): LiveCommand {
  return {
    command_id: `cmd-${kind}`,
    live_session_id: "live-pi-1",
    issued_at: new Date(0).toISOString(),
    kind,
    ...extra,
  } as LiveCommand;
}

function setup(...wireArgs: ConstructorParameters<typeof FakeWire>) {
  const wire = new FakeWire(...wireArgs);
  const calls: { argv: readonly string[]; cwd: string }[] = [];
  const transport = new PiRpcTransport({
    ...FAST,
    spawner: async (argv, cwd) => {
      calls.push({ argv, cwd });
      return wire;
    },
  });
  return { transport, wire, calls };
}

async function openStartedPi(
  transport: PiRpcTransport,
  wire: FakeWire,
  request = launch(),
  stateData: Record<string, unknown> = { sessionFile: "/pi/s.jsonl", sessionId: "pi-1" },
) {
  const opening = transport.open(request);
  await until(() => {
    if (wire.written.length === 0) {
      throw new Error("handshake get_state not written yet");
    }
  });
  answer(wire, wire.lastFrame(), stateData);
  return await opening;
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

describe("pi-rpc transport (fake wire, pi 0.85.0 dialect)", () => {
  beforeEach(() => {
    vi.useFakeTimers();
  });
  afterEach(() => {
    vi.useRealTimers();
  });

  it("starts optimistically with NO ready frame: first write is the correlated get_state", async () => {
    const { transport, wire, calls } = setup();
    const opening = transport.open(launch());
    await until(() => {
      if (wire.written.length === 0) {
        throw new Error("no handshake frame yet");
      }
    });

    expect(calls).toHaveLength(1);
    expect(calls[0]?.argv).toEqual(["pi", "--mode", "rpc"]);
    expect(calls[0]?.argv).not.toContain("--no-session");
    expect(calls[0]?.cwd).toBe("/tmp/hub-workspace");
    const handshake = wire.lastFrame();
    expect(handshake).toMatchObject({ type: "get_state" });
    expect(typeof handshake.id).toBe("string");

    // Events that beat the handshake response are processed, not discarded.
    wire.pushFrame({ type: "agent_start" });

    answer(wire, handshake, { sessionFile: "/pi/s.jsonl", sessionId: "pi-1" });
    const report = await opening;
    expect(report.pid).toBe(5150);
    expect(report.provider_session_id).toBe("/pi/s.jsonl");

    const pump = new Pump(transport);
    await pump.have(1);
    expect(pump.got[0]?.body).toMatchObject({ kind: "status", status: "running" });
    expect(pump.got[0]?.transport).toBe("pi-rpc");
    expect(pump.got[0]?.live_session_id).toBe("live-pi-1");
  });

  it("reaches idle after a quiet optimistic handshake", async () => {
    const { transport, wire } = setup();
    const report = await openStartedPi(transport, wire);
    const pump = new Pump(transport);

    expect(report.provider_session_id).toBe("/pi/s.jsonl");
    await pump.have(1);
    expect(pump.got[0]?.body).toMatchObject({ kind: "status", status: "idle" });
  });

  it("fails the optimistic handshake on timeout and proves the child gone", async () => {
    const { transport, wire } = setup();
    const opening = transport.open(launch());
    const rejected = expect(opening).rejects.toMatchObject({
      liveError: { code: "LIVE_TRANSPORT_HANDSHAKE_TIMEOUT", stage: "launch", retryable: true },
    });
    await vi.advanceTimersByTimeAsync(140);
    await rejected;

    const pump = new Pump(transport);
    await pump.have(2);
    expect(pump.got[0]?.body).toMatchObject({ kind: "error", error: { code: "LIVE_TRANSPORT_HANDSHAKE_TIMEOUT" } });
    expect(pump.got[1]?.body).toMatchObject({ kind: "exit", intentional: true, exit_signal: "SIGTERM" });
    await pump.closed();
    expect(wire.signals).toContain("SIGTERM");
  });

  it("parses bytes strictly on LF: mid-codepoint splits, U+2028 inside strings, and CRLF", async () => {
    const { transport, wire } = setup();
    await openStartedPi(transport, wire);
    const pump = new Pump(transport);

    const frames = [
      '{"type":"message_start","message":{"role":"assistant","content":[]}}\n',
      `${JSON.stringify({ type: "message_update", assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "ok 🙂" } })}\n`,
      // JSON.stringify leaves U+2028 raw inside the string — Node readline
      // would split this frame; the framer must not.
      `{"type":"message_update","assistantMessageEvent":{"type":"text_delta","contentIndex":0,"delta":"l\u2028r"}}\r\n`,
      '{"type":"message_update","assistantMessageEvent":{"type":"text_end","contentIndex":0}}\n',
    ].join("");
    const bytes = Buffer.from(frames, "utf8");
    for (let offset = 0; offset < bytes.length; offset += 3) {
      wire.pushBytes(bytes.subarray(offset, offset + 3));
    }

    await pump.have(4);
    const [, first, second, finalMarker] = pump.got;
    expect(first?.body).toMatchObject({ kind: "text", role: "assistant", stream_id: "m1:t0", text: { text: "ok 🙂" }, final: false });
    expect(second?.body).toMatchObject({ kind: "text", stream_id: "m1:t0", text: { text: "l\u2028r" }, final: false });
    expect(finalMarker?.body).toMatchObject({ kind: "text", stream_id: "m1:t0", final: true });
  });

  it("maps events, cumulative usage once, notices, and settles only on agent_settled", async () => {
    const { transport, wire } = setup();
    await openStartedPi(transport, wire);
    const pump = new Pump(transport);

    const usage = { input: 10, output: 4, cacheRead: 3, cacheWrite: 0, cost: { input: 0.01, output: 0.04, cacheRead: 0, cacheWrite: 0, total: 0.05 } };
    wire.pushFrame({ type: "agent_start" });
    wire.pushFrame({ type: "message_start", message: { role: "assistant", content: [] } });
    wire.pushFrame({ type: "message_update", usage, assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: "H" } });
    wire.pushFrame({ type: "message_update", usage, assistantMessageEvent: { type: "thinking_delta", contentIndex: 1, delta: "hmm" } });
    wire.pushFrame({ type: "queue_update", steering: [], followUp: ["later"] });
    wire.pushFrame({ type: "tool_execution_start", toolCallId: "call-9", toolName: "read", args: { path: "a.ts" } });
    wire.pushFrame({
      type: "tool_execution_end",
      toolCallId: "call-9",
      toolName: "read",
      isError: true,
      result: { content: [{ type: "text", text: "missing" }] },
    });
    wire.pushFrame({ type: "agent_end", messages: [], willRetry: false });
    wire.pushFrame({ type: "agent_settled" });

    await pump.have(12);
    const [, running, usageEvent, chunk, reasoning, queueNotice, toolStart, toolEnd, endNotice, finalText, finalReasoning, idle] =
      pump.got.map((e) => e.body);
    expect(running).toMatchObject({ kind: "status", status: "running" });
    // message_update carries cumulative usage alongside its delta.
    expect(usageEvent).toMatchObject({
      kind: "usage",
      usage: { input_tokens: 10, output_tokens: 4, cached_tokens: 3, cost_usd: 0.05 },
    });
    expect(chunk).toMatchObject({ kind: "text", role: "assistant", stream_id: "m1:t0", text: { text: "H" } });
    expect(reasoning).toMatchObject({ kind: "text", role: "reasoning", stream_id: "m1:r1", text: { text: "hmm" } });
    expect(queueNotice).toMatchObject({ kind: "log", level: "info", text: { text: "provider_notice:queue_update" } });
    expect(toolStart).toMatchObject({ kind: "tool_start", call_id: "call-9", tool: "read" });
    expect(toolEnd).toMatchObject({ kind: "tool_end", call_id: "call-9", ok: false, output_preview: { text: "missing" } });
    expect(endNotice).toMatchObject({ kind: "log", level: "info", text: { text: "provider_notice:agent_end" } });
    // Settling the turn closes every open stream block with a final chunk…
    expect(finalText).toMatchObject({ kind: "text", role: "assistant", stream_id: "m1:t0", final: true });
    expect(finalReasoning).toMatchObject({ kind: "text", role: "reasoning", stream_id: "m1:r1", final: true });
    expect(idle).toMatchObject({ kind: "status", status: "idle" });
    // The unchanged cumulative usage was emitted exactly once.
    expect(pump.got.filter((event) => event.body.kind === "usage")).toHaveLength(1);
    // queue_update became a bounded notice, never a payload.
    expect(JSON.stringify(pump.got)).not.toContain("later");
  });

  it("rejects idle steer with LIVE_NOT_RUNNING and delivers steer only mid-turn", async () => {
    const { transport, wire } = setup();
    await openStartedPi(transport, wire);
    const framesBefore = wire.written.length;

    await expect(transport.send(command("steer", { text: "too early" }))).rejects.toMatchObject({
      liveError: { code: "LIVE_NOT_RUNNING" },
    });
    expect(wire.written).toHaveLength(framesBefore);

    const pump = new Pump(transport);
    wire.pushFrame({ type: "agent_start" });
    await pump.have(2);
    const steer = transport.send(command("steer", { text: "instead: left" }));
    await delivered(wire, "steer", steer);
    expect(lastCommand(wire, "steer")).toMatchObject({ type: "steer", message: "instead: left" });
  });

  it("sends prompt and follow_up with correlation ids and same-command acks", async () => {
    const { transport, wire } = setup();
    await openStartedPi(transport, wire);

    const prompt = transport.send(command("prompt", { text: "start" }));
    await delivered(wire, "prompt", prompt);
    expect(lastCommand(wire, "prompt")).toMatchObject({ type: "prompt", message: "start" });

    const followUp = transport.send(command("follow_up", { text: "then summarize" }));
    await delivered(wire, "follow_up", followUp);
    expect(lastCommand(wire, "follow_up")).toMatchObject({ type: "follow_up", message: "then summarize" });
  });

  it("cancels via abort, and rejects provider-failed commands with LIVE_COMMAND_REJECTED", async () => {
    const { transport, wire } = setup();
    await openStartedPi(transport, wire);
    const pump = new Pump(transport);

    wire.pushFrame({ type: "agent_start" });
    await pump.have(2);

    const cancelling = transport.send(command("cancel", { reason: "hub cancel" }));
    await delivered(wire, "abort", cancelling);
    await pump.have(4);
    // Skip the handshake-complete idle from the startup backlog.
    const statuses = pump.got
      .filter((event) => event.body.kind === "status")
      .map((event) => event.body)
      .slice(1);
    expect(statuses).toMatchObject([
      { status: "running" },
      { status: "cancelling", note: "hub cancel" },
      { status: "idle" },
    ]);

    const asking = transport.send(command("status"));
    await until(() => {
      const count = wire.frames().filter((frame) => frame.type === "get_state").length;
      if (count < 2) {
        throw new Error("status get_state not written yet");
      }
    });
    const failed = wire.frames().filter((frame) => frame.type === "get_state").at(-1);
    wire.pushFrame({ id: failed?.id, type: "response", command: "get_state", success: false, error: "provider boom" });
    await expect(asking).rejects.toMatchObject({ liveError: { code: "LIVE_COMMAND_REJECTED", stage: "provider" } });
  });

  it("does not fake a resume from a success response whose switch was cancelled", async () => {
    const { transport, wire } = setup();
    const opening = transport.open(
      launch({
        resume: {
          provider: "pi",
          provider_session_id: null,
          verified: false,
          verified_via: null,
          resume_token: "/pi/target.jsonl",
        },
      }),
    );
    await until(() => {
      if (wire.written.length === 0) {
        throw new Error("handshake not written");
      }
    });
    answer(wire, wire.lastFrame(), { sessionFile: "/pi/a.jsonl", sessionId: "pi-a" });
    await until(() => {
      if (!wire.written.some((line) => line.includes('"type":"switch_session"'))) {
        throw new Error("switch_session not written");
      }
    });
    expect(lastCommand(wire, "switch_session")).toMatchObject({ type: "switch_session", sessionPath: "/pi/target.jsonl" });
    // The false success: `success:true` with `cancelled:true`.
    answer(wire, lastCommand(wire, "switch_session"), { cancelled: true });

    await expect(opening).rejects.toMatchObject({ liveError: { code: "LIVE_RESUME_VERIFICATION_FAILED" } });
    expect(transport.resumeVerification).toBeNull();
    expect(wire.signals).toContain("SIGTERM");
  });

  it("verifies resume only when get_state echoes the switched locator", async () => {
    const { transport, wire } = setup();
    const opening = transport.open(
      launch({
        resume: {
          provider: "pi",
          provider_session_id: null,
          verified: false,
          verified_via: null,
          resume_token: "/pi/target.jsonl",
        },
      }),
    );
    await until(() => {
      if (wire.written.length === 0) {
        throw new Error("handshake not written");
      }
    });
    answer(wire, wire.lastFrame(), { sessionFile: "/pi/a.jsonl", sessionId: "pi-a" });
    await until(() => {
      if (!wire.written.some((line) => line.includes('"type":"switch_session"'))) {
        throw new Error("switch_session not written");
      }
    });
    answer(wire, lastCommand(wire, "switch_session"), { cancelled: false });
    await until(() => {
      const count = wire.frames().filter((frame) => frame.type === "get_state").length;
      if (count < 2) {
        throw new Error("resume-verify get_state not written");
      }
    });
    answer(wire, wire.frames().filter((frame) => frame.type === "get_state").at(-1) as Record<string, unknown>, {
      sessionFile: "/pi/target.jsonl",
      sessionId: "pi-target",
    });

    const report = await opening;
    expect(report.provider_session_id).toBe("/pi/target.jsonl");
    expect(transport.resumeVerification).toMatchObject({ verified: true });
  });

  it("escalates terminate to SIGKILL and only reports closed with proof", async () => {
    const { transport, wire } = setup(false);
    await openStartedPi(transport, wire);
    const pump = new Pump(transport);

    const stopping = transport.stop("terminate");
    await vi.advanceTimersByTimeAsync(50);
    expect(wire.signals).toContain("SIGTERM");
    expect(wire.signals).not.toContain("SIGKILL");

    await vi.advanceTimersByTimeAsync(40);
    expect(wire.signals).toContain("SIGKILL");
    wire.exit(137);
    const report = await stopping;
    expect(report.status).toBe("closed");
    expect(report.exit_code).toBe(137);

    await pump.have(2);
    expect(pump.got.at(-1)?.body).toMatchObject({ kind: "exit", intentional: true, exit_code: 137 });
    await pump.closed();
  });

  it("tolerates unknown pi frames as provider notices", async () => {
    const { transport, wire } = setup();
    await openStartedPi(transport, wire);
    const pump = new Pump(transport);

    wire.pushFrame({ type: "some_future_pi_event", hidden: "raw" });
    await pump.have(2);
    expect(pump.got[1]?.body).toMatchObject({ kind: "log", level: "info", text: { text: "provider_notice:some_future_pi_event" } });
    expect(JSON.stringify(pump.got[1])).not.toContain("raw");
  });

  it("maps a structurally unsupported frame to PROVIDER_PROTOCOL_UNSUPPORTED", async () => {
    const { transport, wire } = setup();
    await openStartedPi(transport, wire);
    const pump = new Pump(transport);

    wire.pushBytes(Buffer.from('{"type":"response","id":"x"\n[1,2,3]\n', "utf8"));
    await pump.have(3);
    expect(pump.got[1]?.body).toMatchObject({
      kind: "error",
      error: { code: "PROVIDER_PROTOCOL_UNSUPPORTED", provider: "pi", stage: "protocol", retryable: false },
    });
    expect(pump.got[2]?.body).toMatchObject({ kind: "exit", intentional: true });
    await pump.closed();
  });

  it("leader exits first, helper survives: graceful proves-only; terminate escalates the owned PGID", async () => {
    const wire = new GroupSurvivorWire();
    const transport = new PiRpcTransport({
      ...FAST,
      spawner: async () => wire,
    });
    await openStartedPi(transport, wire);
    new Pump(transport);

    // The leader exits FIRST, before any shutdown is requested; the helper
    // keeps the owned group alive and ignores everything but KILL.
    wire.exit(0);
    await until(() => undefined);

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
});
