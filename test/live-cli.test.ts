import { randomUUID } from "node:crypto";

import { describe, expect, it } from "vitest";

import { AgentHubError } from "../src/errors.js";

import { parseCliCommand, runCli } from "../src/cli.js";
import {
  iterateLiveCommands,
  liveTransportRegistry,
  LiveSessionManager,
  LiveTransportRegistry,
  probeLiveAgent,
  registerLiveTransport,
  runLiveSession,
  supportedLiveAgents,
} from "../src/live/index.js";
import type { LiveIo, LiveSessionManagerOptions } from "../src/live/index.js";
import type {
  LiveBoundedText,
  LiveCapabilities,
  LiveCapabilityClaim,
  LiveCommand,
  LiveEvent,
  LiveEventBody,
  LiveLaunchRequest,
  LiveLaunchReport,
  LiveProviderId,
  LiveSessionState,
  LiveStopMode,
  LiveStopReport,
  LiveTransport,
  LiveTransportDescriptor,
  LiveTransportFactory,
  LiveTransportId,
} from "../src/live/types.js";

// ---------------------------------------------------------------------------
// Scripted transport harness (mirrors the live transport contract exactly;
// every event it emits is normalized, byte-bounded, and seq-contiguous).
// ---------------------------------------------------------------------------

function native(evidence: string): LiveCapabilityClaim {
  return { support: "native", evidence };
}

function unclaimed(): LiveCapabilityClaim {
  return { support: "unsupported", evidence: null };
}

function capabilitySnapshot(
  overrides: Partial<Record<keyof LiveCapabilities, LiveCapabilityClaim>> = {},
): LiveCapabilities {
  const base: LiveCapabilities = {
    prompt: native("scripted prompt round-trip"),
    follow_up: native("scripted follow-up round-trip"),
    steer: native("scripted steer round-trip"),
    cancel: native("scripted cancel round-trip"),
    status: native("scripted status round-trip"),
    permission_response: native("scripted permission round-trip"),
    resume: native("scripted resume round-trip"),
    checkpoint: unclaimed(),
    usage_reporting: native("scripted usage report"),
  };
  return { ...base, ...overrides };
}

function bounded(text: string): LiveBoundedText {
  return { text, truncated: false };
}

const PROVIDER_BY_TRANSPORT: Record<LiveTransportId, LiveProviderId> = {
  "omp-rpc": "omp",
  "agy-stream-json": "agy",
  "pi-rpc": "pi",
  "hermes-acp": "hermes",
};

class ScriptedTransport implements LiveTransport {
  readonly id: LiveTransportId;
  readonly provider: LiveProviderId;
  readonly launchRequests: LiveLaunchRequest[] = [];
  readonly sent: LiveCommand[] = [];
  readonly stopCalls: LiveStopMode[] = [];
  private queue: LiveEvent[] = [];
  private wakeups: Array<() => void> = [];
  private ended = false;
  private seq = 0;
  private turn = 0;
  private sessionId = "";

  constructor(
    readonly transport: LiveTransportId,
    readonly capabilities: LiveCapabilities = capabilitySnapshot(),
  ) {
    this.id = transport;
    this.provider = PROVIDER_BY_TRANSPORT[transport];
  }

  private emit(body: LiveEventBody): void {
    this.queue.push({
      live_session_id: this.sessionId,
      seq: (this.seq += 1),
      transport: this.id,
      occurred_at: new Date().toISOString(),
      body,
    });
    this.wakeups.shift()?.();
  }

  async describe(): Promise<LiveTransportDescriptor> {
    return { transport: this.transport, provider: this.provider, capabilities: this.capabilities };
  }

  async open(request: LiveLaunchRequest): Promise<LiveLaunchReport> {
    this.launchRequests.push(request);
    this.sessionId = request.live_session_id;
    this.emit({ kind: "status", status: "starting", note: null });
    this.emit({ kind: "status", status: "idle", note: null });
    return { pid: 4242, provider_session_id: "psess-1", launched_at: new Date().toISOString() };
  }

  async send(command: LiveCommand): Promise<void> {
    this.sent.push(command);
    // Provider traffic answers asynchronously, like any real transport.
    await Promise.resolve();
    switch (command.kind) {
      case "prompt":
      case "follow_up": {
        this.turn += 1;
        this.emit({ kind: "status", status: "running", note: null });
        if (command.text.includes("boom")) {
          this.emit({
            kind: "error",
            error: {
              code: "PROVIDER_BOOM",
              message: "the scripted provider failed",
              stage: "provider",
              retryable: false,
              provider: this.provider,
            },
          });
          this.emit({ kind: "status", status: "error", note: null });
          return;
        }
        if (command.text.includes("ask")) {
          this.emit({
            kind: "permission_request",
            request_id: "req-9",
            tool: "write",
            summary: bounded("may I write?"),
          });
          return; // stays running until the permission answer arrives
        }
        this.emit({
          kind: "text",
          role: "assistant",
          stream_id: `s-${this.turn}`,
          text: bounded(`Answer: ${command.text}`),
          final: true,
        });
        this.emit({
          kind: "usage",
          usage: { input_tokens: 10, output_tokens: 2, cached_tokens: null, cost_usd: null },
        });
        this.emit({ kind: "status", status: "idle", note: null });
        return;
      }
      case "permission_response": {
        this.emit({
          kind: "text",
          role: "assistant",
          stream_id: `s-${this.turn}`,
          text: bounded(`permitted: ${command.decision}`),
          final: true,
        });
        this.emit({ kind: "status", status: "idle", note: null });
        return;
      }
      case "steer":
        this.emit({ kind: "log", level: "info", text: bounded(`steered: ${command.text}`) });
        return;
      case "cancel":
        this.emit({ kind: "status", status: "cancelling", note: null });
        this.emit({ kind: "status", status: "idle", note: "turn cancelled" });
        return;
      case "status":
        this.emit({ kind: "log", level: "info", text: bounded("status requested by provider") });
        return;
    }
  }

  async *events(): AsyncGenerator<LiveEvent> {
    for (;;) {
      const next = this.queue.shift();
      if (next !== undefined) {
        yield next;
        continue;
      }
      if (this.ended) {
        return;
      }
      await new Promise<void>((resolve) => this.wakeups.push(resolve));
    }
  }

  async stop(mode: LiveStopMode): Promise<LiveStopReport> {
    this.stopCalls.push(mode);
    if (!this.ended) {
      this.emit({ kind: "exit", intentional: true, exit_code: 0, exit_signal: null });
      this.ended = true;
    }
    return { status: "closed", exit_code: 0, exit_signal: null, waited_ms: 5 };
  }
}

class ScriptedFactory implements LiveTransportFactory {
  readonly transport: LiveTransportId;
  readonly provider: LiveProviderId;
  readonly created: ScriptedTransport[] = [];

  constructor(
    transport: LiveTransportId,
    readonly capabilities: LiveCapabilities = capabilitySnapshot(),
    readonly probeResult: { found: boolean; version: string | null; detail: string | null } = {
      found: true,
      version: "9.9.9-scripted",
      detail: "scripted provider present",
    },
  ) {
    this.transport = transport;
    this.provider = PROVIDER_BY_TRANSPORT[transport];
  }

  async probe() {
    return this.probeResult;
  }

  create(): ScriptedTransport {
    const transport = new ScriptedTransport(this.transport, this.capabilities);
    this.created.push(transport);
    return transport;
  }
}

function managerWith(factories: LiveTransportFactory[], options: LiveSessionManagerOptions = {}) {
  const registry = new LiveTransportRegistry();
  for (const factory of factories) {
    registry.register(factory);
  }
  return new LiveSessionManager({
    registry,
    launchSettleMs: 500,
    turnTimeoutMs: 5_000,
    ...options,
  });
}

function makeIo(commands: unknown[]) {
  const documents: Array<Record<string, any>> = [];
  const diagnostics: string[] = [];
  const io: LiveIo = {
    stdin: (async function* () {
      for (const command of commands) {
        yield typeof command === "string" ? command : JSON.stringify(command);
      }
    })(),
    stdout: (document) => documents.push(document),
    stderr: (line) => diagnostics.push(line),
  };
  return { io, documents, diagnostics };
}

function docsOfType(documents: Array<Record<string, any>>, type: string): Array<Record<string, any>> {
  return documents.filter((document) => document.type === type);
}

function liveStateFixture(overrides: Partial<LiveSessionState> = {}): LiveSessionState {
  const iso = new Date().toISOString();
  return {
    schema: 1,
    live_session_id: randomUUID(),
    session_id: null,
    provider: "pi",
    transport: "pi-rpc",
    capabilities: capabilitySnapshot(),
    identity: {
      common_dir: "/repo/.git",
      worktree_root: "/repo",
      branch: "main",
      head: "f".repeat(40),
    },
    base_commit: "f".repeat(40),
    current_commit: "f".repeat(40),
    checkpoint_seq: 0,
    resume: {
      provider: "pi",
      provider_session_id: "psess-9",
      resume_token: "tok-1",
      verified: false,
      verified_via: null,
    },
    status: "orphaned",
    revision: 3,
    last_error: null,
    created_at: iso,
    updated_at: iso,
    ...overrides,
  };
}

// ---------------------------------------------------------------------------
// Parsing
// ---------------------------------------------------------------------------

describe("live CLI parsing", () => {
  it("parses a live start with a v3-only provider", () => {
    const invocation = parseCliCommand(["live", "--agent", "pi", "--workspace", "/tmp/repo"]);
    expect(invocation.kind).toBe("live");
    if (invocation.kind === "live") {
      expect(invocation.request.agent).toBe("pi");
      expect(invocation.request.resumeId).toBeNull();
      expect(invocation.request.workspace).toBe("/tmp/repo");
    }
  });

  it("parses a live resume and a probe", () => {
    const resume = parseCliCommand(["live", "--resume", "live-42", "--workspace", "/tmp/repo"]);
    expect(resume.kind).toBe("live");
    if (resume.kind === "live") {
      expect(resume.request.resumeId).toBe("live-42");
      expect(resume.request.agent).toBeNull();
    }

    const probe = parseCliCommand(["live", "probe", "--agent", "hermes"]);
    expect(probe.kind).toBe("live-probe");
    if (probe.kind === "live-probe") {
      expect(probe.agent).toBe("hermes");
    }
  });

  it("rejects malformed live grammars", () => {
    expect(() => parseCliCommand(["live", "--agent", "pi", "--resume", "x"])).toThrow(
      /either --agent or --resume/,
    );
    expect(() => parseCliCommand(["live"])).toThrow(/live requires --agent/);
    expect(() => parseCliCommand(["live", "--agent", "grok"])).toThrow(
      /must be one of: omp, agy, pi, hermes/,
    );
    expect(() => parseCliCommand(["live", "--agent", "pi", "--mode", "isolated"])).toThrow(
      /Unknown option: --mode/,
    );
    expect(() => parseCliCommand(["live", "--agent", "pi", "stray"])).toThrow(/no positional text/);
    expect(() => parseCliCommand(["live", "--agent", "pi", "--max-output-bytes", "0"])).toThrow(
      /positive integer/,
    );
    expect(() => parseCliCommand(["live", "probe", "--agent", "omp", "--workspace", "/x"])).toThrow(
      /live probe accepts only --agent/,
    );
    expect(() => parseCliCommand(["live", "probe"])).toThrow(/live probe requires --agent/);
  });

  it("keeps the legacy delegate path on the old vocabulary", () => {
    const invocation = parseCliCommand(["--agent", "omp", "--mode", "direct", "plain task"]);
    expect(invocation.kind).toBe("delegate");
  });

  it("fixes the live vocabulary to exactly omp, agy, pi, hermes", () => {
    expect(supportedLiveAgents).toEqual(["omp", "agy", "pi", "hermes"]);
  });

  it("splits stdin chunks into live command lines", async () => {
    async function* chunks() {
      yield '{"action":"sta';
      yield 'tus"}\n{"action":"close"}';
    }
    const lines: string[] = [];
    for await (const line of iterateLiveCommands(chunks())) {
      lines.push(line);
    }
    expect(lines).toEqual(['{"action":"status"}', '{"action":"close"}']);
  });
});

// ---------------------------------------------------------------------------
// The long-lived wire: stdin commands → stdout documents
// ---------------------------------------------------------------------------

describe("live session wire", () => {
  it("streams session, events, results, and close over NDJSON", async () => {
    const factory = new ScriptedFactory("pi-rpc", capabilitySnapshot({ status: { support: "derived", evidence: "hub stream activity" } }));
    const manager = managerWith([factory]);
    const { io, documents, diagnostics } = makeIo([
      { action: "prompt", text: "write tests" },
      { action: "status" },
      { action: "close" },
    ]);

    const exitCode = await runLiveSession(
      { provider: "pi", resumeId: null, workspace: "/tmp/repo", maxTextBytes: 4096 },
      io,
      { manager },
    );

    expect(exitCode).toBe(0);
    expect(documents[0]?.type).toBe("session");
    expect(documents[0]?.session).toMatchObject({
      provider: "pi",
      transport: "pi-rpc",
      pid: 4242,
      capabilities: { status: { support: "derived" } },
    });

    const events = docsOfType(documents, "event").map((document) => document.event as LiveEvent);
    const seqs = events.map((event) => event.seq);
    expect(seqs).toEqual(seqs.map((_, index) => index + 1)); // contiguous from 1
    expect(events.some((event) => event.body.kind === "text")).toBe(true);

    const results = docsOfType(documents, "result");
    const prompt = results.find((document) => document.result.kind === "prompt");
    expect(prompt?.result).toMatchObject({
      outcome: "succeeded",
      final_text: { text: "Answer: write tests", truncated: false },
      checkpoint: null,
    });
    const statusResult = results.find((document) => document.result.kind === "status");
    expect(statusResult?.result.outcome).toBe("succeeded");

    expect(docsOfType(documents, "close")[0]?.close).toMatchObject({
      status: "closed",
      stop: { status: "closed", exit_code: 0 },
    });
    // The `derived` status claim must never be forwarded to the transport.
    expect(factory.created[0]?.sent.map((command) => command.kind)).toEqual(["prompt"]);
    expect(factory.created[0]?.launchRequests[0]?.max_text_bytes).toBe(4096);
    expect(diagnostics.join("\n")).toContain("provider=pi");
  });

  it("rejects malformed stdin commands without dropping the session", async () => {
    const manager = managerWith([new ScriptedFactory("pi-rpc")]);
    const { io, documents } = makeIo(["{not json", { action: "teleport" }, { action: "close" }]);

    const exitCode = await runLiveSession(
      { provider: "pi", resumeId: null, workspace: "/tmp/repo" },
      io,
      { manager },
    );

    expect(exitCode).toBe(0);
    const errors = docsOfType(documents, "error");
    expect(errors).toHaveLength(2);
    for (const document of errors) {
      expect(document.error).toMatchObject({ code: "LIVE_COMMAND_INVALID", stage: "protocol" });
    }
    expect(documents[documents.length - 1]?.type).toBe("close");
  });

  it("refuses an unsupported steer pre-dispatch and never delivers it", async () => {
    const factory = new ScriptedFactory("pi-rpc", capabilitySnapshot({ steer: unclaimed() }));
    const manager = managerWith([factory]);
    const { io, documents } = makeIo([
      { action: "prompt", text: "work" },
      { action: "steer", text: "left" },
      { action: "close" },
    ]);

    expect(
      await runLiveSession({ provider: "pi", resumeId: null, workspace: "/tmp/repo" }, io, { manager }),
    ).toBe(0);

    const steer = docsOfType(documents, "result").find((d) => d.result.kind === "steer");
    expect(steer?.result).toMatchObject({ outcome: "unsupported" });
    expect(steer?.result.error).toMatchObject({ stage: "capability", retryable: false });
    expect(factory.created[0]?.sent.map((command) => command.kind)).toEqual(["prompt"]);
  });

  it("answers an observed permission request over the wire", async () => {
    const factory = new ScriptedFactory("pi-rpc");
    const manager = managerWith([factory]);
    const { io, documents } = makeIo([
      { action: "prompt", text: "ask me first" },
      { action: "permission_response", request_id: "req-9", decision: "allow_once" },
      { action: "close" },
    ]);

    expect(
      await runLiveSession({ provider: "pi", resumeId: null, workspace: "/tmp/repo" }, io, { manager }),
    ).toBe(0);

    const events = docsOfType(documents, "event").map((document) => document.event.body as { kind: string });
    expect(events.some((body) => body.kind === "permission_request")).toBe(true);
    const prompt = docsOfType(documents, "result").find((d) => d.result.kind === "prompt");
    expect(prompt?.result).toMatchObject({
      outcome: "succeeded",
      final_text: { text: "permitted: allow_once" },
    });
  });

  it("reports an unknown permission request id and cancels the pending turn on close", async () => {
    const manager = managerWith([new ScriptedFactory("pi-rpc")]);
    const { io, documents } = makeIo([
      { action: "prompt", text: "ask me first" },
      { action: "permission_response", request_id: "nope", decision: "deny" },
      { action: "close" },
    ]);

    expect(
      await runLiveSession({ provider: "pi", resumeId: null, workspace: "/tmp/repo" }, io, { manager }),
    ).toBe(1); // a failed structured result makes the run fail

    const results = docsOfType(documents, "result");
    const permission = results.find((d) => d.result.kind === "permission_response");
    expect(permission?.result.error?.code).toBe("LIVE_PERMISSION_REQUEST_UNKNOWN");
    const prompt = results.find((d) => d.result.kind === "prompt");
    expect(prompt?.result.outcome).toBe("cancelled");
  });

  it("fails a provider-error turn and exits 1", async () => {
    const manager = managerWith([new ScriptedFactory("pi-rpc")]);
    const { io, documents } = makeIo([{ action: "prompt", text: "boom now" }, { action: "close" }]);

    expect(
      await runLiveSession({ provider: "pi", resumeId: null, workspace: "/tmp/repo" }, io, { manager }),
    ).toBe(1);

    const prompt = docsOfType(documents, "result").find((d) => d.result.kind === "prompt");
    expect(prompt?.result).toMatchObject({ outcome: "failed" });
    expect(prompt?.result.error).toMatchObject({ code: "PROVIDER_BOOM", stage: "provider" });
  });

  it("starts nothing and fails honestly when the provider transport is unwired", async () => {
    const manager = managerWith([]);
    const { io, documents } = makeIo([]);

    const exitCode = await runLiveSession(
      { provider: "omp", resumeId: null, workspace: "/tmp/repo" },
      io,
      { manager },
    );

    expect(exitCode).toBe(1);
    expect(documents).toHaveLength(1);
    expect(documents[0]).toMatchObject({
      type: "error",
      error: { code: "LIVE_TRANSPORT_UNAVAILABLE", stage: "launch", provider: "omp" },
    });
  });

  it("refuses resume while the durable state store is unwired", async () => {
    const manager = managerWith([new ScriptedFactory("pi-rpc")]);
    const { io, documents } = makeIo([]);

    const exitCode = await runLiveSession(
      { provider: null, resumeId: "live-missing", workspace: "/tmp/repo" },
      io,
      { manager }, // no resumeSource → the honest unwired seam
    );

    expect(exitCode).toBe(1);
    expect(documents[0]?.error).toMatchObject({ code: "LIVE_STATE_UNAVAILABLE" });
  });

  it("resumes through the seam with the stored resume state", async () => {
    const factory = new ScriptedFactory("pi-rpc", capabilitySnapshot({ resume: native("proven") }));
    const manager = managerWith([factory]);
    const state = liveStateFixture();
    const { io, documents } = makeIo([{ action: "close" }]);

    const exitCode = await runLiveSession(
      { provider: null, resumeId: state.live_session_id, workspace: "/tmp/repo" },
      io,
      { manager, resumeSource: { load: async () => state } },
    );

    expect(exitCode).toBe(0);
    expect(documents[0]?.session).toMatchObject({
      live_session_id: state.live_session_id,
      provider: "pi",
    });
    expect(factory.created[0]?.launchRequests[0]?.resume).toEqual(state.resume);
  });

  it("refuses a durable state record that violates the seed shape", async () => {
    const manager = managerWith([new ScriptedFactory("pi-rpc")]);
    const broken = { ...liveStateFixture(), schema: 2 } as unknown as LiveSessionState;
    const { io, documents } = makeIo([]);

    const exitCode = await runLiveSession(
      { provider: null, resumeId: "x", workspace: "/tmp/repo" },
      io,
      { manager, resumeSource: { load: async () => broken } },
    );

    expect(exitCode).toBe(1);
    expect(documents[0]?.error).toMatchObject({ code: "LIVE_STATE_INVALID" });
  });
});

// ---------------------------------------------------------------------------
// Probe + default-registry wiring through runCli (no transport ever launched)
// ---------------------------------------------------------------------------

describe("live probe and the default registry", () => {
  it("fails the probe for a provider with no registered transport", async () => {
    const stdout: string[] = [];
    const exitCode = await runCli(["live", "probe", "--agent", "pi"], {
      stdout: (value) => stdout.push(value),
      stderr: () => {},
    });

    expect(exitCode).toBe(1);
    expect(JSON.parse(stdout.join("")).error).toMatchObject({
      code: "LIVE_TRANSPORT_UNAVAILABLE",
    });
  });

  it("registers through the seam and probes the installed provider", async () => {
    registerLiveTransport(new ScriptedFactory("hermes-acp"));

    const stdout: string[] = [];
    const exitCode = await runCli(["live", "probe", "--agent", "hermes"], {
      stdout: (value) => stdout.push(value),
      stderr: () => {},
    });

    expect(exitCode).toBe(0);
    expect(JSON.parse(stdout.join(""))).toMatchObject({
      provider: "hermes",
      transport: "hermes-acp",
      found: true,
      version: "9.9.9-scripted",
    });
  });
  it("refuses a second factory for the same provider and a mismatched pairing", () => {
    expect(() => registerLiveTransport(new ScriptedFactory("hermes-acp"))).toThrow(
      /already has a live transport/,
    );
    const lying = new ScriptedFactory("pi-rpc");
    Object.assign(lying, { provider: "omp" });
    let pairingCode = "";
    try {
      liveTransportRegistry.register(lying);
    } catch (error) {
      pairingCode = (error as AgentHubError).code;
    }
    expect(pairingCode).toBe("LIVE_TRANSPORT_PAIRING_INVALID");
  });
  it("reports an honest not-found probe through the registry", async () => {
    const registry = new LiveTransportRegistry();
    registry.register(
      new ScriptedFactory("omp-rpc", capabilitySnapshot(), {
        found: false,
        version: null,
        detail: "command not installed",
      }),
    );

    const document = await probeLiveAgent("omp", registry);
    expect(document).toMatchObject({
      provider: "omp",
      transport: "omp-rpc",
      found: false,
      version: null,
    });
  });

  it("rejects a legacy-only provider on the live surface with exit 2", async () => {
    const stderr: string[] = [];
    const exitCode = await runCli(["live", "--agent", "bogus", "--workspace", "/tmp"], {
      stdout: () => {},
      stderr: (value) => stderr.push(value),
    });

    expect(exitCode).toBe(2);
    expect(stderr.join("")).toContain("must be one of: omp, agy, pi, hermes");
  });

  it("refuses live resume through runCli while the store seam is unwired", async () => {
    const stdout: string[] = [];
    const exitCode = await runCli(["live", "--resume", "live-42", "--workspace", "/tmp"], {
      stdout: (value) => stdout.push(value),
      stderr: () => {},
    });

    expect(exitCode).toBe(1);
    const document = JSON.parse(stdout.join(""));
    expect(document).toMatchObject({ type: "error" });
    expect(document.error).toMatchObject({ code: "LIVE_STATE_UNAVAILABLE" });
  });
});
