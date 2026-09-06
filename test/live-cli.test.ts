import { randomUUID } from "node:crypto";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { AgentHubError } from "../src/errors.js";
import { resolveRepositoryIdentity } from "../src/git.js";

import { parseCliCommand, runCli } from "../src/cli.js";
import {
  createLiveManager,
  iterateLiveCommands,
  liveStatePath,
  LiveSessionManager,
  LiveTransportRegistry,
  probeLiveAgent,
  runLiveSession,
  supportedLiveAgents,
} from "../src/live/index.js";
import type { LiveIo } from "../src/live/index.js";
import type { LiveLeaseProbes } from "../src/live/lease.js";
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
  LiveStopMode,
  LiveStopReport,
  LiveTransport,
  LiveTransportDescriptor,
  LiveTransportFactory,
  LiveTransportId,
  ProviderResumeState,
} from "../src/live/types.js";
import { createGitRepository, removeDirectory } from "./helpers.js";

// ---------------------------------------------------------------------------
// Scripted transport harness (mirrors the live transport contract exactly;
// the hub re-stamps every envelope, so only event BODIES are scripted).
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
    // The hub re-stamps seq/live_session_id/occurred_at; these placeholders
    // exist only so a leak of transport-side lies would be visible.
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
    // A scripted resume actually round-trips the handle it was handed, so
    // it may honestly report a transport-verified resume state. The `as`
    // only re-unifies the provider-union spread TypeScript cannot express.
    const resume_state =
      request.resume !== null
        ? ({ ...request.resume, verified: true, verified_via: "scripted-session-load" } as ProviderResumeState)
        : undefined;
    return {
      pid: 4242,
      provider_session_id: "psess-1",
      launched_at: new Date().toISOString(),
      ...(resume_state !== undefined ? { resume_state } : {}),
    };
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
          // A failed turn must settle on a terminal boundary like any other:
          // the error event rides the stream, then the turn boundary arrives.
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
          this.emit({ kind: "status", status: "idle", note: "turn failed" });
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
      const { promise, resolve } = Promise.withResolvers<void>();
      this.wakeups.push(resolve);
      await promise;
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

function fakeProbes(): LiveLeaseProbes {
  return {
    probePid: (pid) => (pid === process.pid ? "live" : "dead"),
    startToken: async (pid) => (pid === process.pid ? "hub-tok" : "prov-tok"),
    killGroup: () => true,
    now: () => new Date(),
  };
}

// ---------------------------------------------------------------------------
// Wire harness: the CORE manager production-bootstrapped over a real Git
// repository with the scripted factory registered as its only transport.
// ---------------------------------------------------------------------------

interface WireScope {
  repository: string;
  commonDir: string;
  tmpRoot: string;
  manager: LiveSessionManager;
  factory: ScriptedFactory;
}

async function wireScope(capabilities: LiveCapabilities = capabilitySnapshot()): Promise<WireScope> {
  const repository = await createGitRepository();
  const identity = await resolveRepositoryIdentity(repository);
  const tmpRoot = await mkdtemp(join(tmpdir(), "agent-hub-live-cli-"));
  const factory = new ScriptedFactory("pi-rpc", capabilities);
  const manager = await createLiveManager(repository, {
    withoutProductionTransports: true,
    extraTransportFactories: [factory],
    tmpRoot,
    leaseProbes: fakeProbes(),
  });
  return { repository, commonDir: identity.common_dir, tmpRoot, manager, factory };
}

/** A second hub process over the same durable state (the resume scenario). */
async function secondWireHub(source: WireScope): Promise<LiveSessionManager> {
  return await createLiveManager(source.repository, {
    withoutProductionTransports: true,
    extraTransportFactories: [source.factory],
    tmpRoot: source.tmpRoot,
    leaseProbes: fakeProbes(),
  });
}

async function releaseWire(scope: WireScope): Promise<void> {
  await scope.manager.closeAll();
  await removeDirectory(scope.repository);
  await removeDirectory(scope.tmpRoot);
}

// A stdin entry may be a command (object/string line) or a wait gate: the
// fake client holds the next line until the wire has actually observed
// documents matching the predicate — exactly what a real client does before
// answering an event. The runner's relay is timer-driven (its 25 ms poll and
// bounded drain live inside runLiveSession), so this integration suite must
// await the real emitted documents against the clock; fake timers would
// freeze the runner under test and cannot replace the awaited signal.
interface WaitGate {
  waitFor: (documents: ReadonlyArray<Record<string, any>>) => boolean;
}

type StdinEntry = string | Record<string, unknown> | WaitGate;

function isWaitGate(entry: StdinEntry): entry is WaitGate {
  return typeof entry === "object" && entry !== null && "waitFor" in entry;
}

function makeIo(commands: StdinEntry[]) {
  const documents: Array<Record<string, any>> = [];
  const diagnostics: string[] = [];
  const io: LiveIo = {
    stdin: (async function* () {
      for (const entry of commands) {
        if (isWaitGate(entry)) {
          const deadline = Date.now() + 8_000;
          while (!entry.waitFor(documents)) {
            if (Date.now() > deadline) {
              throw new Error("live stdin gate timed out waiting for a wire document");
            }
            const { promise, resolve } = Promise.withResolvers<void>();
            setTimeout(resolve, 5);
            await promise;
          }
          continue;
        }
        yield typeof entry === "string" ? entry : JSON.stringify(entry);
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

const permissionRequestSeen = (documents: ReadonlyArray<Record<string, any>>) =>
  documents.some(
    (document) =>
      document.type === "event" && document.event?.body?.kind === "permission_request",
  );

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
// The long-lived wire: stdin commands → stdout documents, over the CORE
// manager injected through runLiveSession's dependencies.
// ---------------------------------------------------------------------------

describe("live session wire", () => {
  it(
    "streams session, events, results, and close over NDJSON",
    async () => {
      const scope = await wireScope(
        capabilitySnapshot({ status: { support: "derived", evidence: "hub stream activity" } }),
      );
      const { io, documents, diagnostics } = makeIo([
        { action: "prompt", text: "write tests" },
        { action: "status" },
        { action: "close" },
      ]);

      const exitCode = await runLiveSession(
        { provider: "pi", resumeId: null, workspace: scope.repository, maxTextBytes: 4096 },
        io,
        { manager: scope.manager },
      );
      await releaseWire(scope);

      expect(exitCode).toBe(0);
      expect(documents[0]?.type).toBe("session");
      expect(documents[0]?.session).toMatchObject({
        provider: "pi",
        transport: "pi-rpc",
        session_id: null,
        status: "idle",
        warnings: [],
        capabilities: { status: { support: "derived" } },
      });
      const liveSessionId = documents[0].session.live_session_id as string;
      expect(liveSessionId).toBeTruthy();
      expect(documents[0].session.base_commit).toMatch(/^[0-9a-f]{40}$/);
      expect(documents[0].session.workspace.startsWith(scope.tmpRoot)).toBe(true);

      const events = docsOfType(documents, "event").map((document) => document.event as LiveEvent);
      const seqs = events.map((event) => event.seq);
      expect(seqs).toEqual(seqs.map((_, index) => index + 1)); // contiguous from 1
      expect(events.every((event) => event.live_session_id === liveSessionId)).toBe(true);
      expect(
        events.some(
          (event) => event.body.kind === "text" && event.body.text.text === "Answer: write tests",
        ),
      ).toBe(true);

      const results = docsOfType(documents, "result");
      const prompt = results.find((document) => document.result.kind === "prompt");
      expect(prompt?.result).toMatchObject({
        live_session_id: liveSessionId,
        outcome: "succeeded",
        final_text: { text: "Answer: write tests", truncated: false },
        checkpoint: null,
      });
      expect(prompt?.status).toBe("idle"); // the runner reports the session status beside the result
      const statusResult = results.find((document) => document.result.kind === "status");
      expect(statusResult?.result.outcome).toBe("succeeded");

      const close = docsOfType(documents, "close")[0]?.close;
      expect(close).toMatchObject({
        live_session_id: liveSessionId,
        status: "closed",
        stop: { status: "closed", exit_code: 0 },
        cleanup_errors: [],
      });
      // The worktree tree never changed, so the close checkpoint pinned nothing new.
      expect(close.checkpoint_taken).toBe(false);

      // The `derived` status claim must never be forwarded to the transport.
      expect(scope.factory.created[0]?.sent.map((command) => command.kind)).toEqual(["prompt"]);
      expect(scope.factory.created[0]?.launchRequests[0]?.max_text_bytes).toBe(4096);
      expect(diagnostics.join("\n")).toContain("provider=pi");
    },
    30_000,
  );

  it(
    "rejects malformed stdin commands without dropping the session",
    async () => {
      const scope = await wireScope();
      const { io, documents } = makeIo(["{not json", { action: "teleport" }, { action: "close" }]);

      const exitCode = await runLiveSession(
        { provider: "pi", resumeId: null, workspace: scope.repository },
        io,
        { manager: scope.manager },
      );
      await releaseWire(scope);

      expect(exitCode).toBe(0);
      const errors = docsOfType(documents, "error");
      expect(errors).toHaveLength(2);
      for (const document of errors) {
        expect(document.error).toMatchObject({ code: "LIVE_COMMAND_INVALID", stage: "protocol" });
      }
      expect(documents[documents.length - 1]?.type).toBe("close");
      expect(docsOfType(documents, "close")[0]?.close).toMatchObject({ status: "closed" });
    },
    30_000,
  );

  it(
    "refuses an unsupported steer pre-dispatch and never delivers it",
    async () => {
      const scope = await wireScope(capabilitySnapshot({ steer: unclaimed() }));
      const { io, documents } = makeIo([
        { action: "prompt", text: "work" },
        { action: "steer", text: "left" },
        { action: "close" },
      ]);

      const exitCode = await runLiveSession(
        { provider: "pi", resumeId: null, workspace: scope.repository },
        io,
        { manager: scope.manager },
      );
      await releaseWire(scope);

      expect(exitCode).toBe(0);
      const steer = docsOfType(documents, "result").find((d) => d.result.kind === "steer");
      expect(steer?.result).toMatchObject({ outcome: "unsupported" });
      expect(steer?.result.error).toMatchObject({
        code: "LIVE_CAPABILITY_UNSUPPORTED",
        stage: "capability",
        retryable: false,
      });
      expect(scope.factory.created[0]?.sent.map((command) => command.kind)).toEqual(["prompt"]);
    },
    30_000,
  );

  it(
    "answers an observed permission request over the wire",
    async () => {
      const scope = await wireScope();
      const { io, documents } = makeIo([
        { action: "prompt", text: "ask me first" },
        { waitFor: permissionRequestSeen },
        { action: "permission_response", request_id: "req-9", decision: "allow_once" },
        { action: "close" },
      ]);

      const exitCode = await runLiveSession(
        { provider: "pi", resumeId: null, workspace: scope.repository },
        io,
        { manager: scope.manager },
      );
      await releaseWire(scope);

      expect(exitCode).toBe(0);
      const events = docsOfType(documents, "event").map(
        (document) => document.event.body as { kind: string },
      );
      expect(events.some((body) => body.kind === "permission_request")).toBe(true);
      const permission = docsOfType(documents, "result").find(
        (d) => d.result.kind === "permission_response",
      );
      expect(permission?.result.outcome).toBe("succeeded");
      const prompt = docsOfType(documents, "result").find((d) => d.result.kind === "prompt");
      expect(prompt?.result).toMatchObject({
        outcome: "succeeded",
        final_text: { text: "permitted: allow_once" },
      });
      const delivered = scope.factory.created[0]?.sent.find(
        (command) => command.kind === "permission_response",
      );
      expect(delivered).toMatchObject({ request_id: "req-9", decision: "allow_once" });
    },
    30_000,
  );

  it(
    "refuses a widened permission decision before dispatch with LIVE_COMMAND_INVALID",
    async () => {
      const scope = await wireScope();
      const { io, documents } = makeIo([
        { action: "prompt", text: "ask me first" },
        { waitFor: permissionRequestSeen },
        { action: "permission_response", request_id: "req-9", decision: "allow_session" },
        { action: "close" },
      ]);

      const exitCode = await runLiveSession(
        { provider: "pi", resumeId: null, workspace: scope.repository },
        io,
        { manager: scope.manager },
      );
      await releaseWire(scope);

      // The vocabulary violation is a caller error on the protocol: it rides
      // an error document and never becomes a silently converted deny.
      expect(exitCode).toBe(0);
      const errors = docsOfType(documents, "error");
      expect(errors).toHaveLength(1);
      expect(errors[0]?.error).toMatchObject({
        code: "LIVE_COMMAND_INVALID",
        stage: "protocol",
        provider: "pi",
      });
      expect(
        docsOfType(documents, "result").some((d) => d.result.kind === "permission_response"),
      ).toBe(false);
      expect(
        scope.factory.created[0]?.sent.some((command) => command.kind === "permission_response"),
      ).toBe(false);
    },
    30_000,
  );

  it(
    "reports an unknown permission request id and cancels the pending turn on close",
    async () => {
      const scope = await wireScope();
      const { io, documents } = makeIo([
        { action: "prompt", text: "ask me first" },
        { waitFor: permissionRequestSeen },
        { action: "permission_response", request_id: "nope", decision: "deny" },
        { action: "close" },
      ]);

      const exitCode = await runLiveSession(
        { provider: "pi", resumeId: null, workspace: scope.repository },
        io,
        { manager: scope.manager },
      );
      await releaseWire(scope);

      expect(exitCode).toBe(0); // an undelivered caller error does not fail the session itself
      const errors = docsOfType(documents, "error");
      expect(errors).toHaveLength(1);
      expect(errors[0]?.error).toMatchObject({
        code: "LIVE_PERMISSION_REQUEST_UNKNOWN",
        stage: "protocol",
      });
      const prompt = docsOfType(documents, "result").find((d) => d.result.kind === "prompt");
      expect(prompt?.result.outcome).toBe("cancelled"); // close cancelled the pending turn honestly
      expect(docsOfType(documents, "close")[0]?.close).toMatchObject({ status: "closed" });
    },
    30_000,
  );

  it(
    "fails a provider-error turn and exits 1",
    async () => {
      const scope = await wireScope();
      const { io, documents } = makeIo([{ action: "prompt", text: "boom now" }, { action: "close" }]);

      const exitCode = await runLiveSession(
        { provider: "pi", resumeId: null, workspace: scope.repository },
        io,
        { manager: scope.manager },
      );
      await releaseWire(scope);

      expect(exitCode).toBe(1); // a failed structured result makes the run fail
      const prompt = docsOfType(documents, "result").find((d) => d.result.kind === "prompt");
      expect(prompt?.result).toMatchObject({ outcome: "failed" });
      expect(prompt?.result.error).toMatchObject({ code: "PROVIDER_BOOM", stage: "provider" });
    },
    30_000,
  );

  it("starts nothing and fails honestly when the factory set lacks the provider", async () => {
    // No fixture needed: the core manager refuses before touching the
    // repository — an unpaired provider launches nothing.
    const manager = new LiveSessionManager({
      commonDir: join(tmpdir(), "agent-hub-live-cli-unwired-common"),
      repositoryCwd: join(tmpdir(), "agent-hub-live-cli-unwired-repo"),
      transportFactories: [new ScriptedFactory("pi-rpc")],
    });
    const { io, documents } = makeIo([]);

    const exitCode = await runLiveSession(
      { provider: "omp", resumeId: null, workspace: join(tmpdir(), "agent-hub-live-cli-unwired-repo") },
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

  it(
    "refuses --resume when no durable record exists for the id",
    async () => {
      const scope = await wireScope();
      const { io, documents } = makeIo([]);

      const exitCode = await runLiveSession(
        { provider: null, resumeId: randomUUID(), workspace: scope.repository },
        io,
        { manager: scope.manager },
      );
      await releaseWire(scope);

      expect(exitCode).toBe(1);
      expect(documents).toHaveLength(1);
      expect(documents[0]?.error).toMatchObject({
        code: "LIVE_SESSION_NOT_FOUND",
        stage: "launch",
        provider: null,
      });
    },
    30_000,
  );

  it(
    "resumes the same live id from its durable terminal record with the stored resume state",
    async () => {
      const scope = await wireScope();
      const first = makeIo([{ action: "prompt", text: "work" }, { action: "close" }]);
      expect(
        await runLiveSession(
          { provider: "pi", resumeId: null, workspace: scope.repository },
          first.io,
          { manager: scope.manager },
        ),
      ).toBe(0);
      const liveSessionId = first.documents[0].session.live_session_id as string;
      expect(docsOfType(first.documents, "close")[0]?.close).toMatchObject({ status: "closed" });

      // The terminal record the next hub must continue from, straight off
      // the common dir the start wrote through (before any cleanup).
      const record = JSON.parse(
        await readFile(liveStatePath(scope.commonDir, liveSessionId), "utf8"),
      ) as Record<string, any>;
      expect(record.schema).toBe("agent-hub-live/v1");
      expect(record.status).toBe("closed");

      // A fresh hub process over the same repository finds the terminal record.
      const resumedHub = await secondWireHub(scope);
      const second = makeIo([{ action: "close" }]);
      const exitCode = await runLiveSession(
        { provider: null, resumeId: liveSessionId, workspace: scope.repository },
        second.io,
        { manager: resumedHub },
      );
      scope.manager = resumedHub;
      await releaseWire(scope);

      expect(exitCode).toBe(0);
      expect(second.documents[0]?.session).toMatchObject({
        live_session_id: liveSessionId, // the same live id continues, never a new session
        provider: "pi",
        transport: "pi-rpc",
        status: "idle",
      });

      // The launch under resume carried exactly the durable record's handle.
      const resumedTransport = scope.factory.created[1];
      expect(resumedTransport?.launchRequests[0]?.resume).toEqual(record.resume);
      expect(resumedTransport?.launchRequests[0]?.resume).toMatchObject({
        provider: "pi",
        provider_session_id: "psess-1",
      });
      // The transport verified the round trip, so the session continues honestly.
      expect(docsOfType(second.documents, "close")[0]?.close).toMatchObject({ status: "closed" });
    },
    60_000,
  );
});

// ---------------------------------------------------------------------------
// Probe + default-registry wiring through runCli (no transport ever launched)
// ---------------------------------------------------------------------------

describe("live probe and the default registry", () => {
  it(
    "probes through the production registry wired by runCli itself",
    async () => {
      const stdout: string[] = [];
      const exitCode = await runCli(["live", "probe", "--agent", "pi"], {
        stdout: (value) => stdout.push(value),
        stderr: () => {},
      });

      // `live probe` registers the four real transports before consulting the
      // default registry, so the paired factory is always found; the probe
      // answer itself stays honest — an uninstalled binary is reported, not
      // guessed — and the exit code tracks that answer.
      const document = JSON.parse(stdout.join("")) as Record<string, any>;
      expect(document).toMatchObject({ provider: "pi", transport: "pi-rpc" });
      expect(typeof document.found).toBe("boolean");
      expect(exitCode).toBe(document.found ? 0 : 1);
    },
    30_000,
  );

  it("refuses to probe a provider whose transport is not wired", async () => {
    // An unwired registry stays honest: no guessing, just refusal.
    await expect(probeLiveAgent("hermes", new LiveTransportRegistry())).rejects.toMatchObject({
      code: "LIVE_TRANSPORT_UNAVAILABLE",
    });
  });

  it("reports an honest not-found probe through a registry", async () => {
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

  it("refuses a second factory for the same provider and a mismatched pairing", () => {
    const registry = new LiveTransportRegistry();
    const hermes = new ScriptedFactory("hermes-acp");
    registry.register(hermes);
    registry.register(hermes); // re-registering the same factory stays idempotent
    expect(() => registry.register(new ScriptedFactory("hermes-acp"))).toThrow(
      /already has a live transport/,
    );
    const lying = new ScriptedFactory("pi-rpc");
    Object.assign(lying, { provider: "omp" });
    let pairingCode = "";
    try {
      registry.register(lying);
    } catch (error) {
      pairingCode = (error as AgentHubError).code;
    }
    expect(pairingCode).toBe("LIVE_TRANSPORT_PAIRING_INVALID");
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

  it("refuses a live resume outside a Git repository through runCli", async () => {
    const stray = await mkdtemp(join(tmpdir(), "agent-hub-live-cli-stray-"));
    const stdout: string[] = [];
    const exitCode = await runCli(["live", "--resume", randomUUID(), "--workspace", stray], {
      stdout: (value) => stdout.push(value),
      stderr: () => {},
    });
    await removeDirectory(stray);

    expect(exitCode).toBe(1);
    const document = JSON.parse(stdout.join(""));
    expect(document).toMatchObject({ type: "error" });
    expect(document.error).toMatchObject({ code: "NOT_GIT_REPOSITORY", stage: "launch" });
  });

  it(
    "refuses a live resume with no durable record through the wired store seam",
    async () => {
      // runCli wires the real durable resume source itself: an unknown id in a
      // real repository fails with the honest not-found, never a fake session.
      const repository = await createGitRepository();
      const stdout: string[] = [];
      const exitCode = await runCli(
        ["live", "--resume", randomUUID(), "--workspace", repository],
        { stdout: (value) => stdout.push(value), stderr: () => {} },
      );
      await removeDirectory(repository);

      expect(exitCode).toBe(1);
      const document = JSON.parse(stdout.join(""));
      expect(document).toMatchObject({ type: "error" });
      expect(document.error).toMatchObject({ code: "LIVE_SESSION_NOT_FOUND", stage: "launch" });
    },
    30_000,
  );
});
