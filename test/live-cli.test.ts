import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it, vi } from "vitest";

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
  runLiveRecover,
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
import { deferred } from "../src/deferred.js";

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
      const { promise, resolve } = deferred<void>();
      this.wakeups.push(resolve);
      await promise;
    }
  }

  /** When set, decides the stop outcome per mode — the orphan-vs-closed seam. */
  stopPolicy: ((mode: LiveStopMode) => LiveStopReport) | null = null;

  async stop(mode: LiveStopMode): Promise<LiveStopReport> {
    this.stopCalls.push(mode);
    const report: LiveStopReport =
      this.stopPolicy?.(mode) ?? { status: "closed", exit_code: 0, exit_signal: null, waited_ms: 5 };
    if (report.status === "closed" && !this.ended) {
      this.emit({ kind: "exit", intentional: true, exit_code: 0, exit_signal: null });
      this.ended = true;
    }
    return report;
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
    readonly stopPolicy?: (mode: LiveStopMode) => LiveStopReport,
  ) {
    this.transport = transport;
    this.provider = PROVIDER_BY_TRANSPORT[transport];
  }

  async probe() {
    return this.probeResult;
  }

  create(): ScriptedTransport {
    const transport = new ScriptedTransport(this.transport, this.capabilities);
    if (this.stopPolicy) {
      transport.stopPolicy = this.stopPolicy;
    }
    this.created.push(transport);
    return transport;
  }
}

function fakeProbes(): LiveLeaseProbes {
  return {
    probePid: (pid) => (pid === process.pid ? "live" : "dead"),
    startToken: async (pid) => (pid === process.pid ? "hub-tok" : "prov-tok"),
    killGroup: () => true,
    probeGroup: (pgid) => (pgid === process.pid ? "alive" : "gone"),
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

async function wireScope(
  capabilities: LiveCapabilities = capabilitySnapshot(),
  stopPolicy?: (mode: LiveStopMode) => LiveStopReport,
): Promise<WireScope> {
  const repository = await createGitRepository();
  const identity = await resolveRepositoryIdentity(repository);
  const tmpRoot = await mkdtemp(join(tmpdir(), "agent-hub-live-cli-"));
  const factory = new ScriptedFactory("pi-rpc", capabilities, undefined, stopPolicy);
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
            const { promise, resolve } = deferred<void>();
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

  it("parses the live start gates and their defaults", () => {
    const plain = parseCliCommand(["live", "--agent", "hermes", "--workspace", "/tmp/repo"]);
    expect(plain.kind).toBe("live");
    if (plain.kind === "live") {
      expect(plain.request.allowDirty).toBe(false);
      expect(plain.request.permissionPolicy).toBeNull();
    }

    const selected = parseCliCommand([
      "live",
      "--agent",
      "hermes",
      "--allow-dirty",
      "--permission-policy",
      "interactive",
      "--workspace",
      "/tmp/repo",
    ]);
    expect(selected.kind).toBe("live");
    if (selected.kind === "live") {
      expect(selected.request.allowDirty).toBe(true);
      expect(selected.request.permissionPolicy).toBe("interactive");
    }

    const resumed = parseCliCommand(["live", "--resume", "live-42", "--permission-policy", "deny"]);
    expect(resumed.kind).toBe("live");
    if (resumed.kind === "live") {
      expect(resumed.request.permissionPolicy).toBe("deny");
      expect(resumed.request.allowDirty).toBe(false);
    }

    expect(() =>
      parseCliCommand(["live", "--agent", "pi", "--permission-policy", "allow_session"]),
    ).toThrow(/--permission-policy must be deny or interactive/);
    expect(() =>
      parseCliCommand([
        "live", "--agent", "pi",
        "--permission-policy", "interactive",
        "--permission-policy", "deny",
      ]),
    ).toThrow(/--permission-policy may be specified only once/);
    expect(() => parseCliCommand(["live", "--agent", "pi", "--permission-policy"])).toThrow(
      /requires a value/,
    );
  });

  it("parses live recover", () => {
    expect(parseCliCommand(["live", "recover", "--workspace", "/tmp/repo"])).toEqual({
      kind: "live-recover",
      workspace: "/tmp/repo",
    });
    expect(() => parseCliCommand(["live", "recover", "--agent", "pi"])).toThrow(
      /live recover accepts only --workspace/,
    );
    expect(() => parseCliCommand(["live", "recover", "extra"])).toThrow(
      /live recover accepts only --workspace/,
    );
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

  it(
    "close without terminate stays graceful and never escalates",
    async () => {
      const scope = await wireScope();
      const { io, documents } = makeIo([{ action: "prompt", text: "work" }, { action: "close" }]);
      const exitCode = await runLiveSession(
        { provider: "pi", resumeId: null, workspace: scope.repository, maxTextBytes: 4096 },
        io,
        { manager: scope.manager },
      );
      await releaseWire(scope);
      expect(exitCode).toBe(0);
      // The wire's close defaults to graceful with the manager.
      expect(scope.factory.created[0]?.stopCalls).toEqual(["graceful"]);
      expect(docsOfType(documents, "close")[0]?.close).toMatchObject({ status: "closed" });
    },
    30_000,
  );

  it(
    "close with terminate skips the graceful attempt and escalates directly",
    async () => {
      const scope = await wireScope();
      const { io, documents } = makeIo([
        { action: "prompt", text: "work" },
        { action: "close", terminate: true },
      ]);
      const exitCode = await runLiveSession(
        { provider: "pi", resumeId: null, workspace: scope.repository, maxTextBytes: 4096 },
        io,
        { manager: scope.manager },
      );
      await releaseWire(scope);
      expect(exitCode).toBe(0);
      // terminate:true is honored: the transport never saw a graceful stop.
      expect(scope.factory.created[0]?.stopCalls).toEqual(["terminate"]);
      expect(docsOfType(documents, "close")[0]?.close).toMatchObject({ status: "closed" });
    },
    30_000,
  );

  it(
    "graceful close against a deaf provider reports orphaned and never escalates quietly",
    async () => {
      const gracefulDeaf = (mode: LiveStopMode): LiveStopReport =>
        mode === "graceful"
          ? { status: "orphaned", exit_code: null, exit_signal: null, waited_ms: 5 }
          : { status: "closed", exit_code: null, exit_signal: "SIGKILL", waited_ms: 9 };
      const scope = await wireScope(undefined, gracefulDeaf);
      const { io, documents } = makeIo([{ action: "close" }]);
      const exitCode = await runLiveSession(
        { provider: "pi", resumeId: null, workspace: scope.repository, maxTextBytes: 4096 },
        io,
        { manager: scope.manager },
      );
      await releaseWire(scope);
      expect(exitCode).toBe(1); // an orphaned close is a failed exit
      // graceful is never authorized to escalate: exactly one stop call.
      expect(scope.factory.created[0]?.stopCalls).toEqual(["graceful"]);
      expect(docsOfType(documents, "close")[0]?.close).toMatchObject({
        status: "orphaned",
        stop: { status: "orphaned" },
        checkpoint_taken: false,
      });
    },
    30_000,
  );

  it(
    "terminate close against a provider that ignored graceful closes honestly",
    async () => {
      const killOnlyWorks = (mode: LiveStopMode): LiveStopReport =>
        mode === "graceful"
          ? { status: "orphaned", exit_code: null, exit_signal: null, waited_ms: 5 }
          : { status: "closed", exit_code: null, exit_signal: "SIGKILL", waited_ms: 9 };
      const scope = await wireScope(undefined, killOnlyWorks);
      const { io, documents } = makeIo([{ action: "close", terminate: true }]);
      const exitCode = await runLiveSession(
        { provider: "pi", resumeId: null, workspace: scope.repository, maxTextBytes: 4096 },
        io,
        { manager: scope.manager },
      );
      await releaseWire(scope);
      expect(exitCode).toBe(0);
      expect(scope.factory.created[0]?.stopCalls).toEqual(["terminate"]);
      expect(docsOfType(documents, "close")[0]?.close).toMatchObject({
        status: "closed",
        stop: { status: "closed", exit_signal: "SIGKILL" },
      });
    },
    30_000,
  );

  it(
    "refuses a dirty caller checkout unless --allow-dirty, and carries the selected permission policy across resume",
    async () => {
      const scope = await wireScope();
      try {
        await writeFile(join(scope.repository, "untracked-during-live.txt"), "caller state\n");

        // Default gate: a dirty caller checkout refuses the launch — no
        // transport is created, and the refusal is an honest error document.
        const refused = makeIo([]);
        expect(
          await runLiveSession(
            { provider: "pi", resumeId: null, workspace: scope.repository },
            refused.io,
            { manager: scope.manager },
          ),
        ).toBe(1);
        expect(refused.documents).toHaveLength(1);
        expect(refused.documents[0]).toMatchObject({
          type: "error",
          error: { code: "DIRTY_WORKTREE", stage: "launch" },
        });
        expect(scope.factory.created).toHaveLength(0);

        // --allow-dirty proceeds from committed HEAD, and the selected
        // policy rides the launch request to the transport.
        const started = makeIo([{ action: "close" }]);
        expect(
          await runLiveSession(
            {
              provider: "pi",
              resumeId: null,
              workspace: scope.repository,
              allowDirty: true,
              permissionPolicy: "interactive",
            },
            started.io,
            { manager: scope.manager },
          ),
        ).toBe(0);
        expect(scope.factory.created[0]?.launchRequests[0]?.permission_policy).toBe("interactive");
        const liveSessionId = started.documents[0]?.session?.live_session_id as string;
        expect(docsOfType(started.documents, "close")[0]?.close).toMatchObject({ status: "closed" });

        // A fresh hub resumes the durable id. The policy was NOT selected
        // for the resume, so the resumed launch carries the contract
        // default `deny` — never the previous run's `interactive`.
        const resumedHub = await secondWireHub(scope);
        scope.manager = resumedHub;
        const resumed = makeIo([{ action: "close" }]);
        expect(
          await runLiveSession(
            {
              provider: null,
              resumeId: liveSessionId,
              workspace: scope.repository,
              allowDirty: true,
            },
            resumed.io,
            { manager: resumedHub },
          ),
        ).toBe(0);
        expect(scope.factory.created[1]?.launchRequests[0]?.permission_policy).toBe("deny");
      } finally {
        await releaseWire(scope);
      }
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

// ---------------------------------------------------------------------------
// Relay liveness under fake timers. The injected manager is a pure relay
// fixture driven through the runner's structural manager surface; fake
// timers compress "arbitrary silence" without freezing real integration.
// ---------------------------------------------------------------------------

describe("live event relay across silence (fake timers)", () => {
  it("relays an event that arrives after thirteen seconds of silence", async () => {
    vi.useFakeTimers();
    try {
      const sink = { status: "idle" as LiveStatus, events: [] as LiveEvent[] };
      const fakeState = (status: LiveStatus) =>
        ({ live_session_id: "live-relay-fake", status }) as unknown as LiveSessionState;
      const unused = async (): Promise<never> => {
        throw new Error("unused by this relay test");
      };
      const manager = {
        start: async () => ({
          live_session_id: "live-relay-fake",
          state: fakeState("idle"),
          workspace: "/relay-fake-worktree",
        }),
        resumeFromState: unused,
        prompt: unused,
        followUp: unused,
        steer: unused,
        cancel: unused,
        requestStatus: unused,
        respondPermission: unused,
        view: () => fakeState(sink.status),
        eventsAfter: (_id: string, cursor: number) => ({
          events: sink.events.slice(cursor),
          next_cursor: sink.events.length,
        }),
        eventCursor: () => sink.events.length,
        close: async () => {
          sink.status = "closed";
          return {
            state: fakeState("closed"),
            stop: {
              status: "closed",
              exit_code: 0,
              exit_signal: null,
              waited_ms: 1,
            } as LiveStopReport,
            checkpoint_taken: false,
            cleanup_errors: [],
          };
        },
      };

      const silence = deferred<void>();
      const documents: Array<Record<string, any>> = [];
      const running = runLiveSession(
        { provider: "pi", resumeId: null, workspace: "/relay-fake" },
        {
          stdin: (async function* () {
            await silence.promise; // arbitrary silence: no commands, no events
            yield JSON.stringify({ action: "close" });
          })(),
          stdout: (document) => documents.push(document),
          stderr: () => {},
        },
        { manager },
      );

      // Thirteen seconds of total silence — well past the old ~10 s relay
      // cutoff. Nothing has arrived, and nothing may have been dropped.
      await vi.advanceTimersByTimeAsync(13_000);
      expect(docsOfType(documents, "event")).toHaveLength(0);

      // The provider finally says something; the relay must still be alive.
      sink.events.push({
        live_session_id: "live-relay-fake",
        seq: 1,
        transport: "pi-rpc",
        occurred_at: new Date(0).toISOString(),
        body: {
          kind: "log",
          level: "info",
          text: { text: "output after silence", truncated: false },
        },
      });
      silence.resolve();
      await vi.runAllTimersAsync();
      const exitCode = await running;

      expect(exitCode).toBe(0);
      const eventIndex = documents.findIndex((document) => document.type === "event");
      const closeIndex = documents.findIndex((document) => document.type === "close");
      expect(eventIndex).toBeGreaterThan(-1);
      expect(closeIndex).toBeGreaterThan(eventIndex);
      expect(documents[eventIndex]?.event?.seq).toBe(1);
      expect(documents[eventIndex]?.event?.body?.text?.text).toBe("output after silence");
    } finally {
      vi.useRealTimers();
    }
  }, 30_000);
});

// ---------------------------------------------------------------------------
// `agent-hub live recover`: outcome mapping and exit semantics
// ---------------------------------------------------------------------------

describe("live recover", () => {
  it("maps kept-live / recovered / manual outcomes to the document and exit code", async () => {
    const mixed = await runLiveRecover("/unused", {
      manager: {
        recover: async () => ({
          scanned: 3,
          sessions: [
            { live_session_id: "s-1", outcome: "recovered" as const, detail: "provider reaped, worktree pinned" },
            { live_session_id: "s-2", outcome: "kept-live" as const, detail: "this hub process owns the session" },
            { live_session_id: "s-3", outcome: "manual" as const, detail: "lease corrupt; refusing to guess" },
          ],
        }),
      },
    });
    expect(mixed.document).toMatchObject({ scanned: 3, requires_manual: true });
    expect(mixed.exitCode).toBe(1); // a manual outcome is the caller's to act on

    const settled = await runLiveRecover("/unused", {
      manager: {
        recover: async () => ({
          scanned: 2,
          sessions: [
            { live_session_id: "s-1", outcome: "kept-live" as const, detail: "still owned" },
            { live_session_id: "s-2", outcome: "recovered" as const, detail: "rewritten to orphaned" },
          ],
        }),
      },
    });
    expect(settled.document.requires_manual).toBe(false);
    expect(settled.exitCode).toBe(0); // settled outcomes — auto or humanless — exit clean
  });

  it("reports a lease-free workspace through runCli with exit 0", async () => {
    const repository = await createGitRepository();
    const stdout: string[] = [];
    const exitCode = await runCli(["live", "recover", "--workspace", repository], {
      stdout: (value) => stdout.push(value),
      stderr: () => {},
    });
    await removeDirectory(repository);

    expect(exitCode).toBe(0);
    expect(JSON.parse(stdout.join(""))).toMatchObject({
      scanned: 0,
      sessions: [],
      requires_manual: false,
    });
  });

  it("exits 1 through runCli when a corrupt lease needs a human", async () => {
    const repository = await createGitRepository();
    const identity = await resolveRepositoryIdentity(repository);
    const leasesRoot = join(identity.common_dir, "agent-hub", "live", "leases");
    await mkdir(leasesRoot, { recursive: true });
    // Recovery must refuse to guess ownership of an unparseable lease.
    await writeFile(join(leasesRoot, `${randomUUID()}.lease.json`), "{ not a lease");

    const stdout: string[] = [];
    const exitCode = await runCli(["live", "recover", "--workspace", repository], {
      stdout: (value) => stdout.push(value),
      stderr: () => {},
    });
    await removeDirectory(repository);

    expect(exitCode).toBe(1);
    const document = JSON.parse(stdout.join(""));
    expect(document).toMatchObject({ scanned: 1, requires_manual: true });
    expect(document.sessions[0]).toMatchObject({ outcome: "manual" });
  });
});
