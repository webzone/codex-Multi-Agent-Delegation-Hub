import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { describe, expect, it } from "vitest";

import { createHubServer } from "../src/mcp.js";
import { resolveRepositoryIdentity } from "../src/git.js";
import { LiveSessionManager, LIVE_DEFAULT_MAX_TEXT_BYTES } from "../src/live/manager.js";
import { liveRefFor, liveStatePath } from "../src/live/state.js";
import type { LiveLeaseProbes } from "../src/live/lease.js";
import type { RepositoryIdentity } from "../src/types.js";
import type {
  LiveBoundedText,
  LiveCapabilities,
  LiveCapabilityClaim,
  LiveCommand,
  LiveEvent,
  LiveEventBody,
  LiveLaunchRequest,
  LiveLaunchReport,
  LiveProbeResult,
  LiveProviderId,
  LiveStopMode,
  LiveStopReport,
  LiveTransport,
  LiveTransportDescriptor,
  LiveTransportFactory,
  LiveTransportId,
  ProviderResumeState,
} from "../src/live/types.js";

import { createGitRepository, removeDirectory, resolveRef } from "./helpers.js";

// ---------------------------------------------------------------------------
// A scripted transport wired into the CORE LiveSessionManager, injected as
// `dependencies.live` — the exact production seam (production builds the same
// manager via `createLiveManager`; here the transports are fakes and the
// repository is a throwaway git fixture under the OS temp dir).
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

/**
 * Post-handshake resume state built from what the fake actually observed,
 * mirroring the real transports: `verified` only ever becomes true when a
 * durable hint round-tripped through the fake session identity.
 */
function scriptedResumeState(
  provider: LiveProviderId,
  observedSessionId: string,
  prior: ProviderResumeState | null,
): ProviderResumeState {
  const roundTripped =
    prior !== null && prior.provider === provider && prior.provider_session_id === observedSessionId;
  const verification = roundTripped
    ? { verified: true as const, verified_via: `fake-stream-echo:${observedSessionId}` }
    : { verified: false as const, verified_via: null };
  switch (provider) {
    case "omp":
      return {
        provider: "omp",
        provider_session_id: observedSessionId,
        ...verification,
        last_event_seq: prior?.provider === "omp" ? prior.last_event_seq : 0,
      };
    case "agy":
      return {
        provider: "agy",
        provider_session_id: observedSessionId,
        ...verification,
        resume_argv_verified: false,
      };
    case "pi":
      return {
        provider: "pi",
        provider_session_id: observedSessionId,
        ...verification,
        resume_token: prior?.provider === "pi" ? prior.resume_token : "tok-1",
      };
    case "hermes":
      return {
        provider: "hermes",
        provider_session_id: observedSessionId,
        ...verification,
        session_load_advertised:
          prior?.provider === "hermes" ? prior.session_load_advertised : false,
      };
  }
}

class ScriptedTransport implements LiveTransport {
  readonly id: LiveTransportId;
  readonly provider: LiveProviderId;
  readonly launchRequests: LiveLaunchRequest[] = [];
  readonly sent: LiveCommand[] = [];
  readonly stopCalls: LiveStopMode[] = [];
  reportProcessCalls = 0;
  /** Gates every stop() until resolved; proves manager pipelines never overlap. */
  holdStop: Promise<void> | null = null;
  stopPolicy: ((mode: LiveStopMode) => LiveStopReport | undefined) | null = null;
  activeStops = 0;
  maxActiveStops = 0;
  private queue: LiveEvent[] = [];
  private wakeups: Array<() => void> = [];
  private ended = false;
  private seq = 0;
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
    // The new launch contract: a spawned provider reports its process facts
    // before any handshake can fail.
    if (request.report_process) {
      this.reportProcessCalls += 1;
      await request.report_process({ pid: 4321, pgid: 4321 });
    }
    return {
      pid: 4321,
      provider_session_id: "psess-1",
      launched_at: new Date().toISOString(),
      resume_state: scriptedResumeState(this.provider, "psess-1", request.resume),
    };
  }

  async send(command: LiveCommand): Promise<void> {
    this.sent.push(command);
    await Promise.resolve();
    switch (command.kind) {
      case "prompt":
      case "follow_up": {
        this.emit({ kind: "status", status: "running", note: null });
        if (command.text.includes("ask")) {
          this.emit({
            kind: "permission_request",
            request_id: "req-9",
            tool: "write",
            summary: bounded("may I write?"),
          });
          return;
        }
        this.emit({
          kind: "text",
          role: "assistant",
          stream_id: "s-1",
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
          stream_id: "s-1",
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
      const wake = Promise.withResolvers<void>();
      this.wakeups.push(wake.resolve);
      await wake.promise;
    }
  }

  async stop(mode: LiveStopMode): Promise<LiveStopReport> {
    this.stopCalls.push(mode);
    this.activeStops += 1;
    this.maxActiveStops = Math.max(this.maxActiveStops, this.activeStops);
    try {
      if (this.holdStop) {
        await this.holdStop;
      }
      const report: LiveStopReport =
        this.stopPolicy?.(mode) ?? { status: "closed", exit_code: 0, exit_signal: null, waited_ms: 5 };
      if (report.status === "closed" && !this.ended) {
        this.emit({ kind: "exit", intentional: true, exit_code: 0, exit_signal: null });
        this.ended = true;
      }
      return report;
    } finally {
      this.activeStops -= 1;
    }
  }
}

class ScriptedFactory implements LiveTransportFactory {
  readonly transport: LiveTransportId;
  readonly provider: LiveProviderId;
  readonly created: ScriptedTransport[] = [];

  constructor(
    transport: LiveTransportId,
    readonly capabilities: LiveCapabilities = capabilitySnapshot(),
  ) {
    this.transport = transport;
    this.provider = PROVIDER_BY_TRANSPORT[transport];
  }

  async probe(): Promise<LiveProbeResult> {
    return { found: true, version: "9.9.9-scripted", detail: "scripted provider present" };
  }

  create(): ScriptedTransport {
    const transport = new ScriptedTransport(this.transport, this.capabilities);
    this.created.push(transport);
    return transport;
  }
}

/** Lease probes that never touch the real OS (same fixture as live-core). */
function fakeProbes(): LiveLeaseProbes {
  return {
    probePid: (pid) => (pid === process.pid ? "live" : "dead"),
    startToken: async (pid) => (pid === process.pid ? "hub-tok" : "prov-tok"),
    killGroup: () => true,
    probeGroup: (pgid) => (pgid === process.pid ? "alive" : "gone"),
    now: () => new Date(),
  };
}

interface Harness {
  repository: string;
  identity: RepositoryIdentity;
  commonDir: string;
  tmpRoot: string;
  manager: LiveSessionManager;
  factories: ScriptedFactory[];
  client: Client;
  cleanup: () => Promise<void>;
}

async function harness(
  options: { capabilities?: LiveCapabilities; factories?: ScriptedFactory[] } = {},
): Promise<Harness> {
  const repository = await createGitRepository();
  const identity = await resolveRepositoryIdentity(repository);
  const tmpRoot = await mkdtemp(join(tmpdir(), "agent-hub-live-mcp-"));
  const factories = options.factories ?? [new ScriptedFactory("pi-rpc", options.capabilities)];
  const manager = new LiveSessionManager({
    commonDir: identity.common_dir,
    repositoryCwd: repository,
    transportFactories: factories,
    tmpRoot,
    leaseProbes: fakeProbes(),
  });
  const client = await connectClient(createHubServer({ live: manager }));
  return {
    repository,
    identity,
    commonDir: identity.common_dir,
    tmpRoot,
    manager,
    factories,
    client,
    cleanup: async () => {
      await manager.closeAll();
      await removeDirectory(repository);
      await removeDirectory(tmpRoot);
    },
  };
}

async function tick(): Promise<void> {
  const deferred = Promise.withResolvers<void>();
  setImmediate(deferred.resolve);
  await deferred.promise;
}

/**
 * The hub event ring is stamped BEFORE the pump handles each event, and the
 * permission_request branch adds to `open_permissions` synchronously, so a
 * request visible in the ring is guaranteed answerable. Poll it rather than
 * racing the durable status writes the pump performs in between.
 */
async function waitForPermissionRequests(
  manager: LiveSessionManager,
  liveSessionId: string,
  count: number,
): Promise<void> {
  for (let attempt = 0; attempt < 10_000; attempt += 1) {
    const { events } = manager.eventsAfter(liveSessionId, 0);
    const seen = events.filter((event) => event.body.kind === "permission_request").length;
    if (seen >= count) {
      return;
    }
    await tick();
  }
  throw new Error(`permission request #${count} never reached the hub event ring`);
}

async function connectClient(server: McpServer): Promise<Client> {
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  const client = new Client({ name: "live-test-client", version: "0.0.0" }, { capabilities: {} });
  await client.connect(clientTransport);
  return client;
}

function textOf(result: unknown): string {
  const missing = new Error("expected a content-bearing tool result");
  if (typeof result !== "object" || result === null || !("content" in result)) {
    throw missing;
  }
  const content: unknown = result.content;
  const first = Array.isArray(content) ? content[0] : undefined;
  if (
    typeof first !== "object" ||
    first === null ||
    !("text" in first) ||
    typeof first.text !== "string"
  ) {
    throw missing;
  }
  return first.text;
}

function documentFrom(result: unknown): Record<string, any> {
  return JSON.parse(textOf(result)) as Record<string, any>;
}

async function startLive(
  h: Harness,
  agent: LiveProviderId = "pi",
): Promise<Record<string, any>> {
  const result = await h.client.callTool({
    name: "live_session_start",
    arguments: { agent, workspace: h.repository },
  });
  expect(result.isError ?? false).toBe(false);
  return documentFrom(result);
}

async function command(
  h: Harness,
  liveSessionId: string,
  args: Record<string, unknown>,
): Promise<unknown> {
  return h.client.callTool({
    name: "live_session_command",
    arguments: { live_session_id: liveSessionId, ...args },
  });
}

const HEAVY = 30_000;

// ---------------------------------------------------------------------------

describe("live MCP tools", () => {
  it("registers the five live tools next to the unchanged legacy tools", async () => {
    const h = await harness();
    const { tools } = await h.client.listTools();

    expect(tools.map((tool) => tool.name)).toEqual(
      expect.arrayContaining([
        "delegate_task",
        "fanout_candidates",
        "compete_candidates",
        "session_create",
        "session_resume",
        "live_session_start",
        "live_session_resume",
        "live_session_command",
        "live_session_events",
        "live_session_close",
      ]),
    );
    await h.cleanup();
  });

  it("starts a live session and answers with the frozen capability snapshot", async () => {
    const h = await harness();
    const factory = h.factories[0]!;

    const document = await startLive(h);
    expect(document).toMatchObject({
      capabilities: capabilitySnapshot(),
      state: {
        schema: "agent-hub-live/v1",
        provider: "pi",
        transport: "pi-rpc",
        status: "idle",
        revision: 1,
        checkpoint_seq: 0,
        last_checkpoint_reason: null,
        base_commit: h.identity.head,
        current_commit: h.identity.head,
      },
    });
    const liveSessionId = document.live_session_id as string;
    expect(liveSessionId).toMatch(/^[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}$/);

    // The provider runs in a hub-owned OS-temp worktree, never the caller checkout.
    expect(document.state.worktree_path).toBe(document.workspace);
    expect(document.state.worktree_parent).toBe(dirname(document.workspace as string));
    expect((document.workspace as string).startsWith(join(h.tmpRoot, "agent-hub-live-"))).toBe(
      true,
    );
    expect(document.workspace).not.toBe(h.repository);
    expect(document.state.identity).toEqual(h.identity);

    // The launch request carries provider cwd = the hub worktree, the default
    // byte bound, no resume hint, and reported its process facts at spawn.
    const launch = factory.created[0]!.launchRequests[0]!;
    expect(launch.workspace).toBe(document.workspace);
    expect(launch.live_session_id).toBe(liveSessionId);
    expect(launch.max_text_bytes).toBe(LIVE_DEFAULT_MAX_TEXT_BYTES);
    expect(launch.resume).toBeNull();
    expect(factory.created[0]!.reportProcessCalls).toBe(1);

    // The durable record landed next to the repository's live state root.
    const durable = JSON.parse(
      await readFile(liveStatePath(h.commonDir, liveSessionId), "utf8"),
    ) as Record<string, unknown>;
    expect(durable).toMatchObject({
      schema: "agent-hub-live/v1",
      live_session_id: liveSessionId,
      provider: "pi",
      transport: "pi-rpc",
      worktree_path: document.workspace,
      worktree_parent: document.state.worktree_parent,
    });

    await h.cleanup();
  });

  it("fails honestly when the fake transport registry lacks the provider", async () => {
    const h = await harness({ factories: [] });

    const result = await h.client.callTool({
      name: "live_session_start",
      arguments: { agent: "hermes", workspace: h.repository },
    });

    expect(result.isError).toBe(true);
    expect(documentFrom(result).error).toMatchObject({ code: "LIVE_TRANSPORT_UNAVAILABLE" });
    expect(h.manager.activeCount).toBe(0);
    await h.cleanup();
  });

  it(
    "runs a prompt turn, exposes events by cursor, and gates the second prompt",
    async () => {
      const h = await harness();
      const start = await startLive(h);
      const id = start.live_session_id as string;

      const prompt = documentFrom(await command(h, id, { action: "prompt", text: "write tests" }));
      expect(prompt.status).toBe("idle");
      expect(prompt.result).toMatchObject({
        kind: "prompt",
        outcome: "succeeded",
        final_text: { text: "Answer: write tests", truncated: false },
        checkpoint: null,
        usage: { input_tokens: 10, output_tokens: 2, cached_tokens: null, cost_usd: null },
      });

      const page1 = documentFrom(
        await h.client.callTool({
          name: "live_session_events",
          arguments: { live_session_id: id, cursor: 0 },
        }),
      );
      const seen: number[] = page1.events.map((event: { seq: number }) => event.seq);
      expect(seen).toEqual(seen.map((_: number, index: number) => index + 1)); // 1-based, no gaps
      expect(page1.next_cursor).toBe(seen[seen.length - 1]);
      expect(
        page1.events.every(
          (event: { live_session_id: string; transport: string }) =>
            event.live_session_id === id && event.transport === "pi-rpc",
        ),
      ).toBe(true); // the hub owns the envelope, not the transport
      expect(
        page1.events.some(
          (event: { body: { kind: string; text?: { text: string } } }) =>
            event.body.kind === "text" && event.body.text?.text === "Answer: write tests",
        ),
      ).toBe(true);

      const page2 = documentFrom(
        await h.client.callTool({
          name: "live_session_events",
          arguments: { live_session_id: id, cursor: page1.next_cursor },
        }),
      );
      expect(page2.events).toEqual([]);
      expect(page2.next_cursor).toBe(page1.next_cursor);

      // The one prompt is spent — the second prompt is refused as a caller error.
      const second = await command(h, id, { action: "prompt", text: "again" });
      expect(second.isError).toBe(true);
      expect(documentFrom(second).error).toMatchObject({
        code: "LIVE_PROMPT_ALREADY_ACCEPTED",
      });

      // ...and the session remains usable for follow-ups.
      const follow = documentFrom(await command(h, id, { action: "follow_up", text: "and more" }));
      expect(follow.result).toMatchObject({
        kind: "follow_up",
        outcome: "succeeded",
        final_text: { text: "Answer: and more" },
      });

      await h.cleanup();
    },
    HEAVY,
  );

  it("refuses a capability-unsupported steer pre-dispatch with isError", async () => {
    const h = await harness({ capabilities: capabilitySnapshot({ steer: unclaimed() }) });
    const start = await startLive(h);

    const result = await command(h, start.live_session_id as string, {
      action: "steer",
      text: "left",
    });

    expect(result.isError).toBe(true);
    const document = documentFrom(result);
    expect(document.result).toMatchObject({ outcome: "unsupported" });
    expect(document.result.error).toMatchObject({ stage: "capability", retryable: false });
    expect(h.factories[0]!.created[0]!.sent).toHaveLength(0); // never delivered
    await h.cleanup();
  });

  it(
    "cancels an in-flight turn through the command tool",
    async () => {
      const h = await harness();
      const start = await startLive(h);
      const id = start.live_session_id as string;

      // The prompt turn parks on the scripted permission request; cancel settles it.
      const promptCall = command(h, id, { action: "prompt", text: "ask me first" });
      const cancel = documentFrom(
        await command(h, id, { action: "cancel", reason: "changed my mind" }),
      );
      expect(cancel.result).toMatchObject({ kind: "cancel", outcome: "succeeded" });

      const prompt = documentFrom(await promptCall);
      expect(prompt.result).toMatchObject({
        kind: "prompt",
        outcome: "cancelled",
        checkpoint: null,
        error: null,
      });
      await h.cleanup();
    },
    HEAVY,
  );

  it(
    "answers permission requests with allow_once or deny only — unknown ids and widened verdicts are honest errors",
    async () => {
      const h = await harness();
      const start = await startLive(h);
      const id = start.live_session_id as string;

      const promptCall = command(h, id, { action: "prompt", text: "ask me first" });
      await waitForPermissionRequests(h.manager, id, 1);

      const unknown = await command(h, id, {
        action: "permission_response",
        request_id: "not-observed",
        decision: "deny",
      });
      expect(unknown.isError).toBe(true);
      expect(documentFrom(unknown).error).toMatchObject({
        code: "LIVE_PERMISSION_REQUEST_UNKNOWN",
      });

      const allow = documentFrom(
        await command(h, id, {
          action: "permission_response",
          request_id: "req-9",
          decision: "allow_once",
          note: "ship it",
        }),
      );
      expect(allow.result).toMatchObject({ kind: "permission_response", outcome: "succeeded" });
      const turn1 = documentFrom(await promptCall);
      expect(turn1.result).toMatchObject({
        kind: "prompt",
        outcome: "succeeded",
        final_text: { text: "permitted: allow_once" },
      });

      const askAgain = command(h, id, { action: "follow_up", text: "ask again" });
      await waitForPermissionRequests(h.manager, id, 2);
      const deny = await command(h, id, {
        action: "permission_response",
        request_id: "req-9",
        decision: "deny",
      });
      expect(deny.isError ?? false).toBe(false);
      const turn2 = documentFrom(await askAgain);
      expect(turn2.result).toMatchObject({
        kind: "follow_up",
        outcome: "succeeded",
        final_text: { text: "permitted: deny" },
      });

      // A widened verdict never reaches the manager: the tool schema refuses it.
      const widened = await command(h, id, {
        action: "permission_response",
        request_id: "req-9",
        decision: "allow_session",
      });
      expect(widened.isError).toBe(true);
      expect(textOf(widened)).toContain('one of "allow_once"|"deny"');

      // Only the two legal verdicts ever crossed the transport boundary.
      const answered = h.factories[0]!.created
        .flatMap((transport) => transport.sent)
        .filter((sent): sent is Extract<LiveCommand, { kind: "permission_response" }> =>
          sent.kind === "permission_response",
        );
      expect(answered.map((sent) => sent.decision)).toEqual(["allow_once", "deny"]);
      await h.cleanup();
    },
    HEAVY,
  );

  it(
    "closes honestly, pins the close checkpoint, and refuses later commands",
    async () => {
      const h = await harness();
      const start = await startLive(h);
      const id = start.live_session_id as string;

      // Uncommitted work in the hub worktree must be pinned by the close.
      await writeFile(join(start.workspace as string, "close-me.txt"), "pin me\n");
      const closeResult = await h.client.callTool({
        name: "live_session_close",
        arguments: { live_session_id: id, terminate: true },
      });
      expect(closeResult.isError ?? false).toBe(false);
      const close = documentFrom(closeResult);
      expect(close.stop).toMatchObject({ status: "closed", exit_code: 0, exit_signal: null });
      expect(close.checkpoint_taken).toBe(true);
      expect(close.cleanup_errors).toEqual([]);
      expect(close.state).toMatchObject({
        schema: "agent-hub-live/v1",
        status: "closed",
        checkpoint_seq: 1,
        last_checkpoint_reason: "close",
        base_commit: h.identity.head,
        worktree_path: start.workspace,
        worktree_parent: start.state.worktree_parent,
      });
      expect(close.state.current_commit).not.toBe(h.identity.head);
      await expect(resolveRef(h.repository, liveRefFor(id))).resolves.toBe(
        close.state.current_commit,
      );

      // The tool passed `terminate: true` straight through as the stop mode,
      // and teardown took the worktree with it.
      expect(h.factories[0]!.created[0]!.stopCalls).toEqual(["terminate"]);
      await expect(stat(start.workspace as string)).rejects.toThrow();

      // Teardown dropped the session and its ring: later traffic is a hard
      // not-found, never a fake continuation.
      const after = await command(h, id, { action: "follow_up", text: "more" });
      expect(after.isError).toBe(true);
      expect(documentFrom(after).error).toMatchObject({ code: "LIVE_SESSION_NOT_FOUND" });
      const events = await h.client.callTool({
        name: "live_session_events",
        arguments: { live_session_id: id },
      });
      expect(events.isError).toBe(true);
      expect(documentFrom(events).error).toMatchObject({ code: "LIVE_SESSION_NOT_FOUND" });

      await h.cleanup();
    },
    HEAVY,
  );

  it(
    "single-flights concurrent live_session_close calls: one pipeline, one shutdown report",
    async () => {
      const h = await harness();
      const start = await startLive(h);
      const id = start.live_session_id as string;
      const transport = h.factories[0]!.created[0]!;

      let release!: () => void;
      transport.holdStop = new Promise<void>((resolve) => {
        release = resolve;
      });

      const closeA = h.client.callTool({
        name: "live_session_close",
        arguments: { live_session_id: id },
      });
      const closeB = h.client.callTool({
        name: "live_session_close",
        arguments: { live_session_id: id },
      });
      release();
      const [a, b] = await Promise.all([closeA, closeB]);

      expect(a.isError ?? false).toBe(false);
      expect(b.isError ?? false).toBe(false);
      const docA = documentFrom(a);
      expect(documentFrom(b)).toEqual(docA);
      expect(docA.stop).toMatchObject({ status: "closed" });
      // Exactly one shutdown ran behind the two tool calls.
      expect(transport.stopCalls).toEqual(["graceful"]);
      expect(transport.maxActiveStops).toBe(1);
      expect(docA.state).toMatchObject({ status: "closed" });
      await h.cleanup();
    },
    HEAVY,
  );

  it(
    "simultaneous graceful and terminate closes serialize: the terminate upgrades the orphan",
    async () => {
      const h = await harness();
      const start = await startLive(h);
      const id = start.live_session_id as string;
      const transport = h.factories[0]!.created[0]!;

      let release!: () => void;
      transport.holdStop = new Promise<void>((resolve) => {
        release = resolve;
      });
      transport.stopPolicy = (mode) =>
        mode === "graceful"
          ? { status: "orphaned", exit_code: 0, exit_signal: null, waited_ms: 2 }
          : { status: "closed", exit_code: 0, exit_signal: "SIGKILL", waited_ms: 3 };

      const closeA = h.client.callTool({
        name: "live_session_close",
        arguments: { live_session_id: id },
      });
      const closeB = h.client.callTool({
        name: "live_session_close",
        arguments: { live_session_id: id, terminate: true },
      });
      release();
      const [a, b] = await Promise.all([closeA, closeB]);

      // An orphaned close is honestly an isError on this tool — the upgrade
      // below is exactly what the honest report tells the caller to do.
      expect(a.isError).toBe(true);
      expect(b.isError ?? false).toBe(false);
      const orphaned = documentFrom(a);
      const closed = documentFrom(b);
      expect(orphaned.stop).toMatchObject({ status: "orphaned" });
      expect(orphaned.state).toMatchObject({ status: "orphaned" });
      expect(orphaned.checkpoint_taken).toBe(false);
      expect(closed.stop).toMatchObject({ status: "closed", exit_signal: "SIGKILL" });
      expect(closed.state).toMatchObject({ status: "closed" });

      // One stop per pipeline, strictly serialized; one teardown.
      expect(transport.stopCalls).toEqual(["graceful", "terminate"]);
      expect(transport.maxActiveStops).toBe(1);
      await expect(stat(start.workspace as string)).rejects.toThrow();
      await h.cleanup();
    },
    HEAVY,
  );

  it(
    "resumes a terminal durable record under the same live session id",
    async () => {
      const h = await harness();
      const factory = h.factories[0]!;
      const start = await startLive(h);
      const id = start.live_session_id as string;

      await writeFile(join(start.workspace as string, "work.txt"), "durable\n");
      const closed = documentFrom(
        await h.client.callTool({
          name: "live_session_close",
          arguments: { live_session_id: id },
        }),
      );
      expect(closed.state.status).toBe("closed");
      expect(closed.checkpoint_taken).toBe(true);

      const resumed = await h.client.callTool({
        name: "live_session_resume",
        arguments: { live_session_id: id, workspace: h.repository },
      });
      expect(resumed.isError ?? false).toBe(false);
      const document = documentFrom(resumed);
      expect(document.live_session_id).toBe(id); // same ref, never a new one
      expect(document.state).toMatchObject({
        schema: "agent-hub-live/v1",
        live_session_id: id,
        status: "idle",
        provider: "pi",
        transport: "pi-rpc",
        revision: (closed.state.revision as number) + 1,
        current_commit: closed.state.current_commit,
        resume: {
          provider: "pi",
          provider_session_id: "psess-1",
          verified: true,
          verified_via: "fake-stream-echo:psess-1",
        },
      });
      expect(document.workspace).not.toBe(start.workspace); // a FRESH worktree
      expect(document.state.worktree_path).toBe(document.workspace);
      // Materialized at the chain head: the pinned work is on disk again.
      expect(await readFile(join(document.workspace as string, "work.txt"), "utf8")).toBe(
        "durable\n",
      );

      // The transport received the durable resume hint on the restart launch.
      expect(factory.created).toHaveLength(2);
      const resumeLaunch = factory.created[1]!.launchRequests[0]!;
      expect(resumeLaunch.resume).toEqual(closed.state.resume);
      expect(resumeLaunch.workspace).toBe(document.workspace);

      // A malformed id is refused by the id-shape gate; a well-formed unknown
      // id reports the absence honestly.
      const malformed = await h.client.callTool({
        name: "live_session_resume",
        arguments: { live_session_id: "live-unknown", workspace: h.repository },
      });
      expect(malformed.isError).toBe(true);
      expect(documentFrom(malformed).error).toMatchObject({ code: "LIVE_SESSION_ID_INVALID" });
      const missing = await h.client.callTool({
        name: "live_session_resume",
        arguments: { live_session_id: randomUUID(), workspace: h.repository },
      });
      expect(missing.isError).toBe(true);
      expect(documentFrom(missing).error).toMatchObject({ code: "LIVE_SESSION_NOT_FOUND" });

      await h.cleanup();
    },
    HEAVY,
  );

  it("answers unknown sessions with structured tool errors, never a throw", async () => {
    const h = await harness();

    for (const name of ["live_session_command", "live_session_events", "live_session_close"]) {
      const result =
        name === "live_session_command"
          ? await command(h, "live-ghost", { action: "status" })
          : await h.client.callTool({
              name,
              arguments: { live_session_id: "live-ghost" },
            });
      expect(result.isError).toBe(true);
      expect(documentFrom(result).error).toMatchObject({ code: "LIVE_SESSION_NOT_FOUND" });
    }
    await h.cleanup();
  });
});
