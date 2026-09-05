import { randomUUID } from "node:crypto";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { describe, expect, it } from "vitest";

import { AgentHubError } from "../src/errors.js";
import { createHubServer } from "../src/mcp.js";
import { LiveSessionManager, LiveTransportRegistry } from "../src/live/index.js";
import type { LiveResumeSource } from "../src/live/index.js";
import type {
  LiveBoundedText,
  LiveCapabilities,
  LiveCapabilityClaim,
  LiveCommand,
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
// Scripted transport harness (same contract surface as the CLI test; the
// file boundary keeps each test file self-contained).
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
  private queue: Array<{
    live_session_id: string;
    seq: number;
    transport: LiveTransportId;
    occurred_at: string;
    body: LiveEventBody;
  }> = [];
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
    this.emit({ kind: "status", status: "starting", note: null });
    this.emit({ kind: "status", status: "idle", note: null });
    return { pid: 4321, provider_session_id: "psess-1", launched_at: new Date().toISOString() };
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

  async *events() {
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
  ) {
    this.transport = transport;
    this.provider = PROVIDER_BY_TRANSPORT[transport];
  }

  async probe() {
    return { found: true, version: "9.9.9-scripted", detail: "scripted provider present" };
  }

  create(): ScriptedTransport {
    const transport = new ScriptedTransport(this.transport, this.capabilities);
    this.created.push(transport);
    return transport;
  }
}

function managerWith(factories: LiveTransportFactory[]): LiveSessionManager {
  const registry = new LiveTransportRegistry();
  for (const factory of factories) {
    registry.register(factory);
  }
  return new LiveSessionManager({ registry, launchSettleMs: 500, turnTimeoutMs: 5_000 });
}

async function connectClient(server: McpServer): Promise<Client> {
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  const client = new Client({ name: "live-test-client", version: "0.0.0" }, { capabilities: {} });
  await client.connect(clientTransport);
  return client;
}

function documentFrom(result: unknown): Record<string, any> {
  const content = (result as { content?: unknown }).content;
  if (!Array.isArray(content) || content.length === 0) {
    throw new Error("expected a content-bearing tool result");
  }
  return JSON.parse((content[0] as { text: string }).text) as Record<string, any>;
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

describe("live MCP tools", () => {
  it("registers the five live tools next to the unchanged legacy tools", async () => {
    const client = await connectClient(createHubServer({ live: managerWith([]) }));
    const { tools } = await client.listTools();

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
  });

  it("starts a live session and answers with the frozen capability snapshot", async () => {
    const factory = new ScriptedFactory("pi-rpc");
    const client = await connectClient(createHubServer({ live: managerWith([factory]) }));

    const result = await client.callTool({
      name: "live_session_start",
      arguments: { agent: "pi", workspace: "/tmp/repo" },
    });

    expect(result.isError ?? false).toBe(false);
    const document = documentFrom(result);
    expect(document).toMatchObject({
      provider: "pi",
      transport: "pi-rpc",
      pid: 4321,
      provider_session_id: "psess-1",
      capabilities: { prompt: { support: "native", evidence: "scripted prompt round-trip" } },
    });
    // The launch request carries the default byte bound and no resume hint.
    expect(factory.created[0]?.launchRequests[0]).toMatchObject({
      max_text_bytes: 32_768,
      resume: null,
    });
  });

  it("fails honestly when the provider transport is unwired", async () => {
    const client = await connectClient(createHubServer({ live: managerWith([]) }));

    const result = await client.callTool({
      name: "live_session_start",
      arguments: { agent: "hermes", workspace: "/tmp/repo" },
    });

    expect(result.isError).toBe(true);
    expect(documentFrom(result).error).toMatchObject({ code: "LIVE_TRANSPORT_UNAVAILABLE" });
  });

  it("runs a prompt turn, exposes events by cursor, and gates the second prompt", async () => {
    const factory = new ScriptedFactory("pi-rpc");
    const client = await connectClient(createHubServer({ live: managerWith([factory]) }));

    const start = documentFrom(
      await client.callTool({
        name: "live_session_start",
        arguments: { agent: "pi", workspace: "/tmp/repo" },
      }),
    );
    const id = start.live_session_id as string;

    const prompt = await client.callTool({
      name: "live_session_command",
      arguments: { live_session_id: id, action: "prompt", text: "write tests" },
    });
    expect(prompt.isError ?? false).toBe(false);
    const promptDoc = documentFrom(prompt);
    expect(promptDoc).toMatchObject({ status: "idle" });
    expect(promptDoc.result).toMatchObject({
      kind: "prompt",
      outcome: "succeeded",
      final_text: { text: "Answer: write tests" },
      checkpoint: null,
    });

    const page1 = documentFrom(
      await client.callTool({
        name: "live_session_events",
        arguments: { live_session_id: id, cursor: 0, limit: 3 },
      }),
    );
    expect(page1.events).toHaveLength(3);
    expect(page1.next_cursor).toBe(3);
    expect(page1.earliest_seq).toBe(1);
    expect(page1.dropped).toBe(false);

    const page2 = documentFrom(
      await client.callTool({
        name: "live_session_events",
        arguments: { live_session_id: id, cursor: page1.next_cursor, limit: 100 },
      }),
    );
    expect(page2.events[0].seq).toBe(4);
    const seen: number[] = page2.events.map((event: { seq: number }) => event.seq);
    expect(seen).toEqual(seen.map((_: number, index: number) => index + 4)); // contiguous after cursor

    const second = await client.callTool({
      name: "live_session_command",
      arguments: { live_session_id: id, action: "prompt", text: "again" },
    });
    expect(second.isError).toBe(true);
    const secondDoc = documentFrom(second);
    expect(secondDoc.result).toMatchObject({ outcome: "failed" });
    expect(secondDoc.result.error).toMatchObject({ code: "LIVE_STATE_REJECTED" });
  });

  it("refuses a capability-unsupported steer pre-dispatch with isError", async () => {
    const factory = new ScriptedFactory("pi-rpc", capabilitySnapshot({ steer: unclaimed() }));
    const client = await connectClient(createHubServer({ live: managerWith([factory]) }));

    const start = documentFrom(
      await client.callTool({
        name: "live_session_start",
        arguments: { agent: "pi", workspace: "/tmp/repo" },
      }),
    );
    const result = await client.callTool({
      name: "live_session_command",
      arguments: { live_session_id: start.live_session_id, action: "steer", text: "left" },
    });

    expect(result.isError).toBe(true);
    const document = documentFrom(result);
    expect(document.result).toMatchObject({ outcome: "unsupported" });
    expect(document.result.error).toMatchObject({ stage: "capability", retryable: false });
    expect(factory.created[0]?.sent).toHaveLength(0); // never delivered
  });

  it("cancels an in-flight turn through the command tool", async () => {
    const factory = new ScriptedFactory("pi-rpc");
    const client = await connectClient(createHubServer({ live: managerWith([factory]) }));

    const start = documentFrom(
      await client.callTool({
        name: "live_session_start",
        arguments: { agent: "pi", workspace: "/tmp/repo" },
      }),
    );
    const id = start.live_session_id as string;

    // The prompt turn parks on the scripted permission request; cancel settles it.
    const promptCall = client.callTool({
      name: "live_session_command",
      arguments: { live_session_id: id, action: "prompt", text: "ask me first" },
    });
    const cancel = await client.callTool({
      name: "live_session_command",
      arguments: { live_session_id: id, action: "cancel", reason: "changed my mind" },
    });
    expect(cancel.isError ?? false).toBe(false);
    const prompt = documentFrom(await promptCall);
    expect(prompt.result).toMatchObject({ kind: "prompt", outcome: "cancelled" });
  });

  it("reports an unknown permission request as a failed tool result", async () => {
    const factory = new ScriptedFactory("pi-rpc");
    const client = await connectClient(createHubServer({ live: managerWith([factory]) }));

    const start = documentFrom(
      await client.callTool({
        name: "live_session_start",
        arguments: { agent: "pi", workspace: "/tmp/repo" },
      }),
    );
    const promptCall = client.callTool({
      name: "live_session_command",
      arguments: { live_session_id: start.live_session_id, action: "prompt", text: "ask me first" },
    });
    const answer = await client.callTool({
      name: "live_session_command",
      arguments: {
        live_session_id: start.live_session_id,
        action: "permission_response",
        request_id: "not-observed",
        decision: "deny",
      },
    });

    expect(answer.isError).toBe(true);
    expect(documentFrom(answer).result.error).toMatchObject({
      code: "LIVE_PERMISSION_REQUEST_UNKNOWN",
    });
    await client.callTool({
      name: "live_session_close",
      arguments: { live_session_id: start.live_session_id, terminate: true },
    });
    const prompt = documentFrom(await promptCall);
    expect(prompt.result.outcome).toBe("cancelled");
    // The tool passed `terminate: true` straight to the transport.
    expect(factory.created[0]?.stopCalls).toEqual(["terminate"]);
  });

  it("closes honestly and refuses later commands while events stay readable", async () => {
    const factory = new ScriptedFactory("pi-rpc");
    const client = await connectClient(createHubServer({ live: managerWith([factory]) }));

    const start = documentFrom(
      await client.callTool({
        name: "live_session_start",
        arguments: { agent: "pi", workspace: "/tmp/repo" },
      }),
    );
    const id = start.live_session_id as string;
    await client.callTool({
      name: "live_session_command",
      arguments: { live_session_id: id, action: "prompt", text: "work" },
    });

    const close = await client.callTool({
      name: "live_session_close",
      arguments: { live_session_id: id },
    });
    expect(close.isError ?? false).toBe(false);
    expect(documentFrom(close)).toMatchObject({
      status: "closed",
      stop: { status: "closed", exit_code: 0 },
    });

    const after = await client.callTool({
      name: "live_session_command",
      arguments: { live_session_id: id, action: "follow_up", text: "more" },
    });
    expect(after.isError).toBe(true);
    expect(documentFrom(after).result.error).toMatchObject({ code: "LIVE_SESSION_CLOSED" });

    const events = documentFrom(
      await client.callTool({ name: "live_session_events", arguments: { live_session_id: id } }),
    );
    expect(events.events.some((body: { seq: number }) => body.seq > 0)).toBe(true);
  });

  it("resumes through the durable seam and reports missing sessions honestly", async () => {
    const factory = new ScriptedFactory("pi-rpc");
    const state = liveStateFixture();
    const resumeSource: LiveResumeSource = {
      async load(_workspace: string, liveSessionId: string): Promise<LiveSessionState> {
        if (liveSessionId !== state.live_session_id) {
          throw new AgentHubError("LIVE_SESSION_NOT_FOUND", `no durable live session "${liveSessionId}"`);
        }
        return state;
      },
    };
    const client = await connectClient(
      createHubServer({ live: managerWith([factory]), liveResumeSource: resumeSource }),
    );

    const resumed = await client.callTool({
      name: "live_session_resume",
      arguments: { live_session_id: state.live_session_id, workspace: "/tmp/repo" },
    });
    expect(resumed.isError ?? false).toBe(false);
    const document = documentFrom(resumed);
    expect(document).toMatchObject({ live_session_id: state.live_session_id, provider: "pi" });
    expect(factory.created[0]?.launchRequests[0]?.resume).toEqual(state.resume);

    const missing = await client.callTool({
      name: "live_session_resume",
      arguments: { live_session_id: "live-unknown", workspace: "/tmp/repo" },
    });
    expect(missing.isError).toBe(true);
    expect(documentFrom(missing).error).toMatchObject({ code: "LIVE_SESSION_NOT_FOUND" });
  });

  it("answers unknown sessions with structured tool errors, never a throw", async () => {
    const client = await connectClient(createHubServer({ live: managerWith([]) }));

    for (const name of ["live_session_command", "live_session_events", "live_session_close"]) {
      const result = await client.callTool({
        name,
        arguments: {
          live_session_id: "live-ghost",
          ...(name === "live_session_command" ? { action: "status" as const } : {}),
        },
      });
      expect(result.isError).toBe(true);
      expect(documentFrom(result).error).toMatchObject({ code: "LIVE_SESSION_NOT_FOUND" });
    }
  });
});
