import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { describe, expect, it } from "vitest";

import { createHubServer } from "../src/mcp.js";
import type { AgentHubOptions } from "../src/hub/agent-hub.js";
import { AgentHubSupervisor } from "../src/hub/supervisor.js";
import { bridgeTransportFactory } from "../src/hub/transport-adapter.js";
import {
  fullHubCapabilities,
  hubFakeProbes,
  hubFakeProviderFactory,
  HubFakeFactory,
  type HubFakeTurnBehavior,
} from "./hub-fakes.js";
import { createGitRepository, removeDirectory } from "./helpers.js";

interface McpHarness {
  client: Client;
  repository: string;
  factory: HubFakeFactory;
  cleanup: () => Promise<void>;
}

async function mcpHarness(
  turn: HubFakeTurnBehavior = { writes: { "mcp.md": "worked\n" } },
  options: { resumeEcho?: boolean; capabilities?: () => ReturnType<typeof fullHubCapabilities> } = {},
): Promise<McpHarness> {
  const repository = await createGitRepository();
  const tmpRoot = await mkdtemp(join(tmpdir(), "agent-hub-mcp-"));
  const factory = new HubFakeFactory(
    options.capabilities ?? (() => fullHubCapabilities()),
    { resumeState: options.resumeEcho ? "echo" : undefined },
  );
  factory.defaultTurn = turn;
  const hubOptions: AgentHubOptions = {
    home: tmpRoot,
    transportFactories: [bridgeTransportFactory(factory)],
    providerFactories: [hubFakeProviderFactory],
    probes: hubFakeProbes(),
  };
  // Each harness gets its own supervisor so in-flight process quotas never
  // couple unrelated test files to each other.
  const server = createHubServer({ hubOptions, supervisor: new AgentHubSupervisor() });
  const client = new Client({ name: "hub-test", version: "0.0.0" });
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await Promise.all([server.connect(serverTransport), client.connect(clientTransport)]);
  return {
    client,
    repository,
    factory,
    cleanup: async () => {
      await client.close();
      await removeDirectory(repository);
    },
  };
}

type ToolResult = Awaited<ReturnType<Client["callTool"]>>;

function payload(result: ToolResult): Record<string, unknown> {
  return (result as { structuredContent?: Record<string, unknown> }).structuredContent as Record<
    string,
    unknown
  >;
}

function isError(result: ToolResult): boolean {
  return (result as { isError?: boolean }).isError === true;
}

async function startSession(
  client: Client,
  repository: string,
  extra: Record<string, unknown> = {},
): Promise<string> {
  const result = await client.callTool({
    name: "hub_start",
    arguments: { provider: "omp", workspace: repository, ...extra },
  });
  expect(isError(result)).toBe(false);
  return payload(result)["session_id"] as string;
}

describe("MCP tool surface", () => {
  it("exposes exactly the hub tool set", async () => {
    const world = await mcpHarness();
    const tools = await world.client.listTools();
    expect(tools.tools.map((tool) => tool.name).sort()).toEqual([
      "hub_cancel",
      "hub_close",
      "hub_command_status",
      "hub_events",
      "hub_follow_up",
      "hub_gc",
      "hub_handoff",
      "hub_permission",
      "hub_probe",
      "hub_prompt",
      "hub_resume",
      "hub_start",
      "hub_status",
      "hub_steer",
    ]);
    // No legacy vocabulary anywhere in descriptions/schemas.
    const blob = JSON.stringify(tools.tools);
    for (const banned of ["delegate", "fanout", "compete", "auto_merge", "live_session"]) {
      expect(blob.toLowerCase()).not.toContain(banned);
    }
    await world.cleanup();
  });

  it("runs start → prompt → events → close → handoff end to end", async () => {
    const world = await mcpHarness();
    const sessionId = await startSession(world.client, world.repository);

    const turn = await world.client.callTool({
      name: "hub_prompt",
      arguments: { session_id: sessionId, workspace: world.repository, text: "do it" },
    });
    expect(isError(turn)).toBe(false);
    expect(payload(turn)["outcome"]).toBe("succeeded");
    expect(payload(turn)["result"]).not.toBeNull();

    const events = await world.client.callTool({
      name: "hub_events",
      arguments: { session_id: sessionId, workspace: world.repository, after: 0 },
    });
    const eventsDoc = payload(events) as {
      status: string;
      events: Array<{ session_id: string }>;
      next_cursor: number;
    };
    expect(eventsDoc.status).toBe("ok");
    expect(eventsDoc.events.every((event) => event.session_id === sessionId)).toBe(true);

    const closed = await world.client.callTool({
      name: "hub_close",
      arguments: { session_id: sessionId, workspace: world.repository },
    });
    expect(isError(closed)).toBe(false);
    expect(payload(closed)["finalize"]).toBeDefined();
    expect((payload(closed)["record"] as { status: string }).status).toBe("closed");

    const result = payload(turn)["result"] as { seq: number; commit: string };

    const handoff = await world.client.callTool({
      name: "hub_handoff",
      arguments: {
        session_id: sessionId,
        workspace: world.repository,
        decision: "accepted",
        result_seq: result.seq,
        commit: result.commit,
      },
    });
    expect((payload(handoff)["handoff"] as { decision: string }).decision).toBe("accepted");

    const status = await world.client.callTool({
      name: "hub_status",
      arguments: { workspace: world.repository },
    });
    expect((payload(status)["sessions"] as unknown[])).toHaveLength(1);
    await world.cleanup();
  });

  it("allows handoff of a closed workspace with no published turns", async () => {
    const world = await mcpHarness();
    const sessionId = await startSession(world.client, world.repository);

    const closed = await world.client.callTool({
      name: "hub_close",
      arguments: { session_id: sessionId, workspace: world.repository },
    });
    expect(isError(closed)).toBe(false);

    const status = await world.client.callTool({
      name: "hub_status",
      arguments: { session_id: sessionId, workspace: world.repository },
    });
    const workspace = payload(status)["workspace"] as {
      last_result_seq: number;
      head_commit: string;
    };
    expect(workspace.last_result_seq).toBe(0);

    const handoff = await world.client.callTool({
      name: "hub_handoff",
      arguments: {
        session_id: sessionId,
        workspace: world.repository,
        decision: "discarded",
        result_seq: 0,
        commit: workspace.head_commit,
      },
    });
    expect(isError(handoff)).toBe(false);
    expect((payload(handoff)["handoff"] as { result_seq: number }).result_seq).toBe(0);
    await world.cleanup();
  });

  it("refuses commands for unknown sessions with structured errors", async () => {
    const world = await mcpHarness();
    const turn = await world.client.callTool({
      name: "hub_prompt",
      arguments: { session_id: "00000000-0000-4000-8000-000000000000", workspace: world.repository, text: "x" },
    });
    expect(isError(turn)).toBe(true);
    expect(String(JSON.stringify(payload(turn)))).toContain("SESSION_NOT_FOUND");
    await world.cleanup();
  });

  it("marks capability-unsupported steer as an isError turn document", async () => {
    const world = await mcpHarness(
      { writes: { "s.md": "x" } },
      {
        capabilities: () => fullHubCapabilities({ steer: { support: "unsupported", evidence: null } }),
      },
    );
    const sessionId = await startSession(world.client, world.repository);
    const steer = await world.client.callTool({
      name: "hub_steer",
      arguments: { session_id: sessionId, workspace: world.repository, text: "left" },
    });
    expect(isError(steer)).toBe(true);
    expect(payload(steer)["outcome"]).toBe("unsupported");
    expect((payload(steer)["error"] as { stage: string }).stage).toBe("capability");
    await world.cleanup();
  });

  it("enforces the two-verdict permission contract", async () => {
    const world = await mcpHarness({
      during: [
        {
          kind: "permission_request",
          request_id: "req-7",
          tool: "shell",
          summary: { text: "danger", truncated: false },
        },
      ],
    });
    const sessionId = await startSession(world.client, world.repository, {
      permission_policy: "interactive",
    });
    await world.client.callTool({
      name: "hub_prompt",
      arguments: { session_id: sessionId, workspace: world.repository, text: "run it" },
    });
    const bad = await world.client.callTool({
      name: "hub_permission",
      arguments: {
        session_id: sessionId,
        workspace: world.repository,
        request_id: "req-7",
        decision: "allow_always",
      },
    });
    expect(isError(bad)).toBe(true);
    expect(String(JSON.stringify(payload(bad)))).toContain("COMMAND_INVALID");

    const good = await world.client.callTool({
      name: "hub_permission",
      arguments: {
        session_id: sessionId,
        workspace: world.repository,
        request_id: "req-7",
        decision: "deny",
      },
    });
    expect(isError(good)).toBe(false);
    await world.cleanup();
  });

  it("answers derived status from stream evidence", async () => {
    const world = await mcpHarness();
    const sessionId = await startSession(world.client, world.repository);
    const status = await world.client.callTool({
      name: "hub_command_status",
      arguments: { session_id: sessionId, workspace: world.repository },
    });
    expect(isError(status)).toBe(false);
    const evidence = JSON.parse(
      (payload(status)["final_text"] as { text: string }).text,
    ) as { status: string };
    expect(evidence.status).toBe("idle");
    await world.cleanup();
  });

  it("resumes a closed session with verified identity", async () => {
    const world = await mcpHarness({}, { resumeEcho: true });
    const sessionId = await startSession(world.client, world.repository);
    await world.client.callTool({
      name: "hub_prompt",
      arguments: { session_id: sessionId, workspace: world.repository, text: "one" },
    });
    await world.client.callTool({
      name: "hub_close",
      arguments: { session_id: sessionId, workspace: world.repository },
    });
    const resumed = await world.client.callTool({
      name: "hub_resume",
      arguments: { session_id: sessionId, workspace: world.repository },
    });
    expect(isError(resumed)).toBe(false);
    expect(payload(resumed)["session_id"]).toBe(sessionId);
    await world.cleanup();
  });

  it("gc dry-run and probe answer without mutation", async () => {
    const world = await mcpHarness();
    const gc = await world.client.callTool({
      name: "hub_gc",
      arguments: { workspace: world.repository },
    });
    expect(isError(gc)).toBe(false);
    expect((payload(gc)["cleanup"] as { deleted: unknown[] }).deleted).toEqual([]);

    const probe = await world.client.callTool({
      name: "hub_probe",
      arguments: { provider: "omp" },
    });
    const probes = payload(probe)["probes"] as Array<{ found: boolean }>;
    expect(probes).toHaveLength(1);
    expect(probes[0]?.found).toBe(true);

    const unknown = await world.client.callTool({
      name: "hub_probe",
      arguments: { provider: "grok" },
    });
    expect(isError(unknown)).toBe(true);
    await world.cleanup();
  });

  it("routes session commands to the process that started them", async () => {
    const world = await mcpHarness();
    const other = await createGitRepository();
    // A different repository's workspace has no knowledge of this session.
    const sessionId = await startSession(world.client, world.repository);
    const cross = await world.client.callTool({
      name: "hub_follow_up",
      arguments: { session_id: sessionId, workspace: other, text: "wrong hub" },
    });
    expect(isError(cross)).toBe(true);
    expect(String(JSON.stringify(payload(cross)))).toContain("SESSION_NOT_FOUND");
    await removeDirectory(other);
    await world.cleanup();
  });
});
