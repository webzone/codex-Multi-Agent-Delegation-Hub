#!/usr/bin/env node
import { realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

import { AgentHubError, asHubError } from "./errors.js";
import { AgentHub, HUB_PROVIDERS, type TurnDocument } from "./hub/agent-hub.js";
import type { AgentHubOptions } from "./hub/agent-hub.js";
import {
  AgentHubSupervisor,
  processHubSupervisor,
  type HubOpen,
} from "./hub/supervisor.js";
import { productionBridgedFactories } from "./hub/transport-adapter.js";
import { isPermissionDecision } from "./kernel/contracts.js";

/**
 * agent-hub-mcp — the MCP surface of the rewrite.
 *
 * One provider-neutral session lifecycle, exposed as plain tools:
 * start / prompt / follow_up / steer / cancel / status / permission /
 * events / close / resume / list / handoff / gc / probe.
 *
 * Sessions live in the hub process that started them: every command for a
 * session must name the same `workspace` so it routes back to the owning
 * hub (cached per Git common dir by the process supervisor). After this
 * process dies, `hub_gc` reconciles and `hub_resume` adopts the durable
 * record from any host. No delegate/fanout/competition/live vocabulary.
 */

type ToolResult = {
  content: Array<{ type: "text"; text: string }>;
  structuredContent?: Record<string, unknown>;
  isError?: boolean;
};

function okTool(document: unknown, isError = false): ToolResult {
  return {
    content: [{ type: "text", text: JSON.stringify(document, null, 2) }],
    structuredContent: (document ?? null) as Record<string, unknown>,
    isError,
  };
}

function failTool(error: { code: string; message: string }): ToolResult {
  const document = { error };
  return {
    content: [{ type: "text", text: JSON.stringify(document, null, 2) }],
    structuredContent: document,
    isError: true,
  };
}

/** Any throw becomes a structured error tool result; nothing escapes raw. */
async function guardTool(handler: () => Promise<ToolResult>): Promise<ToolResult> {
  try {
    return await handler();
  } catch (error) {
    return failTool(asHubError(error));
  }
}

export interface HubToolDependencies {
  /** Test seam: replaces hub construction entirely. */
  openHub?: HubOpen;
  /** Options merged into every hub construction (injected factories etc.). */
  hubOptions?: AgentHubOptions;
  /** Test seam: replace the process supervisor. */
  supervisor?: AgentHubSupervisor;
}

const workspaceShape = z.string().min(1).default(process.cwd());

function turnIsError(turn: TurnDocument): boolean {
  return (
    turn.outcome === "failed" ||
    turn.outcome === "unsupported" ||
    turn.publish_error !== undefined
  );
}

export function createHubServer(dependencies: HubToolDependencies = {}): McpServer {
  const supervisor = dependencies.supervisor ?? processHubSupervisor;
  const open: HubOpen =
    dependencies.openHub ??
    ((workspace, options) => AgentHub.open(workspace, { ...dependencies.hubOptions, ...options }));

  async function hubFor(workspace: string): Promise<AgentHub> {
    return supervisor.hubFor(workspace, dependencies.hubOptions ?? {}, open);
  }

  function launchGuarded(hub: AgentHub, run: () => Promise<ToolResult>): Promise<ToolResult> {
    // The supervisor slot spans the whole launch; the tool result is the
    // launch outcome, so the guard composes rather than nests.
    return supervisor.launch(hub, run);
  }

  const server = new McpServer({ name: "agent-hub", version: "0.2.0" });

  server.registerTool(
    "hub_start",
    {
      description:
        "Start a provider-neutral agent session in a hub-owned isolated worktree under AGENT_HUB_HOME. " +
        "Providers: omp (RPC v2 only), pi (RPC), agy (stream-json), hermes (ACP); the transport is auto-selected and reported as a fact — callers cannot pin it. " +
        `Shipped ids: ${HUB_PROVIDERS.join(", ")}.`,
      inputSchema: {
        provider: z.enum(HUB_PROVIDERS),
        workspace: workspaceShape,
        agent: z.string().min(1).optional(),
        permission_policy: z.enum(["deny", "interactive"]).default("deny"),
        max_text_bytes: z.number().int().positive().optional(),
      },
    },
    async ({ provider, workspace, agent, permission_policy, max_text_bytes }) =>
      guardTool(async () => {
        const hub = await hubFor(workspace);
        return launchGuarded(hub, async () =>
          okTool(
            await hub.start({
              provider,
              ...(agent === undefined ? {} : { agent }),
              permission_policy,
              ...(max_text_bytes === undefined ? {} : { max_text_bytes }),
            }),
          ),
        );
      }),
  );

  const sessionShape = {
    session_id: z.string().min(1),
    workspace: workspaceShape,
  };

  const textShape = { text: z.string().min(1) };

  interface CommandArgs {
    session_id: string;
    workspace: string;
    [key: string]: unknown;
  }

  const commandTool = (
    name: string,
    description: string,
    extra: Record<string, z.ZodType>,
    run: (hub: AgentHub, args: CommandArgs) => Promise<TurnDocument>,
  ): void => {
    server.registerTool(
      name,
      { description, inputSchema: { ...sessionShape, ...extra } },
      async (args) =>
        guardTool(async () => {
          // zod has validated this shape before the handler runs.
          const typed = args as unknown as CommandArgs;
          const hub = await hubFor(typed.workspace);
          const turn = await run(hub, typed);
          return okTool(turn, turnIsError(turn));
        }),
    );
  };

  /** `text` is zod-validated (`z.string().min(1)`) on all three callers. */
  const requireText = (args: CommandArgs): string => args.text as string;

  commandTool(
    "hub_prompt",
    "Send the initial task to an attached session (accepted exactly once, while idle). Settles at the turn boundary; the turn publishes its exact result identity (sequence + commit + tree + ref).",
    textShape,
    (hub, args) => hub.prompt(args.session_id, requireText(args)),
  );
  commandTool(
    "hub_follow_up",
    "Queue/deliver the next-turn input. Delivered immediately when idle, queued (or provider-queued on a native claim) while a turn runs.",
    textShape,
    (hub, args) => hub.followUp(args.session_id, requireText(args)),
  );
  commandTool(
    "hub_steer",
    "Mid-turn guidance. Refused as `unsupported` when the launch snapshot does not claim steer delivery.",
    textShape,
    (hub, args) => hub.steer(args.session_id, requireText(args)),
  );
  commandTool(
    "hub_cancel",
    "Abort the in-flight turn (native or signal path per the launch snapshot). No turn in flight is an honest no-op.",
    { reason: z.string().optional() },
    (hub, args) =>
      hub.cancel(args.session_id, typeof args.reason === "string" ? args.reason : null),
  );
  commandTool(
    "hub_command_status",
    "Ask for authoritative progress. Forwarded when the status claim is native; answered from stream evidence when derived.",
    {},
    (hub, args) => hub.requestStatus(args.session_id),
  );
  commandTool(
    "hub_permission",
    "Answer an observed permission_request. Exactly two verdicts (allow_once, deny); anything else is a caller error.",
    { request_id: z.string().min(1), decision: z.string().min(1), note: z.string().nullable().optional() },
    (hub, args) => {
      if (!isPermissionDecision(args.decision)) {
        throw new AgentHubError(
          "COMMAND_INVALID",
          `decision "${String(args.decision)}" is outside the contract vocabulary (allow_once, deny)`,
        );
      }
      return hub.respondPermission(
        args.session_id,
        args.request_id as string,
        args.decision,
        typeof args.note === "string" ? args.note : null,
      );
    },
  );

  server.registerTool(
    "hub_events",
    {
      description:
        "One-shot event replay after a cursor for an ATTACHED session (this process). Seqs are gapless; `next_cursor` is the resume point. " +
        "An `expired` verdict names the oldest replayable cursor — resynchronize from durable state, never from a guess.",
      inputSchema: { ...sessionShape, after: z.number().int().min(0).default(0) },
    },
    async ({ session_id, workspace, after }) =>
      guardTool(async () => {
        const hub = await hubFor(workspace);
        return okTool(hub.eventsAfter(session_id, after));
      }),
  );

  server.registerTool(
    "hub_close",
    {
      description:
        "Close an attached session. Custody finalization captures the final state and RETAINS everything: the isolated worktree, lease, results, and ref stay under custody until an explicit hub_handoff names the exact result and the retention window expires. An unproven stop answers `orphaned` with ownership retained — `hub_gc` reconciles it.",
      inputSchema: {
        ...sessionShape,
        mode: z.enum(["graceful", "terminate"]).default("graceful"),
      },
    },
    async ({ session_id, workspace, mode }) =>
      guardTool(async () => {
        const hub = await hubFor(workspace);
        const document = await hub.close(session_id, mode);
        supervisor.retireIdle(hub);
        return okTool(document, document.record.status !== "closed");
      }),
  );

  server.registerTool(
    "hub_resume",
    {
      description:
        "Resume a CLOSED durable session on its retained custody worktree — the same durable line, the recorded transport, identity-verified provider resume. " +
        "Refused once a handoff decision exists, while any lease is live or uncertain, or while the runtime mirror is non-terminal; `hub_gc` reconciles first.",
      inputSchema: {
        session_id: z.string().min(1),
        workspace: workspaceShape,
        permission_policy: z.enum(["deny", "interactive"]).default("deny"),
        max_text_bytes: z.number().int().positive().optional(),
      },
    },
    async ({ session_id, workspace, permission_policy, max_text_bytes }) =>
      guardTool(async () => {
        const hub = await hubFor(workspace);
        return launchGuarded(hub, async () =>
          okTool(
            await hub.resume(session_id, {
              permission_policy,
              ...(max_text_bytes === undefined ? {} : { max_text_bytes }),
            }),
          ),
        );
      }),
  );

  server.registerTool(
    "hub_status",
    {
      description:
        "One session's custody document (workspace record + runtime mirror + lease classification + worktree state), or every durable workspace when no id is given.",
      inputSchema: {
        session_id: z.string().min(1).optional(),
        workspace: workspaceShape,
      },
    },
    async ({ session_id, workspace }) =>
      guardTool(async () => {
        const hub = await hubFor(workspace);
        if (session_id === undefined) {
          return okTool({ sessions: await hub.list() });
        }
        return okTool(await hub.status(session_id));
      }),
  );

  server.registerTool(
    "hub_handoff",
    {
      description:
        "The consumer's takeover decision on a CLOSED workspace: accepted or discarded, naming the EXACT published head (result_seq + commit, both from hub_status). " +
        "A matching decision starts the retention window (nothing is deleted here); a mismatch is refused. Decisions are not revisable. Resume is refused after any decision.",
      inputSchema: {
        session_id: z.string().min(1),
        workspace: workspaceShape,
        decision: z.enum(["accepted", "discarded"]),
        result_seq: z.number().int().positive(),
        commit: z.string().min(40),
        consumer: z.string().min(1).nullable().optional(),
      },
    },
    async ({ session_id, workspace, decision, result_seq, commit, consumer }) =>
      guardTool(async () =>
        okTool(
          await (
            await hubFor(workspace)
          ).handoff(session_id, {
            decision,
            result_seq,
            commit,
            consumer: consumer ?? null,
          }),
        ),
      ),
  );

  server.registerTool(
    "hub_gc",
    {
      description:
        "Safe manual reconciliation: recover (re-prove leases, settle provably-dead orphans — deletes nothing) then gc (collect ONLY workspaces whose exact accepted/discarded handoff is on record and whose retention window expired). " +
        "Never deletes unacknowledged, orphaned, uncertain, or actively referenced work; every retained workspace names the failed precondition. Automatic startup catch-up already runs this pass; this tool is the explicit manual path.",
      inputSchema: { workspace: workspaceShape },
    },
    async ({ workspace }) =>
      guardTool(async () => {
        const report = await (await hubFor(workspace)).cleanup();
        const manual =
          report.recovery.inconsistencies.length > 0
          || report.recovery.unclaimed.unknown_segments.length > 0
          || report.cleanup.unclaimed.cleanup_errors.length > 0;
        return okTool({ recovery: report.recovery, cleanup: report.cleanup }, manual);
      }),
  );

  server.registerTool(
    "hub_probe",
    {
      description:
        "Honest capability probe of the installed provider commands (launches nothing). omp answers found=false unless its RPC v2 dialect evidence holds — the hub will then refuse omp launches rather than fall back to v1.",
      inputSchema: { provider: z.enum(HUB_PROVIDERS).optional() },
    },
    async ({ provider }) =>
      guardTool(async () => {
        const bridges = dependencies.hubOptions?.transportFactories ?? productionBridgedFactories();
        const documents: unknown[] = [];
        for (const factory of bridges) {
          if (provider !== undefined && factory.provider !== provider) continue;
          const probe = await factory.probe();
          documents.push({
            provider: factory.provider,
            transport: factory.transport,
            ...probe,
          });
        }
        if (provider !== undefined && documents.length === 0) {
          throw new AgentHubError(
            "TRANSPORT_UNAVAILABLE",
            `no hub transport pairs with provider "${provider}"`,
          );
        }
        return okTool({ probes: documents });
      }),
  );

  return server;
}

function isEntrypoint(): boolean {
  const invoked = process.argv[1];
  if (invoked === undefined) return false;
  try {
    return realpathSync(fileURLToPath(import.meta.url)) === realpathSync(invoked);
  } catch {
    return false;
  }
}

if (isEntrypoint()) {
  const server = createHubServer();
  const transport = new StdioServerTransport();
  await server.connect(transport);
}
