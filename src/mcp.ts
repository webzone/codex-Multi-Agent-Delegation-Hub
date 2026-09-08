#!/usr/bin/env node
import { realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

import { AgentHubError, asDelegateError } from "./errors.js";
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
    return failTool(asDelegateError(error));
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
    turn.checkpoint_error !== undefined
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
        "Start a provider-neutral agent session in a hub-owned checkpointed worktree. " +
        "Providers: omp (RPC v2 only), pi (RPC), agy (stream-json), hermes (ACP); the transport is auto-selected. " +
        `Shipped ids: ${HUB_PROVIDERS.join(", ")}.`,
      inputSchema: {
        provider: z.enum(HUB_PROVIDERS),
        transport: z.string().min(1).optional(),
        workspace: workspaceShape,
        permission_policy: z.enum(["deny", "interactive"]).default("deny"),
        max_text_bytes: z.number().int().positive().optional(),
        allow_dirty: z.boolean().default(false),
      },
    },
    async ({ provider, transport, workspace, permission_policy, max_text_bytes, allow_dirty }) =>
      guardTool(async () => {
        const hub = await hubFor(workspace);
        return launchGuarded(hub, async () =>
          okTool(
            await hub.start({
              provider,
              transport,
              permission_policy,
              max_text_bytes,
              allow_dirty,
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
    "Send the initial task to an attached session (accepted exactly once, while idle). Settles at the turn boundary; the checkpoint chain is pinned for it.",
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
        "Close an attached session. Teardown (checkpoint, worktree removal, lease release) runs only when shutdown is PROVEN; " +
        "an unproven stop answers `orphaned` with ownership retained — a later `terminate` close or `hub_gc` finishes the job.",
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
        return okTool(document, document.cleanup_errors.length > 0);
      }),
  );

  server.registerTool(
    "hub_resume",
    {
      description:
        "Adopt a TERMINAL durable session from this repository's store: fresh hub worktree at the checkpoint-chain head, the recorded provider resume handle replayed and identity-verified, the SAME durable line advanced. " +
        "Unknown ids fail; leased or non-terminal records must be reconciled with hub_gc first.",
      inputSchema: {
        session_id: z.string().min(1),
        workspace: workspaceShape,
        transport: z.string().min(1).optional(),
        permission_policy: z.enum(["deny", "interactive"]).default("deny"),
        max_text_bytes: z.number().int().positive().optional(),
        allow_dirty: z.boolean().default(false),
      },
    },
    async ({ session_id, workspace, transport, permission_policy, max_text_bytes, allow_dirty }) =>
      guardTool(async () => {
        const hub = await hubFor(workspace);
        return launchGuarded(hub, async () =>
          okTool(
            await hub.resume(session_id, {
              transport,
              permission_policy,
              max_text_bytes,
              allow_dirty,
            }),
          ),
        );
      }),
  );

  server.registerTool(
    "hub_status",
    {
      description:
        "One session's durable document (lifecycle state + kernel mirror record + lease facts), or every durable session in this repository when no id is given.",
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
        "Result handoff for a released terminal session: the checkpoint chain (ref, commits, reasons), changed files, diff stat, and the human review/adopt command. Never merges anything.",
      inputSchema: { session_id: z.string().min(1), workspace: workspaceShape },
    },
    async ({ session_id, workspace }) =>
      guardTool(async () =>
        okTool(await (await hubFor(workspace)).handoff(session_id)),
      ),
  );

  server.registerTool(
    "hub_gc",
    {
      description:
        "Safe garbage collection for this repository: re-prove every lease, reap provably-orphaned provider groups, pin surviving worktrees as crash_recovery checkpoints, rewrite orphaned state, release leases last, retry worktree removals, prune. `dry_run` reports every intended action without touching anything.",
      inputSchema: { workspace: workspaceShape, dry_run: z.boolean().default(false) },
    },
    async ({ workspace, dry_run }) =>
      guardTool(async () => {
        const report = await (await hubFor(workspace)).gc({ dry_run });
        return okTool(report, report.sessions.some((session) => session.outcome === "manual"));
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
