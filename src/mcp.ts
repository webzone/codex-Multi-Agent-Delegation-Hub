#!/usr/bin/env node

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

import { runCompetition, type CompetitionResult } from "./competition.js";
import { delegate } from "./delegate.js";
import { fanOut, FANOUT_MAX_CANDIDATES, FANOUT_MAX_CONCURRENCY_LIMIT } from "./fanout.js";
import { autoMerge } from "./merge.js";
import { releaseFanOutArtifactRefs } from "./artifacts.js";
import { createSession, resumeSession } from "./session.js";
import { AgentHubError, asDelegateError } from "./errors.js";
import { supportedAgents } from "./adapters/index.js";
import {
  createLiveManager,
  LiveSessionManager,
  supportedLiveAgents,
} from "./live/index.js";
import type { LivePermissionDecision } from "./live/types.js";
import type { DelegateError, MergeOutcome } from "./types.js";
import { realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";

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

/**
 * Wrap a handler so any throw becomes a structured error tool result: callers
 * always get content + isError back and no transport-level exception escapes.
 */

async function guardTool(handler: () => Promise<ToolResult>): Promise<ToolResult> {
  try {
    return await handler();
  } catch (error) {
    return failTool(asDelegateError(error));
  }
}

function mergeFailed(merge: MergeOutcome | null): boolean {
  return merge !== null && (merge.error !== null || !merge.clean);
}

/**
 * Tool-level seams mirroring the core dependency pattern (`FanOutDependencies`,
 * `MergeDependencies` …): production defaults are the real pipeline; focused
 * tests inject a throwing operation to pin the error-evidence contract.
 */
export interface HubToolDependencies {
  fanOut?: typeof fanOut;
  runCompetition?: typeof runCompetition;
  autoMerge?: typeof autoMerge;
  releaseRefs?: typeof releaseFanOutArtifactRefs;
  /**
   * v3 live surfaces: the CORE manager (the one from `live/manager.ts`).
   * Production builds one per workspace through `liveManagerFor` (which
   * resolves the repository from the requested workspace, registers all four
   * real transports, and wires the durable live-state reader). Focused tests
   * inject a manager wired to scripted transports.
   */
  live?: LiveSessionManager;
  liveManagerFor?: (workspace: string) => Promise<LiveSessionManager>;
}

export function createHubServer(dependencies: HubToolDependencies = {}): McpServer {
  const fanOutFn = dependencies.fanOut ?? fanOut;
  const runCompetitionFn = dependencies.runCompetition ?? runCompetition;
  const autoMergeFn = dependencies.autoMerge ?? autoMerge;
  const releaseRefsFn = dependencies.releaseRefs ?? releaseFanOutArtifactRefs;
  const injectedLive = dependencies.live ?? null;
  const liveManagerFor = dependencies.liveManagerFor ?? ((workspace: string) => createLiveManager(workspace));
  // Which manager owns which live session (the manager holds the live
  // transport; commands must route back to the process that started it).
  const liveOwners = new Map<string, LiveSessionManager>();
  async function liveManagerForWorkspace(workspace: string): Promise<LiveSessionManager> {
    return injectedLive ?? (await liveManagerFor(workspace));
  }
  function liveOwner(liveSessionId: string): LiveSessionManager {
    const owner = injectedLive ?? liveOwners.get(liveSessionId);
    if (!owner) {
      throw new AgentHubError(
        "LIVE_SESSION_NOT_FOUND",
        `no live session "${liveSessionId}" is owned by this hub process; commands must route to the process that started it`,
      );
    }
    return owner;
  }
  const server = new McpServer({
    name: "codex-multi-agent-delegation-hub",
    version: "0.1.0",
  });

  server.registerTool(
    "delegate_task",
    {
      description: "Delegate a coding task to a local AI coding agent.",
      inputSchema: {
        task: z.string().min(1),
        agent: z.enum(supportedAgents),
        mode: z.enum(["direct", "isolated"]).default("isolated"),
        workspace: z.string().min(1).default(process.cwd()),
        allow_dirty: z.boolean().default(false),
        max_output_bytes: z.number().int().positive().optional(),
      },
    },
    async ({ task, agent, mode, workspace, allow_dirty, max_output_bytes }) => {
      const result = await delegate({
        task,
        agent,
        mode,
        workspace,
        allowDirty: allow_dirty,
        maxOutputBytes: max_output_bytes,
      });

      return {
        content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
        structuredContent: { ...result } as Record<string, unknown>,
        isError: result.status === "failure",
      };
    },
  );

  const fanoutShape = {
    workspace: z.string().min(1).default(process.cwd()),
    candidates: z
      .array(
        z.object({
          label: z.string().optional(),
          task: z.string().min(1),
          agent: z.enum(supportedAgents),
        }),
      )
      .max(FANOUT_MAX_CANDIDATES),
    max_concurrency: z.number().int().min(1).max(FANOUT_MAX_CONCURRENCY_LIMIT).optional(),
    allow_dirty: z.boolean().default(false),
    max_output_bytes: z.number().int().positive().optional(),
  };

  server.registerTool(
    "fanout_candidates",
    {
      description:
        "Run isolated candidate agents over one shared base commit. Candidates never execute in the caller checkout and nothing is merged by this tool. Retained artifact refs are released (CAS-safe) before the tool returns, so review candidates from the returned diffs; a ref that could not be released is reported in `ref_cleanup_errors` and makes the tool report `isError`.",
      inputSchema: fanoutShape,
    },
    async ({ workspace, candidates, max_concurrency, allow_dirty, max_output_bytes }) =>
      guardTool(async () => {
        const result = await fanOutFn({
          workspace,
          candidates,
          maxConcurrency: max_concurrency,
          allowDirty: allow_dirty,
          maxOutputBytes: max_output_bytes,
        });
        // This tool result is the last consumer of the candidate artifact
        // refs: release them CAS-safe (externally retargeted refs survive).
        const cleanupErrors = await releaseRefsFn(workspace, result);
        const document =
          cleanupErrors.length > 0
            ? { ...result, ref_cleanup_errors: cleanupErrors }
            : result;
        // A partial or fully-failed fan-out is an operation failure, and so is
        // a ref this tool promised to release but could not.
        return okTool(document, result.status !== "success" || cleanupErrors.length > 0);
      }),
  );

  server.registerTool(
    "compete_candidates",
    {
      description:
        "Run isolated candidates, have a judge select among retained artifacts, and optionally adopt the internal winner by verified fast-forward. Retained artifact refs are released (CAS-safe) before the tool returns; refs this tool could not release are reported in `ref_cleanup_errors` and make the tool report `isError`.",
      inputSchema: {
        ...fanoutShape,
        judge_agent: z.enum(supportedAgents),
        auto_merge: z.boolean().default(false),
      },
    },
    async ({
      workspace,
      candidates,
      max_concurrency,
      allow_dirty,
      max_output_bytes,
      judge_agent,
      auto_merge,
    }) =>
      guardTool(async () => {
        const fan = await fanOutFn({
          workspace,
          candidates,
          maxConcurrency: max_concurrency,
          allowDirty: allow_dirty,
          maxOutputBytes: max_output_bytes,
        });
        let competition: CompetitionResult | null = null;
        let merge: MergeOutcome | null = null;
        let operationError: DelegateError | null = null;
        let cleanupErrors: DelegateError[] = [];
        try {
          competition = await runCompetitionFn({
            fan_out: fan,
            strategy: "judge",
            judge_agent,
            workspace,
            maxOutputBytes: max_output_bytes,
          });
          merge = auto_merge
            ? await autoMergeFn({ workspace, fan_out: fan, competition })
            : null;
        } catch (error) {
          // Caught *inside* the handler: like the CLI terminal path, the
          // operation's error becomes the tool's error document, and the
          // ref-release evidence below still rides along with it. A throw
          // out of here would discard every cleanup fact collected after it.
          operationError = asDelegateError(error);
        } finally {
          // Competition eligibility and merge ref verification have consumed
          // the retained artifact refs by now; release them CAS-safe in all
          // cases, including a judge or merge throw.
          cleanupErrors = await releaseRefsFn(workspace, fan);
        }
        if (operationError !== null) {
          return okTool(
            {
              error: operationError,
              ...(cleanupErrors.length > 0
                ? { ref_cleanup_errors: cleanupErrors }
                : {}),
            },
            true,
          );
        }
        const document = {
          fan_out: fan,
          competition,
          merge,
          ...(cleanupErrors.length > 0 ? { ref_cleanup_errors: cleanupErrors } : {}),
        };
        return okTool(
          document,
          fan.status !== "success" ||
            competition?.error !== null ||
            mergeFailed(merge) ||
            cleanupErrors.length > 0,
        );
      }),
  );

  const sessionCreateShape = {
    workspace: z.string().min(1).default(process.cwd()),
    agent: z.enum(supportedAgents),
    task: z.string().min(1),
    allow_dirty: z.boolean().default(false),
    max_output_bytes: z.number().int().positive().optional(),
  };

  server.registerTool(
    "session_create",
    {
      description:
        "Run one isolated agent turn and persist its artifact for filesystem continuation.",
      inputSchema: sessionCreateShape,
    },
    async ({ workspace, agent, task, allow_dirty, max_output_bytes }) =>
      guardTool(async () => {
        const session = await createSession({
          workspace,
          agent,
          task,
          allowDirty: allow_dirty,
          maxOutputBytes: max_output_bytes,
        });
        return okTool(session, session.run.status !== "success" || session.cleanup_error !== null);
      }),
  );

  server.registerTool(
    "session_resume",
    {
      description:
        "Resume a persisted agent session from its latest artifact commit in a fresh isolated worktree.",
      inputSchema: {
        session_id: z.string().min(1),
        task: z.string().min(1),
        workspace: z.string().min(1).default(process.cwd()),
        max_output_bytes: z.number().int().positive().optional(),
      },
    },
    async ({ session_id, task, workspace, max_output_bytes }) =>
      guardTool(async () => {
        const session = await resumeSession({
          session_id,
          task,
          workspace,
          maxOutputBytes: max_output_bytes,
        });
        return okTool(session, session.run.status !== "success" || session.cleanup_error !== null);
      }),
  );


  // ---------------------------------------------------------------------
  // v3 live surfaces (additive): the CORE manager behind start/resume/
  // command/events/close, with the durable live-state reader wired by the
  // production bootstrap.
  // ---------------------------------------------------------------------

  server.registerTool(
    "live_session_start",
    {
      description:
        "Start a long-lived interactive live session (v3) for one provider (omp, agy, pi, hermes). The provider runs in a hub-owned isolated OS-temp worktree materialized from this workspace; durable state, the lifetime lease, quotas, and the bounded event ring are all owned by the core live manager. pi/hermes stay live-only and remain rejected by the legacy tools. Fails honestly when the provider probe is not found.",
      inputSchema: {
        agent: z.enum(supportedLiveAgents),
        workspace: z.string().min(1).default(process.cwd()),
        session_id: z.string().min(1).optional(),
        max_output_bytes: z.number().int().positive().optional(),
      },
    },
    async ({ agent, workspace, session_id, max_output_bytes }) =>
      guardTool(async () => {
        const manager = await liveManagerForWorkspace(workspace);
        const started = await manager.start({
          provider: agent,
          session_id: session_id ?? null,
          max_text_bytes: max_output_bytes,
        });
        liveOwners.set(started.live_session_id, manager);
        return okTool(started);
      }),
  );

  server.registerTool(
    "live_session_resume",
    {
      description:
        "Resume a durable live session (hub-live-id): loads the agent-hub-live/v1 record, refuses live/leased sessions, materializes a FRESH hub worktree at current_commit, launches the provider with the recorded opaque resume state, verifies provider identity, and CAS-advances the existing live ref (never a new ref). A resume whose identity does not round-trip fails; there is no fake continuation.",
      inputSchema: {
        live_session_id: z.string().min(1),
        workspace: z.string().min(1).default(process.cwd()),
        max_output_bytes: z.number().int().positive().optional(),
      },
    },
    async ({ live_session_id, workspace, max_output_bytes }) =>
      guardTool(async () => {
        const manager = await liveManagerForWorkspace(workspace);
        const resumed = await manager.resumeFromState({
          live_session_id,
          max_text_bytes: max_output_bytes,
        });
        liveOwners.set(resumed.live_session_id, manager);
        return okTool(resumed);
      }),
  );

  server.registerTool(
    "live_session_command",
    {
      description:
        "Inject one hub command into a live session: prompt (once, while idle), follow_up (native claims are delivered immediately and tracked provider-queued; hub-queued claims wait for the terminal boundary), steer (mid-turn, when claimed), cancel, status (answered from stream evidence when derived), or permission_response (answering an observed permission_request with allow_once or deny only — any other verdict is rejected, never converted). Returns the LiveTurnResult plus the session status; capability-refused commands come back outcome \"unsupported\" with a stage \"capability\" error and are never delivered.",
      inputSchema: {
        live_session_id: z.string().min(1),
        action: z.enum(["prompt", "follow_up", "steer", "cancel", "status", "permission_response"]),
        text: z.string().optional(),
        reason: z.string().nullable().optional(),
        request_id: z.string().optional(),
        decision: z.enum(["allow_once", "deny"]).optional(),
        note: z.string().nullable().optional(),
      },
    },
    async ({ live_session_id, action, text, reason, request_id, decision, note }) =>
      guardTool(async () => {
        const manager = liveOwner(live_session_id);
        const invalid = (message: string) =>
          failTool({ code: "LIVE_COMMAND_INVALID", message });
        let result;
        switch (action) {
          case "prompt":
          case "follow_up":
          case "steer": {
            if (typeof text !== "string") {
              return invalid(`command "${action}" requires a "text" string`);
            }
            result =
              action === "prompt"
                ? await manager.prompt(live_session_id, text)
                : action === "follow_up"
                  ? await manager.followUp(live_session_id, text)
                  : await manager.steer(live_session_id, text);
            break;
          }
          case "cancel": {
            result = await manager.cancel(live_session_id, reason ?? null);
            break;
          }
          case "status": {
            result = await manager.requestStatus(live_session_id);
            break;
          }
          case "permission_response": {
            if (typeof request_id !== "string" || request_id.length === 0) {
              return invalid('command "permission_response" requires "request_id"');
            }
            const verdict: LivePermissionDecision | null =
              decision === "allow_once" || decision === "deny" ? decision : null;
            if (verdict === null) {
              return invalid('command "permission_response" requires decision allow_once or deny');
            }
            result = await manager.respondPermission(
              live_session_id,
              request_id,
              verdict,
              note ?? null,
            );
            break;
          }
        }
        let status: string = result.outcome;
        try {
          status = manager.view(live_session_id).status;
        } catch {
          status = "closed";
        }
        return okTool({ result, status }, result.outcome === "failed" || result.outcome === "unsupported");
      }),
  );

  server.registerTool(
    "live_session_events",
    {
      description:
        "Poll normalized live events after a cursor (events are per-session, 1-based, no gaps). Returns the replay page and the next cursor; when the bounded ring already evicted events behind the cursor this fails with EVENT_CURSOR_EXPIRED (resynchronize from the durable record) instead of silently dropping events.",
      inputSchema: {
        live_session_id: z.string().min(1),
        cursor: z.number().int().nonnegative().default(0),
      },
    },
    async ({ live_session_id, cursor }) =>
      guardTool(async () => {
        const page = liveOwner(live_session_id).eventsAfter(live_session_id, cursor);
        return okTool(page);
      }),
  );

  server.registerTool(
    "live_session_close",
    {
      description:
        "Stop a live session's provider (graceful, or terminate with bounded SIGKILL escalation authorized) and report what shutdown proved: `closed` only with leader reap plus proof the owned process group is gone, `orphaned` otherwise (the lease and worktree then stay for recovery). An orphaned close is an isError.",
      inputSchema: {
        live_session_id: z.string().min(1),
        terminate: z.boolean().default(false),
      },
    },
    async ({ live_session_id, terminate }) =>
      guardTool(async () => {
        const close = await liveOwner(live_session_id).close(
          live_session_id,
          terminate ? "terminate" : "graceful",
        );
        liveOwners.delete(live_session_id);
        return okTool(
          close,
          close.stop === null ? close.state.status === "orphaned" : close.stop.status === "orphaned",
        );
      }),
  );

  return server;
}

function isEntrypoint(): boolean {
  if (!process.argv[1]) {
    return false;
  }

  try {
    return realpathSync(process.argv[1]) === fileURLToPath(import.meta.url);
  } catch {
    return false;
  }
}

if (isEntrypoint()) {
  const server = createHubServer();
  const transport = new StdioServerTransport();
  await server.connect(transport);
}
