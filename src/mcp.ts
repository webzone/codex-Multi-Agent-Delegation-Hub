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
import { asDelegateError } from "./errors.js";
import { supportedAgents } from "./adapters/index.js";
import {
  getLiveResumeSource,
  LiveSessionManager,
  supportedLiveAgents,
} from "./live/index.js";
import type { LiveResumeSource } from "./live/index.js";
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
   * v3 live surfaces: the process-local manager and the durable-state seam.
   * By default each server owns its own process-local manager; focused
   * tests inject an isolated manager wired to a scripted transport registry.
   */
  live?: LiveSessionManager;
  liveResumeSource?: LiveResumeSource;
}

export function createHubServer(dependencies: HubToolDependencies = {}): McpServer {
  const fanOutFn = dependencies.fanOut ?? fanOut;
  const runCompetitionFn = dependencies.runCompetition ?? runCompetition;
  const autoMergeFn = dependencies.autoMerge ?? autoMerge;
  const releaseRefsFn = dependencies.releaseRefs ?? releaseFanOutArtifactRefs;
  // Process-local by contract: one manager per server unless a caller
  // injects one (focused tests wire a scripted transport registry).
  const liveManager = dependencies.live ?? new LiveSessionManager();
  const liveResumeSource = dependencies.liveResumeSource ?? getLiveResumeSource();
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
  // v3 live surfaces (additive): a process-local manager, polling plus
  // cursor event reads, and the six hub commands behind one tool.
  // ---------------------------------------------------------------------

  server.registerTool(
    "live_session_start",
    {
      description:
        "Start a long-lived interactive live session (v3) for one provider (omp, agy, pi, hermes). Providers are validated against the live vocabulary; pi/hermes stay live-only and remain rejected by the legacy tools. Fails honestly when the provider's transport is not wired into this build.",
      inputSchema: {
        agent: z.enum(supportedLiveAgents),
        workspace: z.string().min(1).default(process.cwd()),
        session_id: z.string().min(1).optional(),
        max_output_bytes: z.number().int().positive().optional(),
      },
    },
    async ({ agent, workspace, session_id, max_output_bytes }) =>
      guardTool(async () => {
        const summary = await liveManager.start({
          provider: agent,
          workspace,
          sessionId: session_id ?? null,
          maxTextBytes: max_output_bytes,
        });
        return okTool(summary);
      }),
  );

  server.registerTool(
    "live_session_resume",
    {
      description:
        "Resume a durable live session (hub-live-id) by reopening its provider transport with the stored resume state. Until the durable live-state store is wired, this fails with LIVE_STATE_UNAVAILABLE — never with a fake continuation.",
      inputSchema: {
        live_session_id: z.string().min(1),
        workspace: z.string().min(1).default(process.cwd()),
        max_output_bytes: z.number().int().positive().optional(),
      },
    },
    async ({ live_session_id, workspace, max_output_bytes }) =>
      guardTool(async () => {
        const state = await liveResumeSource.load(workspace, live_session_id);
        const summary = await liveManager.resume({
          state,
          workspace,
          maxTextBytes: max_output_bytes,
        });
        return okTool(summary);
      }),
  );

  server.registerTool(
    "live_session_command",
    {
      description:
        "Inject one hub command into a live session: prompt (once, while idle), follow_up (native when idle or hub-queued mid-turn), steer (mid-turn, when claimed), cancel, status (answered from stream evidence), or permission_response (answering an observed permission_request). Returns the LiveTurnResult plus the session status; capability-refused commands come back outcome \"unsupported\" with a stage \"capability\" error and are never delivered.",
      inputSchema: {
        live_session_id: z.string().min(1),
        action: z.enum(["prompt", "follow_up", "steer", "cancel", "status", "permission_response"]),
        text: z.string().optional(),
        reason: z.string().nullable().optional(),
        request_id: z.string().optional(),
        decision: z.enum(["allow_once", "allow_session", "deny"]).optional(),
        note: z.string().nullable().optional(),
      },
    },
    async ({ live_session_id, action, text, reason, request_id, decision, note }) =>
      guardTool(async () => {
        const result = await liveManager.command(live_session_id, {
          action,
          text: text ?? null,
          reason: reason ?? null,
          request_id: request_id ?? null,
          decision: decision ?? null,
          note: note ?? null,
        });
        const status = liveManager.get(live_session_id)?.status ?? "closed";
        return okTool({ result, status }, result.outcome === "failed" || result.outcome === "unsupported");
      }),
  );

  server.registerTool(
    "live_session_events",
    {
      description:
        "Poll normalized live events after a cursor (events are per-session, 1-based, no gaps). Returns the page, the next cursor, the oldest retained seq, and an honest `dropped` flag when the ring buffer already evicted what the cursor points before.",
      inputSchema: {
        live_session_id: z.string().min(1),
        cursor: z.number().int().nonnegative().default(0),
        limit: z.number().int().min(1).max(1000).default(200),
      },
    },
    async ({ live_session_id, cursor, limit }) =>
      guardTool(async () => okTool(liveManager.events(live_session_id, cursor, limit))),
  );

  server.registerTool(
    "live_session_close",
    {
      description:
        "Stop a live session's provider (graceful, or terminate with bounded SIGKILL escalation authorized) and report what shutdown proved: `closed` only with proof the process is gone, `orphaned` otherwise — an orphaned close is an isError.",
      inputSchema: {
        live_session_id: z.string().min(1),
        terminate: z.boolean().default(false),
      },
    },
    async ({ live_session_id, terminate }) =>
      guardTool(async () => {
        const close = await liveManager.close(live_session_id, terminate ? "terminate" : "graceful");
        return okTool(close, close.stop.status === "orphaned");
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
