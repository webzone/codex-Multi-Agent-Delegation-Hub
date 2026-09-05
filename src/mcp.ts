#!/usr/bin/env node

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

import { runCompetition, type CompetitionResult } from "./competition.js";
import { delegate } from "./delegate.js";
import { fanOut, FANOUT_MAX_CONCURRENCY_LIMIT } from "./fanout.js";
import { autoMerge } from "./merge.js";
import { releaseFanOutArtifactRefs } from "./artifacts.js";
import { createSession, resumeSession } from "./session.js";
import { asDelegateError } from "./errors.js";
import { supportedAgents } from "./adapters/index.js";
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

export function createHubServer(): McpServer {
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
      .min(1),
    max_concurrency: z.number().int().min(1).max(FANOUT_MAX_CONCURRENCY_LIMIT).optional(),
    allow_dirty: z.boolean().default(false),
    max_output_bytes: z.number().int().positive().optional(),
  };

  server.registerTool(
    "fanout_candidates",
    {
      description:
        "Run isolated candidate agents over one shared base commit. Candidates never execute in the caller checkout and nothing is merged by this tool. Retained artifact refs are released (CAS-safe) before the tool returns, so review candidates from the returned diffs.",
      inputSchema: fanoutShape,
    },
    async ({ workspace, candidates, max_concurrency, allow_dirty, max_output_bytes }) =>
      guardTool(async () => {
        const result = await fanOut({
          workspace,
          candidates,
          maxConcurrency: max_concurrency,
          allowDirty: allow_dirty,
          maxOutputBytes: max_output_bytes,
        });
        // This tool result is the last consumer of the candidate artifact
        // refs: release them CAS-safe (externally retargeted refs survive).
        const cleanupErrors = await releaseFanOutArtifactRefs(workspace, result);
        const document =
          cleanupErrors.length > 0
            ? { ...result, ref_cleanup_errors: cleanupErrors }
            : result;
        // A partial or fully-failed fan-out is an operation failure.
        return okTool(document, result.status !== "success");
      }),
  );

  server.registerTool(
    "compete_candidates",
    {
      description:
        "Run isolated candidates, have a judge select among retained artifacts, and optionally adopt the internal winner by verified fast-forward. Retained artifact refs are released (CAS-safe) before the tool returns.",
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
        const fan = await fanOut({
          workspace,
          candidates,
          maxConcurrency: max_concurrency,
          allowDirty: allow_dirty,
          maxOutputBytes: max_output_bytes,
        });
        let competition: CompetitionResult;
        let merge: MergeOutcome | null = null;
        let cleanupErrors: DelegateError[] = [];
        try {
          competition = await runCompetition({
            fan_out: fan,
            strategy: "judge",
            judge_agent,
            workspace,
            maxOutputBytes: max_output_bytes,
          });
          merge = auto_merge
            ? await autoMerge({ workspace, fan_out: fan, competition })
            : null;
        } finally {
          // Competition eligibility and merge ref verification have consumed
          // the retained artifact refs by now; release them CAS-safe in all
          // cases, including a judge or merge throw.
          cleanupErrors = await releaseFanOutArtifactRefs(workspace, fan);
        }
        const document = {
          fan_out: fan,
          competition,
          merge,
          ...(cleanupErrors.length > 0 ? { ref_cleanup_errors: cleanupErrors } : {}),
        };
        return okTool(
          document,
          fan.status !== "success" || competition.error !== null || mergeFailed(merge),
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
