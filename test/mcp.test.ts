import { chmod } from "node:fs/promises";
import { join } from "node:path";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { describe, expect, it } from "vitest";

import { createHubServer } from "../src/mcp.js";
import {
  BLOCKED_WRITE_IS_MEANINGFUL,
  BLOCKING_JUDGE_SCRIPT,
  candidateRefNames,
  createGitRepository,
  removeDirectory,
  resolveRef,
  runGit,
} from "./helpers.js";

async function connectClient(): Promise<Client> {
  const server = createHubServer();
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  const client = new Client({ name: "test-client", version: "0.0.0" }, { capabilities: {} });
  await client.connect(clientTransport);
  return client;
}

function documentFrom(result: unknown): Record<string, unknown> {
  if (!result || typeof result !== "object" || !("content" in result) || !Array.isArray(result.content)) {
    throw new Error("expected a content-bearing tool result");
  }
  const first = result.content[0];
  if (!first || typeof first !== "object" || !("text" in first) || typeof first.text !== "string") {
    throw new Error("expected a text content block in the tool result");
  }
  return JSON.parse(first.text) as Record<string, unknown>;
}

function errorCode(document: Record<string, unknown>): string {
  const error = document.error;
  if (
    !(typeof error === "object" && error !== null && "code" in error && typeof error.code === "string")
  ) {
    throw new Error(`expected an error.code in ${JSON.stringify(document)}`);
  }
  return error.code;
}

describe("MCP server", () => {
  it("registers delegate_task plus the additive v2 tools", async () => {
    const client = await connectClient();
    const { tools } = await client.listTools();

    expect(tools.map((tool) => tool.name)).toEqual(
      expect.arrayContaining([
        "delegate_task",
        "fanout_candidates",
        "compete_candidates",
        "session_create",
        "session_resume",
      ]),
    );
  });

  it("keeps the delegate_task input schema additive and unchanged", async () => {
    const client = await connectClient();
    const { tools } = await client.listTools();
    const delegateTask = tools.find((tool) => tool.name === "delegate_task");
    expect(delegateTask).toBeDefined();

    const schema = delegateTask?.inputSchema as {
      properties?: Record<string, unknown>;
      required?: string[];
    };
    expect(Object.keys(schema.properties ?? {}).sort()).toEqual([
      "agent",
      "allow_dirty",
      "max_output_bytes",
      "mode",
      "task",
      "workspace",
    ]);
    expect(schema.required).toEqual(expect.arrayContaining(["task", "agent"]));
  });

  it("answers fan-out failures with structured content and isError, never a transport throw", async () => {
    const client = await connectClient();
    const result = await client.callTool({
      name: "fanout_candidates",
      arguments: {
        workspace: "/definitely/not/a/repository",
        candidates: [{ agent: "omp", task: "noop" }],
      },
    });

    expect(result.isError).toBe(true);
    expect(errorCode(documentFrom(result))).toBe("NOT_GIT_REPOSITORY");
    expect(result.structuredContent).toMatchObject({ error: { code: "NOT_GIT_REPOSITORY" } });
  });

  it("answers missing sessions as structured tool errors", async () => {
    const client = await connectClient();
    const result = await client.callTool({
      name: "session_resume",
      arguments: { session_id: "sess-missing", task: "continue" },
    });

    expect(result.isError).toBe(true);
    expect(errorCode(documentFrom(result))).toBe("SESSION_ID_INVALID");
  });

  it("keeps delegate_task behavior intact through the refactor", async () => {
    const previousBin = process.env.AGENT_HUB_OMP_BIN;
    const previousArgs = process.env.AGENT_HUB_OMP_ARGS;
    process.env.AGENT_HUB_OMP_BIN = process.execPath;
    process.env.AGENT_HUB_OMP_ARGS = JSON.stringify([
      "-e",
      "require('fs').writeFileSync('mcp.txt', process.argv[1]);",
      "{task}",
    ]);

    const repository = await createGitRepository();
    try {
      const client = await connectClient();
      const result = await client.callTool({
        name: "delegate_task",
        arguments: {
          task: "hello mcp",
          agent: "omp",
          mode: "isolated",
          workspace: repository,
        },
      });

      expect(result.isError ?? false).toBe(false);
      const document = documentFrom(result);
      expect(document.status).toBe("success");
      expect(document.changed_files).toContain("mcp.txt");
    } finally {
      if (previousBin === undefined) delete process.env.AGENT_HUB_OMP_BIN;
      else process.env.AGENT_HUB_OMP_BIN = previousBin;
      if (previousArgs === undefined) delete process.env.AGENT_HUB_OMP_ARGS;
      else process.env.AGENT_HUB_OMP_ARGS = previousArgs;
      await removeDirectory(repository);
    }
  });

  it("creates and resumes a filesystem session through MCP", async () => {
    const previousBin = process.env.AGENT_HUB_OMP_BIN;
    const previousArgs = process.env.AGENT_HUB_OMP_ARGS;
    process.env.AGENT_HUB_OMP_BIN = process.execPath;
    process.env.AGENT_HUB_OMP_ARGS = JSON.stringify([
      "-e",
      "require('fs').writeFileSync('mcp-session.txt', process.argv[1]);",
      "{task}",
    ]);

    const repository = await createGitRepository();
    try {
      const client = await connectClient();
      const createdResult = await client.callTool({
        name: "session_create",
        arguments: {
          workspace: repository,
          agent: "omp",
          task: "first mcp turn",
        },
      });
      expect(createdResult.isError ?? false).toBe(false);
      const created = documentFrom(createdResult);
      expect((created.session as { revision: number }).revision).toBe(1);

      const sessionId = (created.session as { session_id: string }).session_id;
      const resumedResult = await client.callTool({
        name: "session_resume",
        arguments: {
          workspace: repository,
          session_id: sessionId,
          task: "second mcp turn",
        },
      });
      expect(resumedResult.isError ?? false).toBe(false);
      const resumed = documentFrom(resumedResult);
      expect((resumed.session as { revision: number }).revision).toBe(2);
      expect((resumed.continuation as { filesystem: boolean }).filesystem).toBe(true);
    } finally {
      if (previousBin === undefined) delete process.env.AGENT_HUB_OMP_BIN;
      else process.env.AGENT_HUB_OMP_BIN = previousBin;
      if (previousArgs === undefined) delete process.env.AGENT_HUB_OMP_ARGS;
      else process.env.AGENT_HUB_OMP_ARGS = previousArgs;
      await removeDirectory(repository);
    }
  });

  it("reports a partial fan-out as a tool failure and releases refs", async () => {
    const previousBin = process.env.AGENT_HUB_OMP_BIN;
    const previousArgs = process.env.AGENT_HUB_OMP_ARGS;
    const previousGrokBin = process.env.AGENT_HUB_GROK_BIN;
    const previousGrokArgs = process.env.AGENT_HUB_GROK_ARGS;
    process.env.AGENT_HUB_OMP_BIN = process.execPath;
    process.env.AGENT_HUB_OMP_ARGS = JSON.stringify([
      "-e",
      "require('fs').writeFileSync('mcp-partial.txt', process.argv[1]);",
      "{task}",
    ]);
    process.env.AGENT_HUB_GROK_BIN = process.execPath;
    process.env.AGENT_HUB_GROK_ARGS = JSON.stringify(["-e", "process.exit(3)"]);

    const repository = await createGitRepository();
    try {
      const client = await connectClient();
      const result = await client.callTool({
        name: "fanout_candidates",
        arguments: {
          workspace: repository,
          candidates: [
            { agent: "omp", task: "one" },
            { agent: "grok", task: "two" },
          ],
        },
      });

      // Partial is an operation failure, and the refs are gone either way.
      expect(result.isError).toBe(true);
      const document = documentFrom(result);
      expect(document.status).toBe("partial");
      expect(document.ref_cleanup_errors).toBeUndefined();
      expect(await candidateRefNames(repository)).toEqual([]);
    } finally {
      for (const [key, value] of Object.entries({
        AGENT_HUB_OMP_BIN: previousBin,
        AGENT_HUB_OMP_ARGS: previousArgs,
        AGENT_HUB_GROK_BIN: previousGrokBin,
        AGENT_HUB_GROK_ARGS: previousGrokArgs,
      })) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
      await removeDirectory(repository);
    }
  });

  it("adopts the judged winner through compete_candidates and releases every ref", async () => {
    const judgeAwareOmp = [
      "const fs=require('node:fs');",
      "const task=process.argv[1]||'';",
      "if(task.includes('Agent Hub candidate competition')){",
      "  const m=task.match(/^- ([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}) \\|/m);",
      "  if(!m){console.error('judge found no candidate');process.exit(1);}",
      "  console.log('AGENT_HUB_SELECTION: '+JSON.stringify({candidate_id:m[1],reason:'first eligible in request order'}));",
      "}else{fs.writeFileSync('mcp-omp.txt',task);}",
    ].join(" ");

    const previous = {
      OMP_BIN: process.env.AGENT_HUB_OMP_BIN,
      OMP_ARGS: process.env.AGENT_HUB_OMP_ARGS,
      GROK_BIN: process.env.AGENT_HUB_GROK_BIN,
      GROK_ARGS: process.env.AGENT_HUB_GROK_ARGS,
    };
    process.env.AGENT_HUB_OMP_BIN = process.execPath;
    process.env.AGENT_HUB_OMP_ARGS = JSON.stringify(["-e", judgeAwareOmp, "{task}"]);
    process.env.AGENT_HUB_GROK_BIN = process.execPath;
    process.env.AGENT_HUB_GROK_ARGS = JSON.stringify([
      "-e",
      "require('fs').writeFileSync('mcp-grok.txt', process.argv[1]);",
      "{task}",
    ]);

    const repository = await createGitRepository();
    try {
      const client = await connectClient();
      const result = await client.callTool({
        name: "compete_candidates",
        arguments: {
          workspace: repository,
          candidates: [
            { agent: "omp", task: "one" },
            { agent: "grok", task: "two" },
          ],
          judge_agent: "omp",
          auto_merge: true,
        },
      });

      expect(result.isError ?? false).toBe(false);
      const document = documentFrom(result);
      const fan = document.fan_out as { status: string };
      const competition = document.competition as { status: string; mode: string };
      const merge = document.merge as {
        strategy: string;
        clean: boolean;
        error: unknown;
        applied_commit: string;
      };
      expect(fan.status).toBe("success");
      expect(competition.status).toBe("selected");
      expect(competition.mode).toBe("judge");
      expect(merge.strategy).toBe("fast-forward");
      expect(merge.clean).toBe(true);
      expect(merge.error).toBeNull();
      expect(merge.applied_commit).toBe((await runGit(repository, ["rev-parse", "HEAD"])).trim());
      expect(document.ref_cleanup_errors).toBeUndefined();
      expect(await candidateRefNames(repository)).toEqual([]);
    } finally {
      for (const [key, value] of Object.entries({
        AGENT_HUB_OMP_BIN: previous.OMP_BIN,
        AGENT_HUB_OMP_ARGS: previous.OMP_ARGS,
        AGENT_HUB_GROK_BIN: previous.GROK_BIN,
        AGENT_HUB_GROK_ARGS: previous.GROK_ARGS,
      })) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
      await removeDirectory(repository);
    }
  }, 30_000);

  it.runIf(BLOCKED_WRITE_IS_MEANINGFUL)(
    "reports an unreleasable artifact ref as a tool failure",
    async () => {
      const repository = await createGitRepository();
      const refsDir = join(repository, ".git", "refs", "agent-hub", "candidates");
      const previous = {
        OMP_BIN: process.env.AGENT_HUB_OMP_BIN,
        OMP_ARGS: process.env.AGENT_HUB_OMP_ARGS,
        GROK_BIN: process.env.AGENT_HUB_GROK_BIN,
        GROK_ARGS: process.env.AGENT_HUB_GROK_ARGS,
      };
      process.env.AGENT_HUB_OMP_BIN = process.execPath;
      process.env.AGENT_HUB_OMP_ARGS = JSON.stringify([
        "-e",
        BLOCKING_JUDGE_SCRIPT,
        "{task}",
        refsDir,
      ]);
      process.env.AGENT_HUB_GROK_BIN = process.execPath;
      process.env.AGENT_HUB_GROK_ARGS = JSON.stringify([
        "-e",
        "require('fs').writeFileSync('mcp-grok.txt', process.argv[1]);",
        "{task}",
      ]);

      try {
        const client = await connectClient();
        const result = await client.callTool({
          name: "compete_candidates",
          arguments: {
            workspace: repository,
            candidates: [
              { agent: "omp", task: "one" },
              { agent: "grok", task: "two" },
            ],
            judge_agent: "omp",
          },
        });
        await chmod(refsDir, 0o755);

        // Selecting a winner is not enough: refs this tool promised to release
        // are still present, so it reports them and flags the call as failed.
        expect(result.isError ?? false).toBe(true);
        const document = documentFrom(result);
        const fan = document.fan_out as {
          status: string;
          candidates: Array<{ artifact: { ref: string; commit: string } }>;
        };
        const competition = document.competition as { status: string };
        expect(fan.status).toBe("success");
        expect(competition.status).toBe("selected");
        expect(document.merge).toBeNull();
        const cleanup = document.ref_cleanup_errors as Array<{ code: string }>;
        expect(cleanup.map((failure) => failure.code)).toEqual([
          "ARTIFACT_REF_CLEANUP_FAILED",
          "ARTIFACT_REF_CLEANUP_FAILED",
        ]);
        for (const candidate of fan.candidates) {
          expect(await resolveRef(repository, candidate.artifact.ref)).toBe(
            candidate.artifact.commit,
          );
        }
      } finally {
        for (const [key, value] of Object.entries({
          AGENT_HUB_OMP_BIN: previous.OMP_BIN,
          AGENT_HUB_OMP_ARGS: previous.OMP_ARGS,
          AGENT_HUB_GROK_BIN: previous.GROK_BIN,
          AGENT_HUB_GROK_ARGS: previous.GROK_ARGS,
        })) {
          if (value === undefined) delete process.env[key];
          else process.env[key] = value;
        }
        await chmod(refsDir, 0o755).catch(() => {});
        await removeDirectory(repository);
      }
    },
    30_000,
  );
});
