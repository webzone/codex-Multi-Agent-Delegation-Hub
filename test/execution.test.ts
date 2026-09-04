import { access, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { delegate } from "../src/delegate.js";
import type { AgentAdapter } from "../src/types.js";
import { createGitRepository, removeDirectory, runGit } from "./helpers.js";

function changingAdapter(fileName: string, content: string, exitCode = 0): AgentAdapter {
  return {
    id: "fake",
    async execute({ cwd }) {
      await writeFile(join(cwd, fileName), content);
      return {
        exit_code: exitCode,
        stdout: "agent output",
        stderr: exitCode === 0 ? "" : "agent failed",
        session_id: null,
        stdout_truncated: false,
        stderr_truncated: false,
        error: null,
      };
    },
  };
}

async function worktreeCount(repository: string): Promise<number> {
  const output = await runGit(repository, ["worktree", "list", "--porcelain"]);
  return (output.match(/^worktree /gm) ?? []).length;
}

describe("execution safety", () => {
  it("rejects a dirty direct workspace unless explicitly allowed", async () => {
    const repository = await createGitRepository();
    await writeFile(join(repository, "existing.txt"), "existing\n");
    let called = false;
    const adapter = changingAdapter("new.txt", "new\n");
    const wrappedAdapter: AgentAdapter = {
      ...adapter,
      async execute(request) {
        called = true;
        return adapter.execute(request);
      },
    };

    try {
      const result = await delegate(
        { task: "change", agent: "fake", mode: "direct", workspace: repository },
        { resolveAdapter: () => wrappedAdapter },
      );

      expect(result.status).toBe("failure");
      expect(result.error?.code).toBe("DIRTY_WORKTREE");
      expect(called).toBe(false);

      const allowed = await delegate(
        { task: "change", agent: "fake", mode: "direct", workspace: repository, allowDirty: true },
        { resolveAdapter: () => wrappedAdapter },
      );
      expect(allowed.status).toBe("success");
      expect(called).toBe(true);
    } finally {
      await removeDirectory(repository);
    }
  });

  it("keeps isolated changes out of the primary checkout and cleans up", async () => {
    const repository = await createGitRepository();

    try {
      const result = await delegate(
        { task: "change", agent: "fake", mode: "isolated", workspace: repository },
        { resolveAdapter: () => changingAdapter("isolated.txt", "isolated\n") },
      );

      expect(result.status).toBe("success");
      expect(result.changed_files).toContain("isolated.txt");
      await expect(access(join(repository, "isolated.txt"))).rejects.toThrow();
      await expect(access(result.execution_workspace)).rejects.toThrow();
      expect(await worktreeCount(repository)).toBe(1);
    } finally {
      await removeDirectory(repository);
    }
  });

  it("cleans up an isolated worktree when the agent fails", async () => {
    const repository = await createGitRepository();

    try {
      const result = await delegate(
        { task: "fail", agent: "fake", mode: "isolated", workspace: repository },
        { resolveAdapter: () => changingAdapter("failed.txt", "failed\n", 2) },
      );

      expect(result.status).toBe("failure");
      expect(result.error?.code).toBe("AGENT_FAILED");
      expect(await worktreeCount(repository)).toBe(1);
      await expect(readFile(join(repository, "failed.txt"))).rejects.toThrow();
    } finally {
      await removeDirectory(repository);
    }
  });
});
