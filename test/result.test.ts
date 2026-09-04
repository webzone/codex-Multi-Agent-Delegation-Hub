import { describe, expect, it } from "vitest";

import { delegate } from "../src/delegate.js";
import type { AgentAdapter } from "../src/types.js";
import { createGitRepository, removeDirectory } from "./helpers.js";

describe("delegate result contract", () => {
  it("normalizes a successful adapter result", async () => {
    const repository = await createGitRepository();
    const adapter: AgentAdapter = {
      id: "fake",
      async execute() {
        return {
          exit_code: 0,
          stdout: "done",
          stderr: "",
          session_id: "session-1",
          stdout_truncated: false,
          stderr_truncated: false,
          error: null,
        };
      },
    };

    try {
      const result = await delegate(
        { task: "inspect", agent: "fake", mode: "direct", workspace: repository },
        { resolveAdapter: () => adapter },
      );

      expect(result.status).toBe("success");
      expect(result.exit_code).toBe(0);
      expect(result.session_id).toBe("session-1");
      expect(result.stdout).toBe("done");
      expect(result.error).toBeNull();
      expect(result.duration_ms).toBeGreaterThanOrEqual(0);
      expect(JSON.parse(JSON.stringify(result))).toEqual(result);
    } finally {
      await removeDirectory(repository);
    }
  });

  it("normalizes a non-zero adapter result as a failure", async () => {
    const repository = await createGitRepository();
    const adapter: AgentAdapter = {
      id: "fake",
      async execute() {
        return {
          exit_code: 7,
          stdout: "partial",
          stderr: "failed",
          session_id: null,
          stdout_truncated: false,
          stderr_truncated: false,
          error: null,
        };
      },
    };

    try {
      const result = await delegate(
        { task: "run", agent: "fake", mode: "direct", workspace: repository },
        { resolveAdapter: () => adapter },
      );

      expect(result.status).toBe("failure");
      expect(result.exit_code).toBe(7);
      expect(result.error).toEqual({ code: "AGENT_FAILED", message: "Agent exited with code 7" });
    } finally {
      await removeDirectory(repository);
    }
  });

  it("returns validation failures through the same serializable shape", async () => {
    const result = await delegate({
      task: " ",
      agent: "fake",
      mode: "direct",
      workspace: "/tmp/not-used",
    });

    expect(result.status).toBe("failure");
    expect(result.error?.code).toBe("INVALID_TASK");
    expect(result.stdout).toBe("");
    expect(result.changed_files).toEqual([]);
  });
});
