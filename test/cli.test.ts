import { describe, expect, it } from "vitest";

import { parseCliArgs, runCli } from "../src/cli.js";
import { createGitRepository, removeDirectory } from "./helpers.js";

describe("CLI", () => {
  it("parses the delegation command", () => {
    const options = parseCliArgs([
      "delegate",
      "--agent",
      "omp",
      "--mode",
      "isolated",
      "--workspace",
      "/tmp/repository",
      "--allow-dirty",
      "fix",
      "the bug",
    ]);

    expect(options.help).toBe(false);
    expect(options.request).toMatchObject({
      agent: "omp",
      mode: "isolated",
      workspace: "/tmp/repository",
      allowDirty: true,
      task: "fix the bug",
    });
  });

  it("emits a machine-readable result and exit success", async () => {
    const repository = await createGitRepository();
    const previousBin = process.env.AGENT_HUB_OMP_BIN;
    const previousArgs = process.env.AGENT_HUB_OMP_ARGS;
    const stdout: string[] = [];
    const stderr: string[] = [];

    process.env.AGENT_HUB_OMP_BIN = process.execPath;
    process.env.AGENT_HUB_OMP_ARGS = JSON.stringify([
      "-e",
      "require('fs').writeFileSync('cli.txt', process.argv[1]);",
      "{task}",
    ]);

    try {
      const exitCode = await runCli(
        ["delegate", "--agent", "omp", "--mode", "isolated", "--workspace", repository, "hello world"],
        { stdout: (value) => stdout.push(value), stderr: (value) => stderr.push(value) },
      );

      expect(exitCode).toBe(0);
      expect(stderr).toEqual([]);
      expect(JSON.parse(stdout.join("")).status).toBe("success");
    } finally {
      if (previousBin === undefined) delete process.env.AGENT_HUB_OMP_BIN;
      else process.env.AGENT_HUB_OMP_BIN = previousBin;
      if (previousArgs === undefined) delete process.env.AGENT_HUB_OMP_ARGS;
      else process.env.AGENT_HUB_OMP_ARGS = previousArgs;
      await removeDirectory(repository);
    }
  });
});
