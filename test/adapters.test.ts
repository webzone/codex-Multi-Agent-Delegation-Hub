import { describe, expect, it } from "vitest";

import { createAgyAdapter } from "../src/adapters/agy.js";
import { createGrokAdapter } from "../src/adapters/grok.js";
import { createOmpAdapter } from "../src/adapters/omp.js";
import { resolveAdapter } from "../src/adapters/index.js";
import { createGitRepository, removeDirectory } from "./helpers.js";

describe("command adapters", () => {
  it("pass task text as an argument without shell interpolation", async () => {
    const repository = await createGitRepository();
    const marker = "$(touch should-not-exist) task with spaces";
    const environment = {
      AGENT_HUB_OMP_BIN: process.execPath,
      AGENT_HUB_OMP_ARGS: JSON.stringify([
        "-e",
        "process.stdout.write(process.argv[1])",
        "{task}",
      ]),
    };

    try {
      const result = await createOmpAdapter(environment).execute({
        task: marker,
        cwd: repository,
        maxOutputBytes: 1000,
      });

      expect(result.exit_code).toBe(0);
      expect(result.stdout).toBe(marker);
      expect(result.error).toBeNull();
    } finally {
      await removeDirectory(repository);
    }
  });

  it("captures bounded stdout and stderr", async () => {
    const repository = await createGitRepository();
    const environment = {
      AGENT_HUB_OMP_BIN: process.execPath,
      AGENT_HUB_OMP_ARGS: JSON.stringify([
        "-e",
        "process.stdout.write('123456'); process.stderr.write('abcdef')",
        "{task}",
      ]),
    };

    try {
      const result = await createOmpAdapter(environment).execute({
        task: "ignored",
        cwd: repository,
        maxOutputBytes: 3,
      });

      expect(result.stdout).toBe("123");
      expect(result.stderr).toBe("abc");
      expect(result.stdout_truncated).toBe(true);
      expect(result.stderr_truncated).toBe(true);
    } finally {
      await removeDirectory(repository);
    }
  });

  it("exposes replaceable built-in adapters", () => {
    expect(createOmpAdapter().id).toBe("omp");
    expect(createAgyAdapter().id).toBe("agy");
    expect(createGrokAdapter().id).toBe("grok");
    expect(resolveAdapter("grok").id).toBe("grok");
  });
});
