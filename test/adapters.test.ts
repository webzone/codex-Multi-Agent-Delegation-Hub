import { describe, expect, it } from "vitest";

import { createAgyAdapter } from "../src/adapters/agy.js";
import { createGrokAdapter } from "../src/adapters/grok.js";
import { createOmpAdapter } from "../src/adapters/omp.js";
import { resolveAdapter } from "../src/adapters/index.js";
import { createGitRepository, removeDirectory } from "./helpers.js";
import { createCommandAdapter } from "../src/adapters/command-adapter.js";
import {
  asNativeResumeCapableAdapter,
  PROVIDER_SESSION_METADATA_KEY,
} from "../src/adapters/types.js";

const ARGV_ECHO = "process.stdout.write(JSON.stringify(process.argv.slice(1)))";

function echoAdapter(
  environment: NodeJS.ProcessEnv,
  nativeResume?: { verify: () => Promise<boolean>; resumeArguments: (id: string) => string[] },
) {
  return createCommandAdapter(
    {
      id: "echo",
      environmentPrefix: "AGENT_HUB_ECHO",
      defaultExecutable: process.execPath,
      defaultArguments: ["-e", ARGV_ECHO, "{task}"],
      nativeResume,
    },
    environment,
  );
}

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

  it("ignores request metadata when no capability is configured, keeping argv v1-exact", async () => {
    const repository = await createGitRepository();
    try {
      const adapter = echoAdapter({});
      const without = await adapter.execute({ task: "plain task", cwd: repository, maxOutputBytes: 2000 });
      const withMeta = await adapter.execute({
        task: "plain task",
        cwd: repository,
        maxOutputBytes: 2000,
        metadata: { [PROVIDER_SESSION_METADATA_KEY]: "provider-77" },
      });
      expect(without.exit_code).toBe(0);
      expect(without.stdout).toBe(JSON.stringify(["plain task"]));
      expect(withMeta.stdout).toBe(without.stdout);
      expect(withMeta.session_id).toBeNull();
      expect(asNativeResumeCapableAdapter(adapter)).toBeNull();
    } finally {
      await removeDirectory(repository);
    }
  });

  it("appends verified resume arguments as discrete argv elements, never shell text", async () => {
    const repository = await createGitRepository();
    const hostile = "prov id; touch /tmp/agent-hub-should-not-exist && echo $(x)";
    try {
      const adapter = echoAdapter(
        {},
        { verify: async () => true, resumeArguments: (id) => ["--resume", id] },
      );
      const result = await adapter.execute({
        task: "continue",
        cwd: repository,
        maxOutputBytes: 2000,
        metadata: { [PROVIDER_SESSION_METADATA_KEY]: hostile },
      });
      expect(result.exit_code).toBe(0);
      expect(result.error).toBeNull();
      expect(JSON.parse(result.stdout)).toEqual(["continue", "--resume", hostile]);

      // No metadata -> plain v1 argv even with a verified capability.
      const plain = await adapter.execute({ task: "continue", cwd: repository, maxOutputBytes: 2000 });
      expect(JSON.parse(plain.stdout)).toEqual(["continue"]);
    } finally {
      await removeDirectory(repository);
    }
  });

  it("treats unverified or throwing probes as no-native and caches the probe", async () => {
    const repository = await createGitRepository();
    let probes = 0;
    try {
      const adapter = echoAdapter(
        {},
        {
          verify: async () => {
            probes += 1;
            return false;
          },
          resumeArguments: (id) => ["--resume", id],
        },
      );
      const result = await adapter.execute({
        task: "continue",
        cwd: repository,
        maxOutputBytes: 2000,
        metadata: { [PROVIDER_SESSION_METADATA_KEY]: "provider-77" },
      });
      expect(JSON.parse(result.stdout)).toEqual(["continue"]);
      await adapter.execute({
        task: "again",
        cwd: repository,
        maxOutputBytes: 2000,
        metadata: { [PROVIDER_SESSION_METADATA_KEY]: "provider-77" },
      });
      expect(probes).toBe(1); // memoized per adapter instance

      const throwing = echoAdapter(
        {},
        {
          verify: async () => {
            throw new Error("probe exploded");
          },
          resumeArguments: (id) => ["--resume", id],
        },
      );
      const capable = asNativeResumeCapableAdapter(throwing);
      expect(capable).not.toBeNull();
      await expect(capable?.nativeResumeCapability.verify()).resolves.toBe(false);
    } finally {
      await removeDirectory(repository);
    }
  });

  it("built-in omp/agy/grok adapters expose no native-resume capability", () => {
    for (const adapter of [createOmpAdapter(), createAgyAdapter(), createGrokAdapter()]) {
      expect(adapter.id).toBeTruthy();
      expect(asNativeResumeCapableAdapter(adapter)).toBeNull();
    }
  });

  it("resolveAdapter returns replaceable built-ins unchanged", () => {
    expect(resolveAdapter("grok").id).toBe("grok");
    expect(asNativeResumeCapableAdapter(resolveAdapter("omp"))).toBeNull();
  });
});
