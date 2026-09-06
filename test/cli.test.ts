import { chmod, readFile } from "node:fs/promises";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { parseCliArgs, parseCliCommand, runCli } from "../src/cli.js";
import { supportedAgents } from "../src/adapters/index.js";
import { FANOUT_MAX_CANDIDATES } from "../src/fanout.js";
import {
  BLOCKED_WRITE_IS_MEANINGFUL,
  BLOCKING_JUDGE_SCRIPT,
  candidateRefNames,
  createGitRepository,
  removeDirectory,
  resolveRef,
  runGit,
} from "./helpers.js";

async function withFakeAgents<T>(body: () => Promise<T>): Promise<T> {
  const saved = {
    OMP_BIN: process.env.AGENT_HUB_OMP_BIN,
    OMP_ARGS: process.env.AGENT_HUB_OMP_ARGS,
    GROK_BIN: process.env.AGENT_HUB_GROK_BIN,
    GROK_ARGS: process.env.AGENT_HUB_GROK_ARGS,
  };
  process.env.AGENT_HUB_OMP_BIN = process.execPath;
  process.env.AGENT_HUB_OMP_ARGS = JSON.stringify([
    "-e",
    "require('fs').writeFileSync('omp.txt', process.argv[1]);",
    "{task}",
  ]);
  process.env.AGENT_HUB_GROK_BIN = process.execPath;
  process.env.AGENT_HUB_GROK_ARGS = JSON.stringify([
    "-e",
    "require('fs').writeFileSync('grok.txt', process.argv[1]);",
    "{task}",
  ]);
  try {
    return await body();
  } finally {
    for (const [key, value] of Object.entries(saved)) {
      const name = `AGENT_HUB_${key}`;
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}

function collectors() {
  const stdout: string[] = [];
  const stderr: string[] = [];
  return {
    stdout,
    stderr,
    output: {
      stdout: (value: string) => stdout.push(value),
      stderr: (value: string) => stderr.push(value),
    },
  };
}

/**
 * A judge-aware fake agent: when fed the competition prompt it emits a valid
 * selection marker line picking the first candidate enumerated in the prompt
 * (request order); otherwise it behaves like the working `omp` agent.
 */
const JUDGE_AWARE_OMP = [
  "const fs=require('node:fs');",
  "const task=process.argv[1]||'';",
  "if(task.includes('Agent Hub candidate competition')){",
  "  const m=task.match(/^- ([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}) \\|/m);",
  "  if(!m){console.error('judge found no candidate');process.exit(1);}",
  "  console.log('AGENT_HUB_SELECTION: '+JSON.stringify({candidate_id:m[1],reason:'first eligible in request order'}));",
  "}else{fs.writeFileSync('omp.txt',task);}",
].join(" ");

async function withAgentEnv<T>(
  ompArgs: string[],
  grokArgs: string[],
  body: () => Promise<T>,
): Promise<T> {
  const saved = {
    OMP_BIN: process.env.AGENT_HUB_OMP_BIN,
    OMP_ARGS: process.env.AGENT_HUB_OMP_ARGS,
    GROK_BIN: process.env.AGENT_HUB_GROK_BIN,
    GROK_ARGS: process.env.AGENT_HUB_GROK_ARGS,
  };
  process.env.AGENT_HUB_OMP_BIN = process.execPath;
  process.env.AGENT_HUB_GROK_BIN = process.execPath;
  process.env.AGENT_HUB_OMP_ARGS = JSON.stringify(ompArgs);
  process.env.AGENT_HUB_GROK_ARGS = JSON.stringify(grokArgs);
  try {
    return await body();
  } finally {
    for (const [key, value] of Object.entries(saved)) {
      const name = `AGENT_HUB_${key}`;
      if (value === undefined) delete process.env[name];
      else process.env[name] = value;
    }
  }
}

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

  it("keeps the bare-argument v1 invocation on the delegate path", () => {
    const invocation = parseCliCommand(["--agent", "omp", "--mode", "direct", "just a task"]);
    expect(invocation.kind).toBe("delegate");
    if (invocation.kind === "delegate") {
      expect(invocation.options.request.task).toBe("just a task");
    }
  });

  describe("fanout parsing", () => {
    it("pairs agents with tasks one to one", () => {
      const invocation = parseCliCommand([
        "fanout",
        "--agent",
        "omp",
        "--agent",
        "grok",
        "--task",
        "alpha",
        "--task",
        "beta",
        "--concurrency",
        "2",
      ]);

      expect(invocation.kind).toBe("fanout");
      if (invocation.kind === "fanout") {
        expect(invocation.request.candidates).toEqual([
          { agent: "omp", task: "alpha" },
          { agent: "grok", task: "beta" },
        ]);
        expect(invocation.request.maxConcurrency).toBe(2);
        expect(invocation.judge).toBeNull();
        expect(invocation.autoMerge).toBe(false);
      }
    });

    it("replicates a single shared task across agents", () => {
      const invocation = parseCliCommand([
        "fanout",
        "--agent",
        "omp",
        "--agent",
        "agy",
        "--agent",
        "grok",
        "--task",
        "same for all",
      ]);

      if (invocation.kind !== "fanout") throw new Error("expected fanout");
      expect(invocation.request.candidates).toHaveLength(3);
      expect(invocation.request.candidates.every((candidate) => candidate.task === "same for all")).toBe(true);
    });

    it("refuses more candidates than the fixed fan-out maximum", () => {
      const over = ["fanout"];
      for (let index = 0; index <= FANOUT_MAX_CANDIDATES; index += 1) {
        over.push("--agent", "omp", "--task", `task-${index}`);
      }
      expect(() => parseCliCommand(over)).toThrow(/at most \d+ candidates/);

      // One shared task replicated over too many agents is refused too.
      const shared = ["fanout", "--task", "same for all"];
      for (let index = 0; index <= FANOUT_MAX_CANDIDATES; index += 1) {
        shared.push("--agent", "omp");
      }
      expect(() => parseCliCommand(shared)).toThrow(/at most \d+ candidates/);

      // The boundary itself parses.
      const boundary = ["fanout"];
      for (let index = 0; index < FANOUT_MAX_CANDIDATES; index += 1) {
        boundary.push("--agent", "omp", "--task", `task-${index}`);
      }
      const invocation = parseCliCommand(boundary);
      if (invocation.kind !== "fanout") throw new Error("expected fanout");
      expect(invocation.request.candidates).toHaveLength(FANOUT_MAX_CANDIDATES);
    });

    it("rejects a task count that matches neither one nor all agents", () => {
      expect(() =>
        parseCliCommand(["fanout", "--agent", "omp", "--agent", "agy", "--agent", "grok", "--task", "a", "--task", "b"]),
      ).toThrow(/--task must be given once/);
    });

    it("requires --judge before --auto-merge may be requested", () => {
      expect(() => parseCliCommand(["fanout", "--agent", "omp", "--task", "t", "--auto-merge"])).toThrow(
        /--auto-merge requires --judge/,
      );
    });

    it("keeps --auto-merge opt-in and validated against the judge", () => {
      const invocation = parseCliCommand([
        "fanout",
        "--agent",
        "omp",
        "--task",
        "t",
        "--judge",
        "omp",
        "--auto-merge",
      ]);
      if (invocation.kind !== "fanout") throw new Error("expected fanout");
      expect(invocation.judge).toBe("omp");
      expect(invocation.autoMerge).toBe(true);

      expect(() =>
        parseCliCommand(["fanout", "--agent", "omp", "--task", "t", "--judge", "hal9000"]),
      ).toThrow(/--judge must be one of/);
    });

    it("rejects bare positional text in favour of explicit --task", () => {
      expect(() => parseCliCommand(["fanout", "--agent", "omp", "free text"])).toThrow(/must use --task/);
    });

    it("does not accept fan-out competition flags on session create", () => {
      expect(() =>
        parseCliCommand(["session", "create", "--agent", "omp", "--task", "t", "--judge", "omp"]),
      ).toThrow(/Unknown option: --judge/);
      expect(() =>
        parseCliCommand(["session", "create", "--agent", "omp", "--task", "t", "--auto-merge"]),
      ).toThrow(/Unknown option: --auto-merge/);
    });
  });

  describe("session parsing", () => {
    it("rejects the removed persisted competition command", () => {
      expect(() => parseCliCommand(["compete", "--judge", "omp"])).toThrow(
        /use fanout with --judge/,
      );
    });

    it("parses session create and session resume", () => {
      expect(parseCliCommand(["session", "create", "--agent", "omp", "--task", "t"])).toEqual({
        kind: "session-create",
        request: expect.objectContaining({
          agent: "omp",
          task: "t",
          allowDirty: false,
        }),
      });
      expect(parseCliCommand(["session", "resume", "sess-9", "--task", "continue", "--workspace", "/tmp/r"])).toEqual({
        kind: "session-resume",
        request: {
          session_id: "sess-9",
          task: "continue",
          workspace: "/tmp/r",
          maxOutputBytes: undefined,
        },
      });
      expect(() => parseCliCommand(["session", "resume"])).toThrow(/session resume requires/);
      expect(() => parseCliCommand(["session", "resume", "sess-9"])).toThrow(/requires --task/);
      expect(() => parseCliCommand(["session", "teleport"])).toThrow(/session requires "create" or "resume"/);
    });
  });

  describe("run conventions", () => {
    it("prints usage and exits 0 on --help", async () => {
      const io = collectors();
      expect(await runCli(["--help"], io.output)).toBe(0);
      expect(io.stdout.join("")).toContain("agent-hub fanout");
      expect(io.stderr).toEqual([]);
    });

    it("exits 2 with usage on stderr for parse errors", async () => {
      const io = collectors();
      expect(await runCli(["fanout", "--agent", "robot", "--task", "t"], io.output)).toBe(2);
      expect(io.stderr.join("")).toContain("Usage:");
      expect(io.stdout).toEqual([]);
    });

    it("exits 1 with a structured JSON error for operation failures", async () => {
      const io = collectors();
      const repository = await createGitRepository();
      await removeDirectory(repository); // gone again → NOT_GIT_REPOSITORY

      const exitCode = await runCli(
        ["fanout", "--agent", "omp", "--task", "t", "--workspace", repository],
        io.output,
      );
      expect(exitCode).toBe(1);
      expect(io.stderr).toEqual([]);
      expect(JSON.parse(io.stdout.join("")).error.code).toBe("NOT_GIT_REPOSITORY");
    });

    it("runs a fan-out end to end and reports artifacts", async () => {
      const repository = await createGitRepository();
      const io = collectors();
      try {
        const exitCode = await withFakeAgents(() =>
          runCli(
            [
              "fanout",
              "--agent",
              "omp",
              "--agent",
              "grok",
              "--task",
              "alpha",
              "--task",
              "beta",
              "--workspace",
              repository,
            ],
            io.output,
          ),
        );

        expect(exitCode).toBe(0);
        const document = JSON.parse(io.stdout.join(""));
        expect(document.error).toBeNull();
        expect(document.base.head).toBe((await runGit(repository, ["rev-parse", "HEAD"])).trim());
        expect(document.candidates).toHaveLength(2);
        expect(document.candidates[0].artifact.changed_files).toContain("omp.txt");
        expect(document.candidates[1].artifact.changed_files).toContain("grok.txt");
        expect(document.candidates.map((candidate: { status: string }) => candidate.status)).toEqual([
          "success",
          "success",
        ]);
        expect(document.status).toBe("success");
        expect(document.ref_cleanup_errors).toBeUndefined();
        // The terminal CLI released the private artifact refs it created.
        expect(await candidateRefNames(repository)).toEqual([]);
      } finally {
        await removeDirectory(repository);
      }
    });

    it("reports a partial fan-out as an operation failure and releases refs", async () => {
      const repository = await createGitRepository();
      const io = collectors();
      try {
        const exitCode = await withAgentEnv(
          ["-e", "require('node:fs').writeFileSync('omp.txt', process.argv[1]);", "{task}"],
          ["-e", "process.exit(3)"],
          () =>
            runCli(
              [
                "fanout",
                "--agent",
                "omp",
                "--agent",
                "grok",
                "--task",
                "alpha",
                "--task",
                "beta",
                "--workspace",
                repository,
              ],
              io.output,
            ),
        );

        expect(exitCode).toBe(1);
        const document = JSON.parse(io.stdout.join(""));
        expect(document.status).toBe("partial");
        expect(document.error).toBeNull();
        expect(document.candidates.map((c: { status: string }) => c.status)).toEqual([
          "success",
          "failure",
        ]);
        expect(document.ref_cleanup_errors).toBeUndefined();
        expect(await candidateRefNames(repository)).toEqual([]);
      } finally {
        await removeDirectory(repository);
      }
    });

    it("reports an all-failed fan-out as an operation failure", async () => {
      const repository = await createGitRepository();
      const io = collectors();
      try {
        const exitCode = await withAgentEnv(
          ["-e", "process.exit(3)"],
          ["-e", "process.exit(4)"],
          () =>
            runCli(
              [
                "fanout",
                "--agent",
                "omp",
                "--agent",
                "grok",
                "--task",
                "alpha",
                "--workspace",
                repository,
              ],
              io.output,
            ),
        );

        expect(exitCode).toBe(1);
        const document = JSON.parse(io.stdout.join(""));
        expect(document.status).toBe("failure");
      } finally {
        await removeDirectory(repository);
      }
    });

    it(
      "adopts the judged winner by fast-forward and releases every artifact ref",
      async () => {
        const repository = await createGitRepository();
        const io = collectors();
        try {
          const exitCode = await withAgentEnv(
            ["-e", JUDGE_AWARE_OMP, "{task}"],
            ["-e", "require('node:fs').writeFileSync('grok.txt', process.argv[1]);", "{task}"],
            () =>
              runCli(
                [
                  "fanout",
                  "--agent",
                  "omp",
                  "--agent",
                  "grok",
                  "--task",
                  "alpha",
                  "--task",
                  "beta",
                  "--judge",
                  "omp",
                  "--auto-merge",
                  "--workspace",
                  repository,
                ],
                io.output,
              ),
          );

          expect(exitCode).toBe(0);
          const document = JSON.parse(io.stdout.join(""));
          expect(document.fan_out.status).toBe("success");
          expect(document.competition.status).toBe("selected");
          expect(document.competition.mode).toBe("judge");
          expect(document.merge.strategy).toBe("fast-forward");
          expect(document.merge.clean).toBe(true);
          expect(document.merge.error).toBeNull();
          expect(document.merge.applied_commit).toBe(
            (await runGit(repository, ["rev-parse", "HEAD"])).trim(),
          );
          expect(document.ref_cleanup_errors).toBeUndefined();
          expect(await candidateRefNames(repository)).toEqual([]);
          expect(await runGit(repository, ["status", "--porcelain=v1", "--untracked-files=all"])).toBe("");
          // The winner — first in request order, the omp candidate — landed.
          expect(await readFile(join(repository, "omp.txt"), "utf8")).toBe("alpha");
        } finally {
          await removeDirectory(repository);
        }
      },
      30_000,
    );

    it.runIf(BLOCKED_WRITE_IS_MEANINGFUL)(
      "exits 1 with ref_cleanup_errors when the artifact refs cannot be released",
      async () => {
        const repository = await createGitRepository();
        const refsDir = join(repository, ".git", "refs", "agent-hub", "candidates");
        const io = collectors();
        try {
          // The judge is the deterministic moment: it runs after every artifact
          // ref exists and before the command releases any of them.
          const exitCode = await withAgentEnv(
            ["-e", BLOCKING_JUDGE_SCRIPT, "{task}", refsDir],
            ["-e", "require('node:fs').writeFileSync('grok.txt', process.argv[1]);", "{task}"],
            () =>
              runCli(
                [
                  "fanout",
                  "--agent",
                  "omp",
                  "--agent",
                  "grok",
                  "--task",
                  "alpha",
                  "--task",
                  "beta",
                  "--judge",
                  "omp",
                  "--workspace",
                  repository,
                ],
                io.output,
              ),
          );

          await chmod(refsDir, 0o755);

          // The operation itself succeeded; the promised release did not, and
          // that is an operation failure with the refs still in place.
          expect(exitCode).toBe(1);
          const document = JSON.parse(io.stdout.join(""));
          expect(document.fan_out.status).toBe("success");
          expect(document.competition.status).toBe("selected");
          expect(document.merge).toBeUndefined();
          expect(
            document.ref_cleanup_errors.map((failure: { code: string }) => failure.code),
          ).toEqual(["ARTIFACT_REF_CLEANUP_FAILED", "ARTIFACT_REF_CLEANUP_FAILED"]);
          for (const candidate of document.fan_out.candidates as Array<{
            artifact: { ref: string; commit: string };
          }>) {
            expect(await resolveRef(repository, candidate.artifact.ref)).toBe(
              candidate.artifact.commit,
            );
          }
        } finally {
          await chmod(refsDir, 0o755).catch(() => {});
          await removeDirectory(repository);
        }
      },
      30_000,
    );

    it("creates and resumes a durable filesystem session", async () => {
      const repository = await createGitRepository();
      try {
        await withFakeAgents(async () => {
          const createdIo = collectors();
          const createExit = await runCli(
            ["session", "create", "--agent", "omp", "--task", "first turn", "--workspace", repository],
            createdIo.output,
          );
          expect(createExit).toBe(0);
          const created = JSON.parse(createdIo.stdout.join(""));
          expect(created.run.status).toBe("success");
          expect(created.session.revision).toBe(1);
          expect(created.artifact.changed_files).toContain("omp.txt");

          const resumedIo = collectors();
          const resumeExit = await runCli(
            [
              "session",
              "resume",
              created.session.session_id,
              "--task",
              "second turn",
              "--workspace",
              repository,
            ],
            resumedIo.output,
          );
          expect(resumeExit).toBe(0);
          const resumed = JSON.parse(resumedIo.stdout.join(""));
          expect(resumed.run.status).toBe("success");
          expect(resumed.session.revision).toBe(2);
          expect(resumed.artifact.parent).toBe(created.artifact.commit);
          expect(resumed.continuation.filesystem).toBe(true);
        });
      } finally {
        await removeDirectory(repository);
      }
    });
  });

  describe("strict command dispatch", () => {
    it("keeps the explicit delegate subcommand on the delegate path", () => {
      const invocation = parseCliCommand([
        "delegate",
        "--agent",
        "omp",
        "--mode",
        "isolated",
        "a task",
      ]);
      expect(invocation.kind).toBe("delegate");
      if (invocation.kind === "delegate") {
        expect(invocation.options.request.task).toBe("a task");
      }
    });

    it("rejects an unknown leading word instead of treating it as task text", () => {
      expect(() =>
        parseCliCommand(["frobnicate", "--agent", "omp", "--mode", "direct", "t"]),
      ).toThrow(/unknown command "frobnicate"/);
      expect(() => parseCliCommand(["build", "the", "thing"])).toThrow(/unknown command "build"/);
    });

    it("exits 2 on an unknown command and never reaches the agent path", async () => {
      const io = collectors();
      expect(await runCli(["frobnicate", "--agent", "omp", "--mode", "direct", "t"], io.output)).toBe(2);
      expect(io.stderr.join("")).toContain('unknown command "frobnicate"');
      expect(io.stderr.join("")).toContain("Usage:");
      // Nothing operational ran: an unknown command never launches an agent.
      expect(io.stdout.join("")).toBe("");
    });

    it("exits 2 for --help on an unknown command and 0 on recognized ones", async () => {
      const unknownIo = collectors();
      expect(await runCli(["frobnicate", "--help"], unknownIo.output)).toBe(2);
      expect(unknownIo.stdout.join("")).toBe("");

      for (const argv of [
        ["--help"],
        ["delegate", "--help"],
        ["fanout", "--help"],
        ["session", "--help"],
        ["live", "--help"],
      ]) {
        const io = collectors();
        expect(await runCli(argv, io.output)).toBe(0);
        expect(io.stdout.join("")).toContain("Usage:");
      }
    });

    it("supports the `--` terminator so a task may literally start with --", () => {
      const options = parseCliArgs([
        "delegate",
        "--agent",
        "omp",
        "--mode",
        "isolated",
        "--",
        "--flag-like",
        "task",
        "text",
      ]);
      expect(options.request.task).toBe("--flag-like task text");

      // After the terminator, `--help` is task text, not a flag.
      const after = parseCliArgs(["--agent", "omp", "--mode", "direct", "--", "--help"]);
      expect(after.help).toBe(false);
      expect(after.request.task).toBe("--help");

      // Before the terminator, `--help` still wins.
      expect(parseCliArgs(["--help", "--", "text"]).help).toBe(true);
    });

    it("runs a task that starts with -- end to end", async () => {
      const repository = await createGitRepository();
      try {
        const io = collectors();
        // The fake agent reads the task from its own argv; the `--` before
        // {task} keeps node itself from eating a `--task` as a node flag —
        // proof the hub forwarded the terminated text verbatim.
        await withAgentEnv(
          ["-e", "require('node:fs').writeFileSync('omp.txt', process.argv[1]);", "--", "{task}"],
          ["-e", "process.exit(1)"],
          async () => {
            expect(
              await runCli(
                [
                  "delegate",
                  "--agent",
                  "omp",
                  "--mode",
                  "direct",
                  "--workspace",
                  repository,
                  "--",
                  "--carry-this-through",
                ],
                io.output,
              ),
            ).toBe(0);
          },
        );
        expect(JSON.parse(io.stdout.join("")).status).toBe("success");
        expect(await readFile(join(repository, "omp.txt"), "utf8")).toBe("--carry-this-through");
      } finally {
        await removeDirectory(repository);
      }
    });
  });
});

describe("legacy agent vocabulary (unchanged by v3 live surfaces)", () => {
  it("keeps supportedAgents exactly omp, agy, grok", () => {
    expect(supportedAgents).toEqual(["omp", "agy", "grok"]);
  });
  it("rejects pi and hermes at parse time on fanout and session", async () => {
    const cases: string[][] = [
      ["fanout", "--agent", "pi", "--task", "task"],
      ["fanout", "--agent", "omp", "--task", "task", "--judge", "hermes"],
      ["session", "create", "--agent", "hermes", "--task", "task", "--workspace", "/tmp"],
    ];

    for (const argv of cases) {
      const stdout: string[] = [];
      const stderr: string[] = [];
      const exitCode = await runCli(argv, {
        stdout: (value) => stdout.push(value),
        stderr: (value) => stderr.push(value),
      });

      expect(`${argv.join(" ")} → ${exitCode}`).toBe(`${argv.join(" ")} → 2`);
      expect(stderr.join("")).toContain("must be one of: omp, agy, grok");
      expect(stdout).toEqual([]);
    }
  });

  it("rejects pi and hermes at execution time on the delegate path", async () => {
    for (const agent of ["pi", "hermes"]) {
      const stdout: string[] = [];
      const stderr: string[] = [];
      const exitCode = await runCli(
        ["delegate", "--agent", agent, "--mode", "isolated", "--workspace", "/tmp", "task"],
        { stdout: (value) => stdout.push(value), stderr: (value) => stderr.push(value) },
      );

      expect(exitCode).toBe(1);
      const document = JSON.parse(stdout.join(""));
      expect(document.error).toMatchObject({ code: "UNKNOWN_AGENT" });
      expect(document.error.message).toContain("Choose one of: omp, agy, grok");
      expect(stderr).toEqual([]);
    }
  });
});
