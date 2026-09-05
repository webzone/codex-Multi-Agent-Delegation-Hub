import { access, realpath, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { runCompetition, type CompetitionDependencies, type CompetitionStrategy } from "../src/competition.js";
import { AgentHubError } from "../src/errors.js";
import { fanOut } from "../src/fanout.js";
import { resolveRepositoryIdentity } from "../src/git.js";
import { runProcess } from "../src/process.js";
import { SELECTION_MARKER_PREFIX } from "../src/selection.js";
import type {
  AdapterRequest,
  AgentAdapter,
  CandidateArtifact,
  FanOutCandidateResult,
  FanOutResult,
  RepositoryIdentity,
} from "../src/types.js";
import { createGitRepository, removeDirectory, runGit } from "./helpers.js";

function sha(n: number): string {
  return `${n.toString(16).padStart(2, "0")}${"0".repeat(38)}`;
}

function artifactRef(n: number): string {
  return `refs/agent-hub/candidates/hash${n}-${sha(n).slice(0, 12)}`;
}

function validArtifact(base: RepositoryIdentity, n: number): CandidateArtifact {
  return {
    parent: base.head,
    commit: sha(n),
    tree: sha(n + 100),
    ref: artifactRef(n),
    empty: false,
    changed_files: [`f${n}.txt`],
    diff: `DIFF_LEAK_${n}`,
    diff_truncated: false,
  };
}

function emptyArtifact(base: RepositoryIdentity): CandidateArtifact {
  return {
    parent: base.head,
    commit: null,
    tree: null,
    ref: null,
    empty: true,
    changed_files: [],
    diff: "",
    diff_truncated: false,
  };
}

/** Synthetic fanOut() output; artifacts never need to exist in Git because tests inject verifyArtifact. */
function candidateFixture(
  base: RepositoryIdentity,
  index: number,
  options: {
    id?: string;
    agent?: string;
    status?: "success" | "failure";
    artifact?: CandidateArtifact | null;
    finishedAt?: string;
  } = {},
): FanOutCandidateResult {
  const status = options.status ?? "success";
  const artifact = options.artifact === undefined ? validArtifact(base, index + 1) : options.artifact;
  return {
    agent: options.agent ?? `agent-${index}`,
    mode: "isolated",
    status,
    exit_code: status === "success" ? 0 : 1,
    session_id: null,
    summary: "",
    stdout: `CANDIDATE_STDOUT_${index}`,
    stderr: "",
    started_at: "2026-01-01T00:00:00.000Z",
    finished_at: options.finishedAt ?? "2026-01-01T00:00:01.000Z",
    duration_ms: 1000,
    workspace: base.worktree_root,
    execution_workspace: "/nonexistent/execution",
    changed_files: artifact?.changed_files ?? [],
    diff: artifact?.diff ?? "",
    output_truncated: { stdout: false, stderr: false, diff: false },
    error: status === "success" ? null : { code: "AGENT_FAILED", message: "boom" },
    index,
    label: `c${index}`,
    task: `task-${index}`,
    candidate_id: options.id ?? `cand-${index}`,
    artifact,
  };
}

function fanResult(base: RepositoryIdentity, candidates: FanOutCandidateResult[]): FanOutResult {
  const successes = candidates.filter((candidate) => candidate.status === "success").length;
  return {
    base,
    max_concurrency: candidates.length,
    candidates,
    status:
      successes === 0
        ? "failure"
        : successes === candidates.length
          ? "success"
          : "partial",
    started_at: "2026-01-01T00:00:00.000Z",
    finished_at: "2026-01-01T00:00:02.000Z",
    duration_ms: 2000,
    error: null,
  };
}

const verifyAll = async (): Promise<boolean> => true;

interface JudgeBehavior {
  stdout?: string | ((request: AdapterRequest) => string | Promise<string>);
  exitCode?: number;
  processError?: string;
  throws?: Error;
  truncated?: boolean;
}

function fakeJudge(behavior: JudgeBehavior): { adapter: AgentAdapter; requests: AdapterRequest[] } {
  const requests: AdapterRequest[] = [];
  const adapter: AgentAdapter = {
    id: "fake-judge",
    async execute(request) {
      requests.push(request);
      if (behavior.throws) throw behavior.throws;
      const stdout =
        typeof behavior.stdout === "function" ? await behavior.stdout(request) : behavior.stdout ?? "";
      return {
        exit_code: behavior.exitCode ?? 0,
        stdout,
        stderr: "",
        session_id: null,
        stdout_truncated: behavior.truncated ?? false,
        stderr_truncated: false,
        error: behavior.processError ?? null,
      };
    },
  };
  return { adapter, requests };
}

function judgeResolver(adapter: AgentAdapter): (agent: string) => AgentAdapter {
  return (agent) => {
    expect(agent).toBe("judge-1");
    return adapter;
  };
}

function noJudge(): (agent: string) => AgentAdapter {
  return () => {
    throw new Error("judge must not run");
  };
}

function markerLine(id: string, reason = "better in every way"): string {
  return `${SELECTION_MARKER_PREFIX} ${JSON.stringify({ candidate_id: id, reason })}`;
}

async function worktreeCount(repository: string): Promise<number> {
  const out = await runGit(repository, ["worktree", "list", "--porcelain"]);
  return out.split("\n").filter((line) => line.startsWith("worktree ")).length;
}

describe("competition", { timeout: 30_000 }, () => {
  it("fails when no candidate is eligible", async () => {
    const repo = await createGitRepository();
    const base = await resolveRepositoryIdentity(repo);
    try {
      const fan = fanResult(base, [
        candidateFixture(base, 0, { status: "failure" }),
        candidateFixture(base, 1, { artifact: null }),
        candidateFixture(base, 2, { artifact: emptyArtifact(base) }),
      ]);
      const result = await runCompetition(
        { fan_out: fan, strategy: "request_order" },
        { verifyArtifact: verifyAll, resolveAdapter: noJudge() },
      );
      expect(result.status).toBe("failure");
      expect(result.mode).toBe("none");
      expect(result.winner).toBeNull();
      expect(result.judge).toBeNull();
      expect(result.error?.code).toBe("NO_ELIGIBLE_CANDIDATES");
      expect(result.eligible).toEqual([]);
      expect(result.retained_artifact_refs).toEqual([]);
      expect(result.rejected.map((entry) => entry.code)).toEqual([
        "STATUS_NOT_SUCCESS",
        "ARTIFACT_MISSING",
        "ARTIFACT_EMPTY",
      ]);
      expect(() => JSON.stringify(result)).not.toThrow();
    } finally {
      await removeDirectory(repo);
    }
  });

  it("selects the sole success without consulting a judge", async () => {
    const repo = await createGitRepository();
    const base = await resolveRepositoryIdentity(repo);
    try {
      const fan = fanResult(base, [
        candidateFixture(base, 0, { status: "failure" }),
        candidateFixture(base, 1),
      ]);
      // strategy "judge" is requested but must never run for a sole success.
      const result = await runCompetition(
        { fan_out: fan, strategy: "judge", judge_agent: "judge-1" },
        { verifyArtifact: verifyAll, resolveAdapter: noJudge() },
      );
      expect(result.status).toBe("selected");
      expect(result.mode).toBe("sole_success");
      expect(result.winner).toEqual({
        candidate_id: "cand-1",
        reason: "Sole eligible candidate; selected without a judge.",
        basis: "sole_success",
      });
      expect(result.judge).toBeNull();
      expect(result.retained_artifact_refs).toEqual([artifactRef(2)]);
    } finally {
      await removeDirectory(repo);
    }
  });

  it("request order picks the earliest index regardless of completion speed", async () => {
    const repo = await createGitRepository();
    const base = await resolveRepositoryIdentity(repo);
    try {
      // cand-2 finished first, cand-0 last: ordering must follow request index.
      const fan = fanResult(base, [
        candidateFixture(base, 0, { finishedAt: "2026-01-01T00:00:03.000Z" }),
        candidateFixture(base, 1, { finishedAt: "2026-01-01T00:00:02.000Z" }),
        candidateFixture(base, 2, { finishedAt: "2026-01-01T00:00:01.000Z" }),
      ]);
      const result = await runCompetition(
        { fan_out: fan, strategy: "request_order" },
        { verifyArtifact: verifyAll, resolveAdapter: noJudge() },
      );
      expect(result.mode).toBe("request_order");
      expect(result.winner?.candidate_id).toBe("cand-0");
      expect(result.winner?.basis).toBe("request_order");
      expect(result.judge).toBeNull();
      expect(result.eligible.map((entry) => entry.candidate_id)).toEqual([
        "cand-0",
        "cand-1",
        "cand-2",
      ]);
    } finally {
      await removeDirectory(repo);
    }
  });

  it("excludes unsafe ids, wrong parents, and artifact refs that no longer resolve", async () => {
    const repo = await createGitRepository();
    const base = await resolveRepositoryIdentity(repo);
    try {
      const wrongParent = validArtifact(base, 5);
      wrongParent.parent = "f".repeat(40);
      const fan = fanResult(base, [
        candidateFixture(base, 0),
        candidateFixture(base, 1, { id: "cand-lost" }),
        candidateFixture(base, 2, { status: "failure" }),
        candidateFixture(base, 3, { artifact: emptyArtifact(base) }),
        candidateFixture(base, 4, { artifact: wrongParent }),
        candidateFixture(base, 5, { id: "../evil" }),
      ]);
      const deps: CompetitionDependencies = {
        resolveAdapter: noJudge(),
        verifyArtifact: (_workspace, candidate) =>
          Promise.resolve(candidate.candidate_id !== "cand-lost"),
      };
      const result = await runCompetition({ fan_out: fan, strategy: "request_order" }, deps);
      expect(result.status).toBe("selected");
      expect(result.winner?.candidate_id).toBe("cand-0");
      expect(result.eligible).toHaveLength(1);
      expect(result.eligible[0].artifact_commit).toBe(sha(1));
      expect(result.rejected.map((entry) => entry.code)).toEqual([
        "ARTIFACT_REF_NOT_RETAINED",
        "STATUS_NOT_SUCCESS",
        "ARTIFACT_EMPTY",
        "ARTIFACT_PARENT_MISMATCH",
        "ARTIFACT_ID_INVALID",
      ]);
    } finally {
      await removeDirectory(repo);
    }
  });

  it("rejects duplicate eligible ids with a concrete failure", async () => {
    const repo = await createGitRepository();
    const base = await resolveRepositoryIdentity(repo);
    try {
      const fan = fanResult(base, [
        candidateFixture(base, 0),
        candidateFixture(base, 1, { id: "cand-0" }),
      ]);
      const result = await runCompetition(
        { fan_out: fan, strategy: "request_order" },
        { verifyArtifact: verifyAll },
      );
      expect(result.status).toBe("failure");
      expect(result.error?.code).toBe("DUPLICATE_CANDIDATE_ID");
      expect(result.winner).toBeNull();
      // Ambiguous ids abort selection, but the refs exist: cleanup ownership
      // is still handed to the caller.
      expect(result.retained_artifact_refs).toEqual([artifactRef(1), artifactRef(2)]);
    } finally {
      await removeDirectory(repo);
    }
  });

  it("runs the judge in its own detached base worktree on bounded metadata and selects via marker", async () => {
    const repo = await createGitRepository();
    const base = await resolveRepositoryIdentity(repo);
    let judgeHeadSeen = "";
    let judgeCwdRealpath = "";
    try {
      const fan = fanResult(base, [candidateFixture(base, 0), candidateFixture(base, 1)]);
      const judge = fakeJudge({
        stdout: async (request) => {
          judgeHeadSeen = (
            await runProcess("git", ["rev-parse", "HEAD"], {
              cwd: request.cwd,
              maxOutputBytes: 200,
            })
          ).stdout.trim();
          judgeCwdRealpath = await realpath(request.cwd);
          return markerLine("cand-1", "tighter scope");
        },
      });
      const result = await runCompetition(
        { fan_out: fan, strategy: "judge", judge_agent: "judge-1" },
        { verifyArtifact: verifyAll, resolveAdapter: judgeResolver(judge.adapter) },
      );

      expect(result.status).toBe("selected");
      expect(result.mode).toBe("judge");
      expect(result.winner).toEqual({
        candidate_id: "cand-1",
        reason: "tighter scope",
        basis: "judge",
      });
      expect(result.judge).toEqual({
        judge_agent: "judge-1",
        ranking: ["cand-1"],
        winner_id: "cand-1",
        judgements: [
          {
            candidate_id: "cand-1",
            index: 1,
            verdict: "accepted",
            score: 1,
            rationale: "tighter scope",
          },
        ],
        raw_output: markerLine("cand-1", "tighter scope"),
        truncated: false,
        error: null,
      });
      expect(result.retained_artifact_refs).toEqual([artifactRef(1), artifactRef(2)]);

      // The prompt carries only bounded metadata — never candidate stdout/diffs.
      const prompt = judge.requests[0].task;
      expect(prompt).toContain(base.head);
      expect(prompt).toContain("cand-0");
      expect(prompt).toContain("cand-1");
      expect(prompt).toContain(sha(1));
      expect(prompt).toContain(sha(2));
      expect(prompt).toContain(SELECTION_MARKER_PREFIX);
      expect(prompt).toContain("git show");
      for (const leak of ["CANDIDATE_STDOUT_0", "CANDIDATE_STDOUT_1", "DIFF_LEAK_1", "DIFF_LEAK_2"]) {
        expect(prompt).not.toContain(leak);
      }

      // Judge ran detached at the base, not in the primary checkout, and the
      // worktree is gone once selection finishes.
      expect(judgeHeadSeen).toBe(base.head);
      expect(judgeCwdRealpath).not.toBe(await realpath(repo));
      await expect(access(judge.requests[0].cwd)).rejects.toThrow();
      expect(await worktreeCount(repo)).toBe(1);
    } finally {
      await removeDirectory(repo);
    }
  });

  it("returns structured judge failures with preserved raw output and no winner", async () => {
    const repo = await createGitRepository();
    const base = await resolveRepositoryIdentity(repo);
    try {
      const fan = fanResult(base, [candidateFixture(base, 0), candidateFixture(base, 1)]);
      const scenarios: Array<{ behavior: JudgeBehavior; code: string; resolver?: (agent: string) => AgentAdapter }> = [
        { behavior: { processError: "spawn ENOENT" }, code: "JUDGE_PROCESS_ERROR" },
        { behavior: { exitCode: 3, stdout: markerLine("cand-0") }, code: "JUDGE_FAILED" },
        { behavior: { stdout: markerLine("cand-0"), truncated: true }, code: "JUDGE_OUTPUT_TRUNCATED" },
        { behavior: { throws: new Error("adapter exploded") }, code: "JUDGE_EXECUTION_FAILED" },
        {
          behavior: {},
          code: "UNKNOWN_AGENT",
          resolver: () => {
            throw new AgentHubError("UNKNOWN_AGENT", "no such judge");
          },
        },
        { behavior: { stdout: "AGENT_HUB_SELECTION: {oops" }, code: "SELECTION_JSON_INVALID" },
        { behavior: { stdout: markerLine("cand-999") }, code: "SELECTION_CANDIDATE_NOT_ELIGIBLE" },
        { behavior: { stdout: "the second one looks best" }, code: "NO_SELECTION" },
      ];

      for (const scenario of scenarios) {
        const judge = fakeJudge(scenario.behavior);
        const result = await runCompetition(
          { fan_out: fan, strategy: "judge", judge_agent: "judge-1" },
          {
            verifyArtifact: verifyAll,
            resolveAdapter: scenario.resolver ?? judgeResolver(judge.adapter),
          },
        );
        expect(result.status, scenario.code).toBe("failure");
        expect(result.mode, scenario.code).toBe("judge");
        expect(result.winner, scenario.code).toBeNull();
        expect(result.error?.code, scenario.code).toBe(scenario.code);
        expect(result.judge?.error?.code, scenario.code).toBe(scenario.code);
        expect(result.judge?.winner_id, scenario.code).toBeNull();
        // Bounded raw output flag preserved for audit even on failure paths.
        expect(result.judge?.truncated, scenario.code).toBe(scenario.behavior.truncated ?? false);
        if (scenario.behavior.stdout) {
          expect(result.judge?.raw_output, scenario.code).toBe(scenario.behavior.stdout);
        }
        expect(await worktreeCount(repo), scenario.code).toBe(1);
      }
    } finally {
      await removeDirectory(repo);
    }
  });

  it("end-to-end: judge inspects retained fan-out artifacts from its own worktree", async () => {
    const repo = await createGitRepository();
    try {
      const writer = (label: string, exitCode: number): AgentAdapter => ({
        id: label,
        async execute(request) {
          await writeFile(join(request.cwd, `${label}.txt`), `${label} work\n`);
          return {
            exit_code: exitCode,
            stdout: `done ${label}`,
            stderr: "",
            session_id: null,
            stdout_truncated: false,
            stderr_truncated: false,
            error: null,
          };
        },
      });

      const fan = await fanOut(
        {
          workspace: repo,
          maxConcurrency: 1,
          candidates: [
            { label: "w0", task: "write w0", agent: "w0" },
            { label: "w1", task: "write w1", agent: "w1" },
            { label: "w2", task: "write w2", agent: "w2" },
          ],
        },
        { resolveAdapter: (agent) => writer(agent, agent === "w2" ? 1 : 0) },
      );
      const successful = fan.candidates.filter((candidate) => candidate.status === "success");
      expect(successful).toHaveLength(2);

      let judgeHeadSeen = "";
      const judge = fakeJudge({
        stdout: async (request) => {
          judgeHeadSeen = (
            await runProcess("git", ["rev-parse", "HEAD"], {
              cwd: request.cwd,
              maxOutputBytes: 200,
            })
          ).stdout.trim();
          // Resolve every artifact commit listed in the prompt, from the
          // judge worktree, read-only.
          const rows = request.task
            .split("\n")
            .filter((line) => line.startsWith("- "))
            .map((line) => line.slice(2).split(" | "));
          expect(rows).toHaveLength(2);
          for (const [, , commitToken] of rows) {
            const shown = await runProcess("git", ["show", "-s", "--format=%H", commitToken], {
              cwd: request.cwd,
              maxOutputBytes: 200,
            });
            expect(shown.exitCode).toBe(0);
            expect(shown.stdout.trim()).toBe(commitToken);
          }
          return markerLine(rows[1][0], "complete change");
        },
      });

      // Default dependencies (real ref verification, real worktree, real lock).
      const result = await runCompetition(
        { fan_out: fan, strategy: "judge", judge_agent: "judge-1" },
        { resolveAdapter: judgeResolver(judge.adapter) },
      );

      expect(result.status).toBe("selected");
      expect(result.winner?.candidate_id).toBe(successful[1].candidate_id);
      expect(result.eligible).toHaveLength(2);
      expect(result.rejected.map((entry) => entry.code)).toEqual(["STATUS_NOT_SUCCESS"]);
      expect(judgeHeadSeen).toBe(fan.base.head);
      expect(judge.requests[0].task).not.toContain("done w0");

      // All eligible refs are still alive after selection (no GC here); the
      // winner's change is fully inspectable from the primary checkout.
      expect(result.retained_artifact_refs).toHaveLength(2);
      for (const entry of result.eligible) {
        const resolved = await runProcess(
          "git",
          ["rev-parse", "--quiet", "--verify", `${entry.artifact_ref}^{commit}`],
          { cwd: repo, maxOutputBytes: 200 },
        );
        expect(resolved.exitCode).toBe(0);
        expect(resolved.stdout.trim()).toBe(entry.artifact_commit);
      }
      const winnerCommit = successful[1].artifact?.commit as string;
      const diff = await runGit(repo, ["diff", fan.base.head, winnerCommit]);
      expect(diff).toContain("+w1 work");

      await expect(access(judge.requests[0].cwd)).rejects.toThrow();
      expect(await worktreeCount(repo)).toBe(1);
    } finally {
      await removeDirectory(repo);
    }
  });

  it("validates the request shape at the boundary", async () => {
    const repo = await createGitRepository();
    const base = await resolveRepositoryIdentity(repo);
    try {
      const fan = fanResult(base, [candidateFixture(base, 0), candidateFixture(base, 1)]);
      const deps: CompetitionDependencies = { verifyArtifact: verifyAll, resolveAdapter: noJudge() };
      const invalid = [
        { fan_out: fan, strategy: "fastest_fingers_first" as CompetitionStrategy },
        { fan_out: fan, strategy: "judge" as const },
        { fan_out: fan, strategy: "request_order" as const, workspace: "  " },
        { fan_out: fan, strategy: "request_order" as const, maxOutputBytes: 0 },
        { fan_out: { ...fan, base: { ...base, head: "not-a-sha" } }, strategy: "request_order" as const },
      ];
      for (const request of invalid) {
        await expect(runCompetition(request, deps)).rejects.toMatchObject({
          code: "INVALID_COMPETITION_REQUEST",
        });
      }
    } finally {
      await removeDirectory(repo);
    }
  });
});
