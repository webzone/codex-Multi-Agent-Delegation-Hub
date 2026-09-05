import { chmod, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { captureCandidateArtifact } from "../src/artifacts.js";
import { createWorktreeAtBase, removeWorktree, resolveRepositoryIdentity } from "../src/git.js";
import { acquireRepositoryLock } from "../src/locks.js";
import { autoMerge, mergeCandidate, MERGE_LOCK_NAME } from "../src/merge.js";
import type {
  CandidateArtifact,
  FanOutCandidateResult,
  FanOutResult,
  MergeOutcome,
  RepositoryIdentity,
} from "../src/types.js";
import type {
  CompetitionEligibleCandidate,
  CompetitionResult,
} from "../src/competition.js";
import { createGitRepository, removeDirectory, runGit } from "./helpers.js";

async function headOf(repository: string): Promise<string> {
  return (await runGit(repository, ["rev-parse", "HEAD"])).trim();
}

async function statusOf(repository: string): Promise<string> {
  const status = await runGit(repository, ["status", "--porcelain=v1", "--untracked-files=all"]);
  return status.trim();
}

/** Produces a real artifact commit + private ref exactly as fan-out would. */
async function makeArtifact(repository: string, fileName: string, candidateKey = "c1") {
  const base = await resolveRepositoryIdentity(repository);
  const worktree = await createWorktreeAtBase(repository, base.head);
  await writeFile(join(worktree.path, fileName), `${fileName} from candidate\n`);
  const artifact = await captureCandidateArtifact({
    worktreePath: worktree.path,
    base: base.head,
    candidateKey,
  });
  await removeWorktree(repository, worktree);
  return { base, artifact };
}

function refused(outcome: MergeOutcome, code: string) {
  expect(outcome.strategy).toBe("none");
  expect(outcome.clean).toBe(false);
  expect(outcome.applied_commit).toBeNull();
  expect(outcome.error?.code).toBe(code);
  expect(typeof outcome.error?.message).toBe("string");
}

function fakeEligible(
  candidateId: string,
  artifact: CandidateArtifact,
): CompetitionEligibleCandidate {
  return {
    candidate_id: candidateId,
    index: 0,
    agent: "omp",
    artifact_commit: artifact.commit as string,
    artifact_ref: artifact.ref as string,
    changed_file_count: artifact.changed_files.length,
  };
}

/** A real CompetitionResult shape; per-test overrides simulate forged or failed inputs. */
function fakeCompetitionResult(
  base: RepositoryIdentity,
  winnerId: string,
  artifact: CandidateArtifact | null,
  overrides: Partial<CompetitionResult> = {},
): CompetitionResult {
  const eligible =
    artifact?.commit && artifact.ref
      ? [fakeEligible(winnerId, artifact)]
      : [];
  return {
    status: "selected",
    strategy: "judge",
    mode: "judge",
    workspace: base.worktree_root,
    base,
    eligible,
    rejected: [],
    winner: { candidate_id: winnerId, reason: "fake judge", basis: "judge" },
    judge: null,
    retained_artifact_refs: eligible.map((entry) => entry.artifact_ref),
    started_at: "2026-01-01T00:00:00.000Z",
    finished_at: "2026-01-01T00:00:01.000Z",
    duration_ms: 1000,
    error: null,
    ...overrides,
  };
}

function fakeFanOutResult(
  base: RepositoryIdentity,
  artifact: CandidateArtifact | null,
  options: { status?: "success" | "failure" } = {},
): FanOutResult {
  const status = options.status ?? "success";
  const candidate = {
    candidate_id: "winner",
    artifact,
    index: 0,
    label: "",
    task: "candidate task",
    status,
  } as unknown as FanOutCandidateResult;
  return {
    base,
    max_concurrency: 1,
    candidates: [candidate],
    status,
    started_at: "2026-01-01T00:00:00.000Z",
    finished_at: "2026-01-01T00:00:01.000Z",
    duration_ms: 1000,
    error: null,
  };
}

describe("merge", { timeout: 30_000 }, () => {
  it("fast-forwards the named branch onto the candidate commit", async () => {
    const repository = await createGitRepository();
    try {
      const { base, artifact } = await makeArtifact(repository, "candidate.txt");
      const outcome = await mergeCandidate({
        workspace: repository,
        base,
        candidateId: "c1",
        artifact,
      });

      expect(outcome.strategy).toBe("fast-forward");
      expect(outcome.clean).toBe(true);
      expect(outcome.error).toBeNull();
      expect(outcome.applied_commit).toBe(artifact.commit);
      expect(outcome.target_ref).toBe(`refs/heads/${base.branch}`);
      expect(outcome.notes).toContain("post-conditions-verified");
      expect(await headOf(repository)).toBe(artifact.commit);
      expect(await statusOf(repository)).toBe("");
      expect(await readFile(join(repository, "candidate.txt"), "utf8")).toContain("from candidate");
      expect(await runGit(repository, ["rev-parse", "--abbrev-ref", "HEAD"])).toContain(base.branch!);

      // The outcome survives serialization untouched.
      expect(JSON.parse(JSON.stringify(outcome))).toEqual(outcome);
    } finally {
      await removeDirectory(repository);
    }
  });

  it("refuses an untracked-file dirty checkout without touching it", async () => {
    const repository = await createGitRepository();
    try {
      const { base, artifact } = await makeArtifact(repository, "candidate.txt");
      const before = await headOf(repository);
      await writeFile(join(repository, "surprise.txt"), "untracked\n");

      const outcome = await mergeCandidate({ workspace: repository, base, candidateId: "c1", artifact });
      refused(outcome, "MERGE_DIRTY_WORKTREE");
      expect(await headOf(repository)).toBe(before);
    } finally {
      await removeDirectory(repository);
    }
  });

  it("refuses a tracked-modification dirty checkout", async () => {
    const repository = await createGitRepository();
    try {
      const { base, artifact } = await makeArtifact(repository, "candidate.txt");
      const before = await headOf(repository);
      await writeFile(join(repository, "README.md"), "caller edits\n");

      const outcome = await mergeCandidate({ workspace: repository, base, candidateId: "c1", artifact });
      refused(outcome, "MERGE_DIRTY_WORKTREE");
      expect(await headOf(repository)).toBe(before);
    } finally {
      await removeDirectory(repository);
    }
  });

  it("refuses when the base HEAD has moved", async () => {
    const repository = await createGitRepository();
    try {
      const { base, artifact } = await makeArtifact(repository, "candidate.txt");
      await writeFile(join(repository, "later.txt"), "new commit\n");
      await runGit(repository, ["add", "later.txt"]);
      await runGit(repository, ["commit", "-qm", "moved ahead"]);
      const moved = await headOf(repository);

      const outcome = await mergeCandidate({ workspace: repository, base, candidateId: "c1", artifact });
      refused(outcome, "MERGE_BASE_MOVED");
      expect(await headOf(repository)).toBe(moved);
    } finally {
      await removeDirectory(repository);
    }
  });

  it("refuses when the attached branch changed since capture", async () => {
    const repository = await createGitRepository();
    try {
      const { base, artifact } = await makeArtifact(repository, "candidate.txt");
      const before = await headOf(repository);
      await runGit(repository, ["checkout", "-q", "-b", "side-quest"]);

      const outcome = await mergeCandidate({ workspace: repository, base, candidateId: "c1", artifact });
      refused(outcome, "MERGE_BRANCH_CHANGED");
      expect(await headOf(repository)).toBe(before);
    } finally {
      await removeDirectory(repository);
    }
  });

  it("refuses when the checkout detached since capture", async () => {
    const repository = await createGitRepository();
    try {
      const { base, artifact } = await makeArtifact(repository, "candidate.txt");
      const before = await headOf(repository);
      await runGit(repository, ["checkout", "-q", "--detach"]);

      const outcome = await mergeCandidate({ workspace: repository, base, candidateId: "c1", artifact });
      refused(outcome, "MERGE_BRANCH_CHANGED");
      expect(await headOf(repository)).toBe(before);
    } finally {
      await removeDirectory(repository);
    }
  });

  it("refuses a detached capture base — there is no adoption target", async () => {
    const repository = await createGitRepository();
    try {
      const { base, artifact } = await makeArtifact(repository, "candidate.txt");
      const before = await headOf(repository);

      const outcome = await mergeCandidate({
        workspace: repository,
        base: { ...base, branch: null },
        candidateId: "c1",
        artifact,
      });
      refused(outcome, "MERGE_DETACHED_BASE");
      expect(await headOf(repository)).toBe(before);
    } finally {
      await removeDirectory(repository);
    }
  });

  it("refuses a workspace that is not the captured checkout", async () => {
    const repository = await createGitRepository();
    const other = await createGitRepository();
    try {
      const { base, artifact } = await makeArtifact(repository, "candidate.txt");
      const before = await headOf(repository);

      const outcome = await mergeCandidate({ workspace: other, base, candidateId: "c1", artifact });
      refused(outcome, "MERGE_IDENTITY_MISMATCH");
      expect(await headOf(repository)).toBe(before);
      expect(await statusOf(repository)).toBe("");
    } finally {
      await removeDirectory(repository);
      await removeDirectory(other);
    }
  });

  it("refuses when the private artifact ref no longer resolves", async () => {
    const repository = await createGitRepository();
    try {
      const { base, artifact } = await makeArtifact(repository, "candidate.txt");
      const before = await headOf(repository);
      await runGit(repository, ["update-ref", "-d", artifact.ref!]);

      const outcome = await mergeCandidate({ workspace: repository, base, candidateId: "c1", artifact });
      refused(outcome, "MERGE_ARTIFACT_MISSING");
      expect(await headOf(repository)).toBe(before);
    } finally {
      await removeDirectory(repository);
    }
  });

  it("refuses an artifact claiming a different parent", async () => {
    const repository = await createGitRepository();
    try {
      const { base, artifact } = await makeArtifact(repository, "candidate.txt");
      const before = await headOf(repository);

      const outcome = await mergeCandidate({
        workspace: repository,
        base,
        candidateId: "c1",
        artifact: { ...artifact, parent: "f".repeat(40) },
      });
      refused(outcome, "MERGE_ARTIFACT_BASE_MISMATCH");
      expect(await headOf(repository)).toBe(before);
    } finally {
      await removeDirectory(repository);
    }
  });

  it("refuses an artifact ref outside the private namespace", async () => {
    const repository = await createGitRepository();
    try {
      const { base, artifact } = await makeArtifact(repository, "candidate.txt");
      const before = await headOf(repository);

      const outcome = await mergeCandidate({
        workspace: repository,
        base,
        candidateId: "c1",
        artifact: { ...artifact, ref: "refs/heads/evil" },
      });
      refused(outcome, "MERGE_INVALID_ARTIFACT");
      expect(await headOf(repository)).toBe(before);
    } finally {
      await removeDirectory(repository);
    }
  });

  it("refuses a commit that does not descend from the base (diverged history)", async () => {
    const repository = await createGitRepository();
    try {
      const { base } = await makeArtifact(repository, "candidate.txt");
      const before = await headOf(repository);

      // Fabricate a commit on an unrelated root that claims the base as its
      // recorded parent: existence passes, descendant ancestry must not.
      const emptyTree = (await runGit(repository, ["hash-object", "-t", "tree", "/dev/null"])).trim();
      const unrelatedRoot = (await runGit(repository, ["commit-tree", emptyTree, "-m", "unrelated root"])).trim();
      const baseTree = (await runGit(repository, ["rev-parse", `${base.head}^{tree}`])).trim();
      const tampered = (
        await runGit(repository, ["commit-tree", baseTree, "-p", unrelatedRoot, "-m", "tampered"])
      ).trim();
      const ref = "refs/agent-hub/candidates/tampered";
      await runGit(repository, ["update-ref", ref, tampered]);

      const outcome = await mergeCandidate({
        workspace: repository,
        base,
        candidateId: "tampered",
        artifact: {
          parent: base.head,
          commit: tampered,
          tree: baseTree,
          ref,
          empty: false,
          changed_files: [],
          diff: "",
          diff_truncated: false,
        },
      });
      refused(outcome, "MERGE_NOT_DESCENDANT");
      expect(await headOf(repository)).toBe(before);
      expect(await statusOf(repository)).toBe("");
    } finally {
      await removeDirectory(repository);
    }
  });

  it("refuses a no-op candidate with no artifact", async () => {
    const repository = await createGitRepository();
    try {
      const base = await resolveRepositoryIdentity(repository);
      const worktree = await createWorktreeAtBase(repository, base.head);
      const artifact = await captureCandidateArtifact({
        worktreePath: worktree.path,
        base: base.head,
        candidateKey: "noop",
      });
      await removeWorktree(repository, worktree);
      expect(artifact.empty).toBe(true);

      const outcome = await mergeCandidate({ workspace: repository, base, candidateId: "noop", artifact });
      refused(outcome, "MERGE_NO_ARTIFACT");
    } finally {
      await removeDirectory(repository);
    }
  });

  it("adopts despite failing repository hooks (hooks are disabled for adoption)", async () => {
    const repository = await createGitRepository();
    try {
      const { base, artifact } = await makeArtifact(repository, "candidate.txt");
      const hooks = ["post-checkout", "post-merge", "pre-merge-commit", "reference-transaction"];
      for (const hook of hooks) {
        const hookPath = join(repository, ".git", "hooks", hook);
        await writeFile(hookPath, "#!/bin/sh\nexit 1\n");
        await chmod(hookPath, 0o755);
      }

      const outcome = await mergeCandidate({ workspace: repository, base, candidateId: "c1", artifact });
      expect(outcome.error).toBeNull();
      expect(outcome.applied_commit).toBe(artifact.commit);
      expect(await headOf(repository)).toBe(artifact.commit);
      expect(await statusOf(repository)).toBe("");
    } finally {
      await removeDirectory(repository);
    }
  });

  it("refuses with LOCK_BUSY while another merge holds the lock", async () => {
    const repository = await createGitRepository();
    try {
      const { base, artifact } = await makeArtifact(repository, "candidate.txt");
      const before = await headOf(repository);
      const held = await acquireRepositoryLock({ commonDir: base.common_dir, name: MERGE_LOCK_NAME });
      try {
        const outcome = await mergeCandidate(
          { workspace: repository, base, candidateId: "c1", artifact },
          {
            acquireMergeLock: (commonDir) =>
              acquireRepositoryLock({ commonDir, name: MERGE_LOCK_NAME, waitMs: 50 }),
          },
        );
        refused(outcome, "LOCK_BUSY");
        expect(await headOf(repository)).toBe(before);
      } finally {
        await held.release();
      }
    } finally {
      await removeDirectory(repository);
    }
  });

  it("returns structured refusals instead of throwing on malformed input", async () => {
    const emptyWorkspace = await mergeCandidate({
      workspace: "",
      base: undefined as unknown as RepositoryIdentity,
      candidateId: "",
      artifact: null,
    });
    expect(emptyWorkspace.strategy).toBe("none");
    expect(emptyWorkspace.error?.code).toBe("INVALID_WORKSPACE");

    const missingBase = await mergeCandidate({
      workspace: "/does-not-matter",
      base: undefined as unknown as RepositoryIdentity,
      candidateId: "x",
      artifact: null,
    });
    expect(missingBase.strategy).toBe("none");
    expect(missingBase.error?.code).toBe("INVALID_BASE_IDENTITY");
  });

  describe("autoMerge", () => {
    it("adopts the competition winner end to end", async () => {
      const repository = await createGitRepository();
      try {
        const { base, artifact } = await makeArtifact(repository, "winner.txt");
        const outcome = await autoMerge({
          workspace: repository,
          fan_out: fakeFanOutResult(base, artifact),
          competition: fakeCompetitionResult(base, "winner", artifact),
        });

        expect(outcome.strategy).toBe("fast-forward");
        expect(outcome.candidate_id).toBe("winner");
        expect(outcome.applied_commit).toBe(artifact.commit);
        expect(outcome.error).toBeNull();
        expect(await headOf(repository)).toBe(artifact.commit);
      } finally {
        await removeDirectory(repository);
      }
    });

    it("refuses without a competition result (opt-in contract)", async () => {
      const repository = await createGitRepository();
      try {
        const before = await headOf(repository);
        const { base, artifact } = await makeArtifact(repository, "winner.txt");
        const outcome = await autoMerge({
          workspace: repository,
          fan_out: fakeFanOutResult(base, artifact),
          competition: null,
        });
        refused(outcome, "MERGE_NO_WINNER");
        expect(await headOf(repository)).toBe(before);
      } finally {
        await removeDirectory(repository);
      }
    });

    it("refuses a failed competition (status + error)", async () => {
      const repository = await createGitRepository();
      try {
        const before = await headOf(repository);
        const { base, artifact } = await makeArtifact(repository, "winner.txt");
        const outcome = await autoMerge({
          workspace: repository,
          fan_out: fakeFanOutResult(base, artifact),
          competition: fakeCompetitionResult(base, "winner", artifact, {
            status: "failure",
            winner: null,
            error: { code: "JUDGE_TIMEOUT", message: "judge died" },
          }),
        });
        refused(outcome, "MERGE_COMPETITION_FAILED");
        expect(await headOf(repository)).toBe(before);
      } finally {
        await removeDirectory(repository);
      }
    });

    it("refuses a competition that never reached status selected", async () => {
      const repository = await createGitRepository();
      try {
        const { base, artifact } = await makeArtifact(repository, "winner.txt");
        const outcome = await autoMerge({
          workspace: repository,
          fan_out: fakeFanOutResult(base, artifact),
          competition: fakeCompetitionResult(base, "winner", artifact, {
            status: "failure",
            winner: null,
          }),
        });
        refused(outcome, "MERGE_NOT_SELECTED");
        expect(await headOf(repository)).not.toBe(artifact.commit);
      } finally {
        await removeDirectory(repository);
      }
    });

    it("refuses a forged workflow-shaped competition object", async () => {
      const repository = await createGitRepository();
      try {
        const before = await headOf(repository);
        const { base, artifact } = await makeArtifact(repository, "winner.txt");
        // The retired workflow-shaped look-alike (winner_id/ranking, no
        // status/eligible/base) must be refused, never followed.
        const forged = {
          judge_agent: "omp",
          ranking: ["winner"],
          winner_id: "winner",
          judgements: [],
          raw_output: "",
          truncated: false,
          error: null,
        } as unknown as CompetitionResult;
        const outcome = await autoMerge({
          workspace: repository,
          fan_out: fakeFanOutResult(base, artifact),
          competition: forged,
        });
        refused(outcome, "MERGE_INVALID_INPUT");
        expect(await headOf(repository)).toBe(before);
      } finally {
        await removeDirectory(repository);
      }
    });

    it("refuses a winner that is not exactly one eligible entry", async () => {
      const repository = await createGitRepository();
      try {
        const before = await headOf(repository);
        const { base, artifact } = await makeArtifact(repository, "winner.txt");

        const ineligible = await autoMerge({
          workspace: repository,
          fan_out: fakeFanOutResult(base, artifact),
          competition: fakeCompetitionResult(base, "winner", artifact, {
            eligible: [],
            retained_artifact_refs: [],
          }),
        });
        refused(ineligible, "MERGE_WINNER_NOT_ELIGIBLE");

        const entry = fakeEligible("winner", artifact);
        const duplicated = await autoMerge({
          workspace: repository,
          fan_out: fakeFanOutResult(base, artifact),
          competition: fakeCompetitionResult(base, "winner", artifact, {
            eligible: [entry, { ...entry }],
          }),
        });
        refused(duplicated, "MERGE_WINNER_NOT_ELIGIBLE");
        expect(await headOf(repository)).toBe(before);
      } finally {
        await removeDirectory(repository);
      }
    });

    it("refuses when the competition base is not the fan-out base", async () => {
      const repository = await createGitRepository();
      try {
        const before = await headOf(repository);
        const { base, artifact } = await makeArtifact(repository, "winner.txt");
        const outcome = await autoMerge({
          workspace: repository,
          fan_out: fakeFanOutResult(base, artifact),
          competition: fakeCompetitionResult(base, "winner", artifact, {
            base: { ...base, head: "f".repeat(40) },
          }),
        });
        refused(outcome, "MERGE_BASE_MISMATCH");
        expect(await headOf(repository)).toBe(before);
      } finally {
        await removeDirectory(repository);
      }
    });

    it("refuses a winner candidate whose fan-out status is not success", async () => {
      const repository = await createGitRepository();
      try {
        const before = await headOf(repository);
        const { base, artifact } = await makeArtifact(repository, "winner.txt");
        const outcome = await autoMerge({
          workspace: repository,
          fan_out: fakeFanOutResult(base, artifact, { status: "failure" }),
          competition: fakeCompetitionResult(base, "winner", artifact),
        });
        refused(outcome, "MERGE_CANDIDATE_NOT_SUCCESS");
        expect(await headOf(repository)).toBe(before);
      } finally {
        await removeDirectory(repository);
      }
    });

    it("refuses artifacts whose commit or ref disagree with the eligible entry", async () => {
      const repository = await createGitRepository();
      try {
        const before = await headOf(repository);
        const { base, artifact } = await makeArtifact(repository, "winner.txt");

        const wrongCommit = await autoMerge({
          workspace: repository,
          fan_out: fakeFanOutResult(base, artifact),
          competition: fakeCompetitionResult(base, "winner", artifact, {
            eligible: [{ ...fakeEligible("winner", artifact), artifact_commit: "a".repeat(40) }],
          }),
        });
        refused(wrongCommit, "MERGE_CANDIDATE_ARTIFACT_MISMATCH");

        const wrongRef = await autoMerge({
          workspace: repository,
          fan_out: fakeFanOutResult(base, artifact),
          competition: fakeCompetitionResult(base, "winner", artifact, {
            eligible: [{ ...fakeEligible("winner", artifact), artifact_ref: "refs/heads/evil" }],
          }),
        });
        refused(wrongRef, "MERGE_CANDIDATE_ARTIFACT_MISMATCH");
        expect(await headOf(repository)).toBe(before);
      } finally {
        await removeDirectory(repository);
      }
    });

    it("refuses a winner id that is not part of the fan-out", async () => {
      const repository = await createGitRepository();
      try {
        const { base, artifact } = await makeArtifact(repository, "winner.txt");
        const entry = fakeEligible("ghost", artifact);
        const outcome = await autoMerge({
          workspace: repository,
          fan_out: fakeFanOutResult(base, artifact),
          competition: fakeCompetitionResult(base, "winner", artifact, {
            winner: { candidate_id: "ghost", reason: "forged selection", basis: "judge" },
            eligible: [entry],
            retained_artifact_refs: [entry.artifact_ref],
          }),
        });
        refused(outcome, "MERGE_CANDIDATE_NOT_FOUND");
      } finally {
        await removeDirectory(repository);
      }
    });

    it("refuses a winner with no artifact on the candidate", async () => {
      const repository = await createGitRepository();
      try {
        const { base, artifact } = await makeArtifact(repository, "winner.txt");
        const outcome = await autoMerge({
          workspace: repository,
          fan_out: fakeFanOutResult(base, null),
          competition: fakeCompetitionResult(base, "winner", artifact),
        });
        refused(outcome, "MERGE_NO_ARTIFACT");
      } finally {
        await removeDirectory(repository);
      }
    });

    it("refuses a malformed fan-out document", async () => {
      const outcome = await autoMerge({
        workspace: "/does-not-matter",
        fan_out: null as unknown as FanOutResult,
        competition: {} as unknown as CompetitionResult,
      });
      refused(outcome, "MERGE_INVALID_INPUT");
    });
  });

  describe("post-adoption failure reporting", () => {
    it("reports an applied fast-forward when verification sees a different HEAD", async () => {
      const repository = await createGitRepository();
      try {
        const { base, artifact } = await makeArtifact(repository, "candidate.txt");
        const outcome = await mergeCandidate(
          { workspace: repository, base, candidateId: "c1", artifact },
          { verifyPostAdoption: async () => ({ head: "d".repeat(40), clean: true }) },
        );

        expect(outcome.strategy).toBe("fast-forward");
        expect(outcome.clean).toBe(false);
        expect(outcome.error?.code).toBe("MERGE_POSTCONDITION_FAILED");
        // The observed HEAD, reported truthfully — not a no-mutation fiction.
        expect(outcome.applied_commit).toBe("d".repeat(40));
        expect(outcome.notes).toContain("fast-forward-applied");
        // The fast-forward really happened and was never rolled back.
        expect(await headOf(repository)).toBe(artifact.commit);
      } finally {
        await removeDirectory(repository);
      }
    });

    it("reports an applied fast-forward with a null commit when the probe cannot read the checkout", async () => {
      const repository = await createGitRepository();
      try {
        const { base, artifact } = await makeArtifact(repository, "candidate.txt");
        const outcome = await mergeCandidate(
          { workspace: repository, base, candidateId: "c1", artifact },
          {
            verifyPostAdoption: async () => {
              throw new Error("probe exploded");
            },
          },
        );

        expect(outcome.strategy).toBe("fast-forward");
        expect(outcome.clean).toBe(false);
        expect(outcome.error?.code).toBe("MERGE_POSTCONDITION_FAILED");
        expect(outcome.error?.message).toContain("probe exploded");
        expect(outcome.applied_commit).toBeNull();
        expect(await headOf(repository)).toBe(artifact.commit);
      } finally {
        await removeDirectory(repository);
      }
    });

    it("reports an applied fast-forward when the tree is dirty afterwards", async () => {
      const repository = await createGitRepository();
      try {
        const { base, artifact } = await makeArtifact(repository, "candidate.txt");
        const outcome = await mergeCandidate(
          { workspace: repository, base, candidateId: "c1", artifact },
          { verifyPostAdoption: async () => ({ head: artifact.commit as string, clean: false }) },
        );

        expect(outcome.strategy).toBe("fast-forward");
        expect(outcome.clean).toBe(false);
        expect(outcome.error?.code).toBe("MERGE_POSTCONDITION_FAILED");
        expect(outcome.applied_commit).toBe(artifact.commit);
        expect(await headOf(repository)).toBe(artifact.commit);
      } finally {
        await removeDirectory(repository);
      }
    });

    it("surfaces lock-release failure as an applied fast-forward with a structured error", async () => {
      const repository = await createGitRepository();
      try {
        const { base, artifact } = await makeArtifact(repository, "candidate.txt");
        const outcome = await mergeCandidate(
          { workspace: repository, base, candidateId: "c1", artifact },
          {
            acquireMergeLock: async (commonDir) => {
              const lock = await acquireRepositoryLock({ commonDir, name: MERGE_LOCK_NAME });
              return {
                ...lock,
                release: async () => {
                  throw new Error("simulated release failure");
                },
              };
            },
          },
        );

        expect(outcome.strategy).toBe("fast-forward");
        expect(outcome.clean).toBe(true); // the checkout itself was verified
        expect(outcome.error?.code).toBe("LOCK_RELEASE_FAILED");
        expect(outcome.error?.message).toContain("simulated release failure");
        expect(outcome.applied_commit).toBe(artifact.commit);
        expect(outcome.notes.some((note) => note.startsWith("merge-lock-release-failed"))).toBe(true);
        // No rollback: the adoption stands.
        expect(await headOf(repository)).toBe(artifact.commit);
      } finally {
        await removeDirectory(repository);
      }
    });
  });
});
