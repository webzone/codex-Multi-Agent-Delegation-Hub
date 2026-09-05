import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { CANDIDATE_REF_NAMESPACE } from "./artifacts.js";
import type { CompetitionResult } from "./competition.js";
import { asDelegateError, AgentHubError } from "./errors.js";
import { currentRevision, isDirty, resolveRepositoryIdentity, runGit } from "./git.js";
import { acquireRepositoryLock, type RepositoryLock } from "./locks.js";
import type {
  CandidateArtifact,
  CompetitionOutcome,
  DelegateError,
  FanOutResult,
  MergeOutcome,
  RepositoryIdentity,
} from "./types.js";

/**
 * Auto-merge adopts exactly one internally selected candidate artifact into
 * the primary checkout by fast-forward. It is strictly opt-in: callers reach
 * it only through an explicit `--auto-merge` / `auto_merge` decision, and the
 * selection it follows is the competition's internal winner, never an
 * externally supplied ref, path, or branch name.
 *
 * Every normal outcome is serializable: refusals come back as a `MergeOutcome`
 * with `strategy: "none"`, `clean: false`, and a `MERGE_*` (or lock/git) error
 * code. This module never throws for a refusal, and it never mutates the
 * primary checkout unless every revalidation below passes under the merge
 * lock. There is no stash, reset, force, rebase, cherry-pick, or conflict
 * resolution anywhere on this path.
 */

export const MERGE_LOCK_NAME = "merge";

const MERGE_LOCK_WAIT_MS = 30_000;
const MERGE_LOCK_RETRY_MS = 20;

export interface MergeCandidateRequest {
  /** Primary checkout to adopt into. */
  workspace: string;
  /** Repository identity captured before fan-out; the merge contract. */
  base: RepositoryIdentity;
  /** Internal candidate id; echoed back on the outcome only. */
  candidateId: string;
  artifact: CandidateArtifact | null;
}

export interface MergeDependencies {
  acquireMergeLock?: (commonDir: string) => Promise<RepositoryLock>;
}

export interface AutoMergeInput {
  workspace: string;
  /** Fan-out the candidates came from. */
  fan_out: FanOutResult;
  /** Competition result; auto-merge only ever follows its internal winner. */
  competition: CompetitionOutcome | CompetitionResult | null;
}

type MergeCompetition = CompetitionOutcome | CompetitionResult;

function competitionWinnerId(competition: MergeCompetition): string | null {
  return "winner" in competition
    ? competition.winner?.candidate_id ?? null
    : competition.winner_id;
}

function competitionFailure(competition: MergeCompetition): DelegateError | null {
  return competition.error;
}

function refusal(
  candidateId: string | null,
  artifact: CandidateArtifact | null,
  base: RepositoryIdentity | null,
  code: string,
  message: string,
  notes: string[],
): MergeOutcome {
  return {
    strategy: "none",
    candidate_id: candidateId || null,
    artifact_commit: artifact?.commit ?? null,
    target_ref: base?.branch ? `refs/heads/${base.branch}` : null,
    clean: false,
    applied_commit: null,
    notes,
    error: { code, message },
  };
}

function sameCheckout(probed: RepositoryIdentity, base: RepositoryIdentity): boolean {
  return probed.common_dir === base.common_dir && probed.worktree_root === base.worktree_root;
}

async function defaultAcquireMergeLock(commonDir: string): Promise<RepositoryLock> {
  return acquireRepositoryLock({
    commonDir,
    name: MERGE_LOCK_NAME,
    waitMs: MERGE_LOCK_WAIT_MS,
    retryDelayMs: MERGE_LOCK_RETRY_MS,
  });
}

/**
 * Revalidate everything and adopt, or throw an AgentHubError whose code is the
 * refusal code. `mergeCandidate` is the boundary that converts throws into the
 * serializable outcome, so nothing outside this function can observe a throw
 * from a normal refusal.
 */
async function adoptUnderLock(
  request: MergeCandidateRequest,
  notes: string[],
): Promise<MergeOutcome> {
  const { workspace, base, candidateId, artifact } = request;

  // 1. Repository identity: same repository and same attached checkout.
  const identity = await resolveRepositoryIdentity(workspace);
  if (!sameCheckout(identity, base)) {
    throw new AgentHubError(
      "MERGE_IDENTITY_MISMATCH",
      `Checkout no longer matches the fan-out base: captured ${base.worktree_root} (common ${base.common_dir}), found ${identity.worktree_root} (common ${identity.common_dir})`,
    );
  }

  // 2. Same named branch. A detached base names no adoption target; a branch
  //    switched since capture means the fast-forward would land elsewhere.
  if (base.branch === null) {
    throw new AgentHubError(
      "MERGE_DETACHED_BASE",
      "Fan-out base was captured on a detached HEAD; there is no named branch to fast-forward",
    );
  }
  if (identity.branch !== base.branch) {
    throw new AgentHubError(
      "MERGE_BRANCH_CHANGED",
      `Branch changed since fan-out: captured "${base.branch}", now ${identity.branch === null ? "detached" : `"${identity.branch}"`}`,
    );
  }

  // 3. Captured base HEAD must still be current.
  if (identity.head !== base.head) {
    throw new AgentHubError(
      "MERGE_BASE_MOVED",
      `HEAD moved since fan-out base ${base.head}; now ${identity.head}`,
    );
  }

  // 4. Clean status, untracked files included. No caller override: the merge
  //    must never absorb or clobber working-tree state.
  if (await isDirty(workspace)) {
    throw new AgentHubError(
      "MERGE_DIRTY_WORKTREE",
      "Primary checkout has uncommitted or untracked changes; refusing to touch a dirty checkout",
    );
  }
  notes.push("checkout-revalidated");

  // 5. Candidate eligibility and artifact existence.
  if (!artifact || artifact.empty || artifact.commit === null) {
    throw new AgentHubError(
      "MERGE_NO_ARTIFACT",
      "Selected candidate produced no artifact commit to adopt",
    );
  }
  if (artifact.ref === null || !artifact.ref.startsWith(`${CANDIDATE_REF_NAMESPACE}/`)) {
    throw new AgentHubError(
      "MERGE_INVALID_ARTIFACT",
      "Artifact commit has no private candidate ref inside the Agent Hub namespace",
    );
  }
  if (artifact.parent !== base.head) {
    throw new AgentHubError(
      "MERGE_ARTIFACT_BASE_MISMATCH",
      `Artifact parent ${artifact.parent} is not the captured base ${base.head}`,
    );
  }

  const typeProbe = await runGit(workspace, ["cat-file", "-t", artifact.commit], 1000).catch(
    () => null,
  );
  if (!typeProbe || typeProbe.stdout.trim() !== "commit") {
    throw new AgentHubError(
      "MERGE_ARTIFACT_MISSING",
      `Artifact commit ${artifact.commit} no longer exists in the object database`,
    );
  }

  const refProbe = await runGit(workspace, ["rev-parse", "--verify", artifact.ref], 1000).catch(
    () => null,
  );
  if (!refProbe || refProbe.stdout.trim() !== artifact.commit) {
    throw new AgentHubError(
      "MERGE_ARTIFACT_MISSING",
      `Private artifact ref "${artifact.ref}" no longer resolves to ${artifact.commit}`,
    );
  }

  // 6. Descendant ancestry: with HEAD pinned at base (3), an artifact commit
  //    that descends from base can only be adopted by fast-forward.
  const ancestry = await runGit(
    workspace,
    ["merge-base", "--is-ancestor", base.head, artifact.commit],
    1000,
  ).catch(() => null);
  if (!ancestry) {
    throw new AgentHubError(
      "MERGE_NOT_DESCENDANT",
      `Artifact commit ${artifact.commit} does not descend from base ${base.head}; diverged history is never rebased, cherry-picked, or patch-applied here`,
    );
  }
  notes.push("artifact-revalidated");

  // 7. Adopt. `--ff-only` can only move the branch pointer forward; hooks are
  //    disabled by pointing core.hooksPath at an empty private directory and
  //    --no-verify covers commit-side hooks of the merge command itself.
  const hooksDirectory = await mkdtemp(join(tmpdir(), "agent-hub-hooks-"));
  try {
    await runGit(workspace, [
      "-c",
      `core.hooksPath=${hooksDirectory}`,
      "merge",
      "--ff-only",
      "--no-verify",
      artifact.commit,
    ]);
  } catch (error) {
    throw new AgentHubError(
      "MERGE_FAILED",
      `Fast-forward adoption failed: ${error instanceof Error ? error.message : String(error)}`,
    );
  } finally {
    await rm(hooksDirectory, { recursive: true, force: true });
  }
  notes.push("fast-forward-applied");

  // 8. Post-conditions: HEAD is exactly the adopted commit and the checkout
  //    is still clean.
  const headAfter = await currentRevision(workspace);
  if (headAfter !== artifact.commit) {
    throw new AgentHubError(
      "MERGE_POSTCONDITION_FAILED",
      `HEAD is ${headAfter} after adoption, expected ${artifact.commit}`,
    );
  }
  if (await isDirty(workspace)) {
    throw new AgentHubError(
      "MERGE_POSTCONDITION_FAILED",
      "Working tree is dirty after adoption; inspect the checkout",
    );
  }
  notes.push("post-conditions-verified");

  return {
    strategy: "fast-forward",
    candidate_id: candidateId || null,
    artifact_commit: artifact.commit,
    target_ref: `refs/heads/${base.branch}`,
    clean: true,
    applied_commit: headAfter,
    notes,
    error: null,
  };
}

/**
 * Opt-in adoption of one candidate artifact into the primary checkout. The
 * full revalidation (identity, named branch, base HEAD, clean status including
 * untracked files, artifact existence, and descendant ancestry) runs again
 * inside the repository-local merge lock, immediately before the checkout is
 * touched. Returns a serializable `MergeOutcome` for every refusal; it does
 * not throw for normal refusal conditions.
 */
export async function mergeCandidate(
  request: MergeCandidateRequest,
  dependencies: MergeDependencies = {},
): Promise<MergeOutcome> {
  const notes: string[] = [];
  const { workspace, base, candidateId, artifact } = request;

  try {
    if (!workspace?.trim()) {
      throw new AgentHubError("INVALID_WORKSPACE", "workspace must not be empty");
    }
    if (!base?.head || !base.common_dir || !base.worktree_root) {
      throw new AgentHubError("INVALID_BASE_IDENTITY", "base repository identity is required");
    }

    // Cheap pre-flight outside the lock: if the caller points at a different
    // checkout (or no repository), refuse without contending for a lock.
    const probed = await resolveRepositoryIdentity(workspace);
    if (!sameCheckout(probed, base)) {
      return refusal(
        candidateId || null,
        artifact,
        base,
        "MERGE_IDENTITY_MISMATCH",
        `Workspace ${workspace} is not the checkout captured at fan-out (${base.worktree_root})`,
        notes,
      );
    }

    const acquireMergeLock = dependencies.acquireMergeLock ?? defaultAcquireMergeLock;
    const lock = await acquireMergeLock(base.common_dir);
    notes.push("merge-lock-acquired");
    try {
      return await adoptUnderLock(request, notes);
    } finally {
      try {
        await lock.release();
      } catch (error) {
        notes.push(
          `merge-lock-release-failed: ${error instanceof Error ? error.message : String(error)}`,
        );
      }
    }
  } catch (error) {
    const { code, message } = asDelegateError(error);
    return refusal(candidateId || null, artifact, base ?? null, code, message, notes);
  }
}

/**
 * Session-level auto-merge. Follows only the competition's internal winner;
 * every input gap (no competition, judge failure, no winner, unknown candidate,
 * empty artifact) is a serializable refusal. On success delegates to
 * `mergeCandidate`, which performs the locked revalidation and fast-forward.
 */
export async function autoMerge(
  input: AutoMergeInput,
  dependencies: MergeDependencies = {},
): Promise<MergeOutcome> {
  const { workspace, fan_out: fanOut, competition } = input;
  const notes: string[] = [];

  try {
    if (!fanOut || !Array.isArray(fanOut.candidates) || !fanOut.base) {
      return refusal(null, null, fanOut?.base ?? null, "MERGE_INVALID_INPUT", "fan-out result with base identity is required", notes);
    }
    if (competition === null || competition === undefined) {
      return refusal(
        null,
        null,
        fanOut.base,
        "MERGE_NO_WINNER",
        "Auto-merge is opt-in and requires a completed competition to select a candidate; none was supplied",
        notes,
      );
    }
    const competitionError = competitionFailure(competition);
    if (competitionError !== null) {
      return refusal(
        null,
        null,
        fanOut.base,
        "MERGE_COMPETITION_FAILED",
        `Competition did not complete cleanly (${competitionError.code}): ${competitionError.message}`,
        notes,
      );
    }
    const winnerId = competitionWinnerId(competition);
    if (winnerId === null) {
      return refusal(
        null,
        null,
        fanOut.base,
        "MERGE_NO_WINNER",
        "Judge produced no winner; nothing is adopted without an internal selection",
        notes,
      );
    }

    const winner = fanOut.candidates.find((candidate) => candidate.candidate_id === winnerId);
    if (!winner) {
      return refusal(
        winnerId,
        null,
        fanOut.base,
        "MERGE_CANDIDATE_NOT_FOUND",
        `Winner "${winnerId}" is not part of this fan-out`,
        notes,
      );
    }
    if (!winner.artifact || winner.artifact.empty || winner.artifact.commit === null) {
      return refusal(
        winner.candidate_id,
        winner.artifact,
        fanOut.base,
        "MERGE_NO_ARTIFACT",
        "Winning candidate produced no artifact commit to adopt",
        notes,
      );
    }

    return mergeCandidate(
      {
        workspace,
        base: fanOut.base,
        candidateId: winner.candidate_id,
        artifact: winner.artifact,
      },
      dependencies,
    );
  } catch (error) {
    const { code, message } = asDelegateError(error);
    return refusal(null, null, fanOut?.base ?? null, code, message, notes);
  }
}

export type {
  CandidateArtifact,
  CompetitionOutcome,
  FanOutResult,
  MergeOutcome,
  RepositoryIdentity,
} from "./types.js";
