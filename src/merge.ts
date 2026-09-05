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
  DelegateError,
  FanOutResult,
  MergeOutcome,
  RepositoryIdentity,
} from "./types.js";

/**
 * Auto-merge adopts exactly one internally selected candidate artifact into
 * the primary checkout by fast-forward. It is strictly opt-in: callers reach
 * it only through an explicit `--auto-merge` / `auto_merge` decision, and the
 * selection it follows is a real `runCompetition()` result's internal winner,
 * never an externally supplied ref, path, or branch name.
 *
 * Every normal outcome is serializable. A refusal before any mutation comes
 * back as a `MergeOutcome` with `strategy: "none"`, `clean: false`, and a
 * `MERGE_*` (or lock/git) error code, and this module never throws for a
 * refusal. It never mutates the primary checkout unless every revalidation
 * below passes under the merge lock. Once `git merge --ff-only` has run,
 * though, the outcome truthfully reports `strategy: "fast-forward"` with the
 * observed HEAD: a failed post-adoption verification or lock release carries
 * `MERGE_POSTCONDITION_FAILED` / `LOCK_RELEASE_FAILED` and is never reported
 * as a no-mutation refusal, because there is no rollback on this path. There
 * is no stash, reset, force, rebase, cherry-pick, or conflict resolution
 * anywhere here.
 *
 * Residual race, stated honestly: the merge lock serialises Agent Hub against
 * itself. It cannot fence a concurrent `git switch`, `git checkout`, `git
 * reset`, or any other external writer — Git's own index/HEAD locking is not
 * something this module can hold on the caller's behalf. Checks are therefore
 * snapshots: the pre-check can pass, the fast-forward can land correctly, and
 * an external process can still move HEAD or re-attach the checkout before
 * this function returns. Verification after adoption narrows that window to
 * the probe itself and turns the outcome into a truthful
 * `MERGE_POSTCONDITION_FAILED` rather than a false "adopted and verified"; it
 * does not eliminate the race, and nothing here rolls anything back. Treat an
 * applied-but-failed outcome as "adopted, unverified" and inspect the checkout.
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
  /**
   * Post-adoption probe, run immediately after `git merge --ff-only`
   * succeeds; reports the checkout's observed HEAD, clean status, and the
   * branch it is actually attached to (`null` when detached). The default
   * reads HEAD, `git status`, and `git symbolic-ref`; tests inject this seam to
   * exercise the applied-but-failed path deterministically.
   */
  verifyPostAdoption?: (
    workspace: string,
  ) => Promise<{ head: string; clean: boolean; branch: string | null }>;
}

export interface AutoMergeInput {
  workspace: string;
  /** Fan-out the candidates came from. */
  fan_out: FanOutResult;
  /**
   * Only a real `CompetitionResult` is ever followed — its internal winner,
   * validated against this very fan-out. Anything else is a refusal.
   */
  competition: CompetitionResult | null;
}

function sameIdentity(left: RepositoryIdentity, right: RepositoryIdentity): boolean {
  return (
    left.common_dir === right.common_dir &&
    left.worktree_root === right.worktree_root &&
    left.branch === right.branch &&
    left.head === right.head
  );
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
 * Revalidate everything and adopt, or throw an AgentHubError whose code is
 * the refusal code. `mergeCandidate` is the boundary that converts throws
 * into the serializable outcome, so nothing outside can observe a throw from
 * a normal refusal. Everything that can fail *after* the fast-forward is
 * returned as an applied outcome instead: throwing there would let the
 * catch-all misreport a real mutation as a strategy-"none" refusal.
 */
async function adoptUnderLock(
  request: MergeCandidateRequest,
  notes: string[],
  dependencies: MergeDependencies,
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
  let hooksCleanupError: string | null = null;
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
    try {
      await rm(hooksDirectory, { recursive: true, force: true });
    } catch (error) {
      hooksCleanupError = error instanceof Error ? error.message : String(error);
    }
  }
  notes.push("fast-forward-applied");

  // 8. Post-conditions: HEAD is exactly the adopted commit, the checkout is
  //    still attached to the branch that was fast-forwarded, and the tree is
  //    still clean. The fast-forward is applied at this point: anything that
  //    fails here is reported truthfully — strategy "fast-forward", the
  //    observed HEAD, `clean: false`, a structured error — and never rolled
  //    back or disguised as a no-mutation refusal.
  //
  //    The attached branch is verified, not assumed. Every check above is a
  //    snapshot, and a merge lock only serialises Agent Hub: an external `git
  //    switch`, `git checkout`, or `git reset` can still move HEAD between the
  //    pre-check and this probe. Catching that here is the difference between
  //    reporting "adopted and verified" and reporting it truthfully.
  const verifyPostAdoption =
    dependencies.verifyPostAdoption ??
    (async (target: string) => ({
      head: await currentRevision(target),
      clean: !(await isDirty(target)),
      branch: (await runGit(target, ["symbolic-ref", "--quiet", "--short", "HEAD"], 1000).catch(
        () => null,
      ))?.stdout.trim() ?? null,
    }));

  let observed: { head: string; clean: boolean; branch: string | null } | null = null;
  let postError: DelegateError | null = hooksCleanupError
    ? {
        code: "MERGE_POSTCONDITION_FAILED",
        message: `The fast-forward was applied, but the temporary hooks directory could not be removed: ${hooksCleanupError}. Inspect the checkout; nothing is rolled back`,
      }
    : null;
  if (hooksCleanupError !== null) {
    notes.push(`hooks-directory-cleanup-failed: ${hooksCleanupError}`);
  }
  try {
    observed = await verifyPostAdoption(workspace);
  } catch (error) {
    postError = {
      code: "MERGE_POSTCONDITION_FAILED",
      message: `Adoption verification could not read checkout state: ${error instanceof Error ? error.message : String(error)}. The fast-forward was applied and is never rolled back; inspect the checkout`,
    };
  }
  if (postError === null && observed!.head !== artifact.commit) {
    postError = {
      code: "MERGE_POSTCONDITION_FAILED",
      message: `HEAD is ${observed!.head} after adoption, expected ${artifact.commit}. The fast-forward was applied and is never rolled back; inspect the checkout`,
    };
  }
  if (postError === null && observed!.branch !== base.branch) {
    postError = {
      code: "MERGE_POSTCONDITION_FAILED",
      message:
        `The checkout is attached to ${
          observed!.branch === null ? "a detached HEAD" : `"${observed!.branch}"`
        } after adoption, expected the captured branch "${base.branch}". ` +
        "Something outside Agent Hub changed the attachment after the pre-check passed. The fast-forward was applied and is never rolled back; inspect the checkout",
    };
    notes.push(`attached-branch-diverged: ${observed!.branch ?? "(detached)"}`);
  }
  if (postError === null && !observed!.clean) {
    postError = {
      code: "MERGE_POSTCONDITION_FAILED",
      message: "Working tree is dirty after adoption. The fast-forward was applied and is never rolled back; inspect the checkout",
    };
  }
  if (postError !== null) {
    return {
      strategy: "fast-forward",
      candidate_id: candidateId || null,
      artifact_commit: artifact.commit,
      target_ref: `refs/heads/${base.branch}`,
      clean: false,
      applied_commit: observed?.head ?? null,
      notes,
      error: postError,
    };
  }
  notes.push("post-conditions-verified");

  return {
    strategy: "fast-forward",
    candidate_id: candidateId || null,
    artifact_commit: artifact.commit,
    target_ref: `refs/heads/${base.branch}`,
    clean: true,
    applied_commit: observed!.head,
    notes,
    error: null,
  };
}

/**
 * Opt-in adoption of one candidate artifact into the primary checkout. The
 * full revalidation (identity, named branch, base HEAD, clean status including
 * untracked files, artifact existence, and descendant ancestry) runs again
 * inside the repository-local merge lock, immediately before the checkout is
 * touched. Returns a serializable `MergeOutcome` on every path: refusals use
 * `strategy: "none"` with no mutation, and once a fast-forward has been
 * applied the outcome reports it truthfully — even when post-adoption
 * verification (HEAD, the attached branch, cleanliness) or the lock release
 * afterwards fails. It does not throw for normal outcomes, and it cannot make
 * an external process's later checkout change unobserved; see the residual
 * race in the module comment.
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
    let adopted: MergeOutcome | null = null;
    try {
      adopted = await adoptUnderLock(request, notes, dependencies);
    } finally {
      try {
        await lock.release();
      } catch (error) {
        const detail = error instanceof Error ? error.message : String(error);
        notes.push(`merge-lock-release-failed: ${detail}`);
        // A fast-forward that already happened stays applied even when the
        // lock cannot be released: attach a structured error to the truthful
        // outcome instead of pretending nothing mutated.
        if (adopted !== null && adopted.strategy === "fast-forward" && adopted.error === null) {
          adopted.error = {
            code: "LOCK_RELEASE_FAILED",
            message: `Adoption finished but the repository merge lock could not be released: ${detail}. The checkout change stands; nothing was rolled back`,
          };
        }
      }
    }
    return adopted;
  } catch (error) {
    const { code, message } = asDelegateError(error);
    return refusal(candidateId || null, artifact, base ?? null, code, message, notes);
  }
}

/**
 * Session-level auto-merge. Follows only a genuine `CompetitionResult` from
 * `runCompetition()`: status must be `"selected"` with a null error, its
 * winner must appear exactly once in its own eligible list, its base must be
 * the same identity as the fan-out base, and the fan-out candidate behind the
 * winner must be a successful candidate whose recorded artifact commit/ref
 * match the eligible entry verbatim. A workflow-shaped look-alike (an old
 * `CompetitionOutcome`, a hand-forged object) is refused, never followed.
 * Every gap is a serializable refusal. On success this delegates to
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
    if (
      typeof competition !== "object" ||
      typeof competition.status !== "string" ||
      !Array.isArray(competition.eligible) ||
      !competition.base
    ) {
      return refusal(
        null,
        null,
        fanOut.base,
        "MERGE_INVALID_INPUT",
        "competition must be a CompetitionResult exactly as produced by runCompetition(); forged or workflow-shaped objects are never followed",
        notes,
      );
    }
    if (competition.error !== null) {
      return refusal(
        null,
        null,
        fanOut.base,
        "MERGE_COMPETITION_FAILED",
        `Competition did not complete cleanly (${competition.error.code}): ${competition.error.message}`,
        notes,
      );
    }
    if (competition.status !== "selected") {
      return refusal(
        null,
        null,
        fanOut.base,
        "MERGE_NOT_SELECTED",
        `Competition status is "${competition.status}", not "selected"; nothing is adopted without a completed selection`,
        notes,
      );
    }
    if (competition.winner === null) {
      return refusal(
        null,
        null,
        fanOut.base,
        "MERGE_NO_WINNER",
        "Judge produced no winner; nothing is adopted without an internal selection",
        notes,
      );
    }
    const winnerId = competition.winner.candidate_id;
    const eligibleMatches = competition.eligible.filter(
      (entry) => entry !== null && typeof entry === "object" && entry.candidate_id === winnerId,
    );
    if (eligibleMatches.length !== 1) {
      return refusal(
        winnerId,
        null,
        fanOut.base,
        "MERGE_WINNER_NOT_ELIGIBLE",
        `Winner "${winnerId}" matches ${eligibleMatches.length} entries in the competition's eligible list; exactly one is required`,
        notes,
      );
    }
    if (!sameIdentity(competition.base, fanOut.base)) {
      return refusal(
        winnerId,
        null,
        fanOut.base,
        "MERGE_BASE_MISMATCH",
        `Competition base ${competition.base.head} in ${competition.base.worktree_root} is not the fan-out base ${fanOut.base.head} in ${fanOut.base.worktree_root}`,
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
    if (winner.status !== "success") {
      return refusal(
        winner.candidate_id,
        winner.artifact,
        fanOut.base,
        "MERGE_CANDIDATE_NOT_SUCCESS",
        `Winner "${winnerId}" has status "${winner.status}", not "success"`,
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
    if (
      winner.artifact.commit !== eligibleMatches[0].artifact_commit ||
      winner.artifact.ref !== eligibleMatches[0].artifact_ref
    ) {
      return refusal(
        winner.candidate_id,
        winner.artifact,
        fanOut.base,
        "MERGE_CANDIDATE_ARTIFACT_MISMATCH",
        `Winner artifact (${winner.artifact.commit}, ${winner.artifact.ref}) does not match the eligible entry (${eligibleMatches[0].artifact_commit}, ${eligibleMatches[0].artifact_ref})`,
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
