import { createHash } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { asDelegateError, AgentHubError } from "./errors.js";
import { runGit } from "./git.js";
import { runProcess } from "./process.js";
import type { CandidateArtifact, DelegateError, FanOutResult } from "./types.js";

/**
 * Candidate artifacts are captured as plain Git objects via plumbing only
 * (`read-tree` / `add` / `write-tree` / `commit-tree` / `update-ref`). None of
 * those commands ever run user hooks, and every write goes through a private
 * temporary index, so the caller's checkout and real index are never touched.
 */

const GIT_OUTPUT_LIMIT = 1_000_000;

export const ARTIFACT_IDENTITY_NAME = "Agent Hub";
export const ARTIFACT_IDENTITY_EMAIL = "agent-hub@localhost";
/** Fixed constant message: never contains the task text or agent output. */
export const ARTIFACT_COMMIT_MESSAGE = "Agent Hub candidate artifact";
export const CANDIDATE_REF_NAMESPACE = "refs/agent-hub/candidates";

const SAFE_REF_PATTERN = /^refs\/agent-hub\/candidates\/[0-9a-z-]+$/;

export interface ArtifactCaptureOptions {
  /** Detached candidate worktree checked out at `base`. */
  worktreePath: string;
  /** Captured base SHA; becomes the artifact commit's sole parent. */
  base: string;
  /**
   * Internally generated candidate id. Hashed into the ref name — raw ids are
   * never interpolated into ref names.
   */
  candidateKey: string;
  maxOutputBytes?: number;
}

/**
 * Ref name derived from an internal sha256 digest of the candidate key plus
 * the artifact commit prefix: fully internal, path-safe, collision-tolerant,
 * and free of any caller- or id-supplied characters.
 */
function candidateRef(commit: string, candidateKey: string): string {
  const digest = createHash("sha256").update(`agent-hub-candidate\0${candidateKey}`, "utf8")
    .digest("hex")
    .slice(0, 16);
  const ref = `${CANDIDATE_REF_NAMESPACE}/${digest}-${commit.slice(0, 12)}`;
  if (!SAFE_REF_PATTERN.test(ref)) {
    throw new AgentHubError("ARTIFACT_REF_INVALID", `Generated ref "${ref}" is not path-safe`);
  }
  return ref;
}

export async function captureCandidateArtifact(
  options: ArtifactCaptureOptions,
): Promise<CandidateArtifact> {
  const { worktreePath, base, candidateKey } = options;
  const maxOutputBytes = options.maxOutputBytes ?? GIT_OUTPUT_LIMIT;

  const indexDirectory = await mkdtemp(join(tmpdir(), "agent-hub-artifact-"));
  const indexEnv: NodeJS.ProcessEnv = {
    ...process.env,
    GIT_INDEX_FILE: join(indexDirectory, "index"),
    GIT_AUTHOR_NAME: ARTIFACT_IDENTITY_NAME,
    GIT_AUTHOR_EMAIL: ARTIFACT_IDENTITY_EMAIL,
    GIT_COMMITTER_NAME: ARTIFACT_IDENTITY_NAME,
    GIT_COMMITTER_EMAIL: ARTIFACT_IDENTITY_EMAIL,
  };

  try {
    // Stage base + full working state into the private index.
    await runGit(worktreePath, ["read-tree", base], maxOutputBytes, indexEnv);
    await runGit(worktreePath, ["add", "-A", "--", "."], maxOutputBytes, indexEnv);
    const tree = (await runGit(worktreePath, ["write-tree"], 1000, indexEnv)).stdout.trim();
    const baseTree = (await runGit(worktreePath, ["rev-parse", `${base}^{tree}`], 1000, indexEnv)).stdout.trim();

    if (tree === baseTree) {
      return {
        parent: base,
        commit: null,
        tree: null,
        ref: null,
        empty: true,
        changed_files: [],
        diff: "",
        diff_truncated: false,
      };
    }

    const names = await runGit(worktreePath, ["diff", "--cached", "--name-only", "-z", "--"], maxOutputBytes, indexEnv);
    const diff = await runGit(
      worktreePath,
      ["diff", "--cached", "--binary", "--no-ext-diff", "--"],
      maxOutputBytes,
      indexEnv,
    );

    const commit = (
      await runGit(worktreePath, ["commit-tree", tree, "-p", base, "-m", ARTIFACT_COMMIT_MESSAGE], 1000, indexEnv)
    ).stdout.trim();

    // The ref is only ever pointed at a commit that already exists.
    const ref = candidateRef(commit, candidateKey);
    await runGit(worktreePath, ["update-ref", ref, commit], 1000, indexEnv);

    return {
      parent: base,
      commit,
      tree,
      ref,
      empty: false,
      changed_files: names.stdout.split("\0").filter(Boolean),
      diff: diff.stdout,
      diff_truncated: diff.stdoutTruncated,
    };
  } finally {
    await rm(indexDirectory, { recursive: true, force: true });
  }
}

const ARTIFACT_COMMIT_PATTERN = /^([0-9a-f]{40}|[0-9a-f]{64})$/;

/**
 * What one CAS-safe release attempt actually observed about a private ref.
 * The three states are deliberately not collapsed into a boolean: "already
 * gone", "someone else owns it now", and "I could not delete my own ref" mean
 * different things to the operator and to the exit code.
 */
export type CandidateRefReleaseStatus = "released" | "foreign-retarget" | "cleanup-failed";

export interface CandidateRefRelease {
  status: CandidateRefReleaseStatus;
  ref: string;
  /** What the ref resolves to now; null when it is gone or cannot be probed. */
  found: string | null;
  /** Structured failure, set exactly when `status` is `"cleanup-failed"`. */
  error: DelegateError | null;
}

/**
 * CAS-safe release of one private candidate artifact ref: the ref is deleted
 * only while it still points at exactly `expectedCommit`. `update-ref -d` with
 * an old-value performs that compare-and-delete inside Git itself. The
 * outcomes:
 *
 * - `released`: the delete succeeded, or the ref had already vanished.
 * - `foreign-retarget`: the ref exists and names something else. That foreign
 *   claim is preserved — correct behaviour, not a failure.
 * - `cleanup-failed`: the ref still names *our* commit and the delete was
 *   refused (locked ref, unwritable refs directory, git error), or its
 *   existence cannot be established at all. Our own ref is still occupying the
 *   namespace and could not be removed: never reported as released.
 *
 * Refs outside the Agent Hub candidate namespace are refused outright.
 */
export async function releaseCandidateRef(
  workspace: string,
  ref: string,
  expectedCommit: string,
): Promise<CandidateRefRelease> {
  if (!SAFE_REF_PATTERN.test(ref)) {
    throw new AgentHubError(
      "ARTIFACT_REF_INVALID",
      `"${ref}" is not a private Agent Hub candidate ref; release only ever touches refs this hub created`,
    );
  }
  if (!ARTIFACT_COMMIT_PATTERN.test(expectedCommit)) {
    throw new AgentHubError(
      "ARTIFACT_COMMIT_INVALID",
      `Expected commit "${expectedCommit}" is not a full hex commit SHA`,
    );
  }

  const cas = await runProcess("git", ["update-ref", "-d", ref, expectedCommit], {
    cwd: workspace,
    maxOutputBytes: 4000,
  });
  if (!cas.error && cas.exitCode === 0) {
    return { status: "released", ref, found: null, error: null };
  }
  const gitDetail = (cas.error ?? cas.stderr).trim();

  // The CAS was rejected or failed. Probe the ref itself, not `${ref}^{commit}`:
  // a concurrent retarget to a tree/blob/tag is still a foreign claim and must
  // be preserved, and a probe we cannot trust must never read as "released".
  const probe = await runProcess("git", ["rev-parse", "--quiet", "--verify", ref], {
    cwd: workspace,
    maxOutputBytes: 256,
  });
  const found = probe.stdout.trim();

  if (probe.error) {
    return cleanupFailure(ref, null, "the ref could not be probed", probe.error);
  }
  if (probe.exitCode === 0 && found !== "") {
    if (found === expectedCommit) {
      return cleanupFailure(
        ref,
        found,
        "it still points at the commit this hub created, so the delete was refused rather than mis-targeted",
        gitDetail,
      );
    }
    return { status: "foreign-retarget", ref, found, error: null };
  }
  // Verified absence is exit 1 with no output. Anything else (a fatal git
  // error, exit 0 printing nothing) proves nothing and is never "released".
  if (probe.exitCode === 1 && found === "") {
    return { status: "released", ref, found: null, error: null };
  }
  return cleanupFailure(
    ref,
    null,
    `its existence could not be established (git rev-parse exited ${probe.exitCode ?? "null"})`,
    probe.stderr.trim() || gitDetail,
  );
}

function cleanupFailure(
  ref: string,
  found: string | null,
  why: string,
  gitDetail: string,
): CandidateRefRelease {
  return {
    status: "cleanup-failed",
    ref,
    found,
    error: {
      code: "ARTIFACT_REF_CLEANUP_FAILED",
      message:
        `Private artifact ref "${ref}" could not be released: ${why}` +
        `${gitDetail ? ` (git: ${gitDetail})` : ""}. ` +
        "The ref is still retained; releasing was never attempted blindly and nothing was deleted.",
    },
  };
}

/**
 * Release every retained artifact ref of a fan-out result, CAS-safe. The
 * terminal paths (CLI `fanout`, MCP fan-out tools) run this in a `finally`
 * once the result and any merge are no longer consumers of the refs; the
 * core library never calls it because there the caller owns cleanup.
 *
 * Returned errors are *real* cleanup trouble (`cleanup-failed`): a ref this
 * hub still owns that it could not delete. Foreign-retargeted refs are not
 * errors — preserving them is the point of the CAS. Failures are returned,
 * never thrown: cleanup trouble must not mask the operation it follows, but
 * the terminal paths must fail on it (exit `1` / `isError`) because refs the
 * operator was promised are gone.
 */
export async function releaseFanOutArtifactRefs(
  workspace: string,
  fanOut: FanOutResult,
): Promise<DelegateError[]> {
  const errors: DelegateError[] = [];
  for (const candidate of fanOut?.candidates ?? []) {
    const artifact = candidate?.artifact;
    if (!artifact?.ref || !artifact.commit) {
      continue;
    }
    try {
      const release = await releaseCandidateRef(workspace, artifact.ref, artifact.commit);
      if (release.error) {
        errors.push(release.error);
      }
    } catch (error) {
      errors.push(asDelegateError(error));
    }
  }
  return errors;
}
