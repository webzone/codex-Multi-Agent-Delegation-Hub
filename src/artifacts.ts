import { createHash } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { AgentHubError } from "./errors.js";
import { runGit } from "./git.js";
import type { CandidateArtifact } from "./types.js";

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
