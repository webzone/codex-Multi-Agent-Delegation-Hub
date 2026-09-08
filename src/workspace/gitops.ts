import { mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { AgentHubError } from "../errors.js";
import { runGit } from "../git.js";
import { runProcess } from "../process.js";
import type { CaptureReason } from "./records.js";

/**
 * Git primitives for workspace custody.
 *
 * Everything here is proof-keyed: refs are moved only by compare-and-swap,
 * a probe answers `null` only for a proven-absent ref, and captures use the
 * hub's temporary-index discipline (`read-tree` + `add -A` + `write-tree` +
 * `commit-tree`) with a fixed hub identity and the hub clock, so no hook and
 * no ambient author config can ever shape a custody commit, and no user text
 * can reach a commit message.
 */

const COMMIT_PATTERN = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;

export const WORKSPACE_ARTIFACT_IDENTITY_NAME = "Agent Hub";
export const WORKSPACE_ARTIFACT_IDENTITY_EMAIL = "agent-hub@localhost";

/** Fixed constant message per capture; carries no task text or provider output. */
export function workspaceCaptureMessage(reason: CaptureReason, seq: number | null): string {
  return seq === null
    ? `Agent Hub workspace close checkpoint (${reason})`
    : `Agent Hub workspace result ${seq} (${reason})`;
}
/**
 * Commit the ref resolves to, or null when the ref provably does not exist.
 * A git that could not run is NOT an answer: it throws, so callers can
 * never read "unverifiable" as "absent".
 */
export async function probeRef(repositoryCwd: string, ref: string): Promise<string | null> {
  const result = await runProcess("git", ["rev-parse", "--verify", "--quiet", ref], {
    cwd: repositoryCwd,
    maxOutputBytes: 1000,
  });
  if (result.error) {
    throw new AgentHubError("GIT_COMMAND_FAILED", `cannot probe ref ${ref}: ${result.error}`);
  }
  const sha = result.stdout.trim();
  return result.exitCode === 0 && COMMIT_PATTERN.test(sha) ? sha : null;
}
/**
 * Verify a commit object exists. Unverifiable answers are `false` here —
 * fail-closed: callers must retain what the repository cannot prove.
 */
export async function commitExists(repositoryCwd: string, commit: string): Promise<boolean> {
  const result = await runProcess("git", ["rev-parse", "--verify", "--quiet", `${commit}^{commit}`], {
    cwd: repositoryCwd,
    maxOutputBytes: 1000,
  });
  return !result.error && result.exitCode === 0 && result.stdout.trim() === commit;
}
/** Zero OID matching the object-name width of `sha`, for create-CAS. */
export function zeroOidFor(sha: string): string {
  return "0".repeat(sha.length === 64 ? 64 : 40);
}


/**
 * Compare-and-swap a ref. `null` old value means "create": the CAS uses the
 * width-matched zero OID. A rejected CAS throws `WORKSPACE_REF_CAS_FAILED`
 * — it proves divergence, it never retries.
 */
export async function casRef(
  repositoryCwd: string,
  ref: string,
  expected: string | null,
  next: string,
): Promise<void> {
  const oldValue = expected ?? zeroOidFor(next);
  try {
    await runGit(repositoryCwd, ["update-ref", ref, next, oldValue], 1000);
  } catch {
    throw new AgentHubError(
      "WORKSPACE_REF_CAS_FAILED",
      `ref ${ref} moved concurrently; refused to move it from ${expected ?? "(absent)"} to ${next}`,
    );
  }
}

/** CAS-delete a ref. A ref that no longer points where custody recorded is refused. */
export async function casDeleteRef(
  repositoryCwd: string,
  ref: string,
  expected: string,
): Promise<boolean> {
  try {
    await runGit(repositoryCwd, ["update-ref", "-d", ref, expected], 1000);
    return true;
  } catch {
    return false;
  }
}

export interface CaptureOutcome {
  /** True when the working state differs from `parent`'s tree and a commit was made. */
  advanced: boolean;
  tree: string;
  commit: string;
  taken_at: string;
}

/**
 * Capture a worktree's full working state over `parent` WITHOUT moving any
 * ref. A working state whose tree equals the parent's tree advances nothing:
 * the returned commit is `parent` itself, which is exactly what a
 * no-tree-change result record must name.
 */
export async function captureWorkspaceState(
  worktreePath: string,
  parent: string,
  reason: CaptureReason,
  seq: number | null,
  now: () => Date,
  maxOutputBytes = 1_000_000,
): Promise<CaptureOutcome> {
  const indexDirectory = await mkdtemp(join(tmpdir(), "agent-hub-workspace-index-"));
  const takenAt = now().toISOString();
  const indexEnv: NodeJS.ProcessEnv = {
    ...process.env,
    GIT_INDEX_FILE: join(indexDirectory, "index"),
    GIT_AUTHOR_NAME: WORKSPACE_ARTIFACT_IDENTITY_NAME,
    GIT_AUTHOR_EMAIL: WORKSPACE_ARTIFACT_IDENTITY_EMAIL,
    GIT_COMMITTER_NAME: WORKSPACE_ARTIFACT_IDENTITY_NAME,
    GIT_COMMITTER_EMAIL: WORKSPACE_ARTIFACT_IDENTITY_EMAIL,
    GIT_AUTHOR_DATE: `${takenAt} +0000`,
    GIT_COMMITTER_DATE: `${takenAt} +0000`,
  };
  try {
    await runGit(worktreePath, ["read-tree", parent], maxOutputBytes, indexEnv);
    await runGit(worktreePath, ["add", "-A", "--", "."], maxOutputBytes, indexEnv);
    const tree = (await runGit(worktreePath, ["write-tree"], 1000, indexEnv)).stdout.trim();
    const parentTree = (await runGit(worktreePath, ["rev-parse", `${parent}^{tree}`], 1000)).stdout.trim();
    if (tree === parentTree) {
      return { advanced: false, tree, commit: parent, taken_at: takenAt };
    }
    const commit = (
      await runGit(
        worktreePath,
        ["commit-tree", tree, "-p", parent, "-m", workspaceCaptureMessage(reason, seq)],
        1000,
        indexEnv,
      )
    ).stdout.trim();
    return { advanced: true, tree, commit, taken_at: takenAt };
  } finally {
    await rm(indexDirectory, { recursive: true, force: true });
  }
}

/** Create an isolated detached worktree at `base` under the hub-owned path. */
export async function addWorktree(repositoryCwd: string, path: string, base: string): Promise<void> {
  await runGit(repositoryCwd, ["worktree", "add", "--detach", path, base]);
}

export type WorktreeInspection =
  | { state: "present"; head: string; common_dir: string }
  | { state: "missing" }
  | { state: "foreign"; reason: string };

/**
 * Prove a recorded worktree path is still a usable linked worktree of the
 * recorded repository. Absent directories are reported, never recreated; a
 * directory that is no longer this repository's worktree is `foreign`.
 */
export async function inspectWorktree(
  repositoryCwd: string,
  worktreePath: string,
): Promise<WorktreeInspection> {
  try {
    await stat(worktreePath);
  } catch {
    return { state: "missing" };
  }
  const head = await runGit(worktreePath, ["rev-parse", "HEAD"], 1000).catch(() => null);
  const common = await runGit(
    worktreePath,
    ["rev-parse", "--path-format=absolute", "--git-common-dir"],
    1000,
  ).catch(() => null);
  const expected = await runGit(
    repositoryCwd,
    ["rev-parse", "--path-format=absolute", "--git-common-dir"],
    1000,
  );
  if (head === null || common === null) {
    return { state: "foreign", reason: "the path exists but is no longer a readable git worktree" };
  }
  if (common.stdout.trim() !== expected.stdout.trim()) {
    return { state: "foreign", reason: "the worktree belongs to a different repository" };
  }
  return { head: head.stdout.trim(), state: "present", common_dir: common.stdout.trim() };
}

export type WorktreeRemoval =
  | { removed: true; cleanup_error: string | null }
  | { removed: false; reason: string };

/**
 * Remove a custody worktree. Callers may only invoke this for hub-provisioned
 * custody paths once a handoff decision authorized forcing (`--force` is
 * never inferred from state). If the directory is already gone, git is
 * allowed to simply forget it via `prune`.
 */
export async function removeWorktree(
  repositoryCwd: string,
  expectedPath: string,
): Promise<WorktreeRemoval> {
  const exists = await stat(expectedPath).catch(() => null);
  if (exists === null) {
    await runGit(repositoryCwd, ["worktree", "prune"]).catch(() => undefined);
    return { removed: true, cleanup_error: "worktree directory was already absent; pruned administration" };
  }
  try {
    await runGit(repositoryCwd, ["worktree", "remove", "--force", expectedPath]);
    return { removed: true, cleanup_error: null };
  } catch (error) {
    const detail = error instanceof AgentHubError ? error.message : String(error);
    return { removed: false, reason: detail };
  }
}

/** One prune pass per administrative window, under the workspace-admin lock. */
export async function pruneWorktrees(repositoryCwd: string): Promise<void> {
  await runGit(repositoryCwd, ["worktree", "prune"]);
}
