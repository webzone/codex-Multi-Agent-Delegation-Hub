import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve as resolvePath } from "node:path";

import { AgentHubError } from "./errors.js";
import { runProcess } from "./process.js";
import type { RepositoryIdentity } from "./types.js";

const GIT_OUTPUT_LIMIT = 1_000_000;

export interface GitSnapshot {
  changedFiles: string[];
  diff: string;
  diffTruncated: boolean;
}

export interface TemporaryWorktree {
  path: string;
  parentPath: string;
  revision: string;
}

export async function runGit(
  cwd: string,
  args: string[],
  maxOutputBytes = GIT_OUTPUT_LIMIT,
  environment: NodeJS.ProcessEnv = process.env,
) {
  const result = await runProcess("git", args, { cwd, env: environment, maxOutputBytes });
  if (result.error || result.exitCode !== 0) {
    const detail = result.error ?? (result.stderr.trim() || `exit code ${result.exitCode}`);
    throw new AgentHubError("GIT_COMMAND_FAILED", `git ${args.join(" ")} failed: ${detail}`);
  }
  return result;
}

export async function ensureGitRepository(cwd: string): Promise<void> {
  const result = await runProcess(
    "git",
    ["rev-parse", "--is-inside-work-tree"],
    { cwd, maxOutputBytes: 1000 },
  );

  if (result.error || result.exitCode !== 0 || result.stdout.trim() !== "true") {
    throw new AgentHubError("NOT_GIT_REPOSITORY", `${cwd} is not a Git worktree`);
  }
}

export async function isDirty(cwd: string): Promise<boolean> {
  const result = await runGit(cwd, ["status", "--porcelain=v1", "--untracked-files=all"]);
  return result.stdout.length > 0;
}

export async function currentRevision(cwd: string): Promise<string> {
  const result = await runGit(cwd, ["rev-parse", "HEAD"], 1000);
  return result.stdout.trim();
}

/**
 * Capture repository identity, attached branch (if any), and HEAD exactly
 * once. Fan-out calls this before dispatching candidates so every candidate
 * is pinned to the same base SHA regardless of what the caller does later.
 */
export async function resolveRepositoryIdentity(cwd: string): Promise<RepositoryIdentity> {
  await ensureGitRepository(cwd);

  let commonDir: string;
  try {
    const absolute = await runGit(cwd, ["rev-parse", "--path-format=absolute", "--git-common-dir"], 4000);
    commonDir = absolute.stdout.trim();
  } catch {
    // Older Git without --path-format prints the common dir *relative to the
    // queried cwd* (a linked worktree reports e.g. `../.git`). It must be
    // resolved against that target cwd — resolving it against process.cwd()
    // silently points every workspace's state at some unrelated directory.
    const relative = await runGit(cwd, ["rev-parse", "--git-common-dir"], 4000);
    commonDir = resolvePath(cwd, relative.stdout.trim());
  }

  const toplevel = await runGit(cwd, ["rev-parse", "--show-toplevel"], 4000);

  let head: string;
  try {
    head = (await runGit(cwd, ["rev-parse", "HEAD"], 1000)).stdout.trim();
  } catch {
    throw new AgentHubError("NO_BASE_COMMIT", `${cwd} has no commits; create an initial commit before fan-out`);
  }

  const branchProbe = await runProcess("git", ["symbolic-ref", "--quiet", "--short", "HEAD"], {
    cwd,
    maxOutputBytes: 1000,
  });
  if (branchProbe.error || (branchProbe.exitCode !== 0 && branchProbe.exitCode !== 1)) {
    const detail = branchProbe.error || branchProbe.stderr.trim() || `exit code ${branchProbe.exitCode}`;
    throw new AgentHubError("GIT_COMMAND_FAILED", `git symbolic-ref HEAD failed: ${detail}`);
  }

  return {
    common_dir: resolvePath(commonDir),
    worktree_root: toplevel.stdout.trim(),
    branch: branchProbe.exitCode === 0 && branchProbe.stdout.trim() ? branchProbe.stdout.trim() : null,
    head,
  };
}

function parseChangedFiles(status: string): string[] {
  const entries = status.split("\0").filter(Boolean);
  const files: string[] = [];

  for (let index = 0; index < entries.length; index += 1) {
    const entry = entries[index];
    const code = entry.slice(0, 2);
    const file = entry.slice(3);
    files.push(file);

    if (code.includes("R") || code.includes("C")) {
      index += 1;
      const destination = entries[index];
      if (destination) {
        files[files.length - 1] = destination;
      }
    }
  }

  return files;
}

export async function gitSnapshot(cwd: string, maxOutputBytes = GIT_OUTPUT_LIMIT): Promise<GitSnapshot> {
  const status = await runGit(
    cwd,
    ["status", "--porcelain=v1", "-z", "--untracked-files=all"],
    maxOutputBytes,
  );
  const indexDirectory = await mkdtemp(join(tmpdir(), "agent-hub-index-"));
  const indexPath = join(indexDirectory, "index");
  const temporaryIndexEnvironment = { ...process.env, GIT_INDEX_FILE: indexPath };
  let diff;

  try {
    await runGit(cwd, ["read-tree", "HEAD"], maxOutputBytes, temporaryIndexEnvironment);
    await runGit(cwd, ["add", "-A", "--", "."], maxOutputBytes, temporaryIndexEnvironment);
    diff = await runGit(
      cwd,
      ["diff", "--cached", "--binary", "--no-ext-diff", "--"],
      maxOutputBytes,
      temporaryIndexEnvironment,
    );
  } finally {
    await rm(indexDirectory, { recursive: true, force: true });
  }

  return {
    changedFiles: parseChangedFiles(status.stdout),
    diff: diff.stdout,
    diffTruncated: diff.stdoutTruncated,
  };
}

export interface BaseWorktree {
  path: string;
  parentPath: string;
  base: string;
}

/**
 * Create a detached worktree pinned to an explicit base SHA. Caller-side
 * checkout and index are untouched; `git worktree add` only writes admin
 * metadata. Serialize calls with the repository lock when running in
 * parallel — Git's worktree administration is not concurrency-safe.
 */
export async function createWorktreeAtBase(cwd: string, base: string): Promise<BaseWorktree> {
  const parentPath = await mkdtemp(join(tmpdir(), "agent-hub-"));
  const path = join(parentPath, "worktree");

  try {
    await runGit(cwd, ["worktree", "add", "--detach", path, base]);
  } catch (error) {
    await rm(parentPath, { recursive: true, force: true });
    throw error;
  }

  return { path, parentPath, base };
}

/**
 * Remove a worktree without pruning: `git worktree prune` from one candidate
 * would race with another candidate's in-flight `worktree add`, so fan-out
 * prunes exactly once, under the same lock, after all removals.
 */
export async function removeWorktree(cwd: string, worktree: BaseWorktree): Promise<void> {
  try {
    await runGit(cwd, ["worktree", "remove", "--force", worktree.path]);
  } catch (error) {
    await rm(worktree.parentPath, { recursive: true, force: true });
    throw new AgentHubError(
      "WORKTREE_CLEANUP_FAILED",
      error instanceof Error ? error.message : String(error),
    );
  }

  await rm(worktree.parentPath, { recursive: true, force: true });
}

export async function pruneWorktrees(cwd: string): Promise<void> {
  await runGit(cwd, ["worktree", "prune"]);
}

export async function createTemporaryWorktree(cwd: string): Promise<TemporaryWorktree> {
  const revision = await currentRevision(cwd);
  const worktree = await createWorktreeAtBase(cwd, revision);
  return { path: worktree.path, parentPath: worktree.parentPath, revision };
}

export async function removeTemporaryWorktree(
  cwd: string,
  worktree: TemporaryWorktree,
): Promise<void> {
  let cleanupError: unknown;

  try {
    await runGit(cwd, ["worktree", "remove", "--force", worktree.path]);
  } catch (error) {
    cleanupError = error;
  }

  try {
    await pruneWorktrees(cwd);
  } catch (error) {
    cleanupError ??= error;
  }

  await rm(worktree.parentPath, { recursive: true, force: true });

  if (cleanupError) {
    throw new AgentHubError(
      "WORKTREE_CLEANUP_FAILED",
      cleanupError instanceof Error ? cleanupError.message : String(cleanupError),
    );
  }
}
