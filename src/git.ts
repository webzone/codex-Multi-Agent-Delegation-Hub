import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { AgentHubError } from "./errors.js";
import { runProcess } from "./process.js";

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

async function runGit(cwd: string, args: string[], maxOutputBytes = GIT_OUTPUT_LIMIT) {
  const result = await runProcess("git", args, { cwd, maxOutputBytes });
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
  const diff = await runGit(cwd, ["diff", "--binary", "HEAD", "--"], maxOutputBytes);

  return {
    changedFiles: parseChangedFiles(status.stdout),
    diff: diff.stdout,
    diffTruncated: diff.stdoutTruncated,
  };
}

export async function createTemporaryWorktree(cwd: string): Promise<TemporaryWorktree> {
  const revision = await currentRevision(cwd);
  const parentPath = await mkdtemp(join(tmpdir(), "agent-hub-"));
  const path = join(parentPath, "worktree");

  try {
    await runGit(cwd, ["worktree", "add", "--detach", path, revision]);
  } catch (error) {
    await rm(parentPath, { recursive: true, force: true });
    throw error;
  }

  return { path, parentPath, revision };
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
    await runGit(cwd, ["worktree", "prune"]);
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
