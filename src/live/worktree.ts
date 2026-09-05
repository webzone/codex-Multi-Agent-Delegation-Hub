import { mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { isAbsolute, join, relative, resolve, sep } from "node:path";

import { AgentHubError } from "../errors.js";
import { runGit } from "../git.js";
import { runProcess } from "../process.js";
import type { CheckpointReason, LiveCheckpoint } from "./types.js";

/**
 * Persistent OS-temp live worktree + hook-free checkpoint chain (v3 live,
 * Package 1).
 *
 * Unlike the v2 per-run worktree (created, captured, removed inside one
 * call), the live worktree *outlives the hub process*: the provider runs in
 * it for the session's whole lifetime, its path is recorded in the lease, and
 * after a hub crash a later hub finds it through the lease and pins its
 * surviving state. It lives under the OS temp root in a hub-owned directory
 * (`agent-hub-live-*`), never in the caller checkout.
 *
 * Checkpoints use the exact v2 artifact discipline — temporary index,
 * `read-tree` + `add -A` + `write-tree` + `commit-tree`, fixed Agent Hub
 * identity, full-width object names — with two live-specific rules:
 *
 *   - the commit message is a hub constant per `CheckpointReason`; the reason
 *     survives a hub restart inside the commit object itself, and no task
 *     text or provider output can ever reach a commit message;
 *   - a no-change observation produces `advanced: false` and pins nothing:
 *     the chain only ever contains commits whose tree differs from its
 *     parent's tree, so chain length, `checkpoint_seq` and `current_commit`
 *     stay in exact agreement across recovery.
 */

export const LIVE_WORKTREE_PREFIX = "agent-hub-live-";
export const LIVE_CHECKPOINT_COMMIT_PREFIX = "Agent Hub live checkpoint";
export const LIVE_ARTIFACT_IDENTITY_NAME = "Agent Hub";
export const LIVE_ARTIFACT_IDENTITY_EMAIL = "agent-hub@localhost";

const CHECKPOINT_REASONS: readonly CheckpointReason[] = [
  "turn_end",
  "requested",
  "cancel",
  "close",
  "error",
  "crash_recovery",
];

/** Fixed constant message per reason; parses back exactly, holds no user data. */
export function liveCheckpointMessage(reason: CheckpointReason): string {
  return `${LIVE_CHECKPOINT_COMMIT_PREFIX} (${reason})`;
}

function parseCheckpointReason(message: string): CheckpointReason | null {
  for (const reason of CHECKPOINT_REASONS) {
    if (message === liveCheckpointMessage(reason)) {
      return reason;
    }
  }
  return null;
}

export interface LiveWorktree {
  path: string;
  parentPath: string;
  base: string;
}

function isInside(parent: string, child: string): boolean {
  const rel = relative(resolve(parent), resolve(child));
  return rel === "" || (rel !== ".." && !rel.startsWith(`..${sep}`) && !isAbsolute(rel));
}

/**
 * Create the persistent live worktree detached at `base`. Serialize calls
 * with the live-admin repository lock — Git's worktree administration is not
 * concurrency-safe (same rule v2 fan-out follows).
 */
export async function createLiveWorktree(
  repositoryCwd: string,
  base: string,
  tmpRoot: string = tmpdir(),
): Promise<LiveWorktree> {
  const parentPath = await mkdtemp(join(tmpRoot, LIVE_WORKTREE_PREFIX));
  const path = join(parentPath, "worktree");
  try {
    await runGit(repositoryCwd, ["worktree", "add", "--detach", path, base]);
  } catch (error) {
    await rm(parentPath, { recursive: true, force: true });
    throw error;
  }
  return { path, parentPath, base };
}

/**
 * Prove a recorded worktree path is still usable for this repository. Absent
 * directories are reported, never recreated; a directory that exists but is
 * no longer a linked worktree of the same common dir is a hard inconsistency.
 */
export async function inspectLiveWorktree(
  repositoryCwd: string,
  worktreePath: string,
): Promise<{ head: string; common_dir: string }> {
  try {
    await stat(worktreePath);
  } catch {
    throw new AgentHubError(
      "LIVE_WORKTREE_MISSING",
      `live worktree "${worktreePath}" no longer exists`,
    );
  }
  const head = await runGit(worktreePath, ["rev-parse", "HEAD"], 1000);
  const common = await runGit(worktreePath, ["rev-parse", "--path-format=absolute", "--git-common-dir"], 1000);
  const expected = await runGit(repositoryCwd, ["rev-parse", "--path-format=absolute", "--git-common-dir"], 1000);
  if (common.stdout.trim() !== expected.stdout.trim()) {
    throw new AgentHubError(
      "LIVE_WORKTREE_FOREIGN",
      `live worktree "${worktreePath}" belongs to a different repository`,
    );
  }
  return { head: head.stdout.trim(), common_dir: common.stdout.trim() };
}

export type LiveCheckpointOutcome =
  | { advanced: true; checkpoint: LiveCheckpoint }
  | { advanced: false; tree: string; taken_at: string };

export interface LiveCheckpointCaptureOptions {
  /** 1-based position the new commit would take in the chain. */
  seq: number;
  now: () => Date;
  maxOutputBytes?: number;
}

/**
 * Capture the worktree's full working state as the next commit of the live
 * chain over `parent`. Does NOT move any ref — the caller commits chain head
 * + state record together through the sidecar/CAS transition in state.ts.
 */
export async function captureLiveCheckpoint(
  worktreePath: string,
  parent: string,
  reason: CheckpointReason,
  options: LiveCheckpointCaptureOptions,
): Promise<LiveCheckpointOutcome> {
  const maxOutputBytes = options.maxOutputBytes ?? 1_000_000;
  const indexDirectory = await mkdtemp(join(tmpdir(), "agent-hub-live-index-"));
  const takenAt = options.now().toISOString();
  const indexEnv: NodeJS.ProcessEnv = {
    ...process.env,
    GIT_INDEX_FILE: join(indexDirectory, "index"),
    GIT_AUTHOR_NAME: LIVE_ARTIFACT_IDENTITY_NAME,
    GIT_AUTHOR_EMAIL: LIVE_ARTIFACT_IDENTITY_EMAIL,
    GIT_COMMITTER_NAME: LIVE_ARTIFACT_IDENTITY_NAME,
    GIT_COMMITTER_EMAIL: LIVE_ARTIFACT_IDENTITY_EMAIL,
    // The hub clock owns chain timestamps: reconstruction from Git objects
    // reports the same `taken_at` the capture returned.
    GIT_AUTHOR_DATE: `${takenAt} +0000`,
    GIT_COMMITTER_DATE: `${takenAt} +0000`,
  };

  try {
    await runGit(worktreePath, ["read-tree", parent], maxOutputBytes, indexEnv);
    await runGit(worktreePath, ["add", "-A", "--", "."], maxOutputBytes, indexEnv);
    const tree = (await runGit(worktreePath, ["write-tree"], 1000, indexEnv)).stdout.trim();
    const parentTree = (await runGit(worktreePath, ["rev-parse", `${parent}^{tree}`], 1000, indexEnv)).stdout.trim();

    if (tree === parentTree) {
      return { advanced: false, tree, taken_at: takenAt };
    }

    const commit = (
      await runGit(
        worktreePath,
        ["commit-tree", tree, "-p", parent, "-m", liveCheckpointMessage(reason)],
        1000,
        indexEnv,
      )
    ).stdout.trim();

    return {
      advanced: true,
      checkpoint: {
        seq: options.seq,
        commit,
        parent,
        tree,
        empty: false,
        reason,
        taken_at: takenAt,
      },
    };
  } finally {
    await rm(indexDirectory, { recursive: true, force: true });
  }
}

/**
 * Rebuild the checkpoint chain from the ref itself: seq numbering, parentage,
 * trees, reasons (from the constant commit message) and times all come from
 * Git objects, so a hub that lost every local file except the repository can
 * answer "what did this session pin and why".
 */
export async function describeLiveCheckpointChain(
  repositoryCwd: string,
  ref: string,
  base: string,
): Promise<LiveCheckpoint[]> {
  const headProbe = await runProcess(
    "git",
    ["rev-parse", "--verify", "--quiet", ref],
    { cwd: repositoryCwd, maxOutputBytes: 1000 },
  );
  if (headProbe.error || headProbe.exitCode !== 0 || !headProbe.stdout.trim()) {
    throw new AgentHubError(
      "LIVE_STATE_INCONSISTENT",
      `live ref ${ref} does not resolve; its checkpoint chain cannot be described`,
    );
  }

  const ancestor = await runProcess(
    "git",
    ["merge-base", "--is-ancestor", base, ref],
    { cwd: repositoryCwd, maxOutputBytes: 1000 },
  );
  if (ancestor.error || ancestor.exitCode !== 0) {
    throw new AgentHubError(
      "LIVE_STATE_INCONSISTENT",
      `live ref ${ref} does not descend from base ${base}; the chain is not the recorded lineage`,
    );
  }

  const log = await runGit(
    repositoryCwd,
    ["log", "--reverse", "--format=%H%x09%T%x09%P%x09%aI%x09%s", `${base}..${ref}`],
    1_000_000,
  );
  const baseTree = (await runGit(repositoryCwd, ["rev-parse", `${base}^{tree}`], 1000)).stdout.trim();

  const chain: LiveCheckpoint[] = [];
  let previousTree = baseTree;
  let seq = 1;
  for (const line of log.stdout.split("\n").filter(Boolean)) {
    const [commit, tree, parents, takenAt, ...messageParts] = line.split("\t");
    const parentList = (parents ?? "").split(" ").filter(Boolean);
    if (parentList.length !== 1) {
      throw new AgentHubError(
        "LIVE_STATE_INCONSISTENT",
        `checkpoint commit ${commit} has ${parentList.length} parents; the live chain is linear by construction`,
      );
    }
    const reason = parseCheckpointReason(messageParts.join("\t") ?? "");
    if (reason === null) {
      throw new AgentHubError(
        "LIVE_STATE_INCONSISTENT",
        `checkpoint commit ${commit} carries a message no live checkpoint of this hub would have written`,
      );
    }
    chain.push({
      seq: seq,
      commit,
      parent: parentList[0],
      tree,
      empty: tree === previousTree,
      reason,
      taken_at: takenAt,
    });
    previousTree = tree;
    seq += 1;
  }
  return chain;
}

export interface LiveWorktreeRemoval {
  /** Worktree directory is gone and Git admin metadata was dropped. */
  removed: boolean;
  /** Present when forceful local cleanup was needed or failed. */
  cleanup_error: { code: string; message: string } | null;
}

/**
 * Remove a live worktree after its final checkpoint has landed. Refuses any
 * path outside the hub's own OS-temp prefix — recovery code reading a hostile
 * lease must not be able to rm arbitrary directories. Pruning is deliberately
 * NOT run here: it races with concurrent `worktree add` operations, so the
 * caller prunes once under the live-admin lock.
 */
export async function removeLiveWorktree(
  repositoryCwd: string,
  worktree: LiveWorktree,
): Promise<LiveWorktreeRemoval> {
  if (!isInside(resolve(tmpdir()), worktree.parentPath) || !worktree.parentPath.includes(LIVE_WORKTREE_PREFIX)) {
    return {
      removed: false,
      cleanup_error: new AgentHubError(
        "LIVE_WORKTREE_PATH_UNSAFE",
        `refusing to remove "${worktree.parentPath}": not under the hub's "${LIVE_WORKTREE_PREFIX}" temp namespace`,
      ),
    };
  }

  let cleanupError: { code: string; message: string } | null = null;
  try {
    await runGit(repositoryCwd, ["worktree", "remove", "--force", worktree.path]);
  } catch (error) {
    cleanupError = {
      code: "LIVE_WORKTREE_CLEANUP_FAILED",
      message: error instanceof Error ? error.message : String(error),
    };
    // Fall back to a local teardown; Git admin metadata gets pruned later.
    await rm(worktree.parentPath, { recursive: true, force: true });
  }

  await rm(worktree.parentPath, { recursive: true, force: true });
  return { removed: true, cleanup_error: cleanupError };
}

/** One prune pass per administrative window, under the live-admin lock. */
export async function pruneLiveWorktrees(repositoryCwd: string): Promise<void> {
  await runGit(repositoryCwd, ["worktree", "prune"]);
}
