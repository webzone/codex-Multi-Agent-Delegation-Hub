import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";

import { describe, expect, it } from "vitest";

import { AgentHubError } from "../src/errors.js";
import {
  captureLiveCheckpoint,
  createLiveWorktree,
  describeLiveCheckpointChain,
  inspectLiveWorktree,
  liveCheckpointMessage,
  pruneLiveWorktrees,
  removeLiveWorktree,
} from "../src/live/worktree.js";
import type { CheckpointReason } from "../src/live/types.js";
import { createGitRepository, removeDirectory, runGit } from "./helpers.js";

const COMMIT_PATTERN = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;

async function expectCode(promise: Promise<unknown>, code: string): Promise<void> {
  await expect(promise).rejects.toMatchObject({ code });
}

describe("live worktree lifecycle", () => {
  it("creates a persistent OS-temp detached worktree at the base", async () => {
    const repository = await createGitRepository();
    const base = (await runGit(repository, ["rev-parse", "HEAD"])).trim();
    const tmpRoot = await mkdtemp(join(tmpdir(), "agent-hub-live-test-root-"));

    const worktree = await createLiveWorktree(repository, base, tmpRoot);
    expect(isAbsolute(worktree.path)).toBe(true);
    expect(worktree.path.startsWith(tmpRoot)).toBe(true);
    expect((await runGit(worktree.path, ["rev-parse", "HEAD"])).trim()).toBe(base);
    const listed = await runGit(repository, ["worktree", "list", "--porcelain"]);
    expect(listed).toContain(worktree.path);

    const inspect = await inspectLiveWorktree(repository, worktree.path);
    expect(inspect.head).toBe(base);

    const removal = await removeLiveWorktree(repository, worktree);
    expect(removal.removed).toBe(true);
    expect(removal.cleanup_error).toBeNull();
    await pruneLiveWorktrees(repository);
    await removeDirectory(repository);
    await removeDirectory(tmpRoot);
  });

  it("reports missing and foreign worktrees instead of recreating them", async () => {
    const repository = await createGitRepository();
    const other = await createGitRepository();
    const base = (await runGit(repository, ["rev-parse", "HEAD"])).trim();
    const tmpRoot = await mkdtemp(join(tmpdir(), "agent-hub-live-test-root-"));

    const worktree = await createLiveWorktree(repository, base, tmpRoot);
    await expectCode(inspectLiveWorktree(other, worktree.path), "LIVE_WORKTREE_FOREIGN");

    await rm(worktree.parentPath, { recursive: true, force: true });
    await expectCode(inspectLiveWorktree(repository, worktree.path), "LIVE_WORKTREE_MISSING");

    await removeDirectory(repository);
    await removeDirectory(other);
    await removeDirectory(tmpRoot);
  });

  it("refuses to remove paths outside its own temp namespace", async () => {
    const repository = await createGitRepository();
    const outside = await mkdtemp(join(tmpdir(), "agent-hub-someone-else-"));
    const victim = join(outside, "worktree");
    await mkdir(victim);

    const removal = await removeLiveWorktree(repository, { path: victim, parentPath: outside, base: "" });
    expect(removal.removed).toBe(false);
    expect(removal.cleanup_error?.code).toBe("LIVE_WORKTREE_PATH_UNSAFE");

    // The untouched directory still exists: refusal means refusal.
    await writeFile(join(victim, "keep.txt"), "safe\n");
    await removeDirectory(repository);
    await removeDirectory(outside);
  });
});

describe("live checkpoint chain", () => {
  it("pins changed worktree state as a hook-free commit on the chain", async () => {
    const repository = await createGitRepository();
    const base = (await runGit(repository, ["rev-parse", "HEAD"])).trim();
    const tmpRoot = await mkdtemp(join(tmpdir(), "agent-hub-live-test-root-"));
    const worktree = await createLiveWorktree(repository, base, tmpRoot);
    let clock = new Date("2026-09-05T10:00:00.000Z").getTime();
    const now = () => new Date(clock);

    await writeFile(join(worktree.path, "feature.ts"), "export const answer = 42;\n");
    const first = await captureLiveCheckpoint(worktree.path, base, "turn_end", { seq: 1, now });
    expect(first.advanced).toBe(true);
    clock += 60_000;
    if (!first.advanced) {
      expect.unreachable("changed worktree must advance the chain");
    }
    expect(first.checkpoint.commit).toMatch(COMMIT_PATTERN);
    expect(first.checkpoint.parent).toBe(base);
    expect(first.checkpoint.empty).toBe(false);
    expect(first.checkpoint.reason).toBe("turn_end");
    // Fixed hub identity; no user text anywhere in the commit object.
    const subject = (await runGit(repository, ["log", "-1", "--format=%s", first.checkpoint.commit])).trim();
    expect(subject).toBe(liveCheckpointMessage("turn_end"));
    const author = (await runGit(repository, ["log", "-1", "--format=%an <%ae>", first.checkpoint.commit])).trim();
    expect(author).toBe("Agent Hub <agent-hub@localhost>");

    // Nothing changed: the observation is honest and pins nothing.
    const empty = await captureLiveCheckpoint(worktree.path, first.checkpoint.commit, "requested", {
      seq: 2,
      now,
    });
    expect(empty.advanced).toBe(false);

    clock += 60_000;
    await writeFile(join(worktree.path, "second.ts"), "// second\n");
    const second = await captureLiveCheckpoint(worktree.path, first.checkpoint.commit, "requested", {
      seq: 2,
      now,
    });
    expect(second.advanced).toBe(true);
    clock += 60_000;

    // Publish the chain on a live ref and rebuild it from Git objects only.
    if (!second.advanced) {
      expect.unreachable("changed worktree must advance the chain");
    }
    await runGit(repository, ["update-ref", "refs/agent-hub/live/test-chain", second.checkpoint.commit]);
    const chain = await describeLiveCheckpointChain(repository, "refs/agent-hub/live/test-chain", base);
    expect(chain.map((c) => [c.seq, c.reason, c.empty, c.parent])).toEqual([
      [1, "turn_end", false, base],
      [2, "requested", false, first.checkpoint.commit],
    ]);
    expect(new Date(chain[1].taken_at).toISOString()).toBe(
      new Date("2026-09-05T10:02:00.000Z").toISOString(),
    );

    await removeLiveWorktree(repository, worktree);
    await removeDirectory(repository);
    await removeDirectory(tmpRoot);
  });

  it("captures untracked and nested changes with full working state", async () => {
    const repository = await createGitRepository();
    const base = (await runGit(repository, ["rev-parse", "HEAD"])).trim();
    const tmpRoot = await mkdtemp(join(tmpdir(), "agent-hub-live-test-root-"));
    const worktree = await createLiveWorktree(repository, base, tmpRoot);

    await mkdir(join(worktree.path, "src", "nested"), { recursive: true });
    await writeFile(join(worktree.path, "src", "nested", "deep.txt"), "deep\n");
    await writeFile(join(worktree.path, "README.md"), "modified tracked file\n");
    const capture = await captureLiveCheckpoint(worktree.path, base, "cancel", {
      seq: 1,
      now: () => new Date("2026-09-05T11:00:00.000Z"),
    });
    expect(capture.advanced).toBe(true);
    if (!capture.advanced) {
      expect.unreachable();
    }
    const files = (await runGit(repository, ["ls-tree", "-r", "--name-only", capture.checkpoint.tree]))
      .trim()
      .split("\n");
    expect(files).toContain("src/nested/deep.txt");
    expect((await runGit(repository, ["show", `${capture.checkpoint.commit}:README.md`])).trim()).toBe(
      "modified tracked file",
    );

    await removeLiveWorktree(repository, worktree);
    await removeDirectory(repository);
    await removeDirectory(tmpRoot);
  });

  it("rejects chains that violate their own construction", async () => {
    const repository = await createGitRepository();
    const base = (await runGit(repository, ["rev-parse", "HEAD"])).trim();
    const tree = (await runGit(repository, ["rev-parse", `${base}^{tree}`])).trim();
    const a = (await runGit(repository, ["commit-tree", tree, "-p", base, "-m", liveCheckpointMessage("close")])).trim();

    // Foreign commit message on the chain.
    await runGit(repository, ["update-ref", "refs/agent-hub/live/bad-msg", a]);
    const foreign = (await runGit(repository, ["commit-tree", tree, "-p", a, "-m", "random commit"])).trim();
    await runGit(repository, ["update-ref", "refs/agent-hub/live/bad-msg", foreign]);
    await expectCode(
      describeLiveCheckpointChain(repository, "refs/agent-hub/live/bad-msg", base),
      "LIVE_STATE_INCONSISTENT",
    );

    // A merge commit breaks the linear-chain invariant.
    const side = (await runGit(repository, ["commit-tree", tree, "-p", base, "-m", liveCheckpointMessage("error")])).trim();
    const merged = (
      await runGit(repository, ["commit-tree", tree, "-p", a, "-p", side, "-m", liveCheckpointMessage("close")])
    ).trim();
    await runGit(repository, ["update-ref", "refs/agent-hub/live/merged", merged]);
    await expectCode(
      describeLiveCheckpointChain(repository, "refs/agent-hub/live/merged", base),
      "LIVE_STATE_INCONSISTENT",
    );

    // A ref that does not descend from the recorded base is not the lineage.
    const otherRoot = (await runGit(repository, ["commit-tree", tree, "-m", "unrelated root"])).trim();
    await runGit(repository, ["update-ref", "refs/agent-hub/live/foreign-line", otherRoot]);
    await expectCode(
      describeLiveCheckpointChain(repository, "refs/agent-hub/live/foreign-line", base),
      "LIVE_STATE_INCONSISTENT",
    );

    await removeDirectory(repository);
  });
});

describe("checkpoint reasons", () => {
  it("round-trips every hub constant message", () => {
    const reasons: CheckpointReason[] = [
      "turn_end",
      "requested",
      "cancel",
      "close",
      "error",
      "crash_recovery",
    ];
    for (const reason of reasons) {
      const message = liveCheckpointMessage(reason);
      expect(message.startsWith("Agent Hub live checkpoint (")).toBe(true);
      expect(message).not.toMatch(/[a-z]+task|[Aa]gent prompt/);
    }
  });
});

describe("AgentHubError surface", () => {
  it("keeps error codes on thrown probes", async () => {
    const repository = await createGitRepository();
    const error = await inspectLiveWorktree(repository, join(tmpdir(), "does-not-exist-live")).catch((e) => e);
    expect(error).toBeInstanceOf(AgentHubError);
    await removeDirectory(repository);
  });
});
