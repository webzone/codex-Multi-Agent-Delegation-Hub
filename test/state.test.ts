import { spawn } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { hostname, tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";

import { describe, expect, it } from "vitest";

import { AgentHubError } from "../src/errors.js";
import { resolveRepositoryIdentity } from "../src/git.js";
import { lockPathFor, type LockOwner } from "../src/locks.js";
import { deferred } from "../src/deferred.js";
import {
  applySessionTransition,
  loadSessionState,
  sessionLockName,
  sessionPendingPath,
  sessionRefFor,
  sessionStatePath,
  sessionsRoot,
  withSessionLock,
  type SessionState,
  type SessionTransaction,
} from "../src/state.js";
import { createGitRepository, removeDirectory, runGit } from "./helpers.js";

const SESSION_ID = "11111111-2222-4333-8444-555555555555";

async function expectErrorCode(promise: Promise<unknown>, code: string): Promise<void> {
  await expect(promise).rejects.toMatchObject({ code });
}

function expectThrowCode(run: () => unknown, code: string): void {
  try {
    run();
  } catch (error) {
    expect((error as AgentHubError).code).toBe(code);
    return;
  }
  expect.unreachable(`expected ${code} to be thrown`);
}

/** Real hook-free commit one past `parent`; update-ref only accepts objects. */
async function artifactCommit(repo: string, parent: string, message: string): Promise<string> {
  const tree = (await runGit(repo, ["rev-parse", `${parent}^{tree}`])).trim();
  const out = await runGit(repo, ["commit-tree", tree, "-p", parent, "-m", message]);
  return out.trim();
}

function stateFor(repo: string, head: string, over: Partial<SessionState> = {}): SessionState {
  const base: SessionState = {
    schema: 1,
    session_id: SESSION_ID,
    agent: "fake",
    ref: sessionRefFor(SESSION_ID),
    identity: {
      common_dir: join(repo, ".git"),
      worktree_root: repo,
      branch: "main",
      head,
    },
    base_commit: head,
    current_commit: head,
    provider_session_id: null,
    continuation_mode: "filesystem",
    revision: 1,
    created_at: "2026-01-01T00:00:00.000Z",
    updated_at: "2026-01-01T00:00:00.000Z",
  };
  return { ...base, ...over };
}

function planFor(
  kind: SessionTransaction["kind"],
  expectedRef: string | null,
  newCommit: string,
  next: SessionState,
): SessionTransaction {
  return {
    kind,
    session_id: SESSION_ID,
    ref: next.ref,
    expected_ref: expectedRef,
    new_commit: newCommit,
    next_state: next,
  };
}

function owner(pid: number): LockOwner {
  return {
    token: "planted-token",
    pid,
    hostname: hostname(),
    started_at: "2026-01-01T00:00:00.000Z",
  };
}

async function plantSessionLock(commonDir: string, record: LockOwner): Promise<string> {
  const lockPath = lockPathFor(commonDir, sessionLockName(SESSION_ID));
  await mkdir(lockPath, { recursive: true });
  await writeFile(join(lockPath, "owner.json"), JSON.stringify(record));
  return lockPath;
}

async function deadPid(): Promise<number> {
  const child = spawn(process.execPath, ["-e", "process.exit(0)"]);
  const pid = child.pid as number;
  const { promise, resolve } = deferred<void>();
  child.once("exit", resolve);
  await promise;
  return pid;
}

/** The child's own timer keeps it alive; the test itself awaits only events. */
async function livePid(): Promise<{ pid: number; stop: () => void }> {
  const child = spawn(process.execPath, ["-e", "setTimeout(() => {}, 60_000)"]);
  const pid = child.pid as number;
  const { promise, resolve } = deferred<void>();
  child.once("spawn", resolve);
  await promise;
  return { pid, stop: () => child.kill("SIGKILL") };
}

async function refSha(repo: string, ref: string): Promise<string | null> {
  try {
    return (await runGit(repo, ["rev-parse", "--verify", "--quiet", ref])).trim() || null;
  } catch {
    return null;
  }
}

async function exists(path: string): Promise<boolean> {
  try {
    await readFile(path);
    return true;
  } catch {
    return false;
  }
}

async function readStateFile(commonDir: string, sessionId: string): Promise<Record<string, unknown>> {
  return JSON.parse(await readFile(sessionStatePath(commonDir, sessionId), "utf8")) as Record<string, unknown>;
}

describe("session state layer", { timeout: 20_000 }, () => {
  it("roots state in the absolute common dir, shared across linked worktrees, without dirtying checkouts", async () => {
    const repo = await createGitRepository();
    const linkRoot = await mkdtemp(join(tmpdir(), "agent-hub-link-"));
    const linkWorktree = join(linkRoot, "wt");
    await runGit(repo, ["worktree", "add", "--detach", linkWorktree, "HEAD"]);

    const main = await resolveRepositoryIdentity(repo);
    const linked = await resolveRepositoryIdentity(linkWorktree);
    expect(isAbsolute(main.common_dir)).toBe(true);
    expect(linked.common_dir).toBe(main.common_dir);
    expect(sessionsRoot(main.common_dir)).toBe(join(main.common_dir, "agent-hub", "sessions"));
    expectThrowCode(() => sessionsRoot("relative/.git"), "SESSION_STATE_ROOT_INVALID");

    const head = main.head;
    const commit = await artifactCommit(repo, head, "session artifact 1");
    const next = stateFor(repo, head, { current_commit: commit });
    // Committing through the linked worktree lands in the shared common dir.
    await applySessionTransition(
      { commonDir: linked.common_dir, repositoryCwd: linkWorktree },
      planFor("create", null, commit, next),
    );

    expect(await refSha(repo, next.ref)).toBe(commit);
    expect(await exists(sessionStatePath(main.common_dir, SESSION_ID))).toBe(true);
    expect(await runGit(repo, ["status", "--porcelain=v1", "--untracked-files=all"])).toBe("");
    expect(await runGit(linkWorktree, ["status", "--porcelain=v1", "--untracked-files=all"])).toBe("");

    await removeDirectory(linkRoot);
    await removeDirectory(repo);
  });

  it("refuses non-generated ids before any ref or path is derived", () => {
    for (const bad of [
      "",
      "evil",
      "../escape",
      "refs/agent-hub/sessions/x",
      "TASK-11111111-2222-4333-8444-555555555555",
      "11111111-2222-4333-8444-555555555555 ",
      "111111112222433384445555555555555555",
    ]) {
      expectThrowCode(() => sessionRefFor(bad), "SESSION_ID_INVALID");
    }
    expect(sessionRefFor(SESSION_ID)).toBe(`refs/agent-hub/sessions/${SESSION_ID}`);
    expectThrowCode(() => sessionStatePath("/abs/common", "../escape"), "SESSION_ID_INVALID");
    expectThrowCode(() => sessionLockName("foo/bar"), "SESSION_ID_INVALID");
    expect(sessionLockName(SESSION_ID)).toBe(`session-${SESSION_ID}`);
  });

  it("serializes per-session work: SESSION_BUSY for live owners, reclaims dead same-host owners", async () => {
    const repo = await createGitRepository();
    const { common_dir: commonDir } = await resolveRepositoryIdentity(repo);

    const live = await livePid();
    try {
      const lockPath = await plantSessionLock(commonDir, owner(live.pid));
      await expectErrorCode(
        withSessionLock({ commonDir, sessionId: SESSION_ID }, async () => "ran"),
        "SESSION_BUSY",
      );
      // The live lock is never stolen: it must still exist with its owner.
      expect(await exists(join(lockPath, "owner.json"))).toBe(true);
    } finally {
      live.stop();
    }

    // A demonstrably dead same-host owner is reclaimed (Package 1 semantics).
    const dead = await deadPid();
    await plantSessionLock(commonDir, owner(dead));
    const ran = await withSessionLock({ commonDir, sessionId: SESSION_ID }, async () => "ran");
    expect(ran).toBe("ran");

    await removeDirectory(repo);
  });

  it("creates ref + state together and rejects an id whose ref already exists", async () => {
    const repo = await createGitRepository();
    const { common_dir: commonDir, head } = await resolveRepositoryIdentity(repo);
    const commit = await artifactCommit(repo, head, "first artifact");
    const next = stateFor(repo, head, { current_commit: commit });

    await applySessionTransition(
      { commonDir, repositoryCwd: repo },
      planFor("create", null, commit, next),
    );
    expect(await refSha(repo, next.ref)).toBe(commit);
    expect(await readStateFile(commonDir, SESSION_ID)).toMatchObject({ revision: 1, current_commit: commit });
    expect(await exists(sessionPendingPath(commonDir, SESSION_ID))).toBe(false);
    await expect(
      loadSessionState({ commonDir, repositoryCwd: repo, sessionId: SESSION_ID }),
    ).resolves.toEqual(next);

    // A second create over the same id must not overwrite anything.
    const other = await artifactCommit(repo, head, "other artifact");
    await expectErrorCode(
      applySessionTransition(
        { commonDir, repositoryCwd: repo },
        planFor("create", null, other, stateFor(repo, head, { current_commit: other })),
      ),
      "SESSION_STATE_INCONSISTENT",
    );
    expect(await refSha(repo, next.ref)).toBe(commit);

    await removeDirectory(repo);
  });

  it("CAS-refuses advances whose expected ref state has moved, before and mid-transaction", async () => {
    const repo = await createGitRepository();
    const { common_dir: commonDir, head } = await resolveRepositoryIdentity(repo);
    const c0 = await artifactCommit(repo, head, "artifact 0");
    const c1 = await artifactCommit(repo, c0, "artifact 1");
    const c2 = await artifactCommit(repo, c0, "artifact 2");
    await applySessionTransition(
      { commonDir, repositoryCwd: repo },
      planFor("create", null, c0, stateFor(repo, head, { current_commit: c0 })),
    );

    const ref = sessionRefFor(SESSION_ID);
    const advance = (from: string, to: string, revision: number): SessionTransaction =>
      planFor("advance", from, to, stateFor(repo, head, { current_commit: to, revision }));

    // Stale expectation caught by the pre-check: nothing is written.
    const stale = await artifactCommit(repo, head, "stale plan parent");
    await expectErrorCode(
      applySessionTransition({ commonDir, repositoryCwd: repo }, advance(stale, c1, 2)),
      "SESSION_STATE_INCONSISTENT",
    );
    expect(await exists(sessionPendingPath(commonDir, SESSION_ID))).toBe(false);

    // Genuine mid-transaction race: the ref moves after the sidecar is written,
    // so the CAS itself rejects and the sidecar is cleaned up.
    await expectErrorCode(
      applySessionTransition(
        {
          commonDir,
          repositoryCwd: repo,
          observePhase: async (phase) => {
            if (phase === "sidecar-written") {
              await runGit(repo, ["update-ref", ref, c2, c0]);
            }
          },
        },
        advance(c0, c1, 2),
      ),
      "SESSION_STATE_INCONSISTENT",
    );
    expect(await exists(sessionPendingPath(commonDir, SESSION_ID))).toBe(false);
    expect(await refSha(repo, ref)).toBe(c2);
    expect((await readStateFile(commonDir, SESSION_ID))["current_commit"]).toBe(c0);

    await removeDirectory(repo);
  });

  it("recovers an aborted transaction: sidecar + old ref + old state proves no-op", async () => {
    const repo = await createGitRepository();
    const { common_dir: commonDir, head } = await resolveRepositoryIdentity(repo);
    const c0 = await artifactCommit(repo, head, "artifact 0");
    const c1 = await artifactCommit(repo, c0, "artifact 1");
    await applySessionTransition(
      { commonDir, repositoryCwd: repo },
      planFor("create", null, c0, stateFor(repo, head, { current_commit: c0 })),
    );

    // Crash exactly after the sidecar write, before the ref CAS: the
    // transition rethrows the original failure untouched.
    await expect(
      applySessionTransition(
        {
          commonDir,
          repositoryCwd: repo,
          observePhase: async (phase) => {
            if (phase === "sidecar-written") {
              throw new Error("simulated crash");
            }
          },
        },
        planFor(
          "advance",
          c0,
          c1,
          stateFor(repo, head, { current_commit: c1, revision: 2, updated_at: "2026-01-02T00:00:00.000Z" }),
        ),
      ),
    ).rejects.toThrow("simulated crash");
    expect(await exists(sessionPendingPath(commonDir, SESSION_ID))).toBe(true);

    const loaded = await loadSessionState({ commonDir, repositoryCwd: repo, sessionId: SESSION_ID });
    expect(loaded.current_commit).toBe(c0);
    expect(loaded.revision).toBe(1);
    expect(await exists(sessionPendingPath(commonDir, SESSION_ID))).toBe(false);

    await removeDirectory(repo);
  });

  it("recovers a committed transaction: sidecar + new ref proves the post-state", async () => {
    const repo = await createGitRepository();
    const { common_dir: commonDir, head } = await resolveRepositoryIdentity(repo);
    const c0 = await artifactCommit(repo, head, "artifact 0");
    const c1 = await artifactCommit(repo, c0, "artifact 1");
    await applySessionTransition(
      { commonDir, repositoryCwd: repo },
      planFor("create", null, c0, stateFor(repo, head, { current_commit: c0 })),
    );

    const next = stateFor(repo, head, {
      current_commit: c1,
      revision: 2,
      updated_at: "2026-01-02T00:00:00.000Z",
      provider_session_id: "prov-7",
    });
    // Crash exactly after the ref CAS, before the state JSON write.
    await expect(
      applySessionTransition(
        {
          commonDir,
          repositoryCwd: repo,
          observePhase: async (phase) => {
            if (phase === "ref-updated") {
              throw new Error("simulated crash");
            }
          },
        },
        planFor("advance", c0, c1, next),
      ),
    ).rejects.toThrow("simulated crash");
    expect(await exists(sessionPendingPath(commonDir, SESSION_ID))).toBe(true);
    expect(await refSha(repo, next.ref)).toBe(c1);

    const loaded = await loadSessionState({ commonDir, repositoryCwd: repo, sessionId: SESSION_ID });
    expect(loaded).toEqual(next);
    expect(await readStateFile(commonDir, SESSION_ID)).toMatchObject({ revision: 2, provider_session_id: "prov-7" });
    expect(await exists(sessionPendingPath(commonDir, SESSION_ID))).toBe(false);

    await removeDirectory(repo);
  });

  it("reports explicit inconsistency when the sidecar proves nothing", async () => {
    const repo = await createGitRepository();
    const { common_dir: commonDir, head } = await resolveRepositoryIdentity(repo);
    const c0 = await artifactCommit(repo, head, "artifact 0");
    const c1 = await artifactCommit(repo, c0, "artifact 1");
    const c2 = await artifactCommit(repo, c0, "artifact 2");
    await applySessionTransition(
      { commonDir, repositoryCwd: repo },
      planFor("create", null, c0, stateFor(repo, head, { current_commit: c0 })),
    );

    // Sidecar left behind, ref moved to a third commit: neither outcome holds.
    const pending = sessionPendingPath(commonDir, SESSION_ID);
    await writeFile(
      pending,
      JSON.stringify(planFor("advance", c0, c1, stateFor(repo, head, { current_commit: c1, revision: 2 })), null, 2),
    );
    const ref = sessionRefFor(SESSION_ID);
    await runGit(repo, ["update-ref", ref, c2, c0]);
    await expectErrorCode(
      loadSessionState({ commonDir, repositoryCwd: repo, sessionId: SESSION_ID }),
      "SESSION_STATE_INCONSISTENT",
    );
    // Unprovable sidecars stay on disk for audit.
    expect(await exists(pending)).toBe(true);

    await writeFile(pending, "{ not json");
    await expectErrorCode(
      loadSessionState({ commonDir, repositoryCwd: repo, sessionId: SESSION_ID }),
      "SESSION_STATE_INCONSISTENT",
    );
    await rm(pending, { force: true });

    // Without a sidecar, a moved ref alone is an explicit inconsistency.
    await expectErrorCode(
      loadSessionState({ commonDir, repositoryCwd: repo, sessionId: SESSION_ID }),
      "SESSION_STATE_INCONSISTENT",
    );

    await writeFile(sessionStatePath(commonDir, SESSION_ID), "{corrupt");
    await expectErrorCode(
      loadSessionState({ commonDir, repositoryCwd: repo, sessionId: SESSION_ID }),
      "SESSION_STATE_INCONSISTENT",
    );
    await runGit(repo, ["update-ref", ref, c0, c2]);
    expect(await refSha(repo, ref)).toBe(c0);

    await removeDirectory(repo);
  });

  it("distinguishes unknown sessions from inconsistent ones and keeps records minimal", async () => {
    const repo = await createGitRepository();
    const { common_dir: commonDir, head } = await resolveRepositoryIdentity(repo);
    await expectErrorCode(
      loadSessionState({ commonDir, repositoryCwd: repo, sessionId: SESSION_ID }),
      "SESSION_NOT_FOUND",
    );

    const commit = await artifactCommit(repo, head, "artifact");
    const next = stateFor(repo, head, { current_commit: commit });
    await applySessionTransition({ commonDir, repositoryCwd: repo }, planFor("create", null, commit, next));

    const record = await readStateFile(commonDir, SESSION_ID);
    expect(Object.keys(record).sort()).toEqual([
      "agent",
      "base_commit",
      "continuation_mode",
      "created_at",
      "current_commit",
      "identity",
      "provider_session_id",
      "ref",
      "revision",
      "schema",
      "session_id",
      "updated_at",
    ]);
    // The identity sub-record carries only the RepositoryIdentity contract.
    expect(Object.keys((record["identity"] ?? {}) as object).sort()).toEqual([
      "branch",
      "common_dir",
      "head",
      "worktree_root",
    ]);

    await removeDirectory(repo);
  });
});
