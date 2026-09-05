import { spawn } from "node:child_process";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { hostname, tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";

import { describe, expect, it } from "vitest";

import { AgentHubError } from "../src/errors.js";
import { resolveRepositoryIdentity } from "../src/git.js";
import { acquireRepositoryLock, lockPathFor, type LockOwner } from "../src/locks.js";
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
  zeroOidFor,
  type SessionState,
  type SessionTransaction,
  type TransitionPhase,
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

/**
 * A repository whose object names are 64 hex wide. Everything in the state
 * layer that assumes a hash width has to survive here, and nothing about the
 * protocol is different apart from the width itself.
 */
async function createSha256Repository(): Promise<string> {
  const repository = await mkdtemp(join(tmpdir(), "agent-hub-sha256-"));
  await runGit(repository, ["init", "-q", "--object-format=sha256"]);
  await runGit(repository, ["config", "user.email", "agent-hub@example.test"]);
  await runGit(repository, ["config", "user.name", "Agent Hub Test"]);
  await writeFile(join(repository, "README.md"), "initial\n");
  await runGit(repository, ["add", "README.md"]);
  await runGit(repository, ["commit", "-qm", "initial"]);
  return repository;
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
    expectThrowCode(() => sessionPendingPath("/abs/common", "../escape"), "SESSION_ID_INVALID");
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
    const reclaimed = await withSessionLock({ commonDir, sessionId: SESSION_ID }, async () => "ran");
    expect(reclaimed.value).toBe("ran");
    expect(reclaimed.releaseError).toBeNull();

    await removeDirectory(repo);
  });

  it("keeps the completed operation when only the session lock release fails", async () => {
    // No record is ever planted: the injected release failure is the whole
    // subject, and a fake release cannot wedge anything real.
    const brokenRelease: typeof acquireRepositoryLock = async (options) => ({
      name: options.name,
      path: lockPathFor(options.commonDir, options.name),
      release: async () => {
        throw new Error("simulated session lock release failure");
      },
    });

    const ran = await withSessionLock(
      { commonDir: "/abs/common", sessionId: SESSION_ID, acquireLock: brokenRelease },
      async () => "ran",
    );
    // The completed outcome survives the failed release; only evidence rides in.
    expect(ran.value).toBe("ran");
    expect(ran.releaseError).toMatchObject({
      code: "LOCK_RELEASE_FAILED",
      message: expect.stringContaining("simulated session lock release failure"),
    });

    // Operation failures still propagate — with the release failure appended,
    // never replacing it and never dropping it.
    await expect(
      withSessionLock(
        { commonDir: "/abs/common", sessionId: SESSION_ID, acquireLock: brokenRelease },
        async () => {
          throw new AgentHubError("SESSION_STATE_INCONSISTENT", "operation failed");
        },
      ),
    ).rejects.toMatchObject({
      code: "SESSION_STATE_INCONSISTENT",
      message: expect.stringContaining("session lock could not be released"),
    });
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

  it("derives the zero OID at the object-name width of the commit being written", () => {
    expect(zeroOidFor("f".repeat(40))).toBe("0".repeat(40));
    expect(zeroOidFor("f".repeat(64))).toBe("0".repeat(64));
    // Anything between the two real widths, or outside both, is not a commit
    // id, so its zero OID cannot be derived and the caller is refused.
    for (const notACommitId of [
      "",
      "f".repeat(39),
      "f".repeat(41),
      "f".repeat(63),
      "f".repeat(65),
      "F".repeat(40),
    ]) {
      expectThrowCode(() => zeroOidFor(notACommitId), "SESSION_STATE_INCONSISTENT");
    }
  });

  it("refuses a transition whose new commit id is not a full object name", async () => {
    const repo = await createGitRepository();
    try {
      const { common_dir: commonDir, head } = await resolveRepositoryIdentity(repo);
      // 41 hex: one character past SHA-1 and one short of SHA-256. Git has no
      // object name of that width, so the plan must die at validation instead
      // of putting a value no repository can compare into the CAS.
      const padded = `${head}f`;
      expect(padded).toHaveLength(41);
      const phases: TransitionPhase[] = [];
      await expectErrorCode(
        applySessionTransition(
          {
            commonDir,
            repositoryCwd: repo,
            observePhase: async (phase) => {
              phases.push(phase);
            },
          },
          planFor("create", null, padded, stateFor(repo, padded)),
        ),
        "SESSION_STATE_INCONSISTENT",
      );
      // Turned away before the transaction opened: no sidecar phase, no ref,
      // no state record.
      expect(phases).toEqual([]);
      expect(await refSha(repo, sessionRefFor(SESSION_ID))).toBeNull();
      expect(await exists(sessionStatePath(commonDir, SESSION_ID))).toBe(false);
      expect(await exists(sessionPendingPath(commonDir, SESSION_ID))).toBe(false);
    } finally {
      await removeDirectory(repo);
    }
  });

  it("refuses to load a persisted record carrying an out-of-width commit id", async () => {
    const repo = await createGitRepository();
    try {
      const { common_dir: commonDir, head } = await resolveRepositoryIdentity(repo);
      const commit = await artifactCommit(repo, head, "artifact");
      const created = stateFor(repo, head, { current_commit: commit });
      await applySessionTransition(
        { commonDir, repositoryCwd: repo },
        planFor("create", null, commit, created),
      );

      const statePath = sessionStatePath(commonDir, SESSION_ID);

      // identity.head is a field the session ref can never contradict, so only
      // the width rule alone can reject a padded value there, and a padded
      // value is exactly what a truncated write or a hand-edited record gives.
      await writeFile(
        statePath,
        JSON.stringify({ ...created, identity: { ...created.identity, head: `${head}0` } }),
      );
      await expectErrorCode(
        loadSessionState({ commonDir, repositoryCwd: repo, sessionId: SESSION_ID }),
        "SESSION_STATE_INCONSISTENT",
      );

      // The same rule rejects a padded current_commit, whatever the ref holds.
      await writeFile(statePath, JSON.stringify({ ...created, current_commit: `${commit}0` }));
      await expectErrorCode(
        loadSessionState({ commonDir, repositoryCwd: repo, sessionId: SESSION_ID }),
        "SESSION_STATE_INCONSISTENT",
      );

      // An unloadable record does not take the ref with it.
      expect(await refSha(repo, created.ref)).toBe(commit);
    } finally {
      await removeDirectory(repo);
    }
  });

  it("creates and advances a session in a SHA-256 repository", async () => {
    // The regression the derived zero OID exists for: a hard-coded 40-zero
    // "must not exist yet" value is refused outright by Git in this repository
    // ("not a valid old SHA1"), which failed every session create here.
    const repo = await createSha256Repository();
    try {
      const { common_dir: commonDir, head } = await resolveRepositoryIdentity(repo);
      expect(head).toMatch(/^[0-9a-f]{64}$/);

      const commit = await artifactCommit(repo, head, "sha256 artifact");
      expect(commit).toHaveLength(64);
      const created = stateFor(repo, head, { current_commit: commit });
      await applySessionTransition(
        { commonDir, repositoryCwd: repo },
        planFor("create", null, commit, created),
      );
      expect(await refSha(repo, created.ref)).toBe(commit);
      expect(await readStateFile(commonDir, SESSION_ID)).toMatchObject({
        revision: 1,
        current_commit: commit,
      });

      // The advance path passes the real old value, so it rides the same width.
      const next = await artifactCommit(repo, commit, "sha256 advance");
      await applySessionTransition(
        { commonDir, repositoryCwd: repo },
        planFor("advance", commit, next, stateFor(repo, head, { current_commit: next, revision: 2 })),
      );
      expect(await refSha(repo, created.ref)).toBe(next);
      expect(await readStateFile(commonDir, SESSION_ID)).toMatchObject({
        revision: 2,
        current_commit: next,
      });
      expect(await exists(sessionPendingPath(commonDir, SESSION_ID))).toBe(false);
    } finally {
      await removeDirectory(repo);
    }
  });
});
