import { spawn, type ChildProcess } from "node:child_process";
import { existsSync, watch } from "node:fs";
import { access, mkdir, mkdtemp, readdir, readFile, utimes, writeFile } from "node:fs/promises";
import { hostname, tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";

import { describe, expect, it } from "vitest";

import { AgentHubError } from "../src/errors.js";
import {
  acquireRepositoryLock,
  claimUnderLock,
  lockPathFor,
  type LockOwner,
  type RepositoryLock,
} from "../src/locks.js";
import { deferred } from "../src/deferred.js";

async function fakeCommonDir(): Promise<string> {
  return mkdtemp(join(tmpdir(), "agent-hub-lock-test-"));
}

async function plantLock(commonDir: string, name: string, owner: LockOwner | null): Promise<string> {
  const lockPath = lockPathFor(commonDir, name);
  await mkdir(lockPath, { recursive: true });
  if (owner) {
    await writeFile(join(lockPath, "owner.json"), JSON.stringify(owner));
  }
  return lockPath;
}

function owner(patch: Partial<LockOwner> = {}): LockOwner {
  return {
    token: "planted-token",
    pid: process.pid,
    hostname: hostname(),
    started_at: new Date(0).toISOString(),
    ...patch,
  };
}

async function expectErrorCode(promise: Promise<unknown>, code: string): Promise<void> {
  await expect(promise).rejects.toMatchObject({ code });
}

async function readPlantedOwner(lockPath: string): Promise<LockOwner> {
  return JSON.parse(await readFile(join(lockPath, "owner.json"), "utf8")) as LockOwner;
}

async function deadPid(): Promise<number> {
  const child = spawn(process.execPath, ["-e", "process.exit(0)"]);
  const pid = child.pid as number;
  const { promise, resolve } = deferred<void>();
  child.once("exit", resolve);
  await promise;
  return pid;
}

describe("repository-local lock", () => {
  it("acquires exclusively, records owner metadata, and allows re-acquire after release", async () => {
    const commonDir = await fakeCommonDir();
    const first = await acquireRepositoryLock({ commonDir, name: "worktree-admin" });

    const lockPath = lockPathFor(commonDir, "worktree-admin");
    const stored = await readPlantedOwner(lockPath);
    expect(stored.token).toBe(first.token);
    expect(stored.pid).toBe(process.pid);
    expect(stored.hostname).toBe(hostname());

    await expectErrorCode(
      acquireRepositoryLock({ commonDir, name: "worktree-admin" }),
      "LOCK_BUSY",
    );

    await first.release();
    await expect(access(lockPath)).rejects.toThrow();

    const second = await acquireRepositoryLock({ commonDir, name: "worktree-admin" });
    expect(second.token).not.toBe(first.token);
    await second.release();
  });

  it("releases only for the recorded owner", async () => {
    const commonDir = await fakeCommonDir();
    const handle = await acquireRepositoryLock({ commonDir, name: "worktree-admin" });

    const lockPath = lockPathFor(commonDir, "worktree-admin");
    const stolen = await readPlantedOwner(lockPath);
    await writeFile(join(lockPath, "owner.json"), JSON.stringify({ ...stolen, token: "someone-else" }));

    await expectErrorCode(handle.release(), "LOCK_NOT_OWNER");
    await expect(access(lockPath)).resolves.toBeUndefined();
  });

  it("reclaims a lock whose same-host owner is demonstrably dead", async () => {
    const commonDir = await fakeCommonDir();
    const lockPath = await plantLock(commonDir, "worktree-admin", owner({ pid: await deadPid() }));

    const handle = await acquireRepositoryLock({ commonDir, name: "worktree-admin" });
    expect(handle.token).not.toBe("planted-token");
    const stored = await readPlantedOwner(lockPath);
    expect(stored.token).toBe(handle.token);
    await handle.release();
  });

  it("refuses to reclaim a live same-host owner", async () => {
    const commonDir = await fakeCommonDir();
    await plantLock(commonDir, "worktree-admin", owner({ pid: process.pid }));

    await expectErrorCode(
      acquireRepositoryLock({ commonDir, name: "worktree-admin" }),
      "LOCK_BUSY",
    );
  });

  it("refuses to reclaim an owner on a foreign host", async () => {
    const commonDir = await fakeCommonDir();
    await plantLock(commonDir, "worktree-admin", owner({ hostname: "not-this-host-9f3a" }));

    await expectErrorCode(
      acquireRepositoryLock({ commonDir, name: "worktree-admin" }),
      "LOCK_UNRECOVERABLE",
    );
  });

  it("treats missing or corrupt owner metadata as unrecoverable", async () => {
    const commonDir = await fakeCommonDir();
    await plantLock(commonDir, "empty-lock", null);
    await expectErrorCode(
      acquireRepositoryLock({ commonDir, name: "empty-lock" }),
      "LOCK_UNRECOVERABLE",
    );

    const corrupt = await plantLock(commonDir, "corrupt-lock", null);
    await writeFile(join(corrupt, "owner.json"), "this is not json {{{");
    await expectErrorCode(
      acquireRepositoryLock({ commonDir, name: "corrupt-lock" }),
      "LOCK_UNRECOVERABLE",
    );
  });

  it("rejects lock names that are not a single safe path segment", async () => {
    const commonDir = await fakeCommonDir();
    for (const name of ["../evil", "UPPER", "", "spaced name", "x".repeat(65)]) {
      await expectErrorCode(
        acquireRepositoryLock({ commonDir, name }),
        "LOCK_INVALID_NAME",
      );
    }
  });

  it("never reclaims an ownerless lock directory, however long it has sat there", async () => {
    const commonDir = await fakeCommonDir();
    const lockPath = await plantLock(commonDir, "worktree-admin", null);
    // Backdate it well past any crash-recovery window an age heuristic could use.
    const ancient = new Date(Date.now() - 60 * 60_000);
    await utimes(lockPath, ancient, ancient);

    await expectErrorCode(
      acquireRepositoryLock({ commonDir, name: "worktree-admin" }),
      "LOCK_UNRECOVERABLE",
    );

    // Left exactly as found: no owner file, no reclaim sidecar, no deletion.
    await expect(access(join(lockPath, "owner.json"))).rejects.toThrow();
    const root = join(commonDir, "agent-hub", "locks");
    expect((await readdir(root)).filter((entry) => entry.includes(".reclaim-"))).toEqual([]);
    expect(await readdir(root)).toEqual(["worktree-admin.lock"]);
  });

  it("cannot steal a lock from a creator stopped before it recorded an owner", async () => {
    const commonDir = await fakeCommonDir();
    const lockPath = lockPathFor(commonDir, "worktree-admin");
    const created = join(commonDir, "creator-created");
    const gate = join(commonDir, "creator-resume");
    const secondGate = join(commonDir, "creator-finish");

    // A real creator: exclusive mkdir, then stopped with no owner file — the
    // state an age heuristic cannot tell apart from a crash.
    const child = spawn(
      process.execPath,
      ["-e", PAUSED_CREATOR_SCRIPT, lockPath, created, gate, secondGate],
      { stdio: "ignore" },
    );
    try {
      await waitForFile(created);
      child.kill("SIGSTOP");
      // The pause outlives any crash-recovery grace an age heuristic could use:
      // the directory's mtime is now "ancient", indistinguishable from a crash.
      const ancient = new Date(Date.now() - 60 * 60_000);
      await utimes(lockPath, ancient, ancient);

      // Nothing may take the lock while its creator is merely stopped.
      await expectErrorCode(
        acquireRepositoryLock({ commonDir, name: "worktree-admin" }),
        "LOCK_UNRECOVERABLE",
      );
      await expect(access(lockPath)).resolves.toBeUndefined();
      await expect(access(join(lockPath, "owner.json"))).rejects.toThrow();

      // Let the creator finish: it records exactly one owner, and nobody else
      // ever held the lock alongside it.
      await writeFile(gate, "go");
      child.kill("SIGCONT");
      await waitForFile(join(lockPath, "owner.json"));

      const recorded = await readPlantedOwner(lockPath);
      expect(recorded.token).toBe("paused-creator-token");
      expect(recorded.pid).toBe(child.pid);
      await expectErrorCode(
        acquireRepositoryLock({ commonDir, name: "worktree-admin" }),
        "LOCK_BUSY",
      );
      // It was never moved aside, restored, or shadowed by a reclaim attempt.
      const root = join(commonDir, "agent-hub", "locks");
      expect((await readdir(root)).filter((entry) => entry.includes(".reclaim-"))).toEqual([]);
    } finally {
      await writeFile(gate, "go").catch(() => {});
      await writeFile(secondGate, "done").catch(() => {});
      child.kill("SIGCONT");
      await waitForExit(child);
    }
  });
});

/**
 * Child half of the paused-creator test: claim the directory exclusively,
 * announce itself, wait to be resumed, then record its own owner exactly once.
 * Joined into one line, so no `//` comments inside. With `-e`, `argv[1]` is
 * the first user argument.
 */
const PAUSED_CREATOR_SCRIPT = [
  "const fs=require('node:fs');",
  "const os=require('node:os');",
  "const [lockPath,created,gate,finish]=process.argv.slice(1);",
  "fs.mkdirSync(lockPath,{recursive:true});",
  "fs.writeFileSync(created,String(process.pid));",
  "const park=new Int32Array(new SharedArrayBuffer(4));",
  "const waitUntil=(p,ms)=>{const end=Date.now()+ms;while(!fs.existsSync(p)&&Date.now()<end){Atomics.wait(park,0,0,10);}return fs.existsSync(p);};",
  "if(!waitUntil(gate,30000)){process.exit(7);}",
  "fs.writeFileSync(require('node:path').join(lockPath,'owner.json'),JSON.stringify({token:'paused-creator-token',pid:process.pid,hostname:os.hostname(),started_at:new Date().toISOString()}));",
  "waitUntil(finish,10000);",
].join(" ");

/**
 * Waits for a file another process creates. There is no channel to a stopped
 * child, so its filesystem write is the event; `watch` delivers it instead of
 * a poll loop guessing at a duration.
 */
async function waitForFile(path: string): Promise<void> {
  const target = basename(path);
  const { promise, resolve } = deferred<void>();
  const watcher = watch(dirname(path), (_event, file) => {
    if (file === target) {
      resolve();
    }
  });
  try {
    // Closed the create-before-watch window without polling.
    if (existsSync(path)) {
      resolve();
    }
    await promise;
  } finally {
    watcher.close();
  }
}

async function waitForExit(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) {
    return;
  }
  const { promise, resolve } = deferred<void>();
  child.once("exit", resolve);
  await promise;
}

describe("claimUnderLock", () => {
  const brokenRelease = async (): Promise<RepositoryLock> => ({
    name: "worktree-admin",
    path: "/does/not/matter",
    token: "t",
    release: async () => {
      throw new Error("simulated release failure");
    },
  });

  it("hands back the claimed resource when only the release failed", async () => {
    const claim = await claimUnderLock(brokenRelease, async () => ({ path: "/tmp/wt" }));

    // The handle survives: only its owner can tear the resource down.
    expect(claim.value).toEqual({ path: "/tmp/wt" });
    expect(claim.releaseError?.code).toBe("LOCK_RELEASE_FAILED");
    expect(claim.releaseError?.message).toContain("simulated release failure");
    expect(claim.releaseError?.message).toContain("needs teardown");
  });

  it("propagates an operation failure and never drops a concurrent release failure", async () => {
    const claimUnder = () =>
      claimUnderLock(brokenRelease, async () => {
        throw new AgentHubError("WORKTREE_ADD_FAILED", "git worktree add failed");
      });

    await expect(claimUnder()).rejects.toMatchObject({
      code: "WORKTREE_ADD_FAILED",
      // Both troubles, one error: the lock record is stuck too.
      message: expect.stringContaining("simulated release failure"),
    });

    // With a healthy release the operation's error propagates untouched.
    const healthyRelease = async (): Promise<RepositoryLock> => ({
      name: "worktree-admin",
      path: "/does/not/matter",
      token: "t",
      release: async () => {},
    });
    await expect(
      claimUnderLock(healthyRelease, async () => {
        throw new AgentHubError("WORKTREE_ADD_FAILED", "git worktree add failed");
      }),
    ).rejects.toMatchObject({
      code: "WORKTREE_ADD_FAILED",
      message: "git worktree add failed",
    });
  });
});
