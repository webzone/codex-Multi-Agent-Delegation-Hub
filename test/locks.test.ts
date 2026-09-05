import { spawn } from "node:child_process";
import { access, mkdir, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { hostname, tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { acquireRepositoryLock, lockPathFor, type LockOwner } from "../src/locks.js";

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
  const { promise, resolve } = Promise.withResolvers<void>();
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
});
