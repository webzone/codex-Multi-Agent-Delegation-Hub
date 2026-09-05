import { randomUUID } from "node:crypto";
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { hostname } from "node:os";
import { join } from "node:path";

import { AgentHubError } from "./errors.js";

/**
 * Repository-local advisory lock stored under `<git-common-dir>/agent-hub/locks`.
 * Because it lives in the common dir it is shared by every linked worktree of
 * the same repository, which is exactly the scope Git worktree administration
 * needs. It is intentionally tiny: exclusive directory creation, an owner
 * metadata file, owner-only release, and conservative stale recovery.
 */

// Promise.withResolvers exists in every supported Node runtime but is missing
// from the ES2022 lib this project compiles against.
declare global {
  interface PromiseWithResolvers<T> {
    promise: Promise<T>;
    resolve: (value: T | PromiseLike<T>) => void;
    reject: (reason: unknown) => void;
  }

  interface PromiseConstructor {
    withResolvers<T>(): PromiseWithResolvers<T>;
  }
}

const LOCK_SUBDIR = join("agent-hub", "locks");
const OWNER_FILE = "owner.json";
const LOCK_NAME_PATTERN = /^[a-z0-9][a-z0-9._-]{0,63}$/;

export interface LockOwner {
  token: string;
  pid: number;
  hostname: string;
  started_at: string;
}

export interface RepositoryLock {
  readonly name: string;
  readonly path: string;
  readonly token: string;
  /** Removes the lock. Rejects unless the on-disk owner token still matches ours. */
  release(): Promise<void>;
}

export interface LockAcquireOptions {
  /** Absolute path reported by `git rev-parse --git-common-dir`. */
  commonDir: string;
  /** Lock name; validated safe for a single path segment. */
  name: string;
  /** Total time to keep retrying while the lock is held. Default 0 (single attempt). */
  waitMs?: number;
  /** Delay between retries while busy. Default 25ms. */
  retryDelayMs?: number;
  /** Seam for liveness probing in tests. Default: `process.kill(pid, 0)`. */
  probePid?: (pid: number) => "live" | "dead";
  now?: () => Date;
  newToken?: () => string;
}

function locksRoot(commonDir: string): string {
  return join(commonDir, LOCK_SUBDIR);
}

export function lockPathFor(commonDir: string, name: string): string {
  return join(locksRoot(commonDir), `${name}.lock`);
}

function defaultProbePid(pid: number): "live" | "dead" {
  if (!Number.isInteger(pid) || pid < 1) {
    return "live"; // Not a credible PID: never treat as demonstrably dead.
  }
  try {
    process.kill(pid, 0);
    return "live";
  } catch (error) {
    const code = (error as NodeJS.ErrnoException).code;
    // EPERM means the process exists but belongs to another user: unverifiable,
    // so it must count as live. Only ESRCH proves death.
    return code === "ESRCH" ? "dead" : "live";
  }
}

async function readOwner(lockPath: string): Promise<LockOwner | null> {
  let raw: string;
  try {
    raw = await readFile(join(lockPath, OWNER_FILE), "utf8");
  } catch {
    return null;
  }

  try {
    const parsed = JSON.parse(raw) as Partial<LockOwner>;
    if (
      typeof parsed.token !== "string" ||
      !Number.isInteger(parsed.pid) ||
      (parsed.pid as number) < 1 ||
      typeof parsed.hostname !== "string" ||
      typeof parsed.started_at !== "string"
    ) {
      return null;
    }
    return parsed as LockOwner;
  } catch {
    return null;
  }
}

async function sleep(ms: number): Promise<void> {
  const { promise, resolve } = Promise.withResolvers<void>();
  setTimeout(resolve, ms);
  return promise;
}

type Staleness =
  | { state: "busy"; owner: LockOwner }
  | { state: "stale-dead"; owner: LockOwner }
  | { state: "unverifiable"; reason: string };

/**
 * Classify a lock we failed to create. Recovery is only ever signalled for an
 * owner that is demonstrably dead on this same host; anything else — live,
 * foreign-host, missing or corrupt metadata — must surface as a concrete
 * busy/recovery error instead of being deleted.
 */
async function classify(
  lockPath: string,
  probePid: (pid: number) => "live" | "dead",
): Promise<Staleness> {
  const owner = await readOwner(lockPath);
  if (!owner) {
    return {
      state: "unverifiable",
      reason: "lock exists but its owner metadata is missing or corrupt; refusing to guess ownership",
    };
  }
  if (owner.hostname !== hostname()) {
    return {
      state: "unverifiable",
      reason: `lock is owned by pid ${owner.pid} on foreign host "${owner.hostname}"; cannot verify liveness from here`,
    };
  }
  if (probePid(owner.pid) === "live") {
    return { state: "busy", owner };
  }
  return { state: "stale-dead", owner };
}

/**
 * Reclaim a demonstrably dead same-host lock. Uses an atomic rename into a
 * uniquely named reclaim path as the arbiter: whoever renames the stale dir
 * away gets to inspect and delete it; a mis-grabbed fresh lock (token
 * mismatch) is renamed straight back. Returns true when the stale lock was
 * removed and the caller may retry exclusive creation.
 */
async function tryRecover(
  lockPath: string,
  observed: LockOwner,
  ourToken: string,
  attempt: number,
): Promise<boolean> {
  const reclaimPath = `${lockPath}.reclaim-${ourToken}-${attempt}`;
  try {
    await rename(lockPath, reclaimPath);
  } catch {
    return false; // Vanished or raced; caller retries.
  }

  const current = await readOwner(reclaimPath);
  if (current && current.token === observed.token) {
    await rm(reclaimPath, { recursive: true, force: true });
    return true;
  }

  // Token changed under us (or metadata vanished): this is not the stale lock
  // we judged. Restore it untouched and back off.
  try {
    await rename(reclaimPath, lockPath);
  } catch {
    // lockPath reappeared meanwhile; the reclaimed copy can no longer be
    // restored safely, so drop it rather than leave a shadow lock around.
    await rm(reclaimPath, { recursive: true, force: true }).catch(() => {});
  }
  return false;
}

function busyError(name: string, stale: Staleness): AgentHubError {
  if (stale.state === "unverifiable") {
    return new AgentHubError(
      "LOCK_UNRECOVERABLE",
      `Repository lock "${name}" cannot be acquired: ${stale.reason}. Inspect the lock directory and remove it only after confirming no Agent Hub process owns it.`,
    );
  }
  return new AgentHubError(
    "LOCK_BUSY",
    `Repository lock "${name}" is held by pid ${stale.owner.pid} on ${stale.owner.hostname} since ${stale.owner.started_at}`,
  );
}

/**
 * Acquire `<common-dir>/agent-hub/locks/<name>.lock` exclusively.
 *
 * Ownership proof is a random token recorded in `owner.json` together with
 * pid/hostname/time. Release is owner-only. A lock left behind by a process
 * that is demonstrably dead on the same host is reclaimed automatically;
 * live, foreign, or unverifiable ownership produces LOCK_BUSY / LOCK_UNRECOVERABLE.
 */
export async function acquireRepositoryLock(options: LockAcquireOptions): Promise<RepositoryLock> {
  const {
    commonDir,
    name,
    waitMs = 0,
    retryDelayMs = 25,
    probePid = defaultProbePid,
    now = () => new Date(),
    newToken = () => randomUUID(),
  } = options;

  if (!LOCK_NAME_PATTERN.test(name)) {
    throw new AgentHubError(
      "LOCK_INVALID_NAME",
      `Lock name "${name}" must match ${LOCK_NAME_PATTERN} (single safe path segment)`,
    );
  }
  if (!commonDir.trim()) {
    throw new AgentHubError("LOCK_INVALID_TARGET", "commonDir must not be empty");
  }

  const root = locksRoot(commonDir);
  const lockPath = lockPathFor(commonDir, name);
  await mkdir(root, { recursive: true });

  const deadline = now().getTime() + Math.max(0, waitMs);
  const token = newToken();

  for (let attempt = 0; ; attempt += 1) {
    try {
      await mkdir(lockPath);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== "EEXIST") {
        throw error;
      }

      const stale = await classify(lockPath, probePid);

      // Dead same-host ownership is reclaimed immediately; waiting cannot make
      // a dead owner release, and recovery never requires the wait window.
      if (stale.state === "stale-dead" && (await tryRecover(lockPath, stale.owner, token, attempt))) {
        continue;
      }
      if (now().getTime() >= deadline) {
        throw busyError(name, stale);
      }
      await sleep(retryDelayMs);
      continue;
    }

    // We created the directory; we are the sole writer of its owner file.
    const owner: LockOwner = {
      token,
      pid: process.pid,
      hostname: hostname(),
      started_at: now().toISOString(),
    };
    try {
      await writeFileAtomic(join(lockPath, OWNER_FILE), JSON.stringify(owner, null, 2));
    } catch (error) {
      await rm(lockPath, { recursive: true, force: true });
      throw error;
    }

    return {
      name,
      path: lockPath,
      token,
      async release(): Promise<void> {
        const current = await readOwner(lockPath);
        if (!current || current.token !== token) {
          throw new AgentHubError(
            "LOCK_NOT_OWNER",
            `Refusing to release repository lock "${name}": on-disk ownership no longer matches this handle`,
          );
        }
        await rm(lockPath, { recursive: true, force: true });
      },
    };
  }
}

async function writeFileAtomic(path: string, contents: string): Promise<void> {
  const tempPath = `${path}.${process.pid}.${randomUUID()}.tmp`;
  await writeFile(tempPath, contents, "utf8");
  await rename(tempPath, path);
}
