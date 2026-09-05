import { randomUUID } from "node:crypto";
import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { hostname } from "node:os";
import { join } from "node:path";

import { deferred } from "./deferred.js";
import { AgentHubError } from "./errors.js";

/**
 * Repository-local advisory lock stored under `<git-common-dir>/agent-hub/locks`.
 * Because it lives in the common dir it is shared by every linked worktree of
 * the same repository, which is exactly the scope Git worktree administration
 * needs. It is intentionally tiny: exclusive directory creation, an owner
 * metadata file, owner-only release, and conservative stale recovery.
 *
 * Crash recovery, in order of what is provable about a lock we failed to
 * create:
 *
 * - Valid owner metadata, same host, owner PID demonstrably dead (ESRCH):
 *   reclaimed via an atomic rename arbiter that re-checks the token.
 * - Live, unverifiable-PID, or foreign-host ownership: LOCK_BUSY /
 *   LOCK_UNRECOVERABLE; never deleted.
 * - Ownerless directory (no `owner.json` at all): the only state a crash can
 *   leave inside the window between the exclusive `mkdir` and the atomic
 *   owner write. Once the directory's own mtime is older than
 *   `ownerlessGraceMs` (default 60s, orders of magnitude wider than that
 *   window), it is reclaimed through the same rename arbiter — and if an
 *   owner file appears while the reclaim is in flight, the directory is put
 *   back untouched. A fresh ownerless directory (grace not elapsed) is left
 *   alone with LOCK_UNRECOVERABLE instead of being guessed about.
 * - A present-but-corrupt `owner.json` is never age-recovered: writes are
 *   atomic, so corrupt metadata means external tampering, not a crash
 *   remnant. LOCK_UNRECOVERABLE.
 */

const LOCK_SUBDIR = join("agent-hub", "locks");
const OWNER_FILE = "owner.json";
const LOCK_NAME_PATTERN = /^[a-z0-9][a-z0-9._-]{0,63}$/;

/** Crash-recovery grace for lock directories that never received an owner file. */
export const OWNERLESS_GRACE_MS = 60_000;

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
  /** Grace before an ownerless lock directory (no owner.json at all) is reclaimed,
   *  measured from the directory's mtime. Default 60s; 0 reclaims on sight. */
  ownerlessGraceMs?: number;
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

type StoredOwner =
  | { status: "absent" }
  | { status: "corrupt" }
  | { status: "valid"; owner: LockOwner };

/** `absent` is proven only by ENOENT; every other read/parse failure is `corrupt`. */
async function readOwner(lockPath: string): Promise<StoredOwner> {
  let raw: string;
  try {
    raw = await readFile(join(lockPath, OWNER_FILE), "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return { status: "absent" };
    }
    return { status: "corrupt" };
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
      return { status: "corrupt" };
    }
    return { status: "valid", owner: parsed as LockOwner };
  } catch {
    return { status: "corrupt" };
  }
}

async function sleep(ms: number): Promise<void> {
  const { promise, resolve } = deferred<void>();
  setTimeout(resolve, ms);
  return promise;
}

type Staleness =
  | { state: "busy"; owner: LockOwner }
  | { state: "stale-dead"; owner: LockOwner }
  | { state: "ownerless-expired"; ageMs: number }
  | { state: "unverifiable"; reason: string };

/** Milliseconds since the lock directory was last modified; null if unreadable. */
async function ownerlessAgeMs(lockPath: string, now: () => Date): Promise<number | null> {
  try {
    const info = await stat(lockPath);
    // Filesystem timestamps may run slightly ahead of our clock: a negative
    // age means "just created", never "already expired".
    return Math.max(0, now().getTime() - info.mtimeMs);
  } catch {
    return null;
  }
}

/**
 * Classify a lock we failed to create. Recovery is signalled only for what is
 * provable: an owner demonstrably dead on this same host, or an ownerless
 * directory whose crash-recovery grace window has elapsed. Everything else —
 * live, foreign-host, fresh-ownerless, corrupt metadata — surfaces as a
 * concrete busy/recovery error instead of being deleted.
 */
async function classify(
  lockPath: string,
  probePid: (pid: number) => "live" | "dead",
  ownerlessGraceMs: number,
  now: () => Date,
): Promise<Staleness> {
  const stored = await readOwner(lockPath);
  if (stored.status === "corrupt") {
    return {
      state: "unverifiable",
      reason: "lock exists but its owner metadata is corrupt; refusing to guess ownership",
    };
  }
  if (stored.status === "absent") {
    // Ownerless directory: the only crash trace possible between the
    // exclusive mkdir and the atomic owner write. Reclaim only once the
    // grace window has elapsed on the directory's own mtime.
    const ageMs = await ownerlessAgeMs(lockPath, now);
    if (ageMs === null) {
      return {
        state: "unverifiable",
        reason: "lock directory vanished while its ownership was being probed",
      };
    }
    if (ageMs >= ownerlessGraceMs) {
      return { state: "ownerless-expired", ageMs };
    }
    return {
      state: "unverifiable",
      reason: `lock exists without owner metadata and has been ownerless for only ${Math.max(
        0,
        Math.round(ageMs),
      )}ms of the ${ownerlessGraceMs}ms crash-recovery grace window; refusing to reclaim a lock that may still be mid-claim`,
    };
  }
  const owner = stored.owner;
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
  if (current.status === "valid" && current.owner.token === observed.token) {
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

/**
 * Reclaim an ownerless directory whose grace window expired. Same rename
 * arbiter: whoever moves it away re-checks it. If owner metadata appeared (or
 * the directory turned out corrupt) while it was moved aside, it is a live or
 * unexplainable claim — renamed straight back, untouched.
 */
async function tryRecoverOwnerless(
  lockPath: string,
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
  if (current.status === "absent") {
    await rm(reclaimPath, { recursive: true, force: true });
    return true;
  }

  try {
    await rename(reclaimPath, lockPath);
  } catch {
    await rm(reclaimPath, { recursive: true, force: true }).catch(() => {});
  }
  return false;
}

function busyError(name: string, stale: Staleness): AgentHubError {
  if (stale.state === "ownerless-expired") {
    return new AgentHubError(
      "LOCK_UNRECOVERABLE",
      `Repository lock "${name}" is an ownerless ${Math.round(
        stale.ageMs / 1000,
      )}s-old remnant that could not be reclaimed concurrently. Retry, or inspect the lock directory.`,
    );
  }
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
 * that is demonstrably dead on the same host, and an ownerless lock directory
 * whose `ownerlessGraceMs` window has elapsed (crash between mkdir and owner
 * write), are reclaimed automatically; live, foreign, fresh-ownerless, or
 * corrupt ownership produces LOCK_BUSY / LOCK_UNRECOVERABLE.
 */
export async function acquireRepositoryLock(options: LockAcquireOptions): Promise<RepositoryLock> {
  const {
    commonDir,
    name,
    waitMs = 0,
    retryDelayMs = 25,
    probePid = defaultProbePid,
    ownerlessGraceMs = OWNERLESS_GRACE_MS,
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

      const stale = await classify(lockPath, probePid, ownerlessGraceMs, now);

      // Dead same-host ownership and expired ownerless remnants are reclaimed
      // immediately: waiting cannot make a dead owner release, and recovery
      // never requires the wait window.
      if (stale.state === "stale-dead" && (await tryRecover(lockPath, stale.owner, token, attempt))) {
        continue;
      }
      if (stale.state === "ownerless-expired" && (await tryRecoverOwnerless(lockPath, token, attempt))) {
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
        const stored = await readOwner(lockPath);
        if (stored.status !== "valid" || stored.owner.token !== token) {
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
