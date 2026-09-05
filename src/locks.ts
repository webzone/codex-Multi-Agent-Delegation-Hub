import { randomUUID } from "node:crypto";
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { hostname } from "node:os";
import { join } from "node:path";

import { deferred } from "./deferred.js";
import { asDelegateError, AgentHubError } from "./errors.js";

/**
 * Repository-local advisory lock stored under `<git-common-dir>/agent-hub/locks`.
 * Because it lives in the common dir it is shared by every linked worktree of
 * the same repository, which is exactly the scope Git worktree administration
 * needs. It is intentionally tiny: exclusive directory creation, an owner
 * metadata file, owner-only release, and recovery that is only ever triggered
 * by proof.
 *
 * Crash recovery, in order of what is provable about a lock we failed to
 * create:
 *
 * - Valid owner metadata, same host, owner PID demonstrably dead (ESRCH):
 *   reclaimed via an atomic rename arbiter that re-checks the token.
 * - Live, unverifiable-PID, or foreign-host ownership: LOCK_BUSY /
 *   LOCK_UNRECOVERABLE; never deleted.
 * - Ownerless directory (no `owner.json` at all): LOCK_UNRECOVERABLE, always.
 *   Age is not evidence of death. A creator that is stopped (SIGSTOP),
 *   suspended by the OS, or simply stalled on a hung filesystem sits inside
 *   the window between the exclusive `mkdir` and the owner write indefinitely
 *   while looking exactly like a crash remnant; reclaiming on age alone would
 *   hand the same lock to a second holder while the first one is still about
 *   to write its owner file and start working. Automatic recovery would need
 *   acquisition to be a genuinely atomic claim — the owner record materialised
 *   by the same syscall that creates the lock (a hardlink or symlink of a
 *   pre-written content blob), not "directory first, owner second" — and this
 *   lock does not do that. An ownerless directory is therefore a manual
 *   inspection item: confirm no Agent Hub process owns it, then remove it.
 * - A present-but-corrupt `owner.json` is never age-recovered either: writes
 *   are atomic, so corrupt metadata means external tampering, not a crash
 *   remnant. LOCK_UNRECOVERABLE.
 */

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
  | { state: "unverifiable"; reason: string };

/**
 * Classify a lock we failed to create. Recovery is signalled only for what is
 * provable: an owner demonstrably dead on this same host. Everything else —
 * live, foreign-host, ownerless, corrupt metadata — surfaces as a concrete
 * busy/recovery error instead of being deleted.
 */
async function classify(
  lockPath: string,
  probePid: (pid: number) => "live" | "dead",
): Promise<Staleness> {
  const stored = await readOwner(lockPath);
  if (stored.status === "corrupt") {
    return {
      state: "unverifiable",
      reason: "lock exists but its owner metadata is corrupt; refusing to guess ownership",
    };
  }
  if (stored.status === "absent") {
    // Ownerless directory. A creator that is stopped (SIGSTOP), throttled, or
    // stuck on a hung filesystem sits in this state just as a crash does, and
    // its directory mtime says nothing about which it is — stealing it would
    // give a second holder the same lock. Automatic recovery needs a genuinely
    // atomic claim (owner recorded by the syscall that creates the lock);
    // acquisition here is "directory, then owner", so this stays manual.
    return {
      state: "unverifiable",
      reason:
        "lock directory exists without owner metadata, which a live-but-stalled creator leaves behind exactly as a crash does; it is never reclaimed automatically",
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
 * live, foreign, ownerless, or corrupt ownership produces LOCK_BUSY /
 * LOCK_UNRECOVERABLE and is never deleted. An ownerless directory has no
 * provable owner, so it is a manual inspection item — see the module comment.
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

      // Dead same-host ownership is reclaimed immediately: waiting cannot make
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

/** Result of {@link claimUnderLock}. */
export interface LockedClaim<T> {
  /** Whatever the operation produced; the caller owns it even if release failed. */
  value: T;
  /** Set when the lock could not be released *after* the operation succeeded. */
  releaseError: AgentHubError | null;
}

/**
 * Run `operation` while holding a lock, then release it — with one deliberate
 * asymmetry over `try/finally`: when the operation *succeeded* and the release
 * then failed, the outcome is returned together with the release error instead
 * of being replaced by a throw. The operation normally created something only
 * the caller can tear down (a Git worktree, an admin directory), and converting
 * a successful claim into a throw loses the handle while leaving the resource
 * behind. A caller that receives a non-null `releaseError` must therefore still
 * use `value` and attempt its teardown — and tell the operator what was left
 * behind: with `describeResource` given, the release error names the resource.
 *
 * An operation failure propagates as a throw: nothing was claimed, so no handle
 * can be orphaned. A release failure on that path is appended to the thrown
 * error rather than dropped, because an unreleased lock record is its own
 * operational problem.
 */
export async function claimUnderLock<T>(
  acquire: () => Promise<RepositoryLock>,
  operation: () => Promise<T>,
  describeResource?: (value: T) => string,
): Promise<LockedClaim<T>> {
  const lock = await acquire();

  let value: T;
  try {
    value = await operation();
  } catch (error) {
    try {
      await lock.release();
    } catch (releaseError) {
      const failure = asDelegateError(error);
      throw new AgentHubError(
        failure.code,
        `${failure.message} (and the repository lock could not be released either: ${
          asDelegateError(releaseError).message
        })`,
      );
    }
    throw error;
  }

  try {
    await lock.release();
    return { value, releaseError: null };
  } catch (releaseError) {
    const failure = asDelegateError(releaseError);
    return {
      value,
      releaseError: new AgentHubError(
        failure.code === "INTERNAL_ERROR" ? "LOCK_RELEASE_FAILED" : failure.code,
        `${failure.message}; the resource that was claimed under it still exists and needs` +
          ` teardown${describeResource ? `: ${describeResource(value)}` : ""}`,
      ),
    };
  }
}
