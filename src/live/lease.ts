import { randomUUID } from "node:crypto";
import { mkdir, open, readFile, readdir, rename, unlink, writeFile } from "node:fs/promises";
import type { FileHandle } from "node:fs/promises";
import { hostname } from "node:os";
import { isAbsolute, join } from "node:path";

import { AgentHubError } from "../errors.js";
import { runProcess } from "../process.js";
import type { LiveProviderId } from "./types.js";

/**
 * Lifetime live-ownership lease (v3 live, Package 1).
 *
 * A lease is NOT a lock. The short-operation locks (`src/locks.ts`, names
 * `live-<id>` / `live-admin`) serialize microseconds-to-seconds critical
 * sections and are held by whoever happens to be running an operation. The
 * lease records *ownership that lasts as long as the session*: which hub
 * process owns this live session, which provider process group it launched,
 * and which persistent worktree belongs to it. It survives hub restarts
 * precisely so a later hub can classify what it finds instead of guessing.
 *
 * Layout: `<git-common-dir>/agent-hub/live/leases/<live_session_id>.lease.json`
 * — repository-local metadata like every other durable live artifact, shared
 * by linked worktrees, never visible in any checkout.
 *
 * PID-reuse honesty: a pid probe proves *presence*, never *identity*. When a
 * lease says "provider pid P is mine", acting on P (signalling its group) is
 * only safe if P is demonstrably still the process launched back then. The
 * lease therefore also records an OS start-time identity for hub and provider;
 * reclaim/reap may only proceed when presence is proven dead (ESRCH), or when
 * presence is alive and the start identity proves an exact match. A live pid
 * whose start identity cannot be read or does not match is *uncertain*: never
 * signalled, never silently reclaimed — an innocent reused pid must not be
 * killed, and a foreign host's session must not be poked.
 */

export const LIVE_LEASE_SUBDIR = join("agent-hub", "live", "leases");
export const LIVE_LEASE_SCHEMA = 1;

const LEASE_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const PROVIDER_IDS: readonly LiveProviderId[] = ["omp", "agy", "pi", "hermes"];

export interface LiveLeaseRecord {
  schema: 1;
  live_session_id: string;
  /** Random per-lease token; the only proof allowed to release the lease. */
  token: string;
  hub_pid: number;
  hub_hostname: string;
  /** OS start identity of the hub pid at launch; null when unreadable. */
  hub_start_token: string | null;
  provider: LiveProviderId;
  provider_pid: number | null;
  /** Process group the hub owns via its detached launch; equals provider_pid. */
  provider_pgid: number | null;
  /** OS start identity of the provider pid at launch; null when unreadable. */
  provider_start_token: string | null;
  /** Persistent OS-temp worktree owned by this live session. */
  worktree_path: string;
  created_at: string;
  updated_at: string;
}

export interface LiveLeaseProbes {
  probePid: (pid: number) => "live" | "dead";
  /** OS start identity (e.g. `ps -o lstart=`), or null when unobtainable. */
  startToken: (pid: number) => Promise<string | null>;
  /** Signal a whole process group. Returns whether the signal was delivered. */
  killGroup: (pgid: number, signal: NodeJS.Signals) => boolean;
  now: () => Date;
}

export type LiveLeaseProviderStatus =
  | { state: "dead" }
  | { state: "alive"; reapable: true }
  | { state: "uncertain"; reapable: false; reason: string };

export type LiveLeaseClassification =
  | { state: "hub-live" }
  | { state: "foreign-host"; owner_hostname: string }
  | { state: "hub-gone"; provider: LiveLeaseProviderStatus };

export function liveLeasePath(commonDir: string, liveSessionId: string): string {
  assertLeaseSessionId(liveSessionId);
  if (!commonDir.trim()) {
    throw new AgentHubError("LIVE_LEASE_INVALID_TARGET", "commonDir must not be empty");
  }
  return join(commonDir, LIVE_LEASE_SUBDIR, `${liveSessionId}.lease.json`);
}

function assertLeaseSessionId(liveSessionId: string): void {
  if (!LEASE_ID_PATTERN.test(liveSessionId)) {
    throw new AgentHubError(
      "LIVE_LEASE_INVALID_ID",
      `live session id "${liveSessionId}" is not a generated UUID; refusing to derive a lease path from it`,
    );
  }
}

function leasesRoot(commonDir: string): string {
  return join(commonDir, LIVE_LEASE_SUBDIR);
}

function parseLiveLease(value: unknown): LiveLeaseRecord | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }
  const raw = value as Record<string, unknown>;
  if (raw.schema !== LIVE_LEASE_SCHEMA) {
    return null;
  }
  if (
    typeof raw.live_session_id !== "string" ||
    !LEASE_ID_PATTERN.test(raw.live_session_id) ||
    typeof raw.token !== "string" ||
    !raw.token.trim() ||
    !Number.isInteger(raw.hub_pid) ||
    (raw.hub_pid as number) < 1 ||
    typeof raw.hub_hostname !== "string" ||
    !raw.hub_hostname.trim() ||
    !(raw.hub_start_token === null || typeof raw.hub_start_token === "string") ||
    typeof raw.provider !== "string" ||
    !PROVIDER_IDS.includes(raw.provider as LiveProviderId) ||
    !(raw.provider_pid === null || (Number.isInteger(raw.provider_pid) && (raw.provider_pid as number) >= 1)) ||
    !(raw.provider_pgid === null || (Number.isInteger(raw.provider_pgid) && (raw.provider_pgid as number) >= 1)) ||
    !(raw.provider_start_token === null || typeof raw.provider_start_token === "string") ||
    typeof raw.worktree_path !== "string" ||
    !isAbsolute(raw.worktree_path) ||
    typeof raw.created_at !== "string" ||
    !raw.created_at.trim() ||
    typeof raw.updated_at !== "string" ||
    !raw.updated_at.trim()
  ) {
    return null;
  }
  return {
    schema: LIVE_LEASE_SCHEMA,
    live_session_id: raw.live_session_id,
    token: raw.token,
    hub_pid: raw.hub_pid as number,
    hub_hostname: raw.hub_hostname,
    hub_start_token: raw.hub_start_token as string | null,
    provider: raw.provider as LiveProviderId,
    provider_pid: raw.provider_pid as number | null,
    provider_pgid: raw.provider_pgid as number | null,
    provider_start_token: raw.provider_start_token as string | null,
    worktree_path: raw.worktree_path,
    created_at: raw.created_at,
    updated_at: raw.updated_at,
  };
}

async function readJson(path: string): Promise<unknown> {
  try {
    return JSON.parse(await readFile(path, "utf8"));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return undefined;
    }
    return null;
  }
}

/** Default probes: kill(pid,0) presence plus the OS-reported process start time. */
export const defaultLiveLeaseProbes: LiveLeaseProbes = {
  probePid(pid: number): "live" | "dead" {
    try {
      process.kill(pid, 0);
      return "live";
    } catch (error) {
      // EPERM still proves presence; only ESRCH proves absence.
      return (error as NodeJS.ErrnoException).code === "ESRCH" ? "dead" : "live";
    }
  },
  async startToken(pid: number): Promise<string | null> {
    const result = await runProcess("ps", ["-o", "lstart=", "-p", String(pid)], {
      cwd: "/",
      maxOutputBytes: 4096,
    });
    if (result.error || result.exitCode !== 0) {
      return null;
    }
    const line = result.stdout.trim();
    return line || null;
  },
  killGroup(pgid: number, signal: NodeJS.Signals): boolean {
    try {
      process.kill(-pgid, signal);
      return true;
    } catch {
      return false;
    }
  },
  now: () => new Date(),
};

let hubStartTokenCache: Promise<string | null> | null = null;

/** Stable identity of *this* hub process, captured once; null when unreadable. */
export function hubProcessStartToken(probes: LiveLeaseProbes = defaultLiveLeaseProbes): Promise<string | null> {
  hubStartTokenCache ??= probes.startToken(process.pid);
  return hubStartTokenCache;
}

export interface CreateLiveLeaseInput {
  commonDir: string;
  live_session_id: string;
  provider: LiveProviderId;
  worktree_path: string;
  provider_pid: number | null;
  provider_pgid: number | null;
  provider_start_token: string | null;
  hub_start_token: string | null;
  now?: () => Date;
  newToken?: () => string;
}

/**
 * Create the lease exclusively. `EEXIST` is a real collision — the same live
 * session id already owns resources — and is never overwritten.
 */
export async function createLiveLease(input: CreateLiveLeaseInput): Promise<LiveLeaseRecord> {
  const now = (input.now ?? (() => new Date()))().toISOString();
  const record: LiveLeaseRecord = {
    schema: LIVE_LEASE_SCHEMA,
    live_session_id: input.live_session_id,
    token: (input.newToken ?? randomUUID)(),
    hub_pid: process.pid,
    hub_hostname: hostname(),
    hub_start_token: input.hub_start_token,
    provider: input.provider,
    provider_pid: input.provider_pid,
    provider_pgid: input.provider_pgid,
    provider_start_token: input.provider_start_token,
    worktree_path: input.worktree_path,
    created_at: now,
    updated_at: now,
  };
  assertLeaseSessionId(record.live_session_id);

  const path = liveLeasePath(input.commonDir, record.live_session_id);
  await mkdir(join(path, ".."), { recursive: true });
  // O_CREAT|O_EXCL is the atomic claim: the lease record exists exactly when
  // its creator wrote it, and a collision is a real double-ownership attempt.
  let handle: FileHandle;
  try {
    handle = await open(path, "wx");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "EEXIST") {
      throw new AgentHubError(
        "LIVE_LEASE_EXISTS",
        `a live lease for session "${record.live_session_id}" already exists; refusing to double-own it`,
      );
    }
    throw error;
  }
  await handle.writeFile(`${JSON.stringify(record, null, 2)}\n`, "utf8");
  await handle.close();
  return record;
}

/** Absent → undefined; present-but-unparseable → corrupt (never guessed). */
export async function readLiveLease(
  commonDir: string,
  liveSessionId: string,
): Promise<LiveLeaseRecord | undefined> {
  const raw = await readJson(liveLeasePath(commonDir, liveSessionId));
  if (raw === undefined) {
    return undefined;
  }
  const record = parseLiveLease(raw);
  if (record === null) {
    throw new AgentHubError(
      "LIVE_LEASE_CORRUPT",
      `lease for "${liveSessionId}" exists but is not a valid lease record; refusing to guess ownership`,
    );
  }
  return record;
}

export interface ListedLease {
  live_session_id: string;
  path: string;
  /** Null when the file exists but fails validation (counted conservatively). */
  record: LiveLeaseRecord | null;
}

/** Scan every lease in the common dir; corrupt entries surface unvalidated. */
export async function listLiveLeases(commonDir: string): Promise<ListedLease[]> {
  let names: string[];
  try {
    names = await readdir(leasesRoot(commonDir));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return [];
    }
    throw error;
  }
  const out: ListedLease[] = [];
  for (const name of names) {
    if (!name.endsWith(".lease.json")) {
      continue;
    }
    const liveSessionId = name.slice(0, -".lease.json".length);
    const path = join(leasesRoot(commonDir), name);
    const raw = await readJson(path);
    const record = raw === undefined || raw === null ? null : parseLiveLease(raw);
    out.push({
      live_session_id: LEASE_ID_PATTERN.test(liveSessionId) ? liveSessionId : "",
      path,
      record,
    });
  }
  return out;
}

/**
 * Patch the provider facts a launch produced. Token-checked: a handle that no
 * longer matches the on-disk lease may not write through it.
 */
export async function updateLiveLeaseProvider(
  commonDir: string,
  lease: LiveLeaseRecord,
  patch: {
    provider_pid: number | null;
    provider_pgid: number | null;
    provider_start_token: string | null;
  },
  now: () => Date = () => new Date(),
): Promise<LiveLeaseRecord> {
  const current = await readLiveLease(commonDir, lease.live_session_id);
  if (current === undefined || current.token !== lease.token) {
    throw new AgentHubError(
      "LIVE_LEASE_NOT_OWNER",
      `lease for "${lease.live_session_id}" vanished or changed owner; refusing to update it`,
    );
  }
  const updated: LiveLeaseRecord = {
    ...current,
    provider_pid: patch.provider_pid,
    provider_pgid: patch.provider_pgid,
    provider_start_token: patch.provider_start_token,
    updated_at: now().toISOString(),
  };
  const path = liveLeasePath(commonDir, lease.live_session_id);
  const tempPath = `${path}.${process.pid}.${randomUUID()}.tmp`;
  await writeFile(tempPath, `${JSON.stringify(updated, null, 2)}\n`, "utf8");
  await rename(tempPath, path);
  return updated;
}

/**
 * Release with the recorded token. A lease this handle no longer owns is left
 * exactly where it is; releasing is the same proof discipline as lock release.
 */
export async function removeLiveLease(
  commonDir: string,
  liveSessionId: string,
  token: string,
): Promise<void> {
  const current = await readLiveLease(commonDir, liveSessionId);
  if (current === undefined) {
    throw new AgentHubError(
      "LIVE_LEASE_NOT_FOUND",
      `no live lease for session "${liveSessionId}" to release`,
    );
  }
  if (current.token !== token) {
    throw new AgentHubError(
      "LIVE_LEASE_NOT_OWNER",
      `lease for "${liveSessionId}" no longer belongs to this handle; refusing to release it`,
    );
  }
  await unlink(liveLeasePath(commonDir, liveSessionId));
}

/**
 * Classify what ownership evidence says right now. Every branch is proof-
 * keyed: foreign host and live-with-matching-identity are hands-off; a dead
 * hub pid (ESRCH is presence-proof) hands the provider over for reaping, and
 * a live provider pid may only be reaped when its start identity matches the
 * lease exactly.
 */
export async function classifyLiveLease(
  lease: LiveLeaseRecord,
  probes: LiveLeaseProbes = defaultLiveLeaseProbes,
  thisHostname: string = hostname(),
): Promise<LiveLeaseClassification> {
  if (lease.hub_hostname !== thisHostname) {
    return { state: "foreign-host", owner_hostname: lease.hub_hostname };
  }

  let provider: LiveLeaseProviderStatus;
  if (lease.provider_pid === null) {
    // A null pid proves recorded ownership never landed — never provider
    // death. A hub that died between lease creation and spawn and one that
    // died after spawning without recording facts look identical here, so
    // nothing may be inferred: the session goes to manual review and its
    // worktree survives. The launch path records provider facts via
    // `updateLiveLeaseProvider` the moment a process exists.
    provider = {
      state: "uncertain",
      reapable: false,
      reason:
        "lease records no provider pid; provider liveness can be neither proven nor disproven, so nothing is assumed",
    };
  } else if (probes.probePid(lease.provider_pid) === "dead") {
    provider = { state: "dead" };
  } else {
    const observedStart = await probes.startToken(lease.provider_pid);
    if (
      lease.provider_start_token !== null &&
      observedStart !== null &&
      lease.provider_start_token === observedStart &&
      (lease.provider_pgid === null || lease.provider_pgid === lease.provider_pid)
    ) {
      provider = { state: "alive", reapable: true };
    } else if (
      lease.provider_start_token !== null &&
      observedStart !== null &&
      lease.provider_start_token !== observedStart
    ) {
      provider = {
        state: "uncertain",
        reapable: false,
        reason: `pid ${lease.provider_pid} is alive but its start identity differs from the lease: the pid was reused; the original provider is gone but signalling this pid would hit an innocent process`,
      };
    } else {
      provider = {
        state: "uncertain",
        reapable: false,
        reason: `pid ${lease.provider_pid} is alive but its start identity cannot be matched to the lease; signalling it could hit an unrelated process`,
      };
    }
  }

  if (probes.probePid(lease.hub_pid) === "live") {
    // The owning hub is present. Only an exact identity match may call the
    // session "hub-live"; anything else (reused pid, unreadable identity) is
    // treated as hub-gone for *bookkeeping*, but the hands-off rule keeps
    // reaping conservative: reapable providers still require their own proof.
    const observedHubStart = await probes.startToken(lease.hub_pid);
    if (lease.hub_start_token === null || observedHubStart === null) {
      return { state: "hub-live" };
    }
    if (lease.hub_start_token === observedHubStart) {
      return { state: "hub-live" };
    }
  }

  return { state: "hub-gone", provider };
}

export type LiveLeaseReapOutcome =
  | { status: "reaped"; waited_ms: number }
  | { status: "survived"; waited_ms: number }
  | { status: "not-attempted"; reason: string };

export interface ReapOptions {
  graceMs?: number;
  killWaitMs?: number;
  pollMs?: number;
}

/**
 * Terminate an orphaned provider group the lease provably owns. This is the
 * ONLY path that signals a pid the calling process did not spawn, and it
 * fires solely for `reapable: true` classifications. SIGTERM gets the grace
 * window; SIGKILL escalation is bounded and the final answer is still just
 * what the pid probe observed.
 */
export async function reapOrphanedProvider(
  lease: LiveLeaseRecord,
  status: LiveLeaseProviderStatus,
  probes: LiveLeaseProbes = defaultLiveLeaseProbes,
  options: ReapOptions = {},
): Promise<LiveLeaseReapOutcome> {
  if (status.state !== "alive") {
    return {
      status: "not-attempted",
      reason: status.state === "uncertain" ? status.reason : "no reapable provider process",
    };
  }
  const target = lease.provider_pgid ?? lease.provider_pid;
  if (target === null) {
    return { status: "not-attempted", reason: "lease records no pid to signal" };
  }

  const graceMs = options.graceMs ?? 5_000;
  const killWaitMs = options.killWaitMs ?? 5_000;
  const pollMs = options.pollMs ?? 25;
  const started = probes.now().getTime();

  const goneWithin = async (windowMs: number): Promise<boolean> => {
    const deadline = probes.now().getTime() + windowMs;
    for (;;) {
      if (probes.probePid(target) === "dead") {
        return true;
      }
      if (probes.now().getTime() >= deadline) {
        return false;
      }
      await new Promise((resolve) => setTimeout(resolve, pollMs));
    }
  };

  probes.killGroup(target, "SIGTERM");
  if (await goneWithin(graceMs)) {
    return { status: "reaped", waited_ms: probes.now().getTime() - started };
  }
  probes.killGroup(target, "SIGKILL");
  if (await goneWithin(killWaitMs)) {
    return { status: "reaped", waited_ms: probes.now().getTime() - started };
  }
  return { status: "survived", waited_ms: probes.now().getTime() - started };
}
