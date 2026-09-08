import { hostname } from "node:os";

import { AgentHubError } from "../errors.js";
import { deferred } from "../deferred.js";
import { runProcess } from "../process.js";
import type { ProcessFacts } from "../kernel/contracts.js";
import { leasePath, readJsonFile, removeFile, writeJsonAtomic } from "./home.js";

/**
 * Provider process leases (P2 WorkspaceLifecycle).
 *
 * Written the moment the transport reports spawn facts — before any
 * handshake can fail — so a provider process can never exist without a
 * durable owner. The lease is provider-agnostic by construction: pid/pgid
 * plus the OS-reported start identity is the whole of ownership; provider
 * and transport ids are opaque strings copied from runtime evidence.
 *
 * Classification (mirroring the v3 live discipline) is proof-keyed:
 *   - foreign host → hands-off, always;
 *   - a live hub pid whose start identity matches → `hub-live`, hands-off;
 *   - hub gone → the provider's own fate decides: only an exact start-token
 *     match against a group the detached leader provably leads (pgid == pid)
 *     is reapable; leader death alone is not group death; pid reuse,
 *     missing group identity, or an unreadable token is `uncertain`, which
 *     blocks every cleanup path exactly like a live process does.
 */

export const WORKSPACE_LEASE_SCHEMA = "agent-hub-workspace-lease/v1" as const;

export interface WorkspaceLeaseRecord {
  schema: typeof WORKSPACE_LEASE_SCHEMA;
  session_id: string;
  provider: string | null;
  transport: string | null;
  provider_pid: number;
  provider_pgid: number | null;
  provider_start_token: string | null;
  hub_pid: number;
  hub_hostname: string;
  hub_start_token: string | null;
  created_at: string;
}

export interface LeaseProbes {
  probePid(pid: number): "live" | "dead";
  probeGroup(pgid: number): "alive" | "gone" | "uncertain";
  startToken(pid: number): Promise<string | null>;
  killGroup(pgid: number, signal: NodeJS.Signals): boolean;
}

/** Presence is `kill(pid, 0)`; only ESRCH proves absence. Group probes follow the same rule. */
export const defaultLeaseProbes: LeaseProbes = {
  probePid(pid: number): "live" | "dead" {
    try {
      process.kill(pid, 0);
      return "live";
    } catch (error) {
      return (error as NodeJS.ErrnoException).code === "ESRCH" ? "dead" : "live";
    }
  },
  probeGroup(pgid: number): "alive" | "gone" | "uncertain" {
    try {
      process.kill(-pgid, 0);
      return "alive";
    } catch (error) {
      return (error as NodeJS.ErrnoException).code === "ESRCH" ? "gone" : "uncertain";
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
    return result.stdout.trim() || null;
  },
  killGroup(pgid: number, signal: NodeJS.Signals): boolean {
    try {
      process.kill(-pgid, signal);
      return true;
    } catch {
      return false;
    }
  },
};

let hubStartTokenCache: Promise<string | null> | null = null;

/** Stable identity of *this* process, captured once; null when unreadable. */
export async function hubProcessStartToken(probes: LeaseProbes = defaultLeaseProbes): Promise<string | null> {
  hubStartTokenCache ??= probes.startToken(process.pid);
  return hubStartTokenCache;
}

function parseLease(value: unknown): WorkspaceLeaseRecord | null {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    return null;
  }
  const v = value as Record<string, unknown>;
  if (v.schema !== WORKSPACE_LEASE_SCHEMA) {
    return null;
  }
  if (typeof v.session_id !== "string" || typeof v.hub_hostname !== "string") {
    return null;
  }
  if (typeof v.provider_pid !== "number" || !Number.isInteger(v.provider_pid) || v.provider_pid < 1) {
    return null;
  }
  if (typeof v.hub_pid !== "number" || !Number.isInteger(v.hub_pid) || v.hub_pid < 1) {
    return null;
  }
  if (v.provider_pgid !== null && (typeof v.provider_pgid !== "number" || !Number.isInteger(v.provider_pgid))) {
    return null;
  }
  return {
    schema: WORKSPACE_LEASE_SCHEMA,
    session_id: v.session_id,
    provider: typeof v.provider === "string" ? v.provider : null,
    transport: typeof v.transport === "string" ? v.transport : null,
    provider_pid: v.provider_pid,
    provider_pgid: v.provider_pgid as number | null,
    provider_start_token: typeof v.provider_start_token === "string" ? v.provider_start_token : null,
    hub_pid: v.hub_pid,
    hub_hostname: v.hub_hostname,
    hub_start_token: typeof v.hub_start_token === "string" ? v.hub_start_token : null,
    created_at: typeof v.created_at === "string" ? v.created_at : "",
  };
}

export interface RecordLeaseInput {
  home: string;
  session_id: string;
  facts: ProcessFacts;
  provider: string | null;
  transport: string | null;
  hubStartToken: string | null;
  now: () => Date;
  probes?: LeaseProbes;
  thisHostname?: string;
}

/**
 * Record spawn facts durably. An existing lease naming the same process is
 * an idempotent replay; any different process under the same session id, or
 * a corrupt lease file, is a real ownership collision — never overwritten.
 */
export async function recordWorkspaceLease(input: RecordLeaseInput): Promise<WorkspaceLeaseRecord> {
  const probes = input.probes ?? defaultLeaseProbes;
  const existing = await readLease(input.home, input.session_id);
  if (existing.status === "present") {
    const same =
      existing.lease.provider_pid === input.facts.pid &&
      existing.lease.provider_pgid === input.facts.pgid;
    if (same) {
      return existing.lease;
    }
    throw new AgentHubError(
      "WORKSPACE_LEASE_CONFLICT",
      `session "${input.session_id}" already has a lease for pid ${existing.lease.provider_pid}; refusing to overwrite ownership`,
    );
  }
  if (existing.status === "corrupt") {
    throw new AgentHubError(
      "WORKSPACE_LEASE_CONFLICT",
      `lease file for session "${input.session_id}" exists but is corrupt; ownership cannot be re-recorded over an unverifiable owner`,
    );
  }
  const lease: WorkspaceLeaseRecord = {
    schema: WORKSPACE_LEASE_SCHEMA,
    session_id: input.session_id,
    provider: input.provider,
    transport: input.transport,
    provider_pid: input.facts.pid,
    provider_pgid: Number.isInteger(input.facts.pgid) ? input.facts.pgid : null,
    // The provider's own start identity is captured at spawn; a failure to
    // read it is honest (`null`), which classification treats as uncertain.
    provider_start_token: await probes.startToken(input.facts.pid),
    hub_pid: process.pid,
    hub_hostname: input.thisHostname ?? hostname(),
    hub_start_token: input.hubStartToken,
    created_at: input.now().toISOString(),
  };
  await writeJsonAtomic(leasePath(input.home, input.session_id), lease);
  return lease;
}

export type LeaseRead =
  | { status: "absent" }
  | { status: "corrupt" }
  | { status: "present"; lease: WorkspaceLeaseRecord };

export async function readLease(home: string, sessionId: string): Promise<LeaseRead> {
  const raw = await readJsonFile(leasePath(home, sessionId));
  if (raw === undefined) {
    return { status: "absent" };
  }
  if (raw === null) {
    return { status: "corrupt" };
  }
  const lease = parseLease(raw);
  return lease ? { status: "present", lease } : { status: "corrupt" };
}

/**
 * Delete a lease file. Only reachable from paths that already PROVED the
 * provider dead (or never existed) under the custody lock — the proof check
 * lives in the callers (GC, recovery close), never here.
 */
export async function deleteLeaseFile(home: string, sessionId: string): Promise<void> {
  await removeFile(leasePath(home, sessionId));
}

export type ProviderFate =
  | { state: "dead" }
  | { state: "alive"; reapable: true }
  | { state: "uncertain"; reapable: false; reason: string };

export type LeaseClassification =
  | { state: "hub-live" }
  | { state: "foreign-host"; owner_hostname: string }
  | { state: "hub-gone"; provider: ProviderFate };

/**
 * Classify ownership evidence as it reads right now. See the module doc for
 * the proof rules; every `uncertain` branch blocks reaping and cleanup.
 */
export async function classifyLease(
  lease: WorkspaceLeaseRecord,
  probes: LeaseProbes = defaultLeaseProbes,
  thisHostname: string = hostname(),
  thisPid: number = process.pid,
): Promise<LeaseClassification> {
  if (lease.hub_hostname !== thisHostname) {
    return { state: "foreign-host", owner_hostname: lease.hub_hostname };
  }

  let provider: ProviderFate;
  if (probes.probePid(lease.provider_pid) === "dead") {
    const group = lease.provider_pgid;
    if (group === null) {
      provider = {
        state: "uncertain",
        reapable: false,
        reason: `provider pid ${lease.provider_pid} is gone but the lease records no process-group identity; helper survival cannot be probed`,
      };
    } else if (group !== lease.provider_pid) {
      provider = {
        state: "uncertain",
        reapable: false,
        reason: `provider pid ${lease.provider_pid} is gone and the recorded group ${group} is not the group the detached leader provably leads; group death is not proven`,
      };
    } else {
      const groupState = probes.probeGroup(group);
      if (groupState === "gone") {
        provider = { state: "dead" };
      } else if (groupState === "alive") {
        provider = {
          state: "uncertain",
          reapable: false,
          reason: `provider pid ${lease.provider_pid} is dead but process group ${group} still exists: helpers may still mutate the worktree`,
        };
      } else {
        provider = {
          state: "uncertain",
          reapable: false,
          reason: `provider pid ${lease.provider_pid} is dead but group ${group} cannot be probed (only ESRCH proves absence)`,
        };
      }
    }
  } else {
    const observedStart = await probes.startToken(lease.provider_pid);
    const identityMatched =
      lease.provider_start_token !== null &&
      observedStart !== null &&
      lease.provider_start_token === observedStart;
    if (identityMatched && lease.provider_pgid === lease.provider_pid) {
      provider = { state: "alive", reapable: true };
    } else if (identityMatched) {
      provider = {
        state: "uncertain",
        reapable: false,
        reason: `pid ${lease.provider_pid} is alive with a matching start identity, but the recorded group ${lease.provider_pgid} is not the group the detached leader provably leads`,
      };
    } else if (
      lease.provider_start_token !== null &&
      observedStart !== null &&
      lease.provider_start_token !== observedStart
    ) {
      provider = {
        state: "uncertain",
        reapable: false,
        reason: `pid ${lease.provider_pid} is alive but its start identity differs from the lease: the pid was reused; signalling it would hit an innocent process`,
      };
    } else {
      provider = {
        state: "uncertain",
        reapable: false,
        reason: `pid ${lease.provider_pid} is alive but its start identity cannot be matched to the lease`,
      };
    }
  }

  if (lease.hub_pid !== thisPid && probes.probePid(lease.hub_pid) === "live") {
    const observedHubStart = await probes.startToken(lease.hub_pid);
    if (
      lease.hub_start_token === null ||
      observedHubStart === null ||
      lease.hub_start_token === observedHubStart
    ) {
      return { state: "hub-live" };
    }
    // Live pid with a mismatched start token is pid reuse: bookkeeping says
    // the owning hub is gone; the provider's own fate (computed above) still
    // gates every action.
  }

  return { state: "hub-gone", provider };
}

export type ReapOutcome =
  | { status: "reaped"; waited_ms: number }
  | { status: "survived"; waited_ms: number }
  | { status: "not-attempted"; reason: string };

function sleep(ms: number): Promise<void> {
  const { promise, resolve } = deferred<void>();
  setTimeout(resolve, ms);
  return promise;
}

/**
 * Terminate an orphaned provider group ONLY when classification says the
 * lease provably owns it (`alive` + `reapable` + leader-led group).
 * SIGTERM, bounded wait, one SIGKILL escalation, then a group re-probe;
 * survival is reported honestly, never assumed reaped.
 */
export async function reapProviderLease(
  lease: WorkspaceLeaseRecord,
  fate: ProviderFate,
  probes: LeaseProbes = defaultLeaseProbes,
  options: { graceMs?: number; pollMs?: number } = {},
): Promise<ReapOutcome> {
  const graceMs = options.graceMs ?? 5_000;
  const pollMs = options.pollMs ?? 50;
  if (fate.state !== "alive" || !fate.reapable || lease.provider_pgid !== lease.provider_pid) {
    return {
      status: "not-attempted",
      reason: "reaping requires an exact-identity, leader-led group the lease provably owns",
    };
  }
  const started = Date.now();
  probes.killGroup(lease.provider_pgid, "SIGTERM");
  const escalated = started + graceMs;
  let killed = false;
  while (Date.now() - started < graceMs * 4) {
    if (probes.probeGroup(lease.provider_pgid) === "gone") {
      return { status: "reaped", waited_ms: Date.now() - started };
    }
    if (!killed && Date.now() >= escalated) {
      probes.killGroup(lease.provider_pgid, "SIGKILL");
      killed = true;
    }
    await sleep(pollMs);
  }
  return probes.probeGroup(lease.provider_pgid) === "gone"
    ? { status: "reaped", waited_ms: Date.now() - started }
    : { status: "survived", waited_ms: Date.now() - started };
}
