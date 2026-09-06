import { realpathSync } from "node:fs";

import { AgentHubError } from "../errors.js";
import { resolveRepositoryIdentity } from "../git.js";
import { LIVE_PROCESS_SESSION_QUOTA, type LiveSessionManager } from "./manager.js";

/**
 * Process-level live supervisor for the MCP surface.
 *
 * The core manager enforces two quotas: `LIVE_PROCESS_SESSION_QUOTA` per
 * manager instance and `LIVE_COMMON_DIR_SESSION_QUOTA` durable leases per Git
 * common dir. If one MCP process built a fresh manager per
 * `live_session_start`, the process quota would silently multiply by the
 * number of repositories it served. The supervisor makes the process quota
 * true for the whole process:
 *
 *   - managers are CACHED per canonical Git common dir, so every workspace
 *     of one repository (including linked worktrees and repeat calls) reuses
 *     the exact manager that owns its sessions and transports;
 *   - every launch (start or resume) reserves a process-wide slot BEFORE the
 *     manager call and releases it when the call settles, so concurrent
 *     launches across different managers can never race the shared count;
 *   - a manager with nothing running on it is retired from the cache (the
 *     total quota and durable state never depend on it staying), so the
 *     cache does not leak managers across a long-lived hub process. A caller
 *     holding a session id routes by its own recorded reference, so retiring
 *     an idle manager never strands a live session.
 *
 * The per-common-dir quota stays owned by the manager — it is durable-lease
 * based and must keep working across processes; the supervisor never
 * re-implements or relaxes it.
 */

export type LiveManagerFactory = (workspace: string) => Promise<LiveSessionManager>;

/** Canonical cache key: realpath'd Git common dir, shared by every worktree. */
function canonicalCommonDir(commonDir: string): string {
  try {
    return realpathSync(commonDir);
  } catch {
    // Unreadable path — the raw value is the best honest key; two spellings
    // of the same common dir would then be distinct keys, never a wrong one.
    return commonDir;
  }
}

export class LiveSessionSupervisor {
  private readonly ready = new Map<string, LiveSessionManager>();
  private readonly pending = new Map<string, Promise<LiveSessionManager>>();
  /** Launches reserved but not yet registered as a manager session. */
  private inFlight = 0;

  constructor(private readonly processQuota: number = LIVE_PROCESS_SESSION_QUOTA) {}

  /** Live sessions owned by every manager this supervisor caches. */
  get activeCount(): number {
    let total = 0;
    for (const manager of this.ready.values()) {
      total += manager.activeCount;
    }
    return total;
  }

  /** Cached managers (settled or being built) — observable for leak checks. */
  get cachedManagers(): number {
    return this.ready.size + this.pending.size;
  }

  /**
   * The manager owning `workspace`'s repository, built once per canonical
   * Git common dir and shared by every caller of this process afterwards.
   * Concurrent requests for the same repository share exactly one build.
   */
  async managerFor(workspace: string, create: LiveManagerFactory): Promise<LiveSessionManager> {
    const identity = await resolveRepositoryIdentity(workspace);
    const key = canonicalCommonDir(identity.common_dir);
    const ready = this.ready.get(key);
    if (ready) {
      return ready;
    }
    const pending = this.pending.get(key);
    if (pending) {
      return await pending;
    }
    const created = create(workspace);
    this.pending.set(key, created);
    try {
      const manager = await created;
      if (this.pending.get(key) === created) {
        this.pending.delete(key);
        this.ready.set(key, manager);
      }
      return manager;
    } catch (error) {
      if (this.pending.get(key) === created) {
        this.pending.delete(key);
      }
      throw error;
    }
  }

  /**
   * Run one launch (start or resume) under the process-wide slot. The check
   * and the reservation share a synchronous step, so concurrent launches
   * cannot both pass the last slot. `run` must route through `manager`; the
   * reserved slot converts to a counted session when the launch registers,
   * and returns here when it fails.
   */
  async launch<T>(manager: LiveSessionManager, run: () => Promise<T>): Promise<T> {
    const active = this.activeCount;
    if (active + this.inFlight >= this.processQuota) {
      // The refused launch built nothing durable; do not leave its empty
      // manager in the cache either.
      this.retireIdle(manager);
      throw new AgentHubError(
        "LIVE_QUOTA_EXCEEDED",
        `this hub process already owns ${active} live sessions with ${this.inFlight} launches in flight ` +
          `(process quota ${this.processQuota}); close a session before starting another`,
      );
    }
    this.inFlight += 1;
    try {
      return await run();
    } finally {
      this.inFlight -= 1;
      this.retireIdle(manager);
    }
  }

  /**
   * Drop a cached manager once it runs nothing and no launch is in flight.
   * Safe at any observation point: an idle manager owns no session, and the
   * durable lease (not this cache) arbitrates across processes.
   */
  retireIdle(manager: LiveSessionManager): void {
    if (this.inFlight > 0 || manager.activeCount > 0) {
      return;
    }
    for (const [key, candidate] of this.ready) {
      if (candidate === manager) {
        this.ready.delete(key);
      }
    }
  }
}

/** The one supervisor shared by every hub server in this process. */
export const processLiveSupervisor = new LiveSessionSupervisor();
