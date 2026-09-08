import { realpathSync } from "node:fs";

import { AgentHubError } from "../errors.js";
import { resolveRepositoryIdentity } from "../git.js";
import { AgentHub, HUB_PROCESS_SESSION_QUOTA, type AgentHubOptions } from "./agent-hub.js";

/**
 * Process-level hub supervisor for hub hosts (MCP server, long-lived CLIs).
 *
 * Two quotas matter: `HUB_PROCESS_SESSION_QUOTA` live sessions per process
 * and a durable lease quota per Git common dir. If one host built a fresh
 * `AgentHub` per workspace request, the process quota would silently
 * multiply by the number of repositories served. The supervisor makes the
 * process quota true for the whole process:
 *
 *   - hubs are CACHED per canonical Git common dir, so every workspace of
 *     one repository (linked worktrees included) reuses the exact hub that
 *     owns its sessions and transports;
 *   - every launch reserves a process-wide slot BEFORE the hub call and
 *     releases it when the call settles;
 *   - an idle hub is retired from the cache — durable state and the lease,
 *     not the cache, arbitrate ownership across processes.
 *
 * Mirrors the proven `live` supervisor at the public integration layer;
 * the per-common-dir lease quota stays owned by the lifecycle.
 */

export type HubOpen = (workspace: string, options: AgentHubOptions) => Promise<AgentHub>;

function canonicalCommonDir(commonDir: string): string {
  try {
    return realpathSync(commonDir);
  } catch {
    return commonDir;
  }
}

export class AgentHubSupervisor {
  private readonly ready = new Map<string, AgentHub>();
  private inFlight = 0;

  constructor(private readonly processQuota: number = HUB_PROCESS_SESSION_QUOTA) {}

  get activeCount(): number {
    let total = 0;
    for (const hub of this.ready.values()) total += hub.activeCount;
    return total;
  }

  get cachedHubs(): number {
    return this.ready.size;
  }

  /** The hub owning `workspace`'s repository, built once per common dir. */
  async hubFor(
    workspace: string,
    options: AgentHubOptions = {},
    open: HubOpen = (path, hubOptions) => AgentHub.open(path, hubOptions),
  ): Promise<AgentHub> {
    // Resolve the identity before constructing a fresh hub. Constructing one
    // first would run its automatic recovery pass; on every subsequent MCP
    // command that could misclassify a live provider owned by the cached hub
    // (especially when a probe cannot see the provider's PID) and close its
    // custody before the cached hub handles the command.
    const identity = await resolveRepositoryIdentity(workspace);
    const key = canonicalCommonDir(identity.common_dir);
    const cached = this.ready.get(key);
    if (cached !== undefined) {
      return cached;
    }
    const hub = await open(workspace, options);
    const openedKey = canonicalCommonDir(hub.commonDir);
    const existing = this.ready.get(openedKey);
    if (existing !== undefined) {
      await hub.settle();
      return existing;
    }
    this.ready.set(openedKey, hub);
    return hub;
  }

  /** Run one launch (start or resume) under the process-wide slot. */
  async launch<T>(hub: AgentHub, run: () => Promise<T>): Promise<T> {
    if (this.activeCount + this.inFlight >= this.processQuota) {
      throw new AgentHubError(
        "QUOTA_EXCEEDED",
        `this hub process already runs ${this.activeCount} sessions with ${this.inFlight} launching (quota ${this.processQuota})`,
      );
    }
    this.inFlight += 1;
    try {
      return await run();
    } finally {
      this.inFlight -= 1;
      this.retireIdle(hub);
    }
  }

  /** Drop a cached hub once it runs nothing and no launch is in flight. */
  retireIdle(hub: AgentHub): void {
    if (hub.activeCount > 0 || this.inFlight > 0) return;
    for (const [key, cached] of this.ready) {
      if (cached === hub) this.ready.delete(key);
    }
  }

  /** Host shutdown: close everything this process owns, then forget. */
  async closeAll(): Promise<void> {
    for (const hub of [...this.ready.values()]) {
      await hub.closeAll().catch(() => undefined);
    }
    this.ready.clear();
  }
}

/** The one supervisor shared by every hub host in this process. */
export const processHubSupervisor = new AgentHubSupervisor();
