import { randomUUID } from "node:crypto";
import { mkdir, readFile, readdir, rename, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";

import { AgentHubError, asDelegateError } from "../errors.js";
import { assertCleanUnlessAllowed } from "../execution.js";
import { resolveRepositoryIdentity, runGit } from "../git.js";
import { acquireRepositoryLock, type RepositoryLock } from "../locks.js";
import type { ProcessFacts, ResumeState, SessionRecord } from "../kernel/contracts.js";
import { parseSessionRecord } from "../kernel/contracts.js";
import {
  defaultLiveLeaseProbes,
  hubProcessStartToken,
  createLiveLease,
  readLiveLease,
  listLiveLeases,
  removeLiveLease,
  updateLiveLeaseProvider,
  classifyLiveLease,
  reapOrphanedProvider,
  type LiveLeaseProbes,
  type LiveLeaseRecord,
} from "../live/lease.js";
import { DEFAULT_MAX_TEXT_BYTES } from "../kernel/interaction-kernel.js";
import { kernelErrorOf, verificationOf } from "./transport-adapter.js";
import {
  captureLiveCheckpoint,
  createLiveWorktree,
  describeLiveCheckpointChain,
  inspectLiveWorktree,
  pruneLiveWorktrees,
  removeLiveWorktree,
  type LiveWorktree,
} from "../live/worktree.js";
import {
  applyLiveTransition,
  LIVE_ADMIN_LOCK_NAME,
  LIVE_SCHEMA_VERSION,
  liveRefFor,
  liveStateRoot,
  loadLiveState,
  newLiveSessionId,
  TERMINAL_STATUSES,
  withLiveLock,
} from "../live/state.js";
import { LIVE_COMMON_DIR_SESSION_QUOTA } from "../live/manager.js";
import { validateLiveCapabilities } from "../live/provider-registry.js";
import type { RepositoryIdentity } from "../types.js";
import type {
  CheckpointReason,
  LiveCheckpoint,
  LiveError,
  LiveProviderId,
  LiveSessionState,
  LiveStatus,
  LiveTransportId,
  ProviderResumeState,
} from "../live/types.js";

/**
 * WorkspaceLifecycle — the public durable/workspace companion of the
 * InteractionKernel (P4 composition of the P2/P3 primitives).
 *
 * The kernel owns interaction and never touches Git; this class owns
 * everything Git- and OS-anchored around a session, composed ONLY through
 * the exported primitives (worktree/checkpoint chain, live-ref CAS state
 * store, lifetime lease, lease-classified recovery):
 *
 *   - launch reservation: quota → prune → worktree at a captured base →
 *     exclusive lease, all under the short live-admin lock;
 *   - the durable pair: the kernel's Git-free `SessionRecord` rides a
 *     sidecar under the Git common dir, and every mirror commit projects
 *     onto the lifecycle's own state record advanced through the
 *     sidecar/CAS transition — checkpoint commits, status, merged resume
 *     handles, and last error;
 *   - checkpoint pinning at terminal boundaries (turn end, cancel, error,
 *     close) on the session's private ref;
 *   - close authorization: teardown (worktree removal, lease release) only
 *     after shutdown is PROVEN; an unproven stop retains everything and
 *     rewrites the record to `orphaned`;
 *   - reconciliation (`gc`): re-prove every lease, reap provably-orphaned
 *     provider groups, pin surviving worktrees as `crash_recovery`
 *     checkpoints, rewrite to `orphaned`, release leases last — anything
 *     unprovable is reported, never touched;
 *   - handoff: the checkpoint chain is the artifact; adoption stays a
 *     human `git cherry-pick` away, never an automatic merge.
 */

export const HUB_SESSION_QUOTA = LIVE_COMMON_DIR_SESSION_QUOTA;
export const HUB_REF_NAMESPACE = "refs/agent-hub/live";

/** Durable-ordering seam for tests and integrations. */
export type LifecyclePhase = "checkpoint-captured" | "state-advanced" | "provider-reaped";

export interface WorkspaceLifecycleOptions {
  now?: () => Date;
  tmpRoot?: string;
  acquireLock?: typeof acquireRepositoryLock;
  probes?: LiveLeaseProbes;
  /** Durable leases allowed per Git common dir. */
  commonDirQuota?: number;
  /** Busy-wait window for the per-session state lock. */
  lockWaitMs?: number;
  observePhase?: (phase: LifecyclePhase) => Promise<void> | void;
}

interface ResolvedOptions extends WorkspaceLifecycleOptions {
  now: () => Date;
  commonDirQuota: number;
  lockWaitMs: number;
}

/** Resources reserved for one launch, held by the hub across `kernel.start`. */
export interface PreparedLaunch {
  session_id: string;
  lease: LiveLeaseRecord;
  worktree: LiveWorktree;
  warnings: { code: string; message: string }[];
}

/** A session whose durable pair is owned in-process. */
interface ManagedWorkspace {
  lease: LiveLeaseRecord;
  worktree: LiveWorktree;
  state: LiveSessionState;
  /** One durable write at a time, in issue order (mirrors + checkpoints). */
  durableTail: Promise<unknown>;
}

export interface LaunchRegistration {
  session_id: string;
  provider: LiveProviderId;
  transport: LiveTransportId;
  identity: RepositoryIdentity;
  base: string;
  capabilities: unknown;
  resume: ProviderResumeState | null;
  max_text_bytes: number;
  prepared: PreparedLaunch;
  /** When set, this launch continues the existing state record (resume). */
  continues?: { prior_state: LiveSessionState };
}

export interface ReconcileSessionReport {
  session_id: string;
  outcome: "recovered" | "cleaned" | "kept-live" | "foreign" | "manual" | "released" | "dry-run";
  detail: string;
}

export interface ReconcileReport {
  scanned: number;
  worktrees_pruned: boolean;
  sessions: ReconcileSessionReport[];
}

export interface HandoffDocument {
  session_id: string;
  provider: string;
  transport: string;
  status: LiveStatus;
  ref: string;
  base_commit: string;
  final_commit: string;
  checkpoints: LiveCheckpoint[];
  changed_files: string[];
  diff_stat: string;
  apply_hint: string;
  warning: string | null;
}

const RECORDS_SUBDIR = join("interaction-records");

function isUuid(value: string): boolean {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(value);
}

export class WorkspaceLifecycle {
  private readonly options: ResolvedOptions;
  private readonly managed = new Map<string, ManagedWorkspace>();
  /** Tails for sessions launched-but-not-yet-registered (kernel start window). */
  private readonly launchTails = new Map<string, Promise<unknown>>();

  private constructor(
    readonly repositoryCwd: string,
    readonly commonDir: string,
    readonly identity: RepositoryIdentity,
    options: WorkspaceLifecycleOptions,
  ) {
    this.options = {
      ...options,
      now: options.now ?? (() => new Date()),
      commonDirQuota: options.commonDirQuota ?? HUB_SESSION_QUOTA,
      lockWaitMs: options.lockWaitMs ?? 30_000,
    };
  }

  /** Resolve the repository identity and bind durable state to its common dir. */
  static async open(workspace: string, options: WorkspaceLifecycleOptions = {}): Promise<WorkspaceLifecycle> {
    const identity = await resolveRepositoryIdentity(workspace);
    return new WorkspaceLifecycle(workspace, identity.common_dir, identity, options);
  }

  newSessionId(): string {
    return newLiveSessionId();
  }

  private now(): Date {
    return this.options.now();
  }

  // ---------------------------------------------------------------------------
  // Launch reservation and release (mirrors the proven live-core discipline)
  // ---------------------------------------------------------------------------

  /**
   * Quotas + prune + worktree + exclusive lease under the short live-admin
   * lock — never held across a transport launch or a session lifetime.
   */
  async reserveLaunchResources(
    sessionId: string,
    provider: LiveProviderId,
    base: string,
    options: { allowDirty?: boolean } = {},
  ): Promise<PreparedLaunch> {
    await assertCleanUnlessAllowed(this.repositoryCwd, options.allowDirty);
    const adminLock = await this.acquireAdminLock();
    const warnings: { code: string; message: string }[] = [];
    let worktree: LiveWorktree | null = null;
    try {
      const leases = await listLiveLeases(this.commonDir);
      if (leases.length >= this.options.commonDirQuota) {
        throw new AgentHubError(
          "QUOTA_EXCEEDED",
          `this Git common dir already has ${leases.length} hub leases (quota ${this.options.commonDirQuota}); run \`agent-hub gc\` to reclaim provably-orphaned sessions first`,
        );
      }
      await pruneLiveWorktrees(this.repositoryCwd);
      worktree = await createLiveWorktree(this.repositoryCwd, base, this.options.tmpRoot);
      const lease = await createLiveLease({
        commonDir: this.commonDir,
        live_session_id: sessionId,
        provider,
        worktree_path: worktree.path,
        provider_pid: null,
        provider_pgid: null,
        provider_start_token: null,
        hub_start_token: await hubProcessStartToken(this.probes()),
        now: () => this.now(),
      });
      return { session_id: sessionId, lease, worktree, warnings };
    } catch (error) {
      if (worktree !== null) {
        const removal = await removeLiveWorktree(this.repositoryCwd, worktree);
        if (removal.cleanup_error) {
          throw new AgentHubError(
            "WORKTREE_RETAINED",
            `${asDelegateError(error).message}; the fresh worktree at ${worktree.path} could not be removed (${removal.cleanup_error.message}) and its lease stays as the audit trail`,
          );
        }
      }
      throw error;
    } finally {
      try {
        await adminLock.release();
      } catch (releaseError) {
        warnings.push({
          code: "ADMIN_LOCK_RELEASE_FAILED",
          message: `launch resources were claimed but the admin lock was not released cleanly: ${asDelegateError(releaseError).message}`,
        });
      }
    }
  }

  /** Patch spawn facts onto the launch lease the moment the transport spawns. */
  async recordSpawn(prepared: PreparedLaunch, facts: ProcessFacts): Promise<void> {
    prepared.lease = await updateLiveLeaseProvider(
      this.commonDir,
      prepared.lease,
      {
        provider_pid: facts.pid,
        provider_pgid: facts.pgid,
        provider_start_token: await this.probes().startToken(facts.pid),
      },
      () => this.now(),
    );
  }

  /**
   * Worktree removal ONLY under the admin lock, then lease release. If the
   * lock cannot be had, no worktree mutation runs at all — the lease stays
   * as the audit trail for whatever survives.
   */
  async releaseLaunchResources(prepared: PreparedLaunch): Promise<{ code: string; message: string }[]> {
    const errors: { code: string; message: string }[] = [];
    const adminLock = await this.acquireAdminLock().catch((error: unknown) => {
      errors.push({
        code: "ADMIN_LOCK_UNAVAILABLE",
        message:
          `the admin lock could not be acquired, so no worktree mutation was attempted: ` +
          `${asDelegateError(error).message}; the worktree at ${prepared.worktree.path} and its lease are retained`,
      });
      return null;
    });
    if (adminLock === null) return errors;
    try {
      const removal = await removeLiveWorktree(this.repositoryCwd, prepared.worktree);
      if (removal.cleanup_error) errors.push(removal.cleanup_error);
    } finally {
      try {
        await adminLock.release();
      } catch (releaseError) {
        errors.push({
          code: "ADMIN_LOCK_RELEASE_FAILED",
          message: `worktree operations finished but the admin lock release failed: ${asDelegateError(releaseError).message}`,
        });
      }
    }
    if (errors.length > 0) return errors;
    try {
      await removeLiveLease(this.commonDir, prepared.lease.live_session_id, prepared.lease.token);
    } catch (error) {
      errors.push(asDelegateError(error));
    }
    return errors;
  }

  private async acquireAdminLock(): Promise<RepositoryLock> {
    return (this.options.acquireLock ?? acquireRepositoryLock)({
      commonDir: this.commonDir,
      name: LIVE_ADMIN_LOCK_NAME,
      waitMs: 30_000,
    });
  }

  private probes(): LiveLeaseProbes {
    return this.options.probes ?? defaultLiveLeaseProbes;
  }

  // ---------------------------------------------------------------------------
  // Session registration and the durable pair
  // ---------------------------------------------------------------------------

  /** Commit the create/resume state transition and start managing the session. */
  async register(registration: LaunchRegistration): Promise<LiveSessionState> {
    const { prepared, continues } = registration;
    const createdAt = continues
      ? continues.prior_state.created_at
      : this.now().toISOString();
    const state: LiveSessionState = {
      schema: LIVE_SCHEMA_VERSION,
      live_session_id: prepared.session_id,
      session_id: null,
      provider: registration.provider,
      transport: registration.transport,
      capabilities: validateLiveCapabilities(registration.capabilities),
      identity: registration.identity,
      base_commit: continues ? continues.prior_state.base_commit : registration.base,
      current_commit: continues ? continues.prior_state.current_commit : registration.base,
      checkpoint_seq: continues ? continues.prior_state.checkpoint_seq : 0,
      last_checkpoint_reason: continues
        ? continues.prior_state.last_checkpoint_reason
        : null,
      worktree_path: prepared.worktree.path,
      worktree_parent: prepared.worktree.parentPath,
      resume: registration.resume,
      // `open` resolved ⇒ the transport is ready to accept commands.
      status: "idle",
      revision: continues ? continues.prior_state.revision + 1 : 1,
      last_error: null,
      created_at: createdAt,
      updated_at: this.now().toISOString(),
    };
    await this.withSessionLock(prepared.session_id, async () => {
      if (continues) {
        await applyLiveTransition(
          { commonDir: this.commonDir, repositoryCwd: this.repositoryCwd },
          {
            kind: "advance",
            live_session_id: prepared.session_id,
            ref: liveRefFor(prepared.session_id),
            expected_ref: state.current_commit,
            new_commit: state.current_commit,
            next_state: state,
          },
        );
      } else {
        await applyLiveTransition(
          { commonDir: this.commonDir, repositoryCwd: this.repositoryCwd },
          {
            kind: "create",
            live_session_id: prepared.session_id,
            ref: liveRefFor(prepared.session_id),
            expected_ref: null,
            new_commit: state.current_commit,
            next_state: state,
          },
        );
      }
      await this.options.observePhase?.("state-advanced");
    });
    this.managed.set(prepared.session_id, {
      lease: prepared.lease,
      worktree: prepared.worktree,
      state,
      durableTail: Promise.resolve(),
    });
    return state;
  }

  unregister(sessionId: string): void {
    this.managed.delete(sessionId);
  }

  isManaged(sessionId: string): boolean {
    return this.managed.has(sessionId);
  }

  /** Project every kernel mirror commit: sidecar record + state advance. */
  async commitMirrorRecord(record: SessionRecord): Promise<void> {
    await this.enqueueDurable(record.session_id, async () => {
      await this.writeRecordSidecar(record);
      const managed = this.managed.get(record.session_id);
      if (managed === undefined) {
        // The kernel commits its launch record before the lifecycle create
        // transition exists; the create/resume transition carries it.
        return;
      }
      const locked = await this.withSessionLock(record.session_id, async () => {
        const current = await this.loadState(record.session_id);
        const next: LiveSessionState = {
          ...current,
          status: record.status,
          resume: mergeKernelResume(current.resume, record.resume),
          last_error: record.last_error as LiveError | null,
          revision: current.revision + 1,
          updated_at: record.updated_at,
        };
        await applyLiveTransition(
          { commonDir: this.commonDir, repositoryCwd: this.repositoryCwd },
          {
            kind: "advance",
            live_session_id: record.session_id,
            ref: liveRefFor(record.session_id),
            expected_ref: current.current_commit,
            new_commit: current.current_commit,
            next_state: next,
          },
        );
        return next;
      });
      managed.state = locked.value;
      await this.options.observePhase?.("state-advanced");
    });
  }

  /**
   * Pin the worktree (only when its tree actually changed) and commit chain
   * + status in one sidecar-guarded CAS. Returns the checkpoint when the
   * chain moved, null when there was nothing new to pin.
   */
  async captureCheckpoint(
    sessionId: string,
    reason: CheckpointReason,
    options: { statusOverride?: LiveStatus; lastError?: LiveError | null } = {},
  ): Promise<LiveCheckpoint | null> {
    return this.enqueueDurable(sessionId, async () => {
      const managed = this.managed.get(sessionId);
      const locked = await this.withSessionLock(sessionId, async () => {
        const current = await this.loadState(sessionId);
        const capture = await captureLiveCheckpoint(
          managed?.worktree.path ?? current.worktree_path,
          current.current_commit,
          reason,
          { seq: current.checkpoint_seq + 1, now: () => this.now() },
        );
        await this.options.observePhase?.("checkpoint-captured");
        const advanced = capture.advanced;
        const next: LiveSessionState = {
          ...current,
          current_commit: advanced ? capture.checkpoint.commit : current.current_commit,
          checkpoint_seq: advanced ? current.checkpoint_seq + 1 : current.checkpoint_seq,
          // The reason belongs to the chain head: only a capture that pinned
          // a new commit records its reason.
          last_checkpoint_reason: advanced ? reason : current.last_checkpoint_reason,
          status: options.statusOverride ?? managed?.state.status ?? current.status,
          revision: current.revision + 1,
          last_error:
            options.lastError !== undefined ? options.lastError : current.last_error,
          updated_at: this.now().toISOString(),
        };
        await applyLiveTransition(
          { commonDir: this.commonDir, repositoryCwd: this.repositoryCwd },
          {
            kind: "advance",
            live_session_id: sessionId,
            ref: liveRefFor(sessionId),
            expected_ref: current.current_commit,
            new_commit: next.current_commit,
            next_state: next,
          },
        );
        return { next, checkpoint: advanced ? capture.checkpoint : null };
      });
      if (managed !== undefined) managed.state = locked.value.next;
      await this.options.observePhase?.("state-advanced");
      return locked.value.checkpoint;
    });
  }

  /** After a PROVEN close: final checkpoint, state advance, teardown, release. */
  async finalizeClosed(sessionId: string): Promise<{
    checkpoint_taken: boolean;
    cleanup_errors: { code: string; message: string }[];
  }> {
    const managed = this.mustManaged(sessionId);
    const checkpoint = await this.captureCheckpoint(sessionId, "close", {
      statusOverride: "closed",
    });
    const cleanup_errors = await this.releaseLaunchResources({
      session_id: sessionId,
      lease: managed.lease,
      worktree: managed.worktree,
      warnings: [],
    });
    this.unregister(sessionId);
    return { checkpoint_taken: checkpoint !== null, cleanup_errors };
  }

  /**
   * After an UNPROVEN stop: the kernel mirror already advanced the state to
   * `orphaned`; retain lease + worktree, drop in-process management, and
   * leave ownership facts for `gc`.
   */
  retainOrphan(sessionId: string): void {
    this.unregister(sessionId);
  }

  /**
   * Launch-failure release: resources may be given back only when the
   * lease's own provider facts prove the process is gone — a never-spawned
   * lease is provably idle by construction, a spawned one only when the
   * leader is dead AND the group it provably leads (pgid == pid, the
   * detached-launch invariant) answers ESRCH. Anything less retains the
   * lease and worktree for `gc` or manual cleanup.
   */
  async releaseIfProviderProvenGone(
    prepared: PreparedLaunch,
  ): Promise<{ released: boolean; retained_reason: string | null }> {
    const lease = prepared.lease;
    if (lease.provider_pid !== null) {
      const probes = this.probes();
      const leaderGone = probes.probePid(lease.provider_pid) === "dead";
      const group: "alive" | "gone" | "uncertain" =
        lease.provider_pgid === lease.provider_pid
          ? probes.probeGroup(lease.provider_pgid)
          : "uncertain";
      if (!leaderGone || group !== "gone") {
        return {
          released: false,
          retained_reason:
            `the provider process (pid ${lease.provider_pid}, group ${lease.provider_pgid ?? "none"}) is not provably gone; ` +
            `the lease and the worktree at ${prepared.worktree.path} are retained for \`agent-hub gc\` or manual cleanup`,
        };
      }
    }
    const errors = await this.releaseLaunchResources(prepared);
    return errors.length === 0
      ? { released: true, retained_reason: null }
      : { released: false, retained_reason: errors.map((error) => error.message).join("; ") };
  }

  // ---------------------------------------------------------------------------
  // Durable reads
  // ---------------------------------------------------------------------------

  async loadState(sessionId: string): Promise<LiveSessionState> {
    return loadLiveState({
      commonDir: this.commonDir,
      repositoryCwd: this.repositoryCwd,
      liveSessionId: sessionId,
    });
  }

  async loadMirrorRecord(sessionId: string): Promise<SessionRecord | null> {
    try {
      const raw = await readFile(this.recordSidecarPath(sessionId), "utf8");
      return parseSessionRecord(JSON.parse(raw) as unknown);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        // No sidecar: fall back to a projection of the state record.
        const state = await this.loadState(sessionId);
        return projectRecordFromState(state);
      }
      throw error;
    }
  }

  async leaseFor(sessionId: string): Promise<LiveLeaseRecord | undefined> {
    return readLiveLease(this.commonDir, sessionId);
  }

  /** Every durable session in this common dir, newest record first. */
  async listStates(): Promise<LiveSessionState[]> {
    const root = join(liveStateRoot(this.commonDir), "sessions");
    let names: string[];
    try {
      names = await readdir(root);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
      throw error;
    }
    const states: LiveSessionState[] = [];
    for (const name of names) {
      if (!name.endsWith(".json") || name.endsWith(".pending.json")) continue;
      const candidate = name.slice(0, -".json".length);
      if (!isUuid(candidate)) continue;
      try {
        states.push(await this.loadState(candidate));
      } catch {
        // A pending replay or corrupt record surfaces via `gc`, not `list`.
      }
    }
    return states.sort((a, b) => b.updated_at.localeCompare(a.updated_at));
  }

  // ---------------------------------------------------------------------------
  // Resume rebind
  // ---------------------------------------------------------------------------

  /**
   * Rewrite the durable mirror record so `kernel.resume()` reopens at the
   * freshly materialized worktree with the durable resume handle. The
   * workspace root is a lifecycle handle; the kernel's resume boundary
   * accepts it verbatim.
   */
  async rebindResumeRecord(
    sessionId: string,
    worktreePath: string,
  ): Promise<SessionRecord> {
    return this.enqueueDurable(sessionId, async () => {
      const state = await this.loadState(sessionId);
      if (!TERMINAL_STATUSES.includes(state.status)) {
        throw new AgentHubError(
          "SESSION_NOT_RESUMABLE",
          `session "${sessionId}" is "${state.status}"; run \`agent-hub gc\` to reconcile first — only terminal records resume`,
        );
      }
      const prior = (await this.loadMirrorRecord(sessionId)) ?? projectRecordFromState(state);
      const record: SessionRecord = {
        ...prior,
        workspace: worktreePath,
        resume: state.resume === null ? null : kernelResumeFromState(state.resume),
      };
      await this.writeRecordSidecar(record);
      return record;
    });
  }

  // ---------------------------------------------------------------------------
  // Handoff
  // ---------------------------------------------------------------------------

  /** The checkpoint chain as the artifact; adoption is a human command away. */
  async handoff(sessionId: string): Promise<HandoffDocument> {
    const state = await this.loadState(sessionId);
    const lease = await this.leaseFor(sessionId);
    if (lease !== undefined) {
      throw new AgentHubError(
        "SESSION_STILL_OWNED",
        `session "${sessionId}" still holds a lease (${this.isManaged(sessionId) ? "this process owns it — close it first" : "run 'agent-hub gc' first"}); handoff requires a terminal, released session`,
      );
    }
    const ref = liveRefFor(sessionId);
    const chain = await describeLiveCheckpointChain(this.repositoryCwd, ref, state.base_commit);
    let changedFiles: string[] = [];
    let diffStat = "";
    if (state.current_commit !== state.base_commit) {
      const status = await runGit(this.repositoryCwd, [
        "diff",
        "--name-only",
        state.base_commit,
        state.current_commit,
      ]);
      changedFiles = status.stdout.split("\n").filter(Boolean);
      const stat = await runGit(this.repositoryCwd, [
        "diff",
        "--stat",
        state.base_commit,
        state.current_commit,
      ]);
      diffStat = stat.stdout.trim();
    }
    return {
      session_id: sessionId,
      provider: state.provider,
      transport: state.transport,
      status: state.status,
      ref,
      base_commit: state.base_commit,
      final_commit: state.current_commit,
      checkpoints: chain,
      changed_files: changedFiles,
      diff_stat: diffStat,
      apply_hint:
        state.current_commit === state.base_commit
          ? "the session pinned no changes; there is nothing to adopt"
          : `review, then adopt with: git cherry-pick ${state.base_commit}..${ref}`,
      warning:
        state.status === "orphaned"
          ? "the session ended orphaned; the final checkpoint is crash-recovery state, not an orderly close"
          : null,
    };
  }

  // ---------------------------------------------------------------------------
  // Reconciliation (safe GC)
  // ---------------------------------------------------------------------------

  /**
   * Reconcile every durable lease with what the OS and the repository prove.
   * Liveness is re-proven or the status is rewritten to `orphaned`; an
   * orphaned provider is reaped before the `crash_recovery` checkpoint, the
   * checkpoint before the state rewrite, the rewrite before teardown. A
   * classification this hub cannot prove is reported, never acted on.
   * Finally, terminal sessions whose worktrees outlived their release get a
   * removal retry and the admin worktree prune runs once.
   */
  async reconcile(
    attachedIds: ReadonlySet<string>,
    options: { dryRun?: boolean } = {},
  ): Promise<ReconcileReport> {
    const leases = await listLiveLeases(this.commonDir);
    const report: ReconcileReport = { scanned: leases.length, worktrees_pruned: false, sessions: [] };

    for (const listed of leases) {
      const sessionId = listed.live_session_id;
      if (listed.record === null) {
        report.sessions.push({
          session_id: sessionId,
          outcome: "manual",
          detail: `lease file ${listed.path} is corrupt; refusing to guess ownership`,
        });
        continue;
      }
      const lease = listed.record;
      if (attachedIds.has(sessionId) || this.managed.has(sessionId)) {
        report.sessions.push({
          session_id: sessionId,
          outcome: "kept-live",
          detail: "this hub process owns the session",
        });
        continue;
      }

      const classification = await classifyLiveLease(lease, this.probes());
      if (classification.state === "foreign-host") {
        report.sessions.push({
          session_id: sessionId,
          outcome: "foreign",
          detail: `owned by host ${classification.owner_hostname}`,
        });
        continue;
      }
      if (classification.state === "hub-live") {
        report.sessions.push({
          session_id: sessionId,
          outcome: "kept-live",
          detail: `hub pid ${lease.hub_pid} is alive with a matching process identity`,
        });
        continue;
      }
      const provider = classification.provider;
      if (provider.state === "uncertain") {
        report.sessions.push({ session_id: sessionId, outcome: "manual", detail: provider.reason });
        continue;
      }
      if (options.dryRun) {
        report.sessions.push({
          session_id: sessionId,
          outcome: "dry-run",
          detail:
            provider.state === "alive"
              ? "hub gone; provider reapable — would terminate the owned group, pin a crash_recovery checkpoint, rewrite to orphaned, and release the lease"
              : "hub gone; provider provably dead — would pin a crash_recovery checkpoint, rewrite to orphaned, and release the lease",
        });
        continue;
      }
      if (provider.state === "alive") {
        const reap = await reapOrphanedProvider(lease, provider, this.probes());
        if (reap.status !== "reaped") {
          report.sessions.push({
            session_id: sessionId,
            outcome: "manual",
            detail: `orphaned provider survived bounded termination (${reap.status})`,
          });
          continue;
        }
        await this.options.observePhase?.("provider-reaped");
      }

      const { value: action, releaseError } = await withLiveLock(
        {
          commonDir: this.commonDir,
          liveSessionId: sessionId,
          waitMs: this.options.lockWaitMs,
          acquireLock: this.options.acquireLock,
        },
        async (): Promise<{ kind: "recovered" | "cleaned" | "manual"; detail: string; worktreePath: string | null }> => {
          let state: LiveSessionState;
          try {
            state = await this.loadState(sessionId);
          } catch (error) {
            const failure = asDelegateError(error);
            if (failure.code === "SESSION_NOT_FOUND") {
              return {
                kind: "cleaned",
                detail: "no state record: the launch never completed",
                worktreePath: lease.worktree_path,
              };
            }
            return { kind: "manual", detail: failure.message, worktreePath: null };
          }
          if (TERMINAL_STATUSES.includes(state.status)) {
            return {
              kind: "cleaned",
              detail: `state is already ${state.status}`,
              worktreePath: lease.worktree_path,
            };
          }

          let worktreeProblem: string | null = null;
          try {
            await inspectLiveWorktree(this.repositoryCwd, lease.worktree_path);
          } catch (error) {
            const failure = asDelegateError(error);
            if (failure.code !== "LIVE_WORKTREE_MISSING") {
              return { kind: "manual", detail: failure.message, worktreePath: null };
            }
            worktreeProblem = failure.message;
          }

          if (worktreeProblem === null) {
            const capture = await captureLiveCheckpoint(
              lease.worktree_path,
              state.current_commit,
              "crash_recovery",
              { seq: state.checkpoint_seq + 1, now: () => this.now() },
            );
            await this.options.observePhase?.("checkpoint-captured");
            const advanced = capture.advanced;
            const next: LiveSessionState = {
              ...state,
              current_commit: advanced ? capture.checkpoint.commit : state.current_commit,
              checkpoint_seq: advanced ? state.checkpoint_seq + 1 : state.checkpoint_seq,
              last_checkpoint_reason: advanced ? "crash_recovery" : state.last_checkpoint_reason,
              status: "orphaned",
              revision: state.revision + 1,
              last_error: {
                code: "SESSION_ORPHANED",
                message:
                  "recovered after hub loss: the provider group was terminated and the surviving worktree pinned before this rewrite",
                stage: "shutdown",
                retryable: false,
                provider: state.provider,
              },
              updated_at: this.now().toISOString(),
            };
            await applyLiveTransition(
              { commonDir: this.commonDir, repositoryCwd: this.repositoryCwd },
              {
                kind: "advance",
                live_session_id: sessionId,
                ref: liveRefFor(sessionId),
                expected_ref: state.current_commit,
                new_commit: next.current_commit,
                next_state: next,
              },
            );
            await this.options.observePhase?.("state-advanced");
            return {
              kind: "recovered",
              detail: "provider reaped, worktree pinned, status rewritten to orphaned",
              worktreePath: lease.worktree_path,
            };
          }

          const next: LiveSessionState = {
            ...state,
            status: "orphaned",
            revision: state.revision + 1,
            last_error: {
              code: "WORKTREE_LOST",
              message: "recovered after hub loss: the live worktree no longer exists; nothing could be pinned",
              stage: "state",
              retryable: false,
              provider: state.provider,
            },
            updated_at: this.now().toISOString(),
          };
          await applyLiveTransition(
            { commonDir: this.commonDir, repositoryCwd: this.repositoryCwd },
            {
              kind: "advance",
              live_session_id: sessionId,
              ref: liveRefFor(sessionId),
              expected_ref: state.current_commit,
              new_commit: state.current_commit,
              next_state: next,
            },
          );
          await this.options.observePhase?.("state-advanced");
          return {
            kind: "recovered",
            detail: "worktree lost; status rewritten to orphaned without a checkpoint",
            worktreePath: null,
          };
        },
      );

      let detail = releaseError ? `${action.detail}; reconciliation lock release: ${releaseError.message}` : action.detail;
      if (action.kind === "manual") {
        report.sessions.push({ session_id: sessionId, outcome: "manual", detail });
        continue;
      }

      // Teardown strictly after the durable rewrite; the lease is released
      // last so its audit trail outlives every resource it named. A cleanup
      // that is not proven keeps the lease.
      if (action.worktreePath !== null) {
        const removalOutcome = await this.removeRetainedWorktree(action.worktreePath);
        if (removalOutcome.refused !== null) {
          report.sessions.push({
            session_id: sessionId,
            outcome: action.kind,
            detail:
              `${action.detail}; worktree cleanup refused: ${removalOutcome.refused}; ` +
              "lease and worktree retained (never release a lease whose worktree cleanup did not run under the admin lock)",
          });
          continue;
        }
        if (removalOutcome.cleanupError !== null) {
          detail = `${action.detail}; worktree cleanup reported: ${removalOutcome.cleanupError.message}; lease retained as the audit trail for ${action.worktreePath}`;
          report.sessions.push({ session_id: sessionId, outcome: action.kind, detail });
          continue;
        }
      }
      try {
        await removeLiveLease(this.commonDir, sessionId, lease.token);
      } catch (error) {
        detail = `${detail}; lease release reported: ${asDelegateError(error).message}`;
      }
      report.sessions.push({ session_id: sessionId, outcome: action.kind, detail });
    }

    // Second pass: terminal records whose worktree outlived a failed release
    // (lease already gone — no ownership claim to violate).
    for (const state of await this.listStates()) {
      if (!TERMINAL_STATUSES.includes(state.status)) continue;
      if (this.managed.has(state.live_session_id)) continue;
      const present = await this.retainableWorktree(state.worktree_path);
      if (!present) continue;
      if (options.dryRun) {
        report.sessions.push({
          session_id: state.live_session_id,
          outcome: "dry-run",
          detail: `would remove the retained worktree of terminal session at ${state.worktree_path}`,
        });
        continue;
      }
      const removal = await this.removeRetainedWorktree(state.worktree_path);
      report.sessions.push({
        session_id: state.live_session_id,
        outcome: removal.removed ? "released" : "manual",
        detail: removal.removed
          ? `released terminal session's retained worktree at ${state.worktree_path}`
          : `worktree at ${state.worktree_path} could not be removed: ${removal.refused ?? removal.cleanupError?.message ?? "unknown"}`,
      });
    }

    if (options.dryRun) return report;
    const adminLock = await this.acquireAdminLock().catch(() => null);
    if (adminLock !== null) {
      try {
        await pruneLiveWorktrees(this.repositoryCwd);
        report.worktrees_pruned = true;
      } finally {
        try {
          await adminLock.release();
        } catch {
          // Prune already landed; a lingering lock dir is surfaced by the
          // next operation's lock acquisition.
        }
      }
    }
    return report;
  }

  /** Worktree path exists and is this repository's linked worktree. */
  private async retainableWorktree(path: string): Promise<boolean> {
    try {
      await inspectLiveWorktree(this.repositoryCwd, path);
      return true;
    } catch (error) {
      if (asDelegateError(error).code === "LIVE_WORKTREE_MISSING") return false;
      return true; // inconsistent → report as manual territory
    }
  }

  /** Worktree removal under the admin lock; null fields when unprovable. */
  private async removeRetainedWorktree(
    path: string,
  ): Promise<{
    removed: boolean;
    refused: string | null;
    cleanupError: { code: string; message: string } | null;
  }> {
    let adminLock: RepositoryLock;
    try {
      adminLock = await this.acquireAdminLock();
    } catch (error) {
      return { removed: false, refused: asDelegateError(error).message, cleanupError: null };
    }
    try {
      const removal = await removeLiveWorktree(this.repositoryCwd, {
        path,
        parentPath: dirname(path),
        base: "",
      });
      return { removed: removal.cleanup_error === null, refused: null, cleanupError: removal.cleanup_error };
    } finally {
      try {
        await adminLock.release();
      } catch {
        // reported by the next lock holder
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Internals
  // ---------------------------------------------------------------------------

  private mustManaged(sessionId: string): ManagedWorkspace {
    const managed = this.managed.get(sessionId);
    if (managed === undefined) {
      throw new AgentHubError("SESSION_NOT_LIVE", `no managed session "${sessionId}" in this hub process`);
    }
    return managed;
  }

  recordSidecarPath(sessionId: string): string {
    if (!isUuid(sessionId)) {
      throw new AgentHubError(
        "SESSION_ID_INVALID",
        `session id "${sessionId}" is not a generated UUID; refusing to derive a path from it`,
      );
    }
    return join(liveStateRoot(this.commonDir), RECORDS_SUBDIR, `${sessionId}.json`);
  }

  private async writeRecordSidecar(record: SessionRecord): Promise<void> {
    const path = this.recordSidecarPath(record.session_id);
    await mkdir(dirname(path), { recursive: true });
    const temp = `${path}.${process.pid}.${randomUUID()}.tmp`;
    await writeFile(temp, `${JSON.stringify(record, null, 2)}\n`, "utf8");
    await rename(temp, path);
  }

  private async withSessionLock<T>(sessionId: string, operation: () => Promise<T>) {
    return withLiveLock(
      {
        commonDir: this.commonDir,
        liveSessionId: sessionId,
        waitMs: this.options.lockWaitMs,
        acquireLock: this.options.acquireLock,
      },
      operation,
    );
  }

  /**
   * One durable write at a time per session, in issue order: kernel mirror
   * commits and lifecycle checkpoint/finalize writes share this tail, so
   * in-process lock contention can never degrade a mirror write.
   */
  private async enqueueDurable<T>(sessionId: string, operation: () => Promise<T>): Promise<T> {
    const tailFor = (tail: Promise<unknown>): { next: Promise<T>; chain: Promise<unknown> } => {
      const settled = tail.then(operation, operation);
      return { next: settled, chain: settled.catch(() => undefined) };
    };
    const managed = this.managed.get(sessionId);
    if (managed !== undefined) {
      const { next, chain } = tailFor(managed.durableTail);
      managed.durableTail = chain;
      return next;
    }
    const prior = this.launchTails.get(sessionId) ?? Promise.resolve();
    const { next, chain } = tailFor(prior);
    this.launchTails.set(sessionId, chain);
    void chain.then(() => {
      if (this.launchTails.get(sessionId) === chain) this.launchTails.delete(sessionId);
    });
    return next;
  }

}

// ---------------------------------------------------------------------------
// Resume merging (durable live resume ⊕ kernel-neutral projection)
// ---------------------------------------------------------------------------

/**
 * Project a kernel mirror commit's resume handle onto the durable live-shape
 * handle. Provider data fields survive from the durable prior whenever the
 * kernel projection is silent about them (the kernel's fresh-launch
 * `buildResume` legitimately carries an empty `data`), the kernel cursor
 * always wins over prior claims, and verification only ever upgrades.
 */
export function mergeKernelResume(
  prior: ProviderResumeState | null,
  kernel: ResumeState | null,
): ProviderResumeState | null {
  if (kernel === null) return prior;
  const inherited = prior !== null && prior.provider === kernel.provider ? prior : null;
  const providerSessionId = kernel.provider_session_id ?? inherited?.provider_session_id ?? null;
  const verified =
    kernel.verified === true
      ? { verified: true as const, verified_via: kernel.verified_via }
      : inherited !== null && inherited.verified
        ? { verified: true as const, verified_via: inherited.verified_via }
        : { verified: false as const, verified_via: null };
  const cursor = Math.max(
    kernel.last_event_seq,
    inherited?.provider === "omp" ? inherited.last_event_seq : 0,
  );
  switch (kernel.provider) {
    case "omp":
      return {
        provider: "omp",
        provider_session_id: providerSessionId,
        ...verified,
        last_event_seq: cursor,
      };
    case "agy":
      return {
        provider: "agy",
        provider_session_id: providerSessionId,
        ...verified,
        resume_argv_verified: booleanField(kernel, inherited, "resume_argv_verified"),
      };
    case "pi":
      return {
        provider: "pi",
        provider_session_id: providerSessionId,
        ...verified,
        resume_token: nullableField(kernel, inherited, "resume_token"),
      };
    case "hermes":
      return {
        provider: "hermes",
        provider_session_id: providerSessionId,
        ...verified,
        session_load_advertised: booleanField(kernel, inherited, "session_load_advertised"),
      };
    default:
      // An unknown provider's handle cannot be stored in the live vocabulary;
      // the durable record keeps whatever it already held.
      return inherited;
  }
}

function booleanField(
  kernel: ResumeState,
  inherited: ProviderResumeState | null,
  key: string,
): boolean {
  const fromKernel = kernel.data[key];
  if (typeof fromKernel === "boolean") return fromKernel;
  if (inherited !== null && key in inherited) {
    const value = (inherited as unknown as Record<string, unknown>)[key];
    if (typeof value === "boolean") return value;
  }
  return false;
}

function nullableField(
  kernel: ResumeState,
  inherited: ProviderResumeState | null,
  key: string,
): string | null {
  const fromKernel = kernel.data[key];
  if (typeof fromKernel === "string" || fromKernel === null) return fromKernel;
  if (inherited !== null && key in inherited) {
    const value = (inherited as unknown as Record<string, unknown>)[key];
    if (value === null || typeof value === "string") return value;
  }
  return null;
}

/** The kernel-shaped handle stored durable lifecycle resume carries. */
export function kernelResumeFromState(state: ProviderResumeState): ResumeState {
  const base = {
    provider: state.provider,
    provider_session_id: state.provider_session_id,
    ...verificationOf(state.verified, state.verified_via),
  };
  switch (state.provider) {
    case "omp":
      return { ...base, data: {}, last_event_seq: state.last_event_seq };
    case "agy":
      return { ...base, data: { resume_argv_verified: state.resume_argv_verified }, last_event_seq: 0 };
    case "pi":
      return { ...base, data: { resume_token: state.resume_token }, last_event_seq: 0 };
    case "hermes":
      return { ...base, data: { session_load_advertised: state.session_load_advertised }, last_event_seq: 0 };
  }
}

/**
 * Rebuild the kernel-shaped mirror record from a durable state record, for
 * sessions whose sidecar never landed (pre-registration crash windows).
 * The capabilities come from the state's launch snapshot minus the
 * lifecycle-owned checkpoint claim — the honest minimum.
 */
export function projectRecordFromState(state: LiveSessionState): SessionRecord {
  const { checkpoint: _checkpoint, ...kernelCaps } = state.capabilities as Record<string, unknown>;
  return {
    schema: "agent-hub-interaction/v1",
    session_id: state.live_session_id,
    provider: state.provider,
    transport: state.transport,
    capabilities: kernelCaps as SessionRecord["capabilities"],
    workspace: state.worktree_path,
    max_text_bytes: DEFAULT_MAX_TEXT_BYTES,
    resume: state.resume === null ? null : kernelResumeFromState(state.resume),
    status: state.status,
    revision: state.revision,
    last_error: state.last_error === null ? null : kernelErrorOf(state.last_error),
    created_at: state.created_at,
    updated_at: state.updated_at,
  };
}
