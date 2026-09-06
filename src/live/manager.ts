import { randomUUID } from "node:crypto";
import { dirname } from "node:path";

import { deferred, type Deferred } from "../deferred.js";
import { asDelegateError, AgentHubError } from "../errors.js";
import { resolveRepositoryIdentity } from "../git.js";
import { acquireRepositoryLock, type RepositoryLock } from "../locks.js";
import {
  classifyLiveLease,
  createLiveLease,
  defaultLiveLeaseProbes,
  hubProcessStartToken,
  listLiveLeases,
  readLiveLease,
  reapOrphanedProvider,
  removeLiveLease,
  updateLiveLeaseProvider,
  type LiveLeaseProbes,
  type LiveLeaseRecord,
} from "./lease.js";
import { LiveEventRing, truncateUtf8 } from "./events.js";
import {
  applyLiveTransition,
  liveRefFor,
  LIVE_ADMIN_LOCK_NAME,
  LIVE_SCHEMA_VERSION,
  loadLiveState,
  newLiveSessionId,
  withLiveLock,
} from "./state.js";
import {
  captureLiveCheckpoint,
  createLiveWorktree,
  inspectLiveWorktree,
  pruneLiveWorktrees,
  removeLiveWorktree,
  type LiveWorktree,
} from "./worktree.js";
import type { RepositoryIdentity } from "../types.js";
import type {
  CheckpointReason,
  LiveCancelCommand,
  LiveCheckpoint,
  LiveCommand,
  LiveEvent,
  LiveFollowUpCommand,
  LiveLaunchRequest,
  LivePermissionDecision,
  LivePermissionResponseCommand,
  LivePromptCommand,
  LiveProviderFactory,
  LiveProviderId,
  LiveProviderProcessFacts,
  LiveSessionState,
  LiveSteerCommand,
  LiveStatus,
  LiveStatusCommand,
  LiveStopMode,
  LiveStopReport,
  LiveTransport,
  LiveTransportFactory,
  LiveTransportId,
  LiveTurnResult,
  LiveUsage,
  ProviderResumeState,
} from "./types.js";

/**
 * Live session manager (v3 live, Package 1) — the provider-neutral core.
 *
 * Ownership model, one mechanism per lifetime:
 *   - `LiveLease` (lease.ts)   — who owns this session, its provider group,
 *     its worktree; survives hub restarts; released only by token proof.
 *   - repository locks         — short operations only (per-session
 *     `live-<id>`, create-time `live-admin`).
 *
 * Quotas (enforced atomically under `live-admin` against the *durable* lease
 * set, so they span hub processes): 8 live sessions per Hub process, 4 per
 * Git common dir.
 *
 * Follow-up queue: ≤ 32 queued commands, ≤ 1 MiB queued bytes, ≤ 128 KiB per
 * message; every rejection is `LIVE_QUEUE_FULL`. Queue contents are transient
 * text in memory — durable state never sees them.
 *
 * Ordering guarantees, in this exact order on every terminal path:
 *
 *   reap → checkpoint → state → lease/worktree teardown
 *
 *   - close:  transport.stop proves the process is gone *before* the `close`
 *     checkpoint captures the worktree (a checkpoint taken over a still
 *     running provider pins a torn state);
 *   - live crash: transport.stop is still run to *prove* exit before the
 *     `error` checkpoint, even when the exit event already arrived;
 *   - recovery: an orphaned provider group is reaped (only under start-
 *     identity proof) before the `crash_recovery` checkpoint, the checkpoint
 *     before the state rewrite to `orphaned`, the rewrite before teardown.
 *
 * Durability discipline: every committed transition is a sidecar-guarded CAS
 * on the session's live ref (state.ts). Event bodies, queue text, and
 * transcripts stay in memory or on the wire; `LiveEventRing` is the only
 * place a recent event is ever retained, and eviction is always observable
 * through cursor expiry.
 */

export const LIVE_PROCESS_SESSION_QUOTA = 8;
export const LIVE_COMMON_DIR_SESSION_QUOTA = 4;
export const LIVE_FOLLOW_UP_QUEUE_MAX_MESSAGES = 32;
export const LIVE_FOLLOW_UP_QUEUE_MAX_BYTES = 1024 * 1024;
export const LIVE_FOLLOW_UP_MAX_MESSAGE_BYTES = 128 * 1024;
export const LIVE_DEFAULT_MAX_TEXT_BYTES = 64 * 1024;

const TERMINAL_STATUSES: readonly LiveStatus[] = ["closed", "error", "orphaned"];

/** Durable ordering seam: tests observe these exactly, in order. */
export type LiveManagerPhase =
  | "transport-stopped"
  | "provider-reaped"
  | "checkpoint-captured"
  | "state-advanced";

export interface LiveManagerOptions {
  /** Absolute Git common dir (shared by every linked worktree). */
  commonDir: string;
  /** Any checkout path where hub-side git plumbing may run. */
  repositoryCwd: string;
  transportFactories: readonly LiveTransportFactory[];
  providerFactories?: readonly LiveProviderFactory[];
  maxTextBytes?: number;
  processQuota?: number;
  commonDirQuota?: number;
  tmpRoot?: string;
  stopGraceMs?: number;
  newLiveSessionId?: () => string;
  now?: () => Date;
  acquireLock?: typeof acquireRepositoryLock;
  /** Recovery/orphan probes; defaults hit the real OS, tests inject fakes. */
  leaseProbes?: LiveLeaseProbes;
  /** Crash/ordering seam at durable boundaries. */
  observePhase?: (phase: LiveManagerPhase) => Promise<void>;
}

interface ResolvedOptions extends LiveManagerOptions {
  maxTextBytes: number;
  processQuota: number;
  commonDirQuota: number;
  stopGraceMs: number;
}

export interface LiveStartRequest {
  provider: LiveProviderId;
  /** Pin a specific transport; must pair 1:1 with the provider. */
  transport?: LiveTransportId;
  /** v2 durable session this live session feeds, when any. */
  session_id?: string | null;
  /** Durable resume hint (restart of a previously started live id). */
  resume?: ProviderResumeState | null;
  /** Explicit live id (restart path); a fresh UUID otherwise. */
  live_session_id?: string;
  max_text_bytes?: number;
  /** Base commit; defaults to the captured identity head. */
  base?: string;
}

export interface LiveStartResult {
  live_session_id: string;
  state: LiveSessionState;
  workspace: string;
  capabilities: LiveSessionState["capabilities"];
  /**
   * Non-fatal integrity notes for an otherwise successful launch — today
   * only a live-admin lock release that failed while every resource
   * operation succeeded. Never dropped silently.
   */
  warnings?: { code: string; message: string }[];
}

export interface LiveResumeFromStateRequest {
  /** Durable live session id whose terminal record is continued. */
  live_session_id: string;
  /** Optional transport pin; must pair with the durable record. */
  transport?: LiveTransportId;
  max_text_bytes?: number;
}

export interface LiveCloseResult {
  state: LiveSessionState;
  stop: LiveStopReport | null;
  checkpoint_taken: boolean;
  /** Non-null when teardown left resources behind; state is still committed. */
  cleanup_errors: { code: string; message: string }[];
}

export interface LiveRecoverySessionReport {
  live_session_id: string;
  outcome: "kept-live" | "foreign" | "recovered" | "cleaned" | "manual";
  detail: string;
}

export interface LiveRecoveryReport {
  scanned: number;
  sessions: LiveRecoverySessionReport[];
}

interface ActiveTurn {
  command: LivePromptCommand | LiveFollowUpCommand;
  started_at: string;
  started_at_ms: number;
  result: Deferred<LiveTurnResult>;
  cancel_requested: boolean;
  error_seen: LiveTurnResult["error"];
  usage: LiveUsage | null;
  streams: Map<string, { chunks: string[]; truncated: boolean; final: boolean }>;
  /** True when the command was already delivered to the provider (native queue hand-off). */
  delivered: boolean;
}

interface QueuedFollowUp {
  command: LiveFollowUpCommand;
  bytes: number;
  result: Deferred<LiveTurnResult>;
}

interface ManagedSession {
  id: string;
  transport: LiveTransport;
  lease: LiveLeaseRecord;
  worktree: LiveWorktree;
  state: LiveSessionState;
  ring: LiveEventRing;
  status: LiveStatus;
  prompt_accepted: boolean;
  turn: ActiveTurn | null;
  queue: QueuedFollowUp[];
  /**
   * Follow-ups already delivered to a `native` provider while a turn runs —
   * the provider queued them; the hub only tracks the pending result and
   * never re-sends. Honesty matters here: routing a native claim through the
   * hub queue while still claiming `native` is a contract violation.
   */
  provider_queued: QueuedFollowUp[];
  queue_bytes: number;
  open_permissions: Set<string>;
  pump: Deferred<void>;
  closing: boolean;
  torn_down: boolean;
  durable_tail: Promise<unknown>;
}

export class LiveSessionManager {
  private readonly options: ResolvedOptions;
  private readonly probes: LiveLeaseProbes;
  private readonly sessions = new Map<string, ManagedSession>();

  constructor(options: LiveManagerOptions) {
    this.options = {
      ...options,
      maxTextBytes: options.maxTextBytes ?? LIVE_DEFAULT_MAX_TEXT_BYTES,
      processQuota: options.processQuota ?? LIVE_PROCESS_SESSION_QUOTA,
      commonDirQuota: options.commonDirQuota ?? LIVE_COMMON_DIR_SESSION_QUOTA,
      stopGraceMs: options.stopGraceMs ?? 5_000,
    };
    this.probes = options.leaseProbes ?? defaultLiveLeaseProbes;
  }

  /** Sessions currently owned by *this* hub process (quota view). */
  get activeCount(): number {
    return this.sessions.size;
  }

  private now(): Date {
    return (this.options.now ?? (() => new Date()))();
  }

  // -------------------------------------------------------------------------
  // Lifecycle
  // -------------------------------------------------------------------------

  async start(request: LiveStartRequest): Promise<LiveStartResult> {
    const { commonDir, repositoryCwd } = this.options;
    const provider = request.provider;
    const factory = this.selectFactory(provider, request.transport);

    const probe = await factory.probe();
    if (!probe.found) {
      throw new AgentHubError(
        "LIVE_TRANSPORT_UNAVAILABLE",
        `provider "${provider}" was not found${probe.detail ? `: ${probe.detail}` : "; launching nothing rather than guessing"}`,
      );
    }

    const liveSessionId =
      request.live_session_id ?? (this.options.newLiveSessionId ?? newLiveSessionId)();
    const identity = await resolveRepositoryIdentity(repositoryCwd);
    const base = request.base ?? identity.head;

    const prepared = await this.reserveLaunchResources(liveSessionId, provider, base);
    return this.launchSession({
      liveSessionId,
      provider,
      sessionId: request.session_id ?? null,
      identity,
      base,
      maxTextBytes: request.max_text_bytes ?? this.options.maxTextBytes,
      resume: request.resume ?? null,
      factory,
      worktree: prepared.worktree,
      lease: prepared.lease,
      warnings: prepared.warnings,
    });
  }

  /**
   * Continue the durable live record `live_session_id` from its chain head.
   * Loads the durable record, refuses any session that is not terminal or
   * still leased, acquires a NEW lease, materializes a fresh worktree at
   * `current_commit`, launches the provider with the record's verified
   * opaque resume state, verifies the provider identity round-tripped, and
   * CAS-advances the EXISTING live ref/state (revision + 1) — the live ref
   * namespace is never branched per restart.
   */
  async resumeFromState(request: LiveResumeFromStateRequest): Promise<LiveStartResult> {
    const { commonDir, repositoryCwd } = this.options;
    const liveSessionId = request.live_session_id;
    if (this.sessions.has(liveSessionId)) {
      throw new AgentHubError(
        "LIVE_SESSION_ALREADY_LIVE",
        `live session "${liveSessionId}" is already running in this hub process`,
      );
    }

    const prior = await loadLiveState({ commonDir, repositoryCwd, liveSessionId });
    if (!TERMINAL_STATUSES.includes(prior.status)) {
      throw new AgentHubError(
        "LIVE_SESSION_NOT_RESUMABLE",
        `live session "${liveSessionId}" is "${prior.status}"; only terminal records (closed, error, orphaned) may be resumed`,
      );
    }
    if ((await readLiveLease(commonDir, liveSessionId)) !== undefined) {
      throw new AgentHubError(
        "LIVE_LEASE_EXISTS",
        `live session "${liveSessionId}" still holds a lease; run recovery (or release the lease) before resuming`,
      );
    }

    const factory = this.selectFactory(prior.provider, request.transport ?? prior.transport);
    if (factory.transport !== prior.transport) {
      throw new AgentHubError(
        "LIVE_TRANSPORT_PAIRING_INVALID",
        `durable live session "${liveSessionId}" was launched on "${prior.transport}"; resuming on "${factory.transport}" is refused`,
      );
    }
    const probe = await factory.probe();
    if (!probe.found) {
      throw new AgentHubError(
        "LIVE_TRANSPORT_UNAVAILABLE",
        `provider "${prior.provider}" was not found${probe.detail ? `: ${probe.detail}` : "; launching nothing rather than guessing"}`,
      );
    }

    const prepared = await this.reserveLaunchResources(
      liveSessionId,
      prior.provider,
      prior.current_commit,
    );

    const transport = factory.create();
    let processFacts: LiveProviderProcessFacts | null = null;
    let lease = prepared.lease;
    try {
      const report = await transport.open({
        live_session_id: liveSessionId,
        workspace: prepared.worktree.path,
        max_text_bytes: request.max_text_bytes ?? this.options.maxTextBytes,
        resume: prior.resume,
        report_process: async (facts) => {
          processFacts = facts;
          lease = await this.recordProviderOwnership(lease, facts);
        },
      });

      // Provider identity verification: a durable resume hint must come back
      // as the same provider's session handle, verified by the transport's
      // own evidence. A transport that cannot show the post-handshake resume
      // state at all is refused — a silent fresh session would lie.
      if (prior.resume !== null) {
        const resumed = report.resume_state ?? null;
        if (resumed === null || resumed === undefined) {
          throw new AgentHubError(
            "LIVE_RESUME_VERIFICATION_FAILED",
            `the transport produced no post-handshake resume state for a durable resume of "${liveSessionId}"; continuing without verified provider identity is refused`,
          );
        }
        if (resumed.provider !== prior.resume.provider) {
          throw new AgentHubError(
            "LIVE_RESUME_VERIFICATION_FAILED",
            `resume state came back for provider "${resumed.provider}", not the recorded "${prior.resume.provider}"`,
          );
        }
        if (
          prior.resume.provider_session_id !== null &&
          resumed.provider_session_id !== prior.resume.provider_session_id
        ) {
          throw new AgentHubError(
            "LIVE_RESUME_VERIFICATION_FAILED",
            `the provider resumed a different session identity than the durable handle records`,
          );
        }
      }

      const next: LiveSessionState = {
        ...prior,
        resume:
          report.resume_state ??
          this.initialResume(prior.provider, prior.resume, report.provider_session_id, prior.transport),
        worktree_path: prepared.worktree.path,
        worktree_parent: prepared.worktree.parentPath,
        status: "idle",
        revision: prior.revision + 1,
        last_error: null,
        updated_at: this.now().toISOString(),
      };
      await withLiveLock(
        { commonDir, liveSessionId, acquireLock: this.options.acquireLock },
        () =>
          applyLiveTransition(
            { commonDir, repositoryCwd },
            {
              kind: "advance",
              live_session_id: liveSessionId,
              ref: liveRefFor(liveSessionId),
              expected_ref: prior.current_commit,
              new_commit: prior.current_commit,
              next_state: next,
            },
          ),
      );

      const session = this.registerSession(transport, lease, prepared.worktree, next);
      void this.pumpLoop(session);
      return {
        live_session_id: liveSessionId,
        state: session.state,
        workspace: prepared.worktree.path,
        capabilities: session.state.capabilities,
        warnings: prepared.warnings.length > 0 ? prepared.warnings : undefined,
      };
    } catch (error) {
      throw await this.conservativeLaunchFailure(transport, lease, prepared.worktree, processFacts, error);
    }
  }

  /**
   * Quotas + prune + `worktree add` + lease claim, all under the short
   * live-admin lock — and nothing else. The lock is never held across a
   * transport launch (handshakes are slow) nor for a session lifetime.
   */
  private async reserveLaunchResources(
    liveSessionId: string,
    provider: LiveProviderId,
    base: string,
  ): Promise<{ worktree: LiveWorktree; lease: LiveLeaseRecord; warnings: { code: string; message: string }[] }> {
    const { commonDir, repositoryCwd } = this.options;
    const adminLock = await this.acquireAdminLock();
    const warnings: { code: string; message: string }[] = [];
    let worktree: LiveWorktree | null = null;
    try {
      if (this.sessions.size >= this.options.processQuota) {
        throw new AgentHubError(
          "LIVE_QUOTA_EXCEEDED",
          `this hub process already owns ${this.sessions.size} live sessions (quota ${this.options.processQuota})`,
        );
      }
      const leases = await listLiveLeases(commonDir);
      if (leases.length >= this.options.commonDirQuota) {
        throw new AgentHubError(
          "LIVE_QUOTA_EXCEEDED",
          `this Git common dir already has ${leases.length} live leases (quota ${this.options.commonDirQuota}); recover or release orphaned leases first`,
        );
      }
      await pruneLiveWorktrees(repositoryCwd);
      worktree = await createLiveWorktree(repositoryCwd, base, this.options.tmpRoot);
      const lease = await createLiveLease({
        commonDir,
        live_session_id: liveSessionId,
        provider,
        worktree_path: worktree.path,
        provider_pid: null,
        provider_pgid: null,
        provider_start_token: null,
        hub_start_token: await hubProcessStartToken(this.probes),
        now: () => this.now(),
      });
      return { worktree, lease, warnings };
    } catch (error) {
      if (worktree !== null) {
        const removal = await removeLiveWorktree(repositoryCwd, worktree);
        if (removal.cleanup_error) {
          throw new AgentHubError(
            "LIVE_WORKTREE_RETAINED",
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
          code: "LIVE_ADMIN_LOCK_RELEASE_FAILED",
          message: `worktree resources were claimed but the live-admin lock was not released cleanly: ${asDelegateError(releaseError).message}`,
        });
      }
    }
  }

  private async launchSession(input: {
    liveSessionId: string;
    provider: LiveProviderId;
    sessionId: string | null;
    identity: RepositoryIdentity;
    base: string;
    maxTextBytes: number;
    resume: ProviderResumeState | null;
    factory: LiveTransportFactory;
    worktree: LiveWorktree;
    lease: LiveLeaseRecord;
    warnings: { code: string; message: string }[];
  }): Promise<LiveStartResult> {
    const { commonDir, repositoryCwd } = this.options;
    const { liveSessionId, provider, worktree } = input;
    const transport = input.factory.create();
    let processFacts: LiveProviderProcessFacts | null = null;
    let lease = input.lease;
    try {
      const launchRequest: LiveLaunchRequest = {
        live_session_id: liveSessionId,
        workspace: worktree.path,
        max_text_bytes: input.maxTextBytes,
        resume: input.resume,
        report_process: async (facts) => {
          processFacts = facts;
          lease = await this.recordProviderOwnership(lease, facts);
        },
      };
      const report = await transport.open(launchRequest);
      const descriptor = await transport.describe();
      const createdAt = this.now().toISOString();
      const state: LiveSessionState = {
        schema: LIVE_SCHEMA_VERSION,
        live_session_id: liveSessionId,
        session_id: input.sessionId,
        provider,
        transport: descriptor.transport,
        capabilities: descriptor.capabilities,
        identity: input.identity,
        base_commit: input.base,
        current_commit: input.base,
        checkpoint_seq: 0,
        last_checkpoint_reason: null,
        worktree_path: worktree.path,
        worktree_parent: worktree.parentPath,
        resume:
          report.resume_state ??
          this.initialResume(provider, input.resume, report.provider_session_id, descriptor.transport),
        // `open` resolved ⇒ the transport is ready to accept commands;
        // `starting` would describe this record only if the write itself
        // were still in flight, which recovery re-derives from the lease.
        status: "idle",
        revision: 1,
        last_error: null,
        created_at: createdAt,
        updated_at: createdAt,
      };

      await withLiveLock(
        { commonDir, liveSessionId, acquireLock: this.options.acquireLock },
        () =>
          applyLiveTransition(
            { commonDir, repositoryCwd },
            {
              kind: "create",
              live_session_id: liveSessionId,
              ref: liveRefFor(liveSessionId),
              expected_ref: null,
              new_commit: input.base,
              next_state: state,
            },
          ),
      );

      const session = this.registerSession(transport, lease, worktree, state);
      void this.pumpLoop(session);
      return {
        live_session_id: liveSessionId,
        state: session.state,
        workspace: worktree.path,
        capabilities: session.state.capabilities,
        warnings: input.warnings.length > 0 ? input.warnings : undefined,
      };
    } catch (error) {
      throw await this.conservativeLaunchFailure(transport, lease, worktree, processFacts, error);
    }
  }

  /**
   * The awaited process-owned boundary: provider facts land on the lease the
   * moment the transport has a spawned process, before any handshake can
   * fail. `pgid` is recorded as observed (not assumed from the pid).
   */
  private async recordProviderOwnership(
    lease: LiveLeaseRecord,
    facts: LiveProviderProcessFacts,
  ): Promise<LiveLeaseRecord> {
    return updateLiveLeaseProvider(
      this.options.commonDir,
      lease,
      {
        provider_pid: facts.pid,
        provider_pgid: facts.pgid,
        provider_start_token: await this.probes.startToken(facts.pid),
      },
      () => this.now(),
    );
  }

  /**
   * A launch that rejected after spawning may only give its resources back
   * when `stop` PROVES the complete owned group is dead. Otherwise the lease
   * (now carrying the provider facts), the worktree, and the recorded
   * ownership metadata are all RETAINED, and the error says so — recovery
   * or a human finishes the job that cannot be proven safe here.
   */
  private async conservativeLaunchFailure(
    transport: LiveTransport,
    lease: LiveLeaseRecord,
    worktree: LiveWorktree,
    processFacts: LiveProviderProcessFacts | null,
    cause: unknown,
  ): Promise<AgentHubError> {
    const failure = asDelegateError(cause);
    let stop: LiveStopReport;
    try {
      stop = await transport.stop("terminate");
    } catch (stopError) {
      // Stop trouble never proves the process is gone.
      stop = {
        status: "orphaned",
        exit_code: null,
        exit_signal: null,
        waited_ms: 0,
      };
      void stopError;
    }

    if (processFacts !== null && stop.status !== "closed") {
      return new AgentHubError(
        failure.code,
        `${failure.message}; the provider process (pid ${processFacts.pid}, group ${processFacts.pgid}) could not be proven gone, so its lease, the worktree at ${worktree.path}, and the recorded ownership facts are retained for recovery or manual cleanup`,
      );
    }

    const cleanupErrors = await this.releaseLaunchResources(lease, worktree);
    if (cleanupErrors.length > 0) {
      return new AgentHubError(
        "LIVE_WORKTREE_RETAINED",
        `${failure.message}; cleanup reported: ${cleanupErrors.map((e) => e.message).join("; ")} — the lease stays as the audit trail for whatever remains`,
      );
    }
    return new AgentHubError(failure.code, failure.message);
  }

  /**
   * Worktree removal ONLY under the live-admin lock, then lease release.
   * If the lock cannot be had, NO worktree mutation runs at all — the
   * structured failure is returned and the lease and worktree are both
   * retained untouched (a half-cleaned tree with a live lease is worse
   * than an untouched one).
   */
  private async releaseLaunchResources(
    lease: LiveLeaseRecord,
    worktree: LiveWorktree,
  ): Promise<{ code: string; message: string }[]> {
    const errors: { code: string; message: string }[] = [];
    const adminLock = await this.acquireAdminLock().catch((error) => {
      errors.push({
        code: "LIVE_ADMIN_LOCK_UNAVAILABLE",
        message:
          `the live-admin lock could not be acquired, so no worktree mutation was attempted: ` +
          `${asDelegateError(error).message}; the worktree at ${worktree.path} and its lease are retained`,
      });
      return null;
    });
    if (adminLock === null) {
      return errors;
    }
    try {
      const removal = await removeLiveWorktree(this.options.repositoryCwd, worktree);
      if (removal.cleanup_error) {
        errors.push(removal.cleanup_error);
      }
    } finally {
      try {
        await adminLock.release();
      } catch (releaseError) {
        errors.push({
          code: "LIVE_ADMIN_LOCK_RELEASE_FAILED",
          message: `worktree operations finished but the live-admin lock release failed: ${asDelegateError(releaseError).message}`,
        });
      }
    }
    if (errors.length > 0) {
      // The lease is the audit trail: it survives whenever the resources it
      // names are not provably gone.
      return errors;
    }
    try {
      await removeLiveLease(this.options.commonDir, lease.live_session_id, lease.token);
    } catch (error) {
      errors.push(asDelegateError(error));
    }
    return errors;
  }

  private async acquireAdminLock(): Promise<RepositoryLock> {
    return (this.options.acquireLock ?? acquireRepositoryLock)({
      commonDir: this.options.commonDir,
      name: LIVE_ADMIN_LOCK_NAME,
      waitMs: 30_000,
    });
  }

  private registerSession(
    transport: LiveTransport,
    lease: LiveLeaseRecord,
    worktree: LiveWorktree,
    state: LiveSessionState,
  ): ManagedSession {
    const session: ManagedSession = {
      id: state.live_session_id,
      transport,
      lease,
      worktree,
      state,
      ring: new LiveEventRing(),
      status: "idle",
      prompt_accepted: false,
      turn: null,
      queue: [],
      provider_queued: [],
      queue_bytes: 0,
      open_permissions: new Set(),
      pump: deferred<void>(),
      closing: false,
      torn_down: false,
      durable_tail: Promise.resolve(),
    };
    this.sessions.set(session.id, session);
    return session;
  }

  private selectFactory(
    provider: LiveProviderId,
    transport: LiveTransportId | undefined,
  ): LiveTransportFactory {
    const candidates = this.options.transportFactories.filter(
      (factory) =>
        factory.provider === provider &&
        (transport === undefined || factory.transport === transport),
    );
    if (candidates.length === 0) {
      throw new AgentHubError(
        "LIVE_TRANSPORT_UNAVAILABLE",
        `no registered transport factory pairs with provider "${provider}"${
          transport ? ` and transport "${transport}"` : ""
        }`,
      );
    }
    const providerFactory = this.options.providerFactories?.find(
      (factory) => factory.provider === provider,
    );
    const selected = providerFactory
      ? providerFactory.selectTransport(
          candidates.filter((candidate) =>
            providerFactory.transports.includes(candidate.transport),
          ),
        )
      : candidates[0];
    if (!selected) {
      throw new AgentHubError(
        "LIVE_TRANSPORT_UNAVAILABLE",
        `provider "${provider}" honestly declined every candidate transport`,
      );
    }
    return selected;
  }

  /**
   * Initial resume state is built from observed launch facts only.
   * `verified` is true for exactly one reason: a prior durable handle was fed
   * into `open` and the same provider session id came back out — a real
   * round trip. No claim, flag, or probe ever earns it.
   */
  private initialResume(
    provider: LiveProviderId,
    prior: ProviderResumeState | null,
    observedSessionId: string | null,
    transport: LiveTransportId,
  ): ProviderResumeState {
    const roundTripped =
      prior !== null &&
      prior.provider === provider &&
      prior.provider_session_id !== null &&
      observedSessionId !== null &&
      prior.provider_session_id === observedSessionId;
    const verification = roundTripped
      ? { verified: true as const, verified_via: `hub-restart:${transport}` }
      : { verified: false as const, verified_via: null };
    switch (provider) {
      case "omp":
        return {
          provider: "omp",
          provider_session_id: observedSessionId,
          ...verification,
          last_event_seq: prior?.provider === "omp" ? prior.last_event_seq : 0,
        };
      case "agy":
        // Native argv verification belongs to the transport's probe, never to
        // an inference here.
        return {
          provider: "agy",
          provider_session_id: observedSessionId,
          ...verification,
          resume_argv_verified: false,
        };
      case "pi":
        return {
          provider: "pi",
          provider_session_id: observedSessionId,
          ...verification,
          resume_token: prior?.provider === "pi" ? prior.resume_token : null,
        };
      case "hermes":
        return {
          provider: "hermes",
          provider_session_id: observedSessionId,
          ...verification,
          session_load_advertised:
            prior?.provider === "hermes" ? prior.session_load_advertised : false,
        };
    }
  }

  // -------------------------------------------------------------------------
  // Commands
  // -------------------------------------------------------------------------

  private must(id: string): ManagedSession {
    const session = this.sessions.get(id);
    if (!session) {
      throw new AgentHubError("LIVE_SESSION_NOT_FOUND", `no live session "${id}" in this hub process`);
    }
    if (TERMINAL_STATUSES.includes(session.status) || session.closing) {
      throw new AgentHubError(
        "LIVE_SESSION_NOT_LIVE",
        `live session "${id}" is ${session.status}; it no longer accepts commands`,
      );
    }
    return session;
  }

  private issued(id: string): { command_id: string; live_session_id: string; issued_at: string } {
    return {
      command_id: randomUUID(),
      live_session_id: id,
      issued_at: this.now().toISOString(),
    };
  }

  private refused(session: ManagedSession, kind: LiveTurnResult["kind"]): LiveTurnResult {
    const at = this.now().toISOString();
    return {
      live_session_id: session.id,
      command_id: randomUUID(),
      kind,
      outcome: "unsupported",
      final_text: null,
      usage: null,
      checkpoint: null,
      exit_code: null,
      exit_signal: null,
      started_at: at,
      finished_at: at,
      duration_ms: 0,
      error: {
        code: "LIVE_CAPABILITY_UNSUPPORTED",
        message: `the launch capability snapshot for live session "${session.id}" marks "${kind}" undeliverable; the command was refused pre-dispatch`,
        stage: "capability",
        retryable: false,
        provider: session.state.provider,
      },
    };
  }

  /** The initial task, exactly once, while the session sits idle. */
  async prompt(id: string, text: string): Promise<LiveTurnResult> {
    const session = this.must(id);
    const claim = session.state.capabilities.prompt;
    if (claim.support === "unsupported" || claim.support === "signal") {
      return this.refused(session, "prompt");
    }
    if (session.prompt_accepted) {
      throw new AgentHubError(
        "LIVE_PROMPT_ALREADY_ACCEPTED",
        `live session "${id}" accepted its one prompt already; use followUp`,
      );
    }
    if (session.status !== "idle" || session.turn !== null) {
      throw new AgentHubError(
        "LIVE_SESSION_NOT_IDLE",
        `live session "${id}" is ${session.status}; the prompt is accepted only while idle before the first turn`,
      );
    }
    const command: LivePromptCommand = { kind: "prompt", text, ...this.issued(id) };
    session.prompt_accepted = true;
    return this.dispatchTurn(session, command).catch((error) => {
      session.prompt_accepted = false;
      throw error;
    });
  }

  /** Next-turn input: delivered now when idle, queued while a turn runs. */
  async followUp(id: string, text: string): Promise<LiveTurnResult> {
    const session = this.must(id);
    const claim = session.state.capabilities.follow_up;
    if (claim.support === "unsupported" || claim.support === "signal") {
      return this.refused(session, "follow_up");
    }
    const bytes = Buffer.byteLength(text, "utf8");
    if (bytes > LIVE_FOLLOW_UP_MAX_MESSAGE_BYTES) {
      throw new AgentHubError(
        "LIVE_QUEUE_FULL",
        `follow-up message is ${bytes} bytes, above the ${LIVE_FOLLOW_UP_MAX_MESSAGE_BYTES}-byte per-message bound`,
      );
    }
    const command: LiveFollowUpCommand = { kind: "follow_up", text, ...this.issued(id) };
    const result = deferred<LiveTurnResult>();

    if (session.turn !== null || session.status === "running") {
      const pendingCount = session.queue.length + session.provider_queued.length;
      if (pendingCount >= LIVE_FOLLOW_UP_QUEUE_MAX_MESSAGES) {
        throw new AgentHubError(
          "LIVE_QUEUE_FULL",
          `live session "${id}" already has ${LIVE_FOLLOW_UP_QUEUE_MAX_MESSAGES} follow-ups pending (hub- and provider-queued together)`,
        );
      }
      if (session.queue_bytes + bytes > LIVE_FOLLOW_UP_QUEUE_MAX_BYTES) {
        throw new AgentHubError(
          "LIVE_QUEUE_FULL",
          `queued bytes would cross ${LIVE_FOLLOW_UP_QUEUE_MAX_BYTES} with a ${bytes}-byte message`,
        );
      }
      if (claim.support === "native") {
        // Capability honesty: a `native` claim means the PROVIDER queues
        // next-turn input mid-run. The hub delivers immediately and tracks
        // the pending result; it must never route the text through its own
        // queue while still claiming `native`.
        try {
          await session.transport.send(command);
        } catch (error) {
          const failure = asDelegateError(error);
          throw new AgentHubError(
            failure.code,
            `live follow-up delivery failed: ${failure.message}`,
          );
        }
        session.provider_queued.push({ command, bytes, result });
        session.queue_bytes += bytes;
        return result.promise;
      }
      session.queue.push({ command, bytes, result });
      session.queue_bytes += bytes;
      return result.promise;
    }

    if (session.status !== "idle") {
      throw new AgentHubError(
        "LIVE_SESSION_NOT_IDLE",
        `live session "${id}" is ${session.status}; follow-ups need idle or running`,
      );
    }
    return this.dispatchTurn(session, command, result);
  }

  private async dispatchTurn(
    session: ManagedSession,
    command: LivePromptCommand | LiveFollowUpCommand,
    result: Deferred<LiveTurnResult> = deferred<LiveTurnResult>(),
    alreadyDelivered = false,
  ): Promise<LiveTurnResult> {
    session.turn = {
      command,
      started_at: this.now().toISOString(),
      started_at_ms: this.now().getTime(),
      result,
      cancel_requested: false,
      error_seen: null,
      usage: null,
      streams: new Map(),
      delivered: alreadyDelivered,
    };
    if (alreadyDelivered) {
      return result.promise;
    }
    try {
      await session.transport.send(command);
    } catch (error) {
      session.turn = null;
      const failure = asDelegateError(error);
      throw new AgentHubError(failure.code, `live command dispatch failed: ${failure.message}`);
    }
    return result.promise;
  }

  private immediateResult(
    session: ManagedSession,
    commandId: string,
    kind: LiveTurnResult["kind"],
    finalText: LiveTurnResult["final_text"],
  ): LiveTurnResult {
    const at = this.now().toISOString();
    return {
      live_session_id: session.id,
      command_id: commandId,
      kind,
      outcome: "succeeded",
      final_text: finalText,
      usage: null,
      checkpoint: null,
      exit_code: null,
      exit_signal: null,
      started_at: at,
      finished_at: at,
      duration_ms: 0,
      error: null,
    };
  }

  /** Mid-turn guidance; native or hub-queued claims only. */
  async steer(id: string, text: string): Promise<LiveTurnResult> {
    const session = this.must(id);
    const claim = session.state.capabilities.steer;
    if (claim.support !== "native" && claim.support !== "hub-queued") {
      return this.refused(session, "steer");
    }
    if (session.turn === null || session.status !== "running") {
      throw new AgentHubError(
        "LIVE_SESSION_NOT_RUNNING",
        `steer is mid-turn guidance; live session "${id}" is ${session.status} with no turn in flight`,
      );
    }
    const command: LiveSteerCommand = { kind: "steer", text, ...this.issued(id) };
    await session.transport.send(command);
    return this.immediateResult(session, command.command_id, "steer", null);
  }

  /** Abort the in-flight turn (native or signal delivery path). */
  async cancel(id: string, reason: string | null): Promise<LiveTurnResult> {
    const session = this.must(id);
    const claim = session.state.capabilities.cancel;
    if (claim.support !== "native" && claim.support !== "signal") {
      return this.refused(session, "cancel");
    }
    const command: LiveCancelCommand = { kind: "cancel", reason, ...this.issued(id) };
    if (session.turn !== null) {
      session.turn.cancel_requested = true;
      await session.transport.send(command);
    }
    // No turn in flight: aborting nothing is a no-op, honestly reported.
    return this.immediateResult(session, command.command_id, "cancel", null);
  }

  /** Authoritative progress: forwarded only when the claim is native. */
  async requestStatus(id: string): Promise<LiveTurnResult> {
    const session = this.must(id);
    const claim = session.state.capabilities.status;
    const command: LiveStatusCommand = { kind: "status", ...this.issued(id) };
    if (claim.support === "native") {
      await session.transport.send(command);
      return this.immediateResult(session, command.command_id, "status", null);
    }
    if (claim.support === "derived") {
      // Hub answers from stream evidence; the contract forbids forwarding.
      const evidence = JSON.stringify({
        status: session.status,
        turn_in_flight: session.turn !== null,
        last_event_seq: session.ring.nextSeq - 1,
        queued_follow_ups: session.queue.length,
      });
      const bounded = truncateUtf8(evidence, this.options.maxTextBytes);
      return this.immediateResult(session, command.command_id, "status", {
        text: bounded,
        truncated: bounded !== evidence,
      });
    }
    return this.refused(session, "status");
  }

  async respondPermission(
    id: string,
    requestId: string,
    decision: LivePermissionDecision,
    note: string | null,
  ): Promise<LiveTurnResult> {
    const session = this.must(id);
    // v3 accepts exactly `allow_once` and `deny`. Anything else is a caller
    // error surfaced as such — never silently converted into a deny.
    if (decision !== "allow_once" && decision !== "deny") {
      throw new AgentHubError(
        "LIVE_COMMAND_INVALID",
        `permission decision "${String(decision)}" is outside the v3 vocabulary (allow_once, deny)`,
      );
    }
    if (session.state.capabilities.permission_response.support === "unsupported") {
      return this.refused(session, "permission_response");
    }
    if (!session.open_permissions.delete(requestId)) {
      throw new AgentHubError(
        "LIVE_PERMISSION_REQUEST_UNKNOWN",
        `live session "${id}" has no open permission request "${requestId}"`,
      );
    }
    const command: LivePermissionResponseCommand = {
      kind: "permission_response",
      request_id: requestId,
      decision,
      note,
      ...this.issued(id),
    };
    await session.transport.send(command);
    return this.immediateResult(session, command.command_id, "permission_response", null);
  }

  // -------------------------------------------------------------------------
  // Event consumption
  // -------------------------------------------------------------------------

  /** Replay recent events after `cursor`, or the honest expiry verdict. */
  eventsAfter(id: string, cursor: number): { events: LiveEvent[]; next_cursor: number } {
    const session = this.sessions.get(id);
    if (!session) {
      throw new AgentHubError("LIVE_SESSION_NOT_FOUND", `no live session "${id}" in this hub process`);
    }
    const replay = session.ring.readAfter(cursor);
    if (replay.status === "expired") {
      throw new AgentHubError(
        "EVENT_CURSOR_EXPIRED",
        `cursor ${replay.cursor} has evicted events behind it; oldest replayable cursor is ${replay.earliest_replayable_cursor} — resynchronize from durable state`,
      );
    }
    return { events: replay.events, next_cursor: replay.next_cursor };
  }

  /** The newest stamped cursor for a session (for expiry resynchronization). */
  eventCursor(id: string): number {
    const session = this.sessions.get(id);
    if (!session) {
      throw new AgentHubError("LIVE_SESSION_NOT_FOUND", `no live session "${id}" in this hub process`);
    }
    return session.ring.nextSeq - 1;
  }

  /** The authoritative durable mirror for a live session. */
  view(id: string): LiveSessionState {
    const session = this.sessions.get(id);
    if (!session) {
      throw new AgentHubError("LIVE_SESSION_NOT_FOUND", `no live session "${id}" in this hub process`);
    }
    return structuredClone(session.state);
  }

  /** Mid-session pin the caller asked for. */
  async checkpointNow(id: string): Promise<LiveSessionState> {
    const session = this.must(id);
    await this.enqueueDurable(session, async () => {
      await this.captureAndCommit(session, "requested");
    });
    return structuredClone(session.state);
  }

  private enqueueDurable<T>(session: ManagedSession, work: () => Promise<T>): Promise<T> {
    const next = session.durable_tail.then(work, work);
    session.durable_tail = next.catch(() => undefined);
    return next;
  }

  private async pumpLoop(session: ManagedSession): Promise<void> {
    try {
      for await (const raw of session.transport.events()) {
        // The hub is authoritative for envelope facts: transport-provided
        // seq/occurred_at cannot be trusted across restarts or providers.
        const event: LiveEvent = {
          live_session_id: session.id,
          seq: session.ring.nextSeq,
          transport: session.transport.id,
          occurred_at: this.now().toISOString(),
          body: raw.body,
        };
        const published = session.ring.push(event);
        await this.handleEvent(session, published.event);
      }
      // Stream ended without the session being closed: treat as a crash,
      // never as success.
      if (!session.closing && !session.torn_down) {
        await this.handleCrash(session, {
          code: "LIVE_TRANSPORT_EXHAUSTED",
          message: "the provider event stream ended without the session reaching a terminal status",
        });
      }
    } catch (error) {
      if (!session.closing && !session.torn_down) {
        await this.handleCrash(session, asDelegateError(error));
      }
    } finally {
      session.pump.resolve();
    }
  }

  private async handleEvent(session: ManagedSession, event: LiveEvent): Promise<void> {
    const body = event.body;
    switch (body.kind) {
      case "status": {
        const seen = body.status;
        if (session.turn !== null && seen === "idle") {
          session.status = "idle";
          await this.settleTurn(session);
          break;
        }
        if (
          !session.closing &&
          !session.torn_down &&
          !TERMINAL_STATUSES.includes(seen) &&
          seen !== session.status
        ) {
          session.status = seen;
          await this.enqueueDurable(session, () => this.commitStatus(session, seen));
        }
        break;
      }
      case "text": {
        if (session.turn === null) {
          break;
        }
        const stream = session.turn.streams.get(body.stream_id) ?? {
          chunks: [],
          truncated: false,
          final: false,
        };
        stream.chunks.push(body.text.text);
        stream.truncated ||= body.text.truncated;
        stream.final ||= body.final;
        session.turn.streams.set(body.stream_id, stream);
        break;
      }
      case "usage": {
        if (session.turn !== null) {
          session.turn.usage = body.usage;
        }
        break;
      }
      case "permission_request": {
        session.open_permissions.add(body.request_id);
        break;
      }
      case "error": {
        if (session.turn !== null && session.turn.error_seen === null) {
          session.turn.error_seen = {
            code: body.error.code,
            message: body.error.message,
            stage: "provider",
            retryable: body.error.retryable,
            provider: body.error.provider,
          };
        }
        break;
      }
      case "exit": {
        if (!session.closing && !session.torn_down) {
          await this.handleCrash(
            session,
            {
              code: "LIVE_PROVIDER_EXITED",
              message: `the provider process exited with ${
                body.exit_signal
                  ? `signal ${body.exit_signal}`
                  : `code ${body.exit_code ?? "unknown"}`
              } while the session was live`,
            },
            { exit_code: body.exit_code, exit_signal: body.exit_signal },
          );
        }
        break;
      }
      default:
        break;
    }
  }

  private async commitStatus(session: ManagedSession, status: LiveStatus): Promise<void> {
    const state = session.state;
    if (state.status === status) {
      return;
    }
    const next: LiveSessionState = {
      ...state,
      status,
      revision: state.revision + 1,
      resume: this.resumeWithCursor(session, state),
      updated_at: this.now().toISOString(),
    };
    await applyLiveTransition(
      { commonDir: this.options.commonDir, repositoryCwd: this.options.repositoryCwd },
      {
        kind: "advance",
        live_session_id: session.id,
        ref: liveRefFor(session.id),
        expected_ref: state.current_commit,
        new_commit: state.current_commit,
        next_state: next,
      },
    );
    session.state = next;
  }

  /**
   * Pin the worktree (only when its tree actually changed) and commit chain
   * + status in one sidecar-guarded CAS. Returns the checkpoint when the
   * chain moved, null when there was nothing new to pin.
   */
  private async captureAndCommit(
    session: ManagedSession,
    reason: CheckpointReason,
    options: { statusOverride?: LiveStatus; lastError?: LiveSessionState["last_error"] } = {},
  ): Promise<LiveCheckpoint | null> {
    const capture = await captureLiveCheckpoint(
      session.worktree.path,
      session.state.current_commit,
      reason,
      { seq: session.state.checkpoint_seq + 1, now: () => this.now() },
    );
    await this.options.observePhase?.("checkpoint-captured");

    const current = session.state;
    const advanced = capture.advanced;
    const next: LiveSessionState = {
      ...current,
      current_commit: advanced ? capture.checkpoint.commit : current.current_commit,
      checkpoint_seq: advanced ? current.checkpoint_seq + 1 : current.checkpoint_seq,
      // The reason belongs to the chain head: only a capture that actually
      // pinned a new commit records its reason.
      last_checkpoint_reason: advanced ? reason : current.last_checkpoint_reason,
      status: options.statusOverride ?? session.status,
      revision: current.revision + 1,
      last_error: options.lastError !== undefined ? options.lastError : current.last_error,
      resume: this.resumeWithCursor(session, current),
      updated_at: this.now().toISOString(),
    };
    await applyLiveTransition(
      { commonDir: this.options.commonDir, repositoryCwd: this.options.repositoryCwd },
      {
        kind: "advance",
        live_session_id: session.id,
        ref: liveRefFor(session.id),
        expected_ref: current.current_commit,
        new_commit: next.current_commit,
        next_state: next,
      },
    );
    await this.options.observePhase?.("state-advanced");
    session.state = next;
    return advanced ? capture.checkpoint : null;
  }

  /**
   * omp replays from a durable cursor: the seq is refreshed on every write
   * that already touches the record. Redelivery between durable writes is
   * deduplicated by seq — revision cadence here is deliberate.
   */
  private resumeWithCursor(
    session: ManagedSession,
    current: LiveSessionState,
  ): LiveSessionState["resume"] {
    const resume = current.resume;
    if (resume === null || resume.provider !== "omp") {
      return resume;
    }
    return { ...resume, last_event_seq: session.ring.nextSeq - 1 };
  }

  private settleTurn(session: ManagedSession): Promise<LiveTurnResult> {
    const turn = session.turn;
    if (turn === null) {
      return Promise.reject(new AgentHubError("INTERNAL_ERROR", "settleTurn without a turn"));
    }
    session.turn = null;
    const outcome: LiveTurnResult["outcome"] = turn.cancel_requested
      ? "cancelled"
      : turn.error_seen !== null
        ? "failed"
        : "succeeded";
    return this.finalizeTurn(session, turn, outcome, null);
  }

  private async finalizeTurn(
    session: ManagedSession,
    turn: ActiveTurn,
    outcome: LiveTurnResult["outcome"],
    exit: { exit_code: number | null; exit_signal: string | null } | null,
  ): Promise<LiveTurnResult> {
    const reason: CheckpointReason =
      outcome === "cancelled" ? "cancel" : outcome === "failed" ? "error" : "turn_end";
    const checkpoint = await this.enqueueDurable(session, () =>
      this.captureAndCommit(session, reason, {
        lastError: turn.error_seen ?? undefined,
      }),
    );
    const result = this.settleTurnResult(session, turn, outcome, exit, checkpoint);

    // The queue drains only toward another turn. A provider-queued follow-up
    // was ALREADY delivered (the native claim promised immediate delivery):
    // it becomes the tracked next turn without re-sending; hub-queued items
    // are delivered now that the terminal boundary has arrived.
    if (
      outcome !== "unsupported" &&
      !session.closing &&
      !session.torn_down &&
      session.turn === null &&
      session.provider_queued.length + session.queue.length > 0
    ) {
      const fromProviderQueue = session.provider_queued.length > 0;
      const nextItem = (
        fromProviderQueue ? session.provider_queued : session.queue
      ).shift() as QueuedFollowUp;
      session.queue_bytes -= nextItem.bytes;
      void this.dispatchTurn(session, nextItem.command, nextItem.result, fromProviderQueue).catch(
        (error) => {
          const failure = asDelegateError(error);
          nextItem.result.resolve(
            this.failedResult(session, nextItem.command, failure.code, failure.message),
          );
        },
      );
    }
    return result;
  }

  /**
   * Assembles and resolves the caller-visible turn result. `checkpoint` is
   * null on every path that could not PROVE the work stopped mutating — a
   * crash whose stop stayed unproven must never pin a possibly-hot tree.
   */
  private settleTurnResult(
    session: ManagedSession,
    turn: ActiveTurn,
    outcome: LiveTurnResult["outcome"],
    exit: { exit_code: number | null; exit_signal: string | null } | null,
    checkpoint: LiveCheckpoint | null,
  ): LiveTurnResult {
    let finalText: LiveTurnResult["final_text"] = null;
    for (const stream of turn.streams.values()) {
      if (stream.final) {
        const joined = stream.chunks.join("");
        const bounded = truncateUtf8(joined, this.options.maxTextBytes);
        finalText = { text: bounded, truncated: stream.truncated || bounded !== joined };
      }
    }

    const finishedAt = this.now();
    const result: LiveTurnResult = {
      live_session_id: session.id,
      command_id: turn.command.command_id,
      kind: turn.command.kind,
      outcome,
      final_text: finalText,
      usage: turn.usage,
      checkpoint,
      exit_code: exit?.exit_code ?? null,
      exit_signal: exit?.exit_signal ?? null,
      started_at: turn.started_at,
      finished_at: finishedAt.toISOString(),
      duration_ms: finishedAt.getTime() - turn.started_at_ms,
      error: turn.error_seen,
    };
    turn.result.resolve(result);
    return result;
  }

  private failedResult(
    session: ManagedSession,
    command: LiveCommand,
    code: string,
    message: string,
  ): LiveTurnResult {
    const at = this.now().toISOString();
    return {
      live_session_id: session.id,
      command_id: command.command_id,
      kind: command.kind,
      outcome: "failed",
      final_text: null,
      usage: null,
      checkpoint: null,
      exit_code: null,
      exit_signal: null,
      started_at: at,
      finished_at: at,
      duration_ms: 0,
      error: { code, message, stage: "transport", retryable: false, provider: session.state.provider },
    };
  }

  // -------------------------------------------------------------------------
  // Terminal paths: reap → checkpoint → state → teardown
  // -------------------------------------------------------------------------

  /**
   * Provider died while the hub watched it. Termination must be PROVEN
   * before anything is pinned: a crash whose `stop("terminate")` throws or
   * cannot show a closed report leaves the tree possibly still mutating, so
   * the session is marked orphaned with no checkpoint, no capture/commit,
   * and no teardown — the lease, worktree, and recorded ownership facts
   * stay for recovery, and an authorized `close(id, "terminate")` may
   * finish what this path could not prove. The in-flight turn still settles
   * honestly, as failed with `checkpoint: null`.
   */
  private async handleCrash(
    session: ManagedSession,
    error: { code: string; message: string },
    exit: { exit_code: number | null; exit_signal: string | null } | null = null,
  ): Promise<void> {
    if (session.torn_down || session.closing || TERMINAL_STATUSES.includes(session.status)) {
      return;
    }
    session.status = "error";

    const turn = session.turn;
    if (turn !== null) {
      session.turn = null;
      turn.error_seen ??= {
        code: error.code,
        message: error.message,
        stage: "provider",
        retryable: false,
        provider: session.state.provider,
      };
    }

    // Reap-before-checkpoint holds on this path too — but the stop REPORT
    // is the gate, not the call: only a proven `closed` may pin or tear
    // down. A stop that throws proves nothing and is treated as unproven.
    let stop: LiveStopReport;
    try {
      stop = await session.transport.stop("terminate");
    } catch {
      stop = { status: "orphaned", exit_code: null, exit_signal: null, waited_ms: 0 };
    }
    await this.options.observePhase?.("transport-stopped");

    if (stop.status !== "closed") {
      if (turn !== null) {
        this.settleTurnResult(session, turn, "failed", exit, null);
      }
      session.status = "orphaned";
      await this.enqueueDurable(session, () =>
        this.commitStatusWith(session, "orphaned", {
          code: "LIVE_STOP_UNPROVEN",
          message:
            `${error.message}; crash handling could not prove the provider process group is gone, ` +
            "so nothing was pinned: the lease, worktree, and ownership facts are retained for recovery",
          stage: "shutdown",
          retryable: false,
          provider: session.state.provider,
        }),
      );
      await this.failQueued(
        session,
        "LIVE_SESSION_CRASHED",
        "the live session crashed before the queued follow-up ran",
      );
      return;
    }

    if (turn !== null) {
      await this.finalizeTurn(session, turn, "failed", exit);
    } else {
      await this.enqueueDurable(session, () =>
        this.captureAndCommit(session, "error", {
          statusOverride: "error",
          lastError: {
            code: error.code,
            message: error.message,
            stage: "provider",
            retryable: false,
            provider: session.state.provider,
          },
        }),
      );
    }
    await this.failQueued(
      session,
      "LIVE_SESSION_CRASHED",
      "the live session crashed before the queued follow-up ran",
    );
    await this.teardown(session);
  }

  /**
   * Orderly shutdown requested by the caller. `graceful` never silently
   * escalates: if the provider will not come down under the graceful
   * attempt, the close honestly reports `orphaned` — bounded TERM→KILL
   * escalation happens only when `terminate` authorizes it, either in this
   * call or in a later `close(id, "terminate")` on a session this one
   * orphaned. An orphan keeps its lease, worktree, and ownership evidence
   * precisely so the later terminate can be proven and completed. A stop
   * that THROWS is as unproven as one that reports `orphaned`.
   */
  async close(id: string, mode: LiveStopMode = "graceful"): Promise<LiveCloseResult> {
    const session = this.sessions.get(id);
    if (!session) {
      throw new AgentHubError("LIVE_SESSION_NOT_FOUND", `no live session "${id}" in this hub process`);
    }
    if (session.torn_down) {
      return {
        state: structuredClone(session.state),
        stop: null,
        checkpoint_taken: false,
        cleanup_errors: [],
      };
    }
    if (
      TERMINAL_STATUSES.includes(session.status) &&
      !(session.status === "orphaned" && mode === "terminate")
    ) {
      // A terminal record gets its shutdown report once; only an authorized
      // terminate may re-attempt shutdown for an orphan.
      return {
        state: structuredClone(session.state),
        stop: null,
        checkpoint_taken: false,
        cleanup_errors: [],
      };
    }
    session.closing = true;
    session.status = "closing";
    await this.failQueued(
      session,
      "LIVE_SESSION_CLOSING",
      "the live session was closed before the queued follow-up ran",
    );
    let stop: LiveStopReport;
    try {
      stop = await session.transport.stop(mode);
    } catch {
      stop = { status: "orphaned", exit_code: null, exit_signal: null, waited_ms: 0 };
    }
    await this.options.observePhase?.("transport-stopped");

    if (stop.status !== "closed") {
      // Honest orphan: no checkpoint (the tree may still be under active
      // mutation), no teardown. Lease, worktree, and ownership facts stay
      // retained — and an authorized terminate close may finish the job.
      await this.enqueueDurable(session, () =>
        this.commitStatusWith(session, "orphaned", {
          code: "LIVE_STOP_UNPROVEN",
          message:
            `shutdown (${mode}) could not prove the provider process group is gone; the session is orphaned, not closed; ` +
            "the lease and worktree are retained and a terminate-authorized close may retry the shutdown",
          stage: "shutdown",
          retryable: false,
          provider: session.state.provider,
        }),
      );
      session.status = "orphaned";
      return { state: structuredClone(session.state), stop, checkpoint_taken: false, cleanup_errors: [] };
    }

    // Turn in flight when close was called: its work is cancelled *and pinned*
    // — after the reap above, before the close checkpoint.
    const turn = session.turn;
    if (turn !== null) {
      session.turn = null;
      turn.cancel_requested = true;
      await this.finalizeTurn(session, turn, "cancelled", {
        exit_code: stop.exit_code,
        exit_signal: stop.exit_signal,
      });
    }

    session.status = "closed";
    const checkpoint = await this.enqueueDurable(session, () =>
      this.captureAndCommit(session, "close", { statusOverride: "closed" }),
    );
    const cleanupErrors = await this.teardown(session);

    return {
      state: structuredClone(session.state),
      stop,
      checkpoint_taken: checkpoint !== null,
      cleanup_errors: cleanupErrors,
    };
  }

  private async commitStatusWith(
    session: ManagedSession,
    status: LiveStatus,
    lastError: LiveSessionState["last_error"],
  ): Promise<void> {
    const current = session.state;
    const next: LiveSessionState = {
      ...current,
      status,
      revision: current.revision + 1,
      last_error: lastError,
      resume: this.resumeWithCursor(session, current),
      updated_at: this.now().toISOString(),
    };
    await applyLiveTransition(
      { commonDir: this.options.commonDir, repositoryCwd: this.options.repositoryCwd },
      {
        kind: "advance",
        live_session_id: session.id,
        ref: liveRefFor(session.id),
        expected_ref: current.current_commit,
        new_commit: current.current_commit,
        next_state: next,
      },
    );
    await this.options.observePhase?.("state-advanced");
    session.state = next;
  }

  async closeAll(): Promise<(LiveCloseResult & { live_session_id: string })[]> {
    const results: (LiveCloseResult & { live_session_id: string })[] = [];
    for (const id of [...this.sessions.keys()]) {
      const session = this.sessions.get(id);
      try {
        results.push({ live_session_id: id, ...(await this.close(id)) });
      } catch (error) {
        const failure = asDelegateError(error);
        results.push({
          live_session_id: id,
          state: session ? structuredClone(session.state) : ({} as LiveSessionState),
          stop: null,
          checkpoint_taken: false,
          cleanup_errors: [failure],
        });
      }
    }
    return results;
  }

  private async failQueued(session: ManagedSession, code: string, message: string): Promise<void> {
    const queued = session.queue.splice(0, session.queue.length);
    const providerQueued = session.provider_queued.splice(0, session.provider_queued.length);
    session.queue_bytes = 0;
    for (const item of [...queued, ...providerQueued]) {
      // Provider-queued follow-ups reached the provider but will never see a
      // terminal boundary here; their caller-visible future still settles.
      item.result.resolve(this.failedResult(session, item.command, code, message));
    }
  }

  /**
   * Lease is the last thing to go: while a worktree or resource survives, the
   * lease keeps its audit trail and blocks double-ownership. The worktree
   * removal runs under the same short live-admin lock as every other worktree
   * mutation; pruning is NOT done here — it races with concurrent
   * `worktree add`; the launch path prunes while holding the lock.
   */
  private async teardown(session: ManagedSession): Promise<{ code: string; message: string }[]> {
    const errors = await this.releaseLaunchResources(session.lease, session.worktree);
    this.sessions.delete(session.id);
    session.torn_down = true;
    return errors;
  }

  // -------------------------------------------------------------------------
  // Recovery
  // -------------------------------------------------------------------------

  /**
   * Reconcile every durable lease with what the OS and the repository prove.
   * Liveness is re-proven or status is rewritten to `orphaned`; an orphaned
   * provider is reaped before the `crash_recovery` checkpoint, the checkpoint
   * before the state rewrite, the rewrite before teardown. A classification
   * this hub cannot prove (foreign host, reused pid, corrupt record, provider
   * identity unverifiable) is reported, never acted on.
   */
  async recover(): Promise<LiveRecoveryReport> {
    const { commonDir, repositoryCwd } = this.options;
    const leases = await listLiveLeases(commonDir);
    const report: LiveRecoveryReport = { scanned: leases.length, sessions: [] };

    for (const listed of leases) {
      if (listed.record === null) {
        report.sessions.push({
          live_session_id: listed.live_session_id,
          outcome: "manual",
          detail: `lease file ${listed.path} is corrupt; refusing to guess ownership`,
        });
        continue;
      }
      const lease = listed.record;
      if (this.sessions.has(lease.live_session_id)) {
        report.sessions.push({
          live_session_id: lease.live_session_id,
          outcome: "kept-live",
          detail: "this hub process owns the session",
        });
        continue;
      }

      const classification = await classifyLiveLease(lease, this.probes);
      if (classification.state === "foreign-host") {
        report.sessions.push({
          live_session_id: lease.live_session_id,
          outcome: "foreign",
          detail: `owned by host ${classification.owner_hostname}`,
        });
        continue;
      }
      if (classification.state === "hub-live") {
        report.sessions.push({
          live_session_id: lease.live_session_id,
          outcome: "kept-live",
          detail: `hub pid ${lease.hub_pid} is alive with a matching process identity`,
        });
        continue;
      }

      const provider = classification.provider;
      if (provider.state === "uncertain") {
        report.sessions.push({
          live_session_id: lease.live_session_id,
          outcome: "manual",
          detail: provider.reason,
        });
        continue;
      }

      if (provider.state === "alive") {
        const reap = await reapOrphanedProvider(lease, provider, this.probes);
        if (reap.status !== "reaped") {
          report.sessions.push({
            live_session_id: lease.live_session_id,
            outcome: "manual",
            detail: `orphaned provider survived bounded termination (${reap.status})`,
          });
          continue;
        }
        await this.options.observePhase?.("provider-reaped");
      }

      const { value: action, releaseError } = await withLiveLock(
        { commonDir, liveSessionId: lease.live_session_id, acquireLock: this.options.acquireLock },
        async (): Promise<RecoverAction> => {
          let state: LiveSessionState;
          try {
            state = await loadLiveState({ commonDir, repositoryCwd, liveSessionId: lease.live_session_id });
          } catch (error) {
            const failure = asDelegateError(error);
            if (failure.code === "LIVE_SESSION_NOT_FOUND") {
              return { kind: "cleaned", detail: "no state record: the launch never completed", worktreePath: lease.worktree_path };
            }
            return { kind: "manual", detail: failure.message, worktreePath: null };
          }
          if (TERMINAL_STATUSES.includes(state.status)) {
            return { kind: "cleaned", detail: `state is already ${state.status}`, worktreePath: lease.worktree_path };
          }

          let worktreeProblem: string | null = null;
          try {
            await inspectLiveWorktree(repositoryCwd, lease.worktree_path);
          } catch (error) {
            const failure = asDelegateError(error);
            if (failure.code !== "LIVE_WORKTREE_MISSING") {
              return { kind: "manual", detail: failure.message, worktreePath: null };
            }
            worktreeProblem = failure.message;
          }

          const current = state;
          if (worktreeProblem === null) {
            const capture = await captureLiveCheckpoint(
              lease.worktree_path,
              current.current_commit,
              "crash_recovery",
              { seq: current.checkpoint_seq + 1, now: () => this.now() },
            );
            await this.options.observePhase?.("checkpoint-captured");
            const advanced = capture.advanced;
            const next: LiveSessionState = {
              ...current,
              current_commit: advanced ? capture.checkpoint.commit : current.current_commit,
              checkpoint_seq: advanced ? current.checkpoint_seq + 1 : current.checkpoint_seq,
              last_checkpoint_reason: advanced ? "crash_recovery" : current.last_checkpoint_reason,
              status: "orphaned",
              revision: current.revision + 1,
              last_error: {
                code: "LIVE_SESSION_ORPHANED",
                message:
                  "recovered after hub loss: the provider group was terminated and the surviving worktree pinned before this rewrite",
                stage: "shutdown",
                retryable: false,
                provider: current.provider,
              },
              updated_at: this.now().toISOString(),
            };
            await applyLiveTransition(
              { commonDir, repositoryCwd },
              {
                kind: "advance",
                live_session_id: lease.live_session_id,
                ref: liveRefFor(lease.live_session_id),
                expected_ref: current.current_commit,
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
            ...current,
            status: "orphaned",
            revision: current.revision + 1,
            last_error: {
              code: "LIVE_WORKTREE_LOST",
              message: "recovered after hub loss: the live worktree no longer exists; nothing could be pinned",
              stage: "state",
              retryable: false,
              provider: current.provider,
            },
            updated_at: this.now().toISOString(),
          };
          await applyLiveTransition(
            { commonDir, repositoryCwd },
            {
              kind: "advance",
              live_session_id: lease.live_session_id,
              ref: liveRefFor(lease.live_session_id),
              expected_ref: current.current_commit,
              new_commit: current.current_commit,
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

      if (action.kind === "manual") {
        report.sessions.push({ live_session_id: lease.live_session_id, outcome: "manual", detail: action.detail });
        continue;
      }

      // Teardown strictly after the durable rewrite; the lease is released
      // last so its audit trail outlives every resource it named. The
      // worktree removal runs under the same short live-admin lock as every
      // other worktree mutation, and the lease may NEVER be released unless
      // that removal ran, under lock, and was proven. A cleanup that is not
      // proven — refused for lock unavailability or reporting a failure —
      // keeps the lease, names the surviving path, and is never clean.
      let detail = action.detail;
      if (action.worktreePath !== null) {
        let adminLock: RepositoryLock;
        try {
          adminLock = await this.acquireAdminLock();
        } catch (error) {
          report.sessions.push({
            live_session_id: lease.live_session_id,
            outcome: action.kind,
            detail:
              `${action.detail}; worktree cleanup refused: ${asDelegateError(error).message}; ` +
              `lease and worktree retained (never release a lease whose worktree cleanup did not run under the admin lock)`,
          });
          continue;
        }
        try {
          const removal = await removeLiveWorktree(repositoryCwd, {
            path: action.worktreePath,
            parentPath: dirname(action.worktreePath),
            // `removeLiveWorktree` never reads `base` and recovery does not
            // re-derive it; the empty string keeps the record honest.
            base: "",
          });
          if (removal.cleanup_error) {
            detail = `${detail}; worktree cleanup reported: ${removal.cleanup_error.message}; lease retained as the audit trail for ${action.worktreePath}`;
            report.sessions.push({ live_session_id: lease.live_session_id, outcome: action.kind, detail });
            continue;
          }
        } finally {
          try {
            await adminLock.release();
          } catch (lockReleaseError) {
            detail = `${detail}; live-admin lock release: ${asDelegateError(lockReleaseError).message}`;
          }
        }
      }
      try {
        await removeLiveLease(commonDir, lease.live_session_id, lease.token);
      } catch (error) {
        detail = `${detail}; lease release reported: ${asDelegateError(error).message}`;
      }
      if (releaseError) {
        detail = `${detail}; recovery lock release: ${releaseError.message}`;
      }
      report.sessions.push({ live_session_id: lease.live_session_id, outcome: action.kind, detail });
    }

    return report;
  }
}

interface RecoverAction {
  kind: "recovered" | "cleaned" | "manual";
  detail: string;
  /** Still-present worktree path to remove after the durable write, if any. */
  worktreePath: string | null;
}
