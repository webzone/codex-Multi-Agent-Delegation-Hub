import { randomUUID } from "node:crypto";

import { AgentHubError, asHubError } from "../errors.js";
import { resolveRepositoryIdentity } from "../git.js";
import type {
  Capabilities,
  PermissionDecision,
  PermissionPolicy,
  ProviderFactory,
  ProviderId,
  SessionRecord,
  StopMode,
  StopReport,
  TurnResult,
} from "../kernel/contracts.js";
import type { StartResult } from "../kernel/interaction-kernel.js";
import {
  InteractionKernel,
  DEFAULT_MAX_TEXT_BYTES,
  DEFAULT_SESSION_QUOTA,
  type ProbeDocument,
} from "../kernel/interaction-kernel.js";
import { WorkspaceLifecycle } from "../workspace/lifecycle.js";
import type {
  FinalizeReport,
  GcReport,
  RecoveryReport,
  WorkspaceInspection,
  WorkspaceLifecycleOptions,
} from "../workspace/lifecycle.js";
import { isWorkspaceSessionId } from "../workspace/home.js";
import type {
  HandoffDecision,
  WorkspaceRecord,
  WorkspaceResultRecord,
} from "../workspace/records.js";
import {
  productionBridgedFactories,
  productionBridgedProviderFactories,
  type BridgedTransportFactory,
} from "./transport-adapter.js";

/**
 * AgentHub — the public, provider-neutral integration of the rewrite.
 *
 * One object, two cores:
 *
 *   - `kernel` (`InteractionKernel`, P1): all interaction — prompt,
 *     follow_up, steer, cancel, status, permission_response, events,
 *     resume, close. Provider traffic crosses ONLY through the shipped
 *     bridges (omp RPC v2-only, pi RPC, agy stream-json, hermes ACP),
 *     auto-selected per provider with the honest-probe gate. Callers never
 *     pin a transport: the selected transport is reported back as a fact.
 *   - `lifecycle` (`WorkspaceLifecycle`, P2 — the ONE durable custody
 *     truth): AGENT_HUB_HOME-anchored records, one isolated worktree per
 *     session, exact result identity per terminal turn, leases, close/
 *     handoff/retention custody, recovery, and the only GC deletion path.
 *
 * Wiring is exclusively through the published seams: `lifecycle.mirror` is
 * the kernel's `DurableMirror`, `lifecycle.onProviderSpawn` is its spawn
 * hook, and `kernel.attached()` is the input to `recover`/`gc`.
 *
 * Custody rules the hub inherits and never bends:
 *   - `close` never deletes anything — the worktree, lease, results, and
 *     ref all stay until an explicit `accepted`/`discarded` handoff names
 *     the exact result, and then only until the retention window expires;
 *   - automatic cleanup (startup catch-up + optional bounded sweep) runs
 *     `recover` + `gc` with this process's attached sessions as the
 *     reference set, so nothing acknowledged, orphaned, uncertain, or
 *     actively referenced is ever aged out.
 */

export const HUB_PROVIDERS = ["omp", "pi", "agy", "hermes"] as const;

/** The default process-wide session quota (per hub host process). */
export const HUB_PROCESS_SESSION_QUOTA = DEFAULT_SESSION_QUOTA;

/** Bounded default for the periodic custody sweep (15 minutes). */
export const HUB_GC_SWEEP_INTERVAL_MS = 15 * 60 * 1000;

export interface AgentHubOptions extends WorkspaceLifecycleOptions {
  /** Injected transport bridges; default: the four shipped productions. */
  transportFactories?: readonly BridgedTransportFactory[];
  /** Provider preference/decline logic; default: production pairings. */
  providerFactories?: readonly ProviderFactory[];
  maxTextBytes?: number;
  processQuota?: number;
  newSessionId?: () => string;
  /**
   * Automatic custody cleanup. Default true: after the hub is constructed,
   * `recover` + `gc` run once (startup catch-up). Never touches anything
   * attached here, unacknowledged, orphaned, uncertain, or referenced.
   */
  autoCleanup?: boolean;
  /**
   * Bounded periodic sweep interval for long-lived hosts. 0 (default) means
   * startup catch-up only; a positive value arms an unref'd interval.
   */
  gcIntervalMs?: number;
}

export interface StartOptions {
  provider: ProviderId;
  /** Display label for the owning agent (custody record metadata). */
  agent?: string;
  permission_policy?: PermissionPolicy;
  max_text_bytes?: number;
  /** Must be a UUID when provided (custody paths are keyed by it). */
  session_id?: string;
}

export interface ResumeOptions {
  permission_policy?: PermissionPolicy;
  max_text_bytes?: number;
}

/** The consumer's exact takeover decision on a closed workspace's head. */
export interface HandoffDecisionInput {
  decision: HandoffDecision;
  result_seq: number;
  commit: string;
  consumer?: string | null;
}

/** Kernel turn result plus its published exact result identity (P2). */
export interface TurnDocument extends TurnResult {
  /** The exact result the turn published; null when not published. */
  result: WorkspaceResultRecord | null;
  /** Why publication legitimately did not happen (non-turn, refused command). */
  publish_skipped_reason: string | null;
  /** Set when publication was attempted and failed; the chain is stale. */
  publish_error?: { code: string; message: string };
}

export interface HubStartDocument {
  session_id: string;
  provider: string;
  /** Reported fact, never an input: selection is the hub's alone. */
  transport: string;
  worktree_path: string;
  capabilities: Capabilities;
  probe: ProbeDocument;
  record: SessionRecord;
  workspace: WorkspaceRecord;
}

export interface HubCloseDocument {
  session_id: string;
  record: SessionRecord;
  stop: StopReport | null;
  /** Custody finalization: captured state, retained everything. */
  finalize: FinalizeReport;
  /** True when this hub's own lease was released because shutdown was PROVEN. */
  lease_released: boolean;
}

export interface HubStatusDocument {
  session_id: string;
  attached_here: boolean;
  workspace: WorkspaceRecord;
  runtime: WorkspaceInspection["runtime"];
  lease: WorkspaceInspection["lease"];
  worktree: WorkspaceInspection["worktree"];
}

export interface HubCleanupDocument {
  recovery: RecoveryReport;
  cleanup: GcReport;
}

export class AgentHub {
  readonly kernel: InteractionKernel;
  readonly lifecycle: WorkspaceLifecycle;

  /** Absolute caller checkout the hub binds sessions to (never mutated). */
  readonly repositoryCwd: string;
  private readonly commonDirResolved: string;
  private readonly bridges: readonly BridgedTransportFactory[];
  private readonly processQuota: number;
  private readonly pendingCommands = new Map<string, Set<Promise<unknown>>>();
  private sweepTimer: ReturnType<typeof setInterval> | null = null;
  private lastCleanupReport: HubCleanupDocument | null = null;
  private closed = false;

  private constructor(
    kernel: InteractionKernel,
    lifecycle: WorkspaceLifecycle,
    bridges: readonly BridgedTransportFactory[],
    repositoryCwd: string,
    commonDir: string,
    processQuota: number,
  ) {
    this.kernel = kernel;
    this.lifecycle = lifecycle;
    this.bridges = bridges;
    this.repositoryCwd = repositoryCwd;
    this.commonDirResolved = commonDir;
    this.processQuota = processQuota;
  }

  /** Bind a hub to one repository checkout (identity resolved eagerly). */
  static async open(workspace: string, options: AgentHubOptions = {}): Promise<AgentHub> {
    const identity = await resolveRepositoryIdentity(workspace);
    const lifecycle = new WorkspaceLifecycle({
      ...(options.home === undefined ? {} : { home: options.home }),
      ...(options.env === undefined ? {} : { env: options.env }),
      ...(options.now === undefined ? {} : { now: options.now }),
      ...(options.retentionMs === undefined ? {} : { retentionMs: options.retentionMs }),
      ...(options.probes === undefined ? {} : { probes: options.probes }),
      ...(options.lockWaitMs === undefined ? {} : { lockWaitMs: options.lockWaitMs }),
      ...(options.observePhase === undefined ? {} : { observePhase: options.observePhase }),
      ...(options.probePid === undefined ? {} : { probePid: options.probePid }),
    });
    const bridges = options.transportFactories ?? productionBridgedFactories();
    const kernel = new InteractionKernel({
      transportFactories: bridges,
      providerFactories: options.providerFactories ?? productionBridgedProviderFactories(),
      maxTextBytes: options.maxTextBytes ?? DEFAULT_MAX_TEXT_BYTES,
      maxLiveSessions: options.processQuota ?? HUB_PROCESS_SESSION_QUOTA,
      ...(options.now === undefined ? {} : { now: options.now }),
      newSessionId: options.newSessionId ?? (() => randomUUID()),
      durable: { commit: lifecycle.mirror.commit },
      onProviderSpawn: lifecycle.onProviderSpawn,
    });
    const hub = new AgentHub(
      kernel,
      lifecycle,
      bridges,
      identity.worktree_root,
      identity.common_dir,
      options.processQuota ?? HUB_PROCESS_SESSION_QUOTA,
    );
    if (options.autoCleanup !== false) {
      // Startup catch-up: settle provably-dead orphans, then collect only
      // expired, decided, unreferenced workspaces. Bounded: one pass.
      await hub.cleanup();
    }
    if (options.gcIntervalMs !== undefined && options.gcIntervalMs > 0) {
      hub.sweepTimer = setInterval(() => {
        void hub.cleanup().catch(() => undefined);
      }, options.gcIntervalMs);
      hub.sweepTimer.unref?.();
    }
    return hub;
  }

  /** Process-wide count of sessions this hub's kernel still runs. */
  get activeCount(): number {
    return this.kernel.attached().length;
  }

  /** The repository's Git common dir (identity anchor for hub caching). */
  get commonDir(): string {
    return this.commonDirResolved;
  }

  /** The AGENT_HUB_HOME custody root this hub writes through. */
  get home(): string {
    return this.lifecycle.home;
  }

  /** The most recent startup/sweep/manual cleanup report, if any. */
  get lastCleanup(): HubCleanupDocument | null {
    return this.lastCleanupReport;
  }

  // ---------------------------------------------------------------------------
  // Launch / resume
  // ---------------------------------------------------------------------------

  async start(options: StartOptions): Promise<HubStartDocument> {
    this.assertNotClosed();
    const sessionId = options.session_id ?? randomUUID();
    if (!isWorkspaceSessionId(sessionId)) {
      throw new AgentHubError(
        "WORKSPACE_ID_INVALID",
        `session id "${sessionId}" must be a UUID; custody paths are keyed by it`,
      );
    }
    if (this.activeCount >= this.processQuota) {
      throw new AgentHubError(
        "SESSION_QUOTA_EXCEEDED",
        `this hub process already runs ${this.activeCount} of ${this.processQuota} live sessions`,
      );
    }
    // Selection before provisioning: a probe decline must not litter custody.
    const selection = await this.kernel.selectTransport(options.provider);
    const workspace = await this.lifecycle.provision({
      session_id: sessionId,
      repository_cwd: this.repositoryCwd,
      ...(options.agent === undefined ? {} : { agent: options.agent }),
    });
    try {
      const started = await this.kernel.start({
        provider: options.provider,
        session_id: sessionId,
        workspace: workspace.worktree_path,
        resume: null,
        permission_policy: options.permission_policy ?? "deny",
        ...(options.max_text_bytes === undefined ? {} : { max_text_bytes: options.max_text_bytes }),
      });
      return this.startDocument(sessionId, started, workspace);
    } catch (error) {
      throw await this.finalizeFailedLaunch(sessionId, error);
    }
  }

  async resume(sessionId: string, options: ResumeOptions = {}): Promise<HubStartDocument> {
    this.assertNotClosed();
    if (this.kernel.attached().some((record) => record.session_id === sessionId)) {
      throw new AgentHubError(
        "SESSION_ALREADY_LIVE",
        `session "${sessionId}" is already running in this hub process`,
      );
    }
    // Custody refuses resume over a handoff decision, a live/uncertain
    // lease, or a non-terminal runtime — before anything launches.
    const workspace = await this.lifecycle.reopenForResume(sessionId);
    const inspected = await this.lifecycle.inspect(sessionId);
    if (inspected.worktree.state !== "present") {
      throw new AgentHubError(
        "WORKSPACE_WORKTREE_UNAVAILABLE",
        `cannot resume "${sessionId}": worktree ${workspace.worktree_path} is ${inspected.worktree.state}`,
      );
    }
    if (inspected.runtime.state !== "present") {
      throw new AgentHubError(
        "WORKSPACE_RUNTIME_MISSING",
        `cannot resume "${sessionId}": its runtime mirror is ${inspected.runtime.state}`,
      );
    }
    const prior = inspected.runtime.record;
    // Internal pin only: a session resumes on the transport it launched on.
    const selection = await this.kernel.selectTransport(prior.provider, prior.transport);
    if (selection.factory.transport !== prior.transport) {
      throw new AgentHubError(
        "TRANSPORT_PAIRING_INVALID",
        `session "${sessionId}" was launched on "${prior.transport}"; resuming on "${selection.factory.transport}" is refused`,
      );
    }
    const started = await this.kernel.resume(
      { ...prior, workspace: workspace.worktree_path },
      {
        permission_policy: options.permission_policy ?? "deny",
        ...(options.max_text_bytes === undefined ? {} : { max_text_bytes: options.max_text_bytes }),
      },
    );
    return this.startDocument(sessionId, started, workspace);
  }

  private startDocument(
    sessionId: string,
    started: StartResult,
    workspace: WorkspaceRecord,
  ): HubStartDocument {
    return {
      session_id: sessionId,
      provider: started.record.provider,
      transport: started.record.transport,
      worktree_path: workspace.worktree_path,
      capabilities: started.capabilities,
      probe: started.probe,
      record: started.record,
      workspace,
    };
  }

  /**
   * A launch that rejected after provisioning closes custody honestly with
   * the failure as evidence. Nothing is deleted: the worktree and any
   * recorded lease stay for recovery/GC to re-prove — handoff still decides.
   */
  private async finalizeFailedLaunch(sessionId: string, cause: unknown): Promise<AgentHubError> {
    const failure = asHubError(cause);
    try {
      await this.lifecycle.finalizeClosure({
        session_id: sessionId,
        evidence: `start failed: ${failure.code}: ${failure.message}`,
        runtime_status: "error",
      });
    } catch (finalizeError) {
      const retained = asHubError(finalizeError);
      return new AgentHubError(
        failure.code,
        `${failure.message}; custody could not be closed (${retained.code}: ${retained.message}) and is retained for \`agent-hub gc\` review`,
      );
    }
    return new AgentHubError(
      failure.code,
      `${failure.message}; the isolated worktree and lease (if any) are retained until an explicit handoff decision`,
    );
  }

  // ---------------------------------------------------------------------------
  // Real-time commands (kernel owns the gates; P2 owns result identity)
  // ---------------------------------------------------------------------------

  /** Keep a command's publication ahead of custody finalization on close. */
  private trackCommand<T extends TurnDocument>(sessionId: string, operation: () => Promise<T>): Promise<T> {
    const pending = operation();
    const commands = this.pendingCommands.get(sessionId) ?? new Set<Promise<unknown>>();
    commands.add(pending);
    this.pendingCommands.set(sessionId, commands);
    const release = (): void => {
      commands.delete(pending);
      if (commands.size === 0) this.pendingCommands.delete(sessionId);
    };
    void pending.then(release, release);
    return pending;
  }

  private async waitForPendingCommands(sessionId: string): Promise<void> {
    for (;;) {
      const pending = this.pendingCommands.get(sessionId);
      if (pending === undefined || pending.size === 0) return;
      await Promise.allSettled([...pending]);
    }
  }

  async prompt(sessionId: string, text: string): Promise<TurnDocument> {
    return this.trackCommand(sessionId, async () => this.publish(await this.kernel.prompt(sessionId, text)));
  }

  async followUp(sessionId: string, text: string): Promise<TurnDocument> {
    return this.trackCommand(sessionId, async () => this.publish(await this.kernel.followUp(sessionId, text)));
  }

  async steer(sessionId: string, text: string): Promise<TurnDocument> {
    return this.trackCommand(sessionId, async () => this.publish(await this.kernel.steer(sessionId, text)));
  }

  async cancel(sessionId: string, reason: string | null): Promise<TurnDocument> {
    return this.trackCommand(sessionId, async () => this.publish(await this.kernel.cancel(sessionId, reason)));
  }

  async requestStatus(sessionId: string): Promise<TurnDocument> {
    return this.trackCommand(sessionId, async () => this.publish(await this.kernel.requestStatus(sessionId)));
  }

  async respondPermission(
    sessionId: string,
    requestId: string,
    decision: PermissionDecision,
    note: string | null = null,
  ): Promise<TurnDocument> {
    return this.trackCommand(sessionId, async () =>
      this.publish(await this.kernel.respondPermission(sessionId, requestId, decision, note)),
    );
  }

  /**
   * Publish the exact result identity of a settled command (P2 decides what
   * is a turn). A publication failure is reported on the document, never
   * silently — the turn's own result always lands first.
   */
  private async publish(turn: TurnResult): Promise<TurnDocument> {
    try {
      const published = await this.lifecycle.publishTurnResult(turn.session_id, turn);
      return {
        ...published.turn,
        result: published.result,
        publish_skipped_reason: published.publish_skipped_reason,
      };
    } catch (error) {
      const failure = asHubError(error);
      return {
        ...turn,
        result: null,
        publish_skipped_reason: null,
        publish_error: { code: failure.code, message: failure.message },
      };
    }
  }

  // ---------------------------------------------------------------------------
  // Events and status
  // ---------------------------------------------------------------------------

  eventsAfter(sessionId: string, cursor: number) {
    return this.kernel.eventsAfter(sessionId, cursor);
  }

  eventCursor(sessionId: string): number {
    return this.kernel.eventCursor(sessionId);
  }

  /** Replay-then-live tail for an attached session (CLI attach, MCP streams). */
  streamEvents(sessionId: string, options: { after?: number } = {}) {
    return this.kernel.streamEvents(sessionId, options);
  }

  attached() {
    return this.kernel.attached();
  }

  async status(sessionId: string): Promise<HubStatusDocument> {
    const inspected = await this.lifecycle.inspect(sessionId);
    return {
      session_id: sessionId,
      attached_here: this.kernel.attached().some((record) => record.session_id === sessionId),
      workspace: inspected.workspace,
      runtime: inspected.runtime,
      lease: inspected.lease,
      worktree: inspected.worktree,
    };
  }

  /** Every durable workspace under AGENT_HUB_HOME for this hub. */
  async list() {
    const records = await this.lifecycle.list();
    const attached = new Set(this.kernel.attached().map((record) => record.session_id));
    return records.map((workspace) => ({
      workspace,
      attached_here: attached.has(workspace.session_id),
    }));
  }

  // ---------------------------------------------------------------------------
  // Close / handoff / cleanup
  // ---------------------------------------------------------------------------

  /**
   * Kernel close + custody finalize. Retains everything: the isolated
   * worktree, lease, results, and ref stay under custody until an explicit
   * `accepted`/`discarded` handoff names this workspace's exact result and
   * the retention window passes.
   */
  async close(sessionId: string, mode: StopMode = "graceful"): Promise<HubCloseDocument> {
    const { close, finalize } = await this.lifecycle.closeSession(
      this.kernel,
      sessionId,
      mode,
      () => this.waitForPendingCommands(sessionId),
    );
    let leaseReleased = false;
    if (close.record.status === "closed") {
      // Proven shutdown releases the ownership lease; an `orphaned` close
      // keeps it exactly where it is for recover/gc to re-prove.
      leaseReleased = await this.lifecycle.releaseClosedLease(sessionId);
    }
    return {
      session_id: sessionId,
      record: close.record,
      stop: close.stop,
      finalize,
      lease_released: leaseReleased,
    };
  }

  async closeAll(): Promise<HubCloseDocument[]> {
    this.stopSweep();
    const results: HubCloseDocument[] = [];
    for (const record of this.kernel.attached()) {
      results.push(await this.close(record.session_id));
    }
    await this.kernel.settle();
    this.closed = true;
    return results;
  }

  /**
   * The consumer's exact decision on a closed workspace's published head.
   * Refused unless `result_seq`/`commit` name the current head exactly; the
   * retention clock starts at the decision. Nothing is deleted here.
   */
  handoff(sessionId: string, decision: HandoffDecisionInput): Promise<WorkspaceRecord> {
    return this.lifecycle.handoff({
      session_id: sessionId,
      decision: decision.decision,
      result_seq: decision.result_seq,
      commit: decision.commit,
      consumer: decision.consumer ?? null,
    });
  }

  /**
   * The safe manual reconciliation path: `recover` first (re-prove leases,
   * settle provably-dead orphans, close what the kernel mirror proves
   * closed — deletes nothing), then `gc` (deletes only workspaces whose
   * handoff decision is exact, whose retention expired, and whose every
   * other precondition is provable). Sessions attached here are referenced
   * by definition and are untouchable.
   */
  async cleanup(): Promise<HubCleanupDocument> {
    const attached = this.kernel.attached();
    const recovery = await this.lifecycle.recover(attached);
    const cleanup = await this.lifecycle.gc(attached);
    this.lastCleanupReport = { recovery, cleanup };
    return this.lastCleanupReport;
  }

  /** Honest probe documents for the shipped providers (launches nothing). */
  async probe(provider?: ProviderId): Promise<ProbeDocument[]> {
    const documents: ProbeDocument[] = [];
    for (const bridge of this.bridges) {
      if (provider !== undefined && bridge.provider !== provider) continue;
      const probe = await bridge.probe();
      documents.push({
        provider: bridge.provider,
        transport: bridge.transport,
        found: probe.found,
        version: probe.version,
        detail: probe.detail,
      });
    }
    if (provider !== undefined && documents.length === 0) {
      throw new AgentHubError(
        "TRANSPORT_UNAVAILABLE",
        `no hub transport pairs with provider "${provider}"`,
      );
    }
    return documents;
  }

  /** Wait until every kernel pump has drained (host shutdown seam). */
  async settle(): Promise<void> {
    this.stopSweep();
    await this.kernel.settle();
  }

  private stopSweep(): void {
    if (this.sweepTimer !== null) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }
  }

  private assertNotClosed(): void {
    if (this.closed) {
      throw new AgentHubError("HUB_CLOSED", "this hub host has been closed; start a new host");
    }
  }
}
