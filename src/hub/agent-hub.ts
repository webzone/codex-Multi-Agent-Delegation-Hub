import { AgentHubError, asDelegateError } from "../errors.js";
import type {
  Capabilities,
  KernelError,
  PermissionDecision,
  PermissionPolicy,
  ProviderId,
  StopMode,
  StopReport,
  TransportId,
  TransportFactory,
  TurnResult,
} from "../kernel/contracts.js";
import {
  InteractionKernel,
  DEFAULT_MAX_TEXT_BYTES,
  DEFAULT_SESSION_QUOTA,
  type ProbeDocument,
} from "../kernel/interaction-kernel.js";
import type {
  CheckpointReason,
  LiveCheckpoint,
  LiveError,
  LiveProviderId,
  LiveSessionState,
  LiveTransportId,
} from "../live/types.js";
import { TERMINAL_STATUSES, liveRefFor } from "../live/state.js";
import { isLiveProvider, LIVE_TRANSPORT_PAIRINGS } from "../live/provider-registry.js";
import type { LiveLeaseProbes } from "../live/lease.js";
import { acquireRepositoryLock } from "../locks.js";
import {
  productionBridgedFactories,
  productionBridgedProviderFactories,
  type BridgedTransportFactory,
} from "./transport-adapter.js";
import {
  WorkspaceLifecycle,
  type HandoffDocument,
  type LifecyclePhase,
  type PreparedLaunch,
  type ReconcileReport,
} from "./workspace-lifecycle.js";

/**
 * AgentHub — the public, provider-neutral integration of the rewrite.
 *
 * One object, two cores, exactly as the contract names them:
 *
 *   - `kernel` (`InteractionKernel`, P1): all interaction — prompt,
 *     follow_up, steer, cancel, status, permission_response, events,
 *     resume, close. Provider traffic crosses ONLY through the injected
 *     transports (omp RPC v2-only, pi RPC, agy stream-json, hermes ACP),
 *     selected automatically per provider with an honest probe gate.
 *   - `lifecycle` (`WorkspaceLifecycle`, P4 composition of the durable
 *     primitives): worktree + checkpoint chain + lease + durable records
 *     around every session, handoff, and safe GC.
 *
 * Every hub-observed terminal boundary pins the checkpoint chain; every
 * kernel durable commit projects onto the lifecycle state; teardown runs
 * only on proven shutdown. The hub adds NO second interaction loop: the
 * kernel owns commands, the lifecycle owns resources, and each durable
 * write serializes through the lifecycle's per-session tail.
 */

export const HUB_PROVIDERS = ["omp", "pi", "agy", "hermes"] as const;
export const HUB_TRANSPORT_BY_PROVIDER: Record<(typeof HUB_PROVIDERS)[number], LiveTransportId> = {
  omp: "omp-rpc",
  pi: "pi-rpc",
  agy: "agy-stream-json",
  hermes: "hermes-acp",
};

/** The default process-wide session quota (per hub host process). */
export const HUB_PROCESS_SESSION_QUOTA = DEFAULT_SESSION_QUOTA;

/** A provider/transport pairing the durable store's vocabulary accepts. */
function assertRecordablePair(provider: ProviderId, transport: TransportId): void {
  if (!isLiveProvider(provider) || LIVE_TRANSPORT_PAIRINGS[transport as LiveTransportId] !== provider) {
    throw new AgentHubError(
      "PROVIDER_UNSUPPORTED",
      `provider/transport "${provider}/${transport}" is outside the hub's shipped pairings (omp RPC v2, pi RPC, agy stream-json, hermes ACP)`,
    );
  }
}

export interface AgentHubOptions {
  /** Injected bridges; default: the four shipped production transports. */
  transportFactories?: readonly BridgedTransportFactory[];
  /** Provider preference/decline logic; default: production pairings. */
  providerFactories?: readonly ProviderFactoryLike[];
  now?: () => Date;
  maxTextBytes?: number;
  processQuota?: number;
  commonDirQuota?: number;
  tmpRoot?: string;
  acquireLock?: typeof acquireRepositoryLock;
  probes?: LiveLeaseProbes;
  observeLifecyclePhase?: (phase: LifecyclePhase) => Promise<void> | void;
  newSessionId?: () => string;
}

/** Kernel-shaped provider factory (the adapter's production wrappers satisfy it). */
export interface ProviderFactoryLike {
  readonly provider: ProviderId;
  readonly transports: readonly TransportId[];
  selectTransport(factories: readonly TransportFactory[]): TransportFactory | null;
}

export interface StartOptions {
  provider: ProviderId;
  transport?: TransportId;
  permission_policy?: PermissionPolicy;
  max_text_bytes?: number;
  allow_dirty?: boolean;
  session_id?: string;
}

export interface ResumeOptions {
  transport?: TransportId;
  permission_policy?: PermissionPolicy;
  max_text_bytes?: number;
  allow_dirty?: boolean;
}

/** Kernel turn result plus the checkpoint pinned for it (null when none). */
export interface TurnDocument extends TurnResult {
  checkpoint: LiveCheckpoint | null;
  /** Set when the terminal boundary could not be pinned; the chain is stale. */
  checkpoint_error?: { code: string; message: string };
}

export interface HubStartDocument {
  session_id: string;
  provider: string;
  transport: string;
  workspace: string;
  capabilities: Capabilities;
  probe: ProbeDocument;
  record: unknown;
  state: LiveSessionState;
  warnings: { code: string; message: string }[];
}

export interface HubCloseDocument {
  session_id: string;
  record: unknown;
  stop: StopReport | null;
  state: LiveSessionState | null;
  checkpoint_taken: boolean;
  cleanup_errors: { code: string; message: string }[];
}

export interface HubStatusDocument {
  session_id: string;
  attached_here: boolean;
  state: LiveSessionState | null;
  record: unknown;
  lease: {
    owned_here: boolean;
    provider_pid: number | null;
    provider_pgid: number | null;
  } | null;
  ref: string;
}

function checkpointReasonFor(outcome: TurnResult["outcome"]): CheckpointReason {
  if (outcome === "cancelled") return "cancel";
  if (outcome === "failed") return "error";
  return "turn_end";
}

/** Project a kernel error onto the durable live vocabulary (provider-agnostic ids → null). */
function liveErrorOf(error: KernelError): LiveError {
  return {
    code: error.code,
    message: error.message,
    stage: error.stage,
    retryable: error.retryable,
    provider: isLiveProvider(error.provider ?? "") ? (error.provider as LiveProviderId) : null,
  };
}

export class AgentHub {
  readonly kernel: InteractionKernel;
  readonly lifecycle: WorkspaceLifecycle;

  /** Reserved launch resources, visible to the spawn hook during `kernel.start`. */
  private readonly launches: Map<string, PreparedLaunch>;
  private readonly bridges: readonly BridgedTransportFactory[];
  private closed = false;

  private constructor(
    kernel: InteractionKernel,
    lifecycle: WorkspaceLifecycle,
    bridges: readonly BridgedTransportFactory[],
    launches: Map<string, PreparedLaunch>,
  ) {
    this.kernel = kernel;
    this.lifecycle = lifecycle;
    this.bridges = bridges;
    this.launches = launches;
  }

  /** Bind a hub to one repository workspace (identity resolved eagerly). */
  static async open(workspace: string, options: AgentHubOptions = {}): Promise<AgentHub> {
    const lifecycle = await WorkspaceLifecycle.open(workspace, {
      now: options.now,
      tmpRoot: options.tmpRoot,
      acquireLock: options.acquireLock,
      probes: options.probes,
      commonDirQuota: options.commonDirQuota,
      observePhase: options.observeLifecyclePhase,
    });
    const bridges = options.transportFactories ?? productionBridgedFactories();
    const launches = new Map<string, PreparedLaunch>();
    const kernel = new InteractionKernel({
      transportFactories: bridges,
      providerFactories: options.providerFactories ?? productionBridgedProviderFactories(),
      maxTextBytes: options.maxTextBytes ?? DEFAULT_MAX_TEXT_BYTES,
      maxLiveSessions: options.processQuota ?? HUB_PROCESS_SESSION_QUOTA,
      newSessionId: options.newSessionId ?? (() => lifecycle.newSessionId()),
      durable: {
        commit: (record) => lifecycle.commitMirrorRecord(record),
      },
      onProviderSpawn: async (sessionId, facts) => {
        const prepared = launches.get(sessionId);
        if (prepared === undefined) {
          throw new AgentHubError(
            "SPAWN_UNOWNED",
            `provider spawn for session "${sessionId}" arrived outside a hub-reserved launch; refusing to record ownership`,
          );
        }
        await lifecycle.recordSpawn(prepared, facts);
      },
    });
    return new AgentHub(kernel, lifecycle, bridges, launches);
  }

  /** Process-wide count of sessions this hub's kernel still runs. */
  get activeCount(): number {
    return this.kernel.attached().length;
  }

  get commonDir(): string {
    return this.lifecycle.commonDir;
  }

  // ---------------------------------------------------------------------------
  // Launch / resume
  // ---------------------------------------------------------------------------

  async start(options: StartOptions): Promise<HubStartDocument> {
    this.assertNotClosed();
    const sessionId = options.session_id ?? this.lifecycle.newSessionId();
    const selection = await this.kernel.selectTransport(options.provider, options.transport);
    assertRecordablePair(selection.factory.provider, selection.factory.transport);
    const bridge = selection.factory as BridgedTransportFactory;
    const base = this.lifecycle.identity.head;
    const prepared = await this.lifecycle.reserveLaunchResources(
      sessionId,
      options.provider as LiveProviderId,
      base,
      { allowDirty: options.allow_dirty },
    );
    this.launches.set(sessionId, prepared);
    try {
      const started = await this.kernel.start({
        provider: options.provider,
        transport: options.transport,
        session_id: sessionId,
        workspace: prepared.worktree.path,
        resume: null,
        permission_policy: options.permission_policy ?? "deny",
        max_text_bytes: options.max_text_bytes,
      });
      const facts = bridge.takeLaunchFacts(sessionId);
      if (facts === null) {
        throw new AgentHubError(
          "CAPABILITY_SNAPSHOT_INVALID",
          `the "${started.record.transport}" transport produced no launch descriptor; the session cannot be recorded honestly`,
        );
      }
      const state = await this.lifecycle.register({
        session_id: sessionId,
        provider: started.record.provider as LiveProviderId,
        transport: started.record.transport as LiveTransportId,
        identity: this.lifecycle.identity,
        base,
        capabilities: facts.capabilities,
        resume: facts.resume_state ?? fallbackLiveResume(started.record.resume),
        max_text_bytes: started.record.max_text_bytes,
        prepared,
      });
      return {
        session_id: sessionId,
        provider: started.record.provider,
        transport: started.record.transport,
        workspace: prepared.worktree.path,
        capabilities: started.capabilities,
        probe: started.probe,
        record: started.record,
        state,
        warnings: prepared.warnings,
      };
    } catch (error) {
      throw await this.cleanupFailedLaunch(prepared, error);
    } finally {
      this.launches.delete(sessionId);
    }
  }

  async resume(sessionId: string, options: ResumeOptions = {}): Promise<HubStartDocument> {
    this.assertNotClosed();
    if (this.lifecycle.isManaged(sessionId)) {
      throw new AgentHubError(
        "SESSION_ALREADY_LIVE",
        `session "${sessionId}" is already running in this hub process`,
      );
    }
    const prior = await this.lifecycle.loadState(sessionId);
    if (!TERMINAL_STATUSES.includes(prior.status)) {
      throw new AgentHubError(
        "SESSION_NOT_RESUMABLE",
        `session "${sessionId}" is "${prior.status}"; only terminal records resume — run \`agent-hub gc\` first`,
      );
    }
    if ((await this.lifecycle.leaseFor(sessionId)) !== undefined) {
      throw new AgentHubError(
        "LIVE_LEASE_EXISTS",
        `session "${sessionId}" still holds a lease; run \`agent-hub gc\` (or close the owning hub) before resuming`,
      );
    }
    const selection = await this.kernel.selectTransport(
      prior.provider,
      options.transport ?? prior.transport,
    );
    if (selection.factory.transport !== prior.transport) {
      throw new AgentHubError(
        "TRANSPORT_PAIRING_INVALID",
        `session "${sessionId}" was launched on "${prior.transport}"; resuming on "${selection.factory.transport}" is refused`,
      );
    }
    const bridge = selection.factory as BridgedTransportFactory;
    const prepared = await this.lifecycle.reserveLaunchResources(
      sessionId,
      prior.provider,
      prior.current_commit,
      { allowDirty: options.allow_dirty },
    );
    this.launches.set(sessionId, prepared);
    try {
      const rebound = await this.lifecycle.rebindResumeRecord(sessionId, prepared.worktree.path);
      const started = await this.kernel.resume(rebound, {
        transport: options.transport,
        permission_policy: options.permission_policy ?? "deny",
        max_text_bytes: options.max_text_bytes,
      });
      const facts = bridge.takeLaunchFacts(sessionId);
      if (facts === null) {
        throw new AgentHubError(
          "CAPABILITY_SNAPSHOT_INVALID",
          `the resumed "${prior.transport}" transport produced no launch descriptor; the session cannot be recorded honestly`,
        );
      }
      const state = await this.lifecycle.register({
        session_id: sessionId,
        provider: prior.provider,
        transport: prior.transport,
        identity: this.lifecycle.identity,
        base: prior.base_commit,
        capabilities: facts.capabilities,
        resume: facts.resume_state ?? fallbackLiveResume(started.record.resume),
        max_text_bytes: started.record.max_text_bytes,
        prepared,
        continues: { prior_state: prior },
      });
      return {
        session_id: sessionId,
        provider: started.record.provider,
        transport: started.record.transport,
        workspace: prepared.worktree.path,
        capabilities: started.capabilities,
        probe: started.probe,
        record: started.record,
        state,
        warnings: prepared.warnings,
      };
    } catch (error) {
      throw await this.cleanupFailedLaunch(prepared, error);
    } finally {
      this.launches.delete(sessionId);
    }
  }

  /**
   * A launch that rejected after reserving resources returns them only on
   * proof (never-spawned, or leader+group provably gone); anything else
   * keeps the lease and worktree for `gc`.
   */
  private async cleanupFailedLaunch(
    prepared: PreparedLaunch,
    cause: unknown,
  ): Promise<AgentHubError> {
    const failure = asDelegateError(cause);
    const release = await this.lifecycle.releaseIfProviderProvenGone(prepared);
    if (!release.released) {
      return new AgentHubError(
        failure.code,
        `${failure.message}; ${release.retained_reason ?? "launch resources are retained"}`,
      );
    }
    return new AgentHubError(failure.code, failure.message);
  }

  // ---------------------------------------------------------------------------
  // Real-time commands (kernel owns the gates; hub pins terminal boundaries)
  // ---------------------------------------------------------------------------

  async prompt(sessionId: string, text: string): Promise<TurnDocument> {
    return this.runTurn(() => this.kernel.prompt(sessionId, text), sessionId);
  }

  async followUp(sessionId: string, text: string): Promise<TurnDocument> {
    return this.runTurn(() => this.kernel.followUp(sessionId, text), sessionId);
  }

  async steer(sessionId: string, text: string): Promise<TurnDocument> {
    return this.runTurn(() => this.kernel.steer(sessionId, text), sessionId);
  }

  async cancel(sessionId: string, reason: string | null): Promise<TurnDocument> {
    return this.runTurn(() => this.kernel.cancel(sessionId, reason), sessionId);
  }

  async requestStatus(sessionId: string): Promise<TurnDocument> {
    return this.runTurn(() => this.kernel.requestStatus(sessionId), sessionId);
  }

  async respondPermission(
    sessionId: string,
    requestId: string,
    decision: PermissionDecision,
    note: string | null = null,
  ): Promise<TurnDocument> {
    return this.runTurn(
      () => this.kernel.respondPermission(sessionId, requestId, decision, note),
      sessionId,
    );
  }

  /**
   * Run one command and, when it settles a TURN (prompt/follow_up), pin the
   * checkpoint chain at that terminal boundary. A pin that cannot land is
   * reported on the document (`checkpoint_error`) — never silently.
   */
  private async runTurn(
    invoke: () => Promise<TurnResult>,
    sessionId: string,
  ): Promise<TurnDocument> {
    const result = await invoke();
    const isTurn = result.kind === "prompt" || result.kind === "follow_up";
    if (!isTurn || result.outcome === "unsupported") {
      return { ...result, checkpoint: null };
    }
    try {
      const checkpoint = await this.lifecycle.captureCheckpoint(
        sessionId,
        checkpointReasonFor(result.outcome),
        { lastError: result.error === null ? undefined : liveErrorOf(result.error) },
      );
      return { ...result, checkpoint };
    } catch (error) {
      const failure = asDelegateError(error);
      return {
        ...result,
        checkpoint: null,
        checkpoint_error: { code: failure.code, message: failure.message },
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
    const state = await this.lifecycle.loadState(sessionId).catch((error: unknown) => {
      if (asDelegateError(error).code === "SESSION_NOT_FOUND") {
        throw new AgentHubError(
          "SESSION_NOT_FOUND",
          `no durable session "${sessionId}" in this repository`,
        );
      }
      throw error;
    });
    const record = await this.lifecycle.loadMirrorRecord(sessionId).catch(() => null);
    const lease = await this.lifecycle.leaseFor(sessionId);
    return {
      session_id: sessionId,
      attached_here: this.lifecycle.isManaged(sessionId),
      state,
      record,
      lease:
        lease === undefined
          ? null
          : {
              owned_here: lease.hub_pid === process.pid,
              provider_pid: lease.provider_pid,
              provider_pgid: lease.provider_pgid,
            },
      ref: liveRefFor(sessionId),
    };
  }

  /** Every durable session in this repository's common dir. */
  async list() {
    const states = await this.lifecycle.listStates();
    return states.map((state) => ({
      ...state,
      attached_here: this.lifecycle.isManaged(state.live_session_id),
    }));
  }

  // ---------------------------------------------------------------------------
  // Close / handoff / GC / probe
  // ---------------------------------------------------------------------------

  async close(sessionId: string, mode: StopMode = "graceful"): Promise<HubCloseDocument> {
    const closed = await this.kernel.close(sessionId, mode);
    const state = await this.lifecycle.loadState(sessionId).catch(() => null);
    if (closed.record.status === "closed") {
      let checkpoint_taken = false;
      let cleanup_errors: { code: string; message: string }[] = [];
      if (this.lifecycle.isManaged(sessionId)) {
        // First close to prove shutdown owns the final boundary and teardown.
        try {
          const teardown = await this.lifecycle.finalizeClosed(sessionId);
          checkpoint_taken = teardown.checkpoint_taken;
          cleanup_errors = teardown.cleanup_errors;
        } catch (error) {
          // Proven shutdown, unpinnable final boundary: retain, report, never
          // pretend the chain advanced.
          cleanup_errors = [asDelegateError(error)];
        }
      }
      return {
        session_id: sessionId,
        record: closed.record,
        stop: closed.stop,
        state: await this.lifecycle.loadState(sessionId).catch(() => state),
        checkpoint_taken,
        cleanup_errors,
      };
    }
    // Orphaned or degraded: ownership and worktree stay exactly where they
    // are; `gc` (or a terminate close) finishes the job.
    this.lifecycle.retainOrphan(sessionId);
    return {
      session_id: sessionId,
      record: closed.record,
      stop: closed.stop,
      state,
      checkpoint_taken: false,
      cleanup_errors: [],
    };
  }

  async closeAll(): Promise<HubCloseDocument[]> {
    const results: HubCloseDocument[] = [];
    for (const record of this.kernel.attached()) {
      results.push(await this.close(record.session_id));
    }
    await this.kernel.settle();
    this.closed = true;
    return results;
  }

  /** The checkpoint chain as the deliverable; adoption is a human command. */
  handoff(sessionId: string): Promise<HandoffDocument> {
    return this.lifecycle.handoff(sessionId);
  }

  /** Safe GC: reconcile leases/worktrees; `dry_run` reports without acting. */
  gc(options: { dry_run?: boolean } = {}): Promise<ReconcileReport> {
    const attached = new Set(this.kernel.attached().map((record) => record.session_id));
    return this.lifecycle.reconcile(attached, { dryRun: options.dry_run ?? false });
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
  settle(): Promise<void> {
    return this.kernel.settle();
  }

  private assertNotClosed(): void {
    if (this.closed) {
      throw new AgentHubError("HUB_CLOSED", "this hub host has been closed; start a new host");
    }
  }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/**
 * The transport reported no resume state at all: build the provider's
 * honest zero-facts handle from observed identity only — `verified` can
 * only be true through an actual round trip, which the kernel has already
 * applied to its own handle.
 */
function fallbackLiveResume(
  kernelResume: { provider: string; provider_session_id: string | null } | null,
): LiveSessionState["resume"] {
  if (kernelResume === null) return null;
  const base = {
    provider_session_id: kernelResume.provider_session_id,
    verified: false as const,
    verified_via: null,
  };
  switch (kernelResume.provider) {
    case "omp":
      return { provider: "omp", ...base, last_event_seq: 0 };
    case "agy":
      return { provider: "agy", ...base, resume_argv_verified: false };
    case "pi":
      return { provider: "pi", ...base, resume_token: null };
    case "hermes":
      return { provider: "hermes", ...base, session_load_advertised: false };
    default:
      return null;
  }
}
