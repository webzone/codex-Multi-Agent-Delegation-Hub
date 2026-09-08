import { randomUUID } from "node:crypto";

import { AgentHubError, asHubError } from "../errors.js";
import { deferred, type Deferred } from "../deferred.js";
import {
  asKernelError,
  isPermissionDecision,
  isTerminalStatus,
  kernelError,
  parseSessionRecord,
  validateCapabilities,
  type BoundedText,
  type CancelCommand,
  type Capabilities,
  type Command,
  type CommandKind,
  type FollowUpCommand,
  type KernelError,
  type LaunchReport,
  type PermissionResponseCommand,
  type PermissionPolicy,
  type ProcessFacts,
  type PromptCommand,
  type ProviderFactory,
  type ProviderId,
  type ProbeResult,
  type ResumeState,
  type SessionEvent,
  type SessionId,
  type SessionRecord,
  type SessionStatus,
  type StatusCommand,
  type SteerCommand,
  type StopMode,
  type StopReport,
  type Transport,
  type TransportFactory,
  type TransportId,
  type TurnResult,
  type Usage,
} from "./contracts.js";
import { EventRing, EventSubscription, truncateUtf8, RING_MAX_EVENTS } from "./events.js";

/**
 * InteractionKernel — the provider-neutral, Git-free interaction core.
 *
 * Responsibilities (and nothing else):
 *   - request correlation: every injected command carries a kernel-issued
 *     `command_id`; the matching `TurnResult` is resolved exactly once on
 *     every path — including the broken-durable-mirror path.
 *   - event streaming: one pump per session re-stamps the envelope
 *     (transport-provided seq/time are untrusted), pushes it through the
 *     bounded ring, and tails live subscribers.
 *   - capability gating: commands are validated against the launch-scoped
 *     snapshot pre-dispatch; undeliverable kinds are refused as caller
 *     errors, never silently queued.
 *   - recovery/resume boundaries: the kernel owns no durability. Every
 *     committed transition leaves through the injected `DurableMirror`; a
 *     record re-enters only through `resume()`, which re-validates the
 *     snapshot, refreshes it from the live transport, seeds the event
 *     cursor, and refuses dishonest resumes. `attached()` names what this
 *     process still runs for the recovery pass (P2's WorkspaceLifecycle
 *     reconciles against its store; leases and pins are not kernel facts).
 *   - transport selection: factories are injected; selection filters by
 *     provider pairing, asks the provider factory to pick among candidates,
 *     and requires an honest `found` probe. A declined or not-found probe
 *     kills the launch — no fallback guess (the OMP RPC v2-only dialect
 *     bar is honored through exactly this gate: the OMP factory's probe
 *     reports found=false without RPC v2 evidence, and the kernel obeys).
 *
 * Ordering guarantees, in this exact order on every terminal path:
 *
 *   stop → observePhase("transport-stopped") → durable record commit → finish
 *
 * A stop that cannot PROVE the process is gone yields `orphaned`, never an
 * assumed `closed`. Mirror writes that fail `degrade` the session: the
 * in-flight turn settles `failed` (never a false success), queued work
 * settles `failed` and is never dispatched, and resources are retained for
 * recovery. The pump never rejects into the host process.
 */

export const DEFAULT_SESSION_QUOTA = 8;
export const DEFAULT_MAX_TEXT_BYTES = 64 * 1024;
export const FOLLOW_UP_QUEUE_MAX_MESSAGES = 32;
export const FOLLOW_UP_QUEUE_MAX_BYTES = 1024 * 1024;
export const FOLLOW_UP_MAX_MESSAGE_BYTES = 128 * 1024;

/** Durable ordering seam: tests and integrations observe these exactly, in order. */
export type KernelPhase = "transport-stopped" | "record-committed";

/** The kernel's ONLY durable outlet: committed full records, in order. */
export interface DurableMirror {
  commit(record: SessionRecord): Promise<void>;
}

export interface KernelOptions {
  transportFactories: readonly TransportFactory[];
  providerFactories?: readonly ProviderFactory[];
  /** Durable outlet for every committed record transition; without it records live in memory only. */
  durable?: DurableMirror;
  /**
   * Durable ownership boundary hook: forwarded to transports as
   * `report_process`, so spawn facts reach the ownership store (P2) before
   * any handshake can fail. A rejecting hook aborts the launch.
   */
  onProviderSpawn?: (sessionId: SessionId, facts: ProcessFacts) => Promise<void>;
  maxTextBytes?: number;
  maxLiveSessions?: number;
  followUpQueue?: {
    maxMessages?: number;
    maxBytes?: number;
    maxMessageBytes?: number;
  };
  ring?: { maxEvents?: number; maxBytes?: number; maxEventBytes?: number };
  newSessionId?: () => string;
  now?: () => Date;
  observePhase?: (phase: KernelPhase) => Promise<void>;
}

interface ResolvedOptions extends KernelOptions {
  maxTextBytes: number;
  maxLiveSessions: number;
  queueMaxMessages: number;
  queueMaxBytes: number;
  queueMaxMessageBytes: number;
}

export interface StartRequest {
  provider: ProviderId;
  /** Pin a specific transport; selection still requires an honest probe. */
  transport?: TransportId;
  /** Opaque workspace root provided by the caller/WorkspaceLifecycle (P2). */
  workspace: string;
  /** Durable resume hint (restart of a previously started id without a full record). */
  resume?: ResumeState | null;
  /** Explicit session id (restart path); a fresh UUID otherwise. */
  session_id?: SessionId;
  max_text_bytes?: number;
  permission_policy?: PermissionPolicy;
}

export interface ResumeOptions {
  transport?: TransportId;
  max_text_bytes?: number;
  permission_policy?: PermissionPolicy;
}

export interface ProbeDocument extends ProbeResult {
  transport: TransportId;
  provider: ProviderId;
}

export interface TransportSelection {
  factory: TransportFactory;
  probe: ProbeDocument;
}

export interface StartResult {
  session_id: SessionId;
  record: SessionRecord;
  capabilities: Capabilities;
  probe: ProbeDocument;
}

export interface CloseResult {
  session_id: SessionId;
  record: SessionRecord;
  stop: StopReport | null;
}

interface ActiveTurn {
  command: PromptCommand | FollowUpCommand;
  started_at: string;
  started_at_ms: number;
  result: Deferred<TurnResult>;
  cancel_requested: boolean;
  error_seen: KernelError | null;
  usage: Usage | null;
  streams: Map<string, { chunks: string[]; truncated: boolean; final: boolean }>;
  /** True when the command was already delivered to a `native` provider. */
  delivered: boolean;
}

interface QueuedFollowUp {
  command: FollowUpCommand;
  bytes: number;
  result: Deferred<TurnResult>;
}

interface LiveSession {
  id: SessionId;
  transport: Transport;
  record: SessionRecord;
  ring: EventRing;
  /** In-memory live status; `record.status` is the last DURABLE mirror status. */
  status: SessionStatus;
  prompt_accepted: boolean;
  turn: ActiveTurn | null;
  queue: QueuedFollowUp[];
  /**
   * Follow-ups already delivered to a `native` provider mid-turn — the
   * provider queued them; the kernel tracks the pending result and never
   * re-sends. Routing a native claim through the kernel queue while still
   * claiming `native` is a contract violation.
   */
  provider_queued: QueuedFollowUp[];
  queue_bytes: number;
  open_permissions: Set<string>;
  subscribers: Set<EventSubscription>;
  pump: Deferred<void>;
  closing: boolean;
  finished: boolean;
  /** Non-null once a durable mirror write failed: the chain is untrustworthy. */
  degrade: KernelError | null;
  record_tail: Promise<unknown>;
  close_in_flight: { mode: StopMode; promise: Promise<CloseResult> } | null;
  close_tail: Promise<unknown>;
}

export class InteractionKernel {
  private readonly options: ResolvedOptions;
  private readonly sessions = new Map<SessionId, LiveSession>();
  private readonly clock: () => Date;

  constructor(options: KernelOptions) {
    this.options = {
      ...options,
      maxTextBytes: options.maxTextBytes ?? DEFAULT_MAX_TEXT_BYTES,
      maxLiveSessions: options.maxLiveSessions ?? DEFAULT_SESSION_QUOTA,
      queueMaxMessages:
        options.followUpQueue?.maxMessages ?? FOLLOW_UP_QUEUE_MAX_MESSAGES,
      queueMaxBytes: options.followUpQueue?.maxBytes ?? FOLLOW_UP_QUEUE_MAX_BYTES,
      queueMaxMessageBytes:
        options.followUpQueue?.maxMessageBytes ?? FOLLOW_UP_MAX_MESSAGE_BYTES,
    };
    this.clock = options.now ?? (() => new Date());
  }

  private now(): Date {
    return this.clock();
  }

  // -------------------------------------------------------------------------
  // Transport selection (injected factories; honesty gates)
  // -------------------------------------------------------------------------

  /**
   * Filters injected factories by provider pairing (and optional pin), lets
   * the provider factory pick among its accepted candidates, then requires
   * an honest `found` probe on the selection. No fallback: a decline or a
   * not-found probe ends the attempt.
   */
  async selectTransport(
    provider: ProviderId,
    pin?: TransportId,
  ): Promise<TransportSelection> {
    const candidates = this.options.transportFactories.filter(
      (factory) =>
        factory.provider === provider &&
        (pin === undefined || factory.transport === pin),
    );
    if (candidates.length === 0) {
      throw new AgentHubError(
        "TRANSPORT_UNAVAILABLE",
        `no injected transport factory pairs with provider "${provider}"${
          pin ? ` and transport "${pin}"` : ""
        }`,
      );
    }
    const providerFactory = this.options.providerFactories?.find(
      (factory) => factory.provider === provider,
    );
    let selected: TransportFactory | null;
    if (providerFactory !== undefined) {
      const offered = candidates.filter((candidate) =>
        providerFactory.transports.includes(candidate.transport),
      );
      selected = providerFactory.selectTransport(offered);
      // The factory may only pick from what the kernel offered; returning
      // an unoffered (possibly mismatched-provider) factory is a decline.
      if (selected !== null && !offered.includes(selected)) {
        selected = null;
      }
      if (selected === null) {
        throw new AgentHubError(
          "TRANSPORT_UNAVAILABLE",
          `provider "${provider}" honestly declined every candidate transport`,
        );
      }
    } else {
      selected = candidates[0] as TransportFactory;
    }
    const probe = await selected.probe();
    if (!probe.found) {
      throw new AgentHubError(
        "TRANSPORT_UNAVAILABLE",
        `provider "${provider}" was not found${
          probe.detail ? `: ${probe.detail}` : "; launching nothing rather than guessing"
        }`,
      );
    }
    return {
      factory: selected,
      probe: {
        transport: selected.transport,
        provider: selected.provider,
        found: probe.found,
        version: probe.version,
        detail: probe.detail,
      },
    };
  }

  // -------------------------------------------------------------------------
  // Launch / resume boundaries
  // -------------------------------------------------------------------------

  async start(request: StartRequest): Promise<StartResult> {
    if (typeof request.workspace !== "string" || request.workspace.length === 0) {
      throw new AgentHubError(
        "COMMAND_INVALID",
        "start requires a non-empty opaque workspace root (provided by WorkspaceLifecycle)",
      );
    }
    this.checkQuota();
    const sessionId = request.session_id ?? (this.options.newSessionId ?? randomUUID)();
    if (this.sessions.has(sessionId)) {
      throw new AgentHubError(
        "SESSION_ALREADY_LIVE",
        `session "${sessionId}" is already known to this kernel`,
      );
    }
    const selection = await this.selectTransport(request.provider, request.transport);
    const maxTextBytes = request.max_text_bytes ?? this.options.maxTextBytes;
    const transport = selection.factory.create();
    let spawned: ProcessFacts | null = null;
    try {
      const report = await transport.open({
        session_id: sessionId,
        workspace: request.workspace,
        max_text_bytes: maxTextBytes,
        resume: request.resume ?? null,
        permission_policy: request.permission_policy ?? "deny",
        report_process: async (facts) => {
          spawned = facts;
          await this.options.onProviderSpawn?.(sessionId, facts);
        },
      });
      const descriptor = await transport.describe();
      if (
        descriptor.provider !== selection.factory.provider ||
        descriptor.transport !== selection.factory.transport
      ) {
        throw new AgentHubError(
          "TRANSPORT_PAIRING_INVALID",
          `the transport factory advertises "${selection.factory.provider}/${selection.factory.transport}" but describes itself as "${descriptor.provider}/${descriptor.transport}"`,
        );
      }
      const capabilities = validateCapabilities(descriptor.capabilities);

      let resume: ResumeState;
      if (request.resume !== null && request.resume !== undefined) {
        resume = this.verifyResumeReport(request.resume, report, descriptor.transport);
      } else {
        resume = this.buildResume(descriptor.provider, null, report.provider_session_id, descriptor.transport);
      }

      const at = this.now().toISOString();
      const record: SessionRecord = {
        schema: "agent-hub-interaction/v1",
        session_id: sessionId,
        provider: descriptor.provider,
        transport: descriptor.transport,
        capabilities,
        workspace: request.workspace,
        max_text_bytes: maxTextBytes,
        resume,
        // `open` resolved ⇒ the transport is ready to accept commands.
        status: "idle",
        revision: 1,
        last_error: null,
        created_at: at,
        updated_at: at,
      };
      const session = this.buildSession(
        transport,
        record,
        (request.resume !== null && request.resume !== undefined ? request.resume.last_event_seq : 0),
      );
      await this.durableWrite(session, record);
      this.sessions.set(sessionId, session);
      void this.pumpLoop(session);
      return {
        session_id: sessionId,
        record: structuredClone(record),
        capabilities: structuredClone(capabilities),
        probe: selection.probe,
      };
    } catch (error) {
      throw await this.conservativeLaunchFailure(transport, request.workspace, spawned, error);
    }
  }

  /**
   * The resume boundary. The durable record (re-parsed field by field) is
   * the ONLY way history re-enters the kernel:
   *
   *   - only terminal records resume; anything else means recovery has not
   *     finished (or the session is still live here);
   *   - the durable transport pairing must pair again, exactly;
   *   - the capability snapshot is REFRESHED from this launch's descriptor
   *     — claims are launch-scoped;
   *   - the resume handle goes to `open()` verbatim; a transport that
   *     cannot show post-handshake resume state under a hint is refused —
   *     a silent fresh session would lie;
   *   - the event cursor seeds the ring gaplessly: seqs continue after the
   *     last durably consumed event, so replay never re-consumes.
   */
  async resume(durableRecord: unknown, options: ResumeOptions = {}): Promise<StartResult> {
    const prior = parseSessionRecord(durableRecord);
    if (!isTerminalStatus(prior.status)) {
      throw new AgentHubError(
        "SESSION_NOT_RESUMABLE",
        `session "${prior.session_id}" is "${prior.status}"; only terminal records (closed, error, orphaned) may be resumed — run recovery first`,
      );
    }
    const existing = this.sessions.get(prior.session_id);
    if (existing !== undefined && !existing.finished) {
      throw new AgentHubError(
        "SESSION_ALREADY_LIVE",
        `session "${prior.session_id}" is already running in this kernel process`,
      );
    }
    this.checkQuota();

    const selection = await this.selectTransport(
      prior.provider,
      options.transport ?? prior.transport,
    );
    if (selection.factory.transport !== prior.transport) {
      throw new AgentHubError(
        "TRANSPORT_PAIRING_INVALID",
        `durable session "${prior.session_id}" was launched on "${prior.transport}"; resuming on "${selection.factory.transport}" is refused`,
      );
    }
    const maxTextBytes = options.max_text_bytes ?? prior.max_text_bytes;
    const transport = selection.factory.create();
    let spawned: ProcessFacts | null = null;
    try {
      const report = await transport.open({
        session_id: prior.session_id,
        workspace: prior.workspace,
        max_text_bytes: maxTextBytes,
        resume: prior.resume,
        permission_policy: options.permission_policy ?? "deny",
        report_process: async (facts) => {
          spawned = facts;
          await this.options.onProviderSpawn?.(prior.session_id, facts);
        },
      });

      // A durable resume hint must come back as the same provider's session
      // handle. A transport that cannot show post-handshake resume state is
      // refused — continuing without verified provider identity would lie.
      const resume =
        prior.resume !== null
          ? this.verifyResumeReport(prior.resume, report, prior.transport)
          : this.buildResume(prior.provider, null, report.provider_session_id, prior.transport);

      const descriptor = await transport.describe();
      if (
        descriptor.provider !== prior.provider ||
        descriptor.transport !== prior.transport
      ) {
        throw new AgentHubError(
          "RESUME_VERIFICATION_FAILED",
          `the resumed transport describes itself as "${descriptor.provider}/${descriptor.transport}", not the durable "${prior.provider}/${prior.transport}" recorded for "${prior.session_id}"`,
        );
      }
      const capabilities = validateCapabilities(descriptor.capabilities);

      const record: SessionRecord = {
        ...prior,
        capabilities,
        max_text_bytes: maxTextBytes,
        resume,
        status: "idle",
        revision: prior.revision + 1,
        last_error: null,
        updated_at: this.now().toISOString(),
      };
      const session = this.buildSession(transport, record, prior.resume?.last_event_seq ?? 0);
      await this.durableWrite(session, record);
      this.sessions.set(prior.session_id, session);
      void this.pumpLoop(session);
      return {
        session_id: prior.session_id,
        record: structuredClone(record),
        capabilities: structuredClone(capabilities),
        probe: selection.probe,
      };
    } catch (error) {
      throw await this.conservativeLaunchFailure(transport, prior.workspace, spawned, error);
    }
  }

  /**
   * Verification upgrade is an observed round trip, never a claim: prior
   * provider session id in, same id out. Deeper provider-specific evidence
   * (e.g. OMP RPC v2 locator echo) is carried on the transport's own
   * `resume_state`; the kernel passes it through and upgrades only on the
   * id round trip. The kernel-owned cursor always wins over transport
   * claims — replay boundaries are kernel facts.
   */
  private verifyResumeReport(
    prior: ResumeState,
    report: LaunchReport,
    transportId: TransportId,
  ): ResumeState {
    const resumed = report.resume_state ?? null;
    if (resumed === null) {
      throw new AgentHubError(
        "RESUME_VERIFICATION_FAILED",
        `the transport produced no post-handshake resume state for a durable resume; continuing without verified provider identity is refused`,
      );
    }
    if (resumed.provider !== prior.provider) {
      throw new AgentHubError(
        "RESUME_VERIFICATION_FAILED",
        `resume state came back for provider "${resumed.provider}", not the recorded "${prior.provider}"`,
      );
    }
    if (
      prior.provider_session_id !== null &&
      resumed.provider_session_id !== prior.provider_session_id
    ) {
      throw new AgentHubError(
        "RESUME_VERIFICATION_FAILED",
        `the provider resumed a different session identity than the durable handle records`,
      );
    }
    const roundTripped =
      prior.provider_session_id !== null &&
      resumed.provider_session_id === prior.provider_session_id;
    const base = {
      provider: resumed.provider,
      provider_session_id: resumed.provider_session_id,
      data: { ...resumed.data },
      last_event_seq: prior.last_event_seq,
    };
    // The kernel-owned cursor always wins over transport claims, and the
    // hub-side upgrade happens only on an observed session-id round trip.
    if (resumed.verified) {
      return { ...base, verified: true, verified_via: resumed.verified_via };
    }
    if (roundTripped) {
      return { ...base, verified: true, verified_via: `hub-resume:${transportId}` };
    }
    return { ...base, verified: false, verified_via: null };
  }

  /** Fresh-handle construction from observed launch facts only. */
  private buildResume(
    provider: ProviderId,
    prior: ResumeState | null,
    observedSessionId: string | null,
    transportId: TransportId,
  ): ResumeState {
    const roundTripped =
      prior !== null &&
      prior.provider === provider &&
      prior.provider_session_id !== null &&
      observedSessionId !== null &&
      prior.provider_session_id === observedSessionId;
    const base = {
      provider,
      provider_session_id: observedSessionId,
      data: prior !== null && prior.provider === provider ? { ...prior.data } : {},
      last_event_seq: prior !== null && prior.provider === provider ? prior.last_event_seq : 0,
    };
    if (roundTripped) {
      return { ...base, verified: true, verified_via: `hub-resume:${transportId}` };
    }
    return { ...base, verified: false, verified_via: null };
  }

  /**
   * A launch that failed after spawning may return its resources only when
   * `stop` PROVES the process group is gone. Otherwise the error says the
   * workspace and ownership facts are retained — the kernel never assumes
   * a cleanup it could not prove (workspace teardown is WorkspaceLifecycle,
   * never the kernel's).
   */
  private async conservativeLaunchFailure(
    transport: Transport,
    workspace: string,
    spawned: ProcessFacts | null,
    cause: unknown,
  ): Promise<AgentHubError> {
    const failure = asHubError(cause);
    let stop: StopReport;
    try {
      stop = await transport.stop("terminate");
    } catch {
      stop = { status: "orphaned", exit_code: null, exit_signal: null, waited_ms: 0 };
    }
    if (spawned !== null && stop.status !== "closed") {
      return new AgentHubError(
        failure.code,
        `${failure.message}; the provider process (pid ${spawned.pid}, group ${spawned.pgid}) could not be proven gone; the workspace "${workspace}" and its ownership facts are retained for recovery or manual cleanup`,
      );
    }
    return new AgentHubError(failure.code, failure.message);
  }

  private checkQuota(): void {
    const live = [...this.sessions.values()].filter((session) => !session.finished).length;
    if (live >= this.options.maxLiveSessions) {
      throw new AgentHubError(
        "SESSION_QUOTA_FULL",
        `this kernel process already runs ${live} of ${this.options.maxLiveSessions} allowed live sessions`,
      );
    }
  }

  private buildSession(
    transport: Transport,
    record: SessionRecord,
    seedCursor: number,
  ): LiveSession {
    return {
      id: record.session_id,
      transport,
      record,
      ring: new EventRing({ ...this.options.ring, seedCursor }),
      status: "idle",
      prompt_accepted: false,
      turn: null,
      queue: [],
      provider_queued: [],
      queue_bytes: 0,
      open_permissions: new Set(),
      subscribers: new Set(),
      pump: deferred<void>(),
      closing: false,
      finished: false,
      degrade: null,
      record_tail: Promise.resolve(),
      close_in_flight: null,
      close_tail: Promise.resolve(),
    };
  }

  // -------------------------------------------------------------------------
  // Commands (capability-gated; correlation by command_id)
  // -------------------------------------------------------------------------

  private must(id: SessionId): LiveSession {
    const session = this.sessions.get(id);
    if (!session) {
      throw new AgentHubError("SESSION_NOT_FOUND", `no session "${id}" in this kernel process`);
    }
    if (isTerminalStatus(session.status) || session.closing) {
      throw new AgentHubError(
        "SESSION_NOT_LIVE",
        `session "${id}" is ${session.status}; it no longer accepts commands`,
      );
    }
    return session;
  }

  private issued(id: SessionId): { command_id: string; session_id: SessionId; issued_at: string } {
    return { command_id: randomUUID(), session_id: id, issued_at: this.now().toISOString() };
  }

  /** Pre-dispatch refusal: caller error, nothing delivered, nothing queued. */
  private refused(session: LiveSession, kind: CommandKind): TurnResult {
    const at = this.now().toISOString();
    return {
      session_id: session.id,
      command_id: randomUUID(),
      kind,
      outcome: "unsupported",
      final_text: null,
      usage: null,
      started_at: at,
      finished_at: at,
      duration_ms: 0,
      error: {
        code: "CAPABILITY_UNSUPPORTED",
        message: `the launch capability snapshot for session "${session.id}" marks "${kind}" undeliverable; the command was refused pre-dispatch`,
        stage: "capability",
        retryable: false,
        provider: session.record.provider,
      },
    };
  }

  private immediateResult(
    session: LiveSession,
    commandId: string,
    kind: CommandKind,
    finalText: BoundedText | null,
  ): TurnResult {
    const at = this.now().toISOString();
    return {
      session_id: session.id,
      command_id: commandId,
      kind,
      outcome: "succeeded",
      final_text: finalText,
      usage: null,
      started_at: at,
      finished_at: at,
      duration_ms: 0,
      error: null,
    };
  }

  /** The initial task, exactly once, while the session sits idle. */
  async prompt(id: SessionId, text: string): Promise<TurnResult> {
    const session = this.must(id);
    const claim = session.record.capabilities.prompt;
    if (claim.support === "unsupported" || claim.support === "signal") {
      return this.refused(session, "prompt");
    }
    if (session.prompt_accepted) {
      throw new AgentHubError(
        "PROMPT_ALREADY_ACCEPTED",
        `session "${id}" accepted its one prompt already; use followUp`,
      );
    }
    if (session.status !== "idle" || session.turn !== null) {
      throw new AgentHubError(
        "SESSION_NOT_IDLE",
        `session "${id}" is ${session.status}; the prompt is accepted only while idle before the first turn`,
      );
    }
    const command: PromptCommand = { kind: "prompt", text, ...this.issued(id) };
    session.prompt_accepted = true;
    return this.dispatchTurn(session, command).catch((error: unknown) => {
      session.prompt_accepted = false;
      throw error;
    });
  }

  /** Next-turn input: delivered now when idle, queued while a turn runs. */
  async followUp(id: SessionId, text: string): Promise<TurnResult> {
    const session = this.must(id);
    const claim = session.record.capabilities.follow_up;
    if (claim.support === "unsupported" || claim.support === "signal") {
      return this.refused(session, "follow_up");
    }
    const bytes = Buffer.byteLength(text, "utf8");
    if (bytes > this.options.queueMaxMessageBytes) {
      throw new AgentHubError(
        "QUEUE_FULL",
        `follow-up message is ${bytes} bytes, above the ${this.options.queueMaxMessageBytes}-byte per-message bound`,
      );
    }
    const command: FollowUpCommand = { kind: "follow_up", text, ...this.issued(id) };
    const result = deferred<TurnResult>();

    if (session.turn !== null || session.status === "running") {
      const pendingCount = session.queue.length + session.provider_queued.length;
      if (pendingCount >= this.options.queueMaxMessages) {
        throw new AgentHubError(
          "QUEUE_FULL",
          `session "${id}" already has ${this.options.queueMaxMessages} follow-ups pending (hub- and provider-queued together)`,
        );
      }
      if (session.queue_bytes + bytes > this.options.queueMaxBytes) {
        throw new AgentHubError(
          "QUEUE_FULL",
          `queued bytes would cross ${this.options.queueMaxBytes} with a ${bytes}-byte message`,
        );
      }
      if (claim.support === "native") {
        // Capability honesty: a `native` claim means the PROVIDER queues
        // next-turn input mid-run. The kernel delivers immediately and
        // tracks the pending result; it must never route the text through
        // its own queue while still claiming `native`.
        await this.deliver(session, command, "follow-up");
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
        "SESSION_NOT_IDLE",
        `session "${id}" is ${session.status}; follow-ups need idle or running`,
      );
    }
    return this.dispatchTurn(session, command, result);
  }

  /** Mid-turn guidance; native or hub-queued claims only, turn in flight only. */
  async steer(id: SessionId, text: string): Promise<TurnResult> {
    const session = this.must(id);
    const claim = session.record.capabilities.steer;
    if (claim.support !== "native" && claim.support !== "hub-queued") {
      return this.refused(session, "steer");
    }
    if (session.turn === null || session.status !== "running") {
      throw new AgentHubError(
        "SESSION_NOT_RUNNING",
        `steer is mid-turn guidance; session "${id}" is ${session.status} with no turn in flight`,
      );
    }
    const command: SteerCommand = { kind: "steer", text, ...this.issued(id) };
    await this.deliver(session, command, "steer");
    return this.immediateResult(session, command.command_id, "steer", null);
  }

  /** Abort the in-flight turn (native or signal delivery path). */
  async cancel(id: SessionId, reason: string | null): Promise<TurnResult> {
    const session = this.must(id);
    const claim = session.record.capabilities.cancel;
    if (claim.support !== "native" && claim.support !== "signal") {
      return this.refused(session, "cancel");
    }
    const command: CancelCommand = { kind: "cancel", reason, ...this.issued(id) };
    if (session.turn !== null) {
      session.turn.cancel_requested = true;
      await this.deliver(session, command, "cancel");
    }
    // No turn in flight: aborting nothing is a no-op, honestly reported.
    return this.immediateResult(session, command.command_id, "cancel", null);
  }

  /** Authoritative progress: forwarded only when the claim is native. */
  async requestStatus(id: SessionId): Promise<TurnResult> {
    const session = this.must(id);
    const claim = session.record.capabilities.status;
    const command: StatusCommand = { kind: "status", ...this.issued(id) };
    if (claim.support === "native") {
      await this.deliver(session, command, "status");
      return this.immediateResult(session, command.command_id, "status", null);
    }
    if (claim.support === "derived") {
      // The contract forbids forwarding a derived status; the kernel
      // answers from stream evidence only.
      const evidence = JSON.stringify({
        status: session.status,
        turn_in_flight: session.turn !== null,
        last_event_seq: session.ring.latest,
        queued_follow_ups: session.queue.length,
      });
      const bounded = truncateUtf8(evidence, session.record.max_text_bytes);
      return this.immediateResult(session, command.command_id, "status", {
        text: bounded,
        truncated: bounded !== evidence,
      });
    }
    return this.refused(session, "status");
  }

  async respondPermission(
    id: SessionId,
    requestId: string,
    decision: unknown,
    note: string | null,
  ): Promise<TurnResult> {
    const session = this.must(id);
    // The contract accepts exactly `allow_once` and `deny`. Anything else
    // is a caller error surfaced as such — never silently converted.
    if (!isPermissionDecision(decision)) {
      throw new AgentHubError(
        "COMMAND_INVALID",
        `permission decision "${String(decision)}" is outside the contract vocabulary (allow_once, deny)`,
      );
    }
    if (session.record.capabilities.permission_response.support === "unsupported") {
      return this.refused(session, "permission_response");
    }
    if (!session.open_permissions.delete(requestId)) {
      throw new AgentHubError(
        "PERMISSION_REQUEST_UNKNOWN",
        `session "${id}" has no open permission request "${requestId}"`,
      );
    }
    const command: PermissionResponseCommand = {
      kind: "permission_response",
      request_id: requestId,
      decision,
      note,
      ...this.issued(id),
    };
    await this.deliver(session, command, "permission_response");
    return this.immediateResult(session, command.command_id, "permission_response", null);
  }

  private async deliver(session: LiveSession, command: Command, verb: string): Promise<void> {
    try {
      await session.transport.send(command);
    } catch (error) {
      const failure = asHubError(error);
      throw new AgentHubError(failure.code, `session "${session.id}" ${verb} delivery failed: ${failure.message}`);
    }
  }

  // -------------------------------------------------------------------------
  // Event consumption (streaming; gapless kernel-stamped seqs)
  // -------------------------------------------------------------------------

  /** One-shot replay after `cursor`, or the honest expiry verdict. */
  eventsAfter(
    id: SessionId,
    cursor: number,
  ): { status: "ok"; events: SessionEvent[]; next_cursor: number } | {
    status: "expired";
    cursor: number;
    earliest_replayable_cursor: number;
  } {
    const session = this.sessions.get(id);
    if (!session) {
      throw new AgentHubError("SESSION_NOT_FOUND", `no session "${id}" in this kernel process`);
    }
    return session.ring.readAfter(cursor);
  }

  /** The newest stamped cursor for a session (for expiry resynchronization). */
  eventCursor(id: SessionId): number {
    const session = this.sessions.get(id);
    if (!session) {
      throw new AgentHubError("SESSION_NOT_FOUND", `no session "${id}" in this kernel process`);
    }
    return session.ring.latest;
  }

  /**
   * Replay-then-live tail. The catch-up read and the subscription install
   * run in one synchronous block, so no event is ever duplicated or lost
   * across the seam. The iterator ends cleanly when the session finishes;
   * a consumer that falls behind ends with `EVENT_CURSOR_EXPIRED` and must
   * resynchronize through `eventsAfter`.
   */
  streamEvents(
    id: SessionId,
    options: { after?: number } = {},
  ): AsyncIterable<SessionEvent> {
    const session = this.sessions.get(id);
    if (!session) {
      throw new AgentHubError("SESSION_NOT_FOUND", `no session "${id}" in this kernel process`);
    }
    const after = options.after ?? 0;
    const maxQueued = this.options.ring?.maxEvents ?? RING_MAX_EVENTS;
    return {
      [Symbol.asyncIterator]: (): AsyncIterator<SessionEvent> => {
        const replay = session.ring.readAfter(after);
        if (replay.status === "expired") {
          return {
            next: () =>
              Promise.reject(
                new AgentHubError(
                  "EVENT_CURSOR_EXPIRED",
                  `cursor ${replay.cursor} has evicted events behind it; oldest replayable cursor is ${replay.earliest_replayable_cursor} — resynchronize from durable state`,
                ),
              ),
          };
        }
        if (replay.events.length > maxQueued) {
          return {
            next: () =>
              Promise.reject(
                new AgentHubError(
                  "EVENT_CURSOR_EXPIRED",
                  `replay of ${replay.events.length} events exceeds the ${maxQueued}-event consumer bound — consume incrementally`,
                ),
              ),
          };
        }
        const sub = new EventSubscription(maxQueued);
        for (const event of replay.events) {
          sub.emit(event);
        }
        if (session.finished) {
          sub.complete();
        } else {
          session.subscribers.add(sub);
        }
        const inner = sub[Symbol.asyncIterator]();
        return {
          next: () => inner.next(),
          return: () => {
            session.subscribers.delete(sub);
            return inner.return?.() ?? Promise.resolve({ value: undefined as never, done: true });
          },
        };
      },
    };
  }

  /** The authoritative durable mirror for a session. */
  view(id: SessionId): SessionRecord {
    const session = this.sessions.get(id);
    if (!session) {
      throw new AgentHubError("SESSION_NOT_FOUND", `no session "${id}" in this kernel process`);
    }
    return structuredClone(session.record);
  }

  /**
   * Recovery boundary: what THIS process still runs, per the kernel. A
   * recovery pass (P2) reconciles these mirrors against its own durable
   * store — anything durable but not attached here was orphaned by a hub
   * loss. The kernel never scans stores or classifies ownership itself.
   */
  attached(): SessionRecord[] {
    return [...this.sessions.values()]
      .filter((session) => !session.finished)
      .map((session) => structuredClone(session.record));
  }

  /** Wait until every session pump has drained (test/shutdown seam). */
  async settle(): Promise<void> {
    for (const session of [...this.sessions.values()]) {
      await session.pump.promise;
    }
  }

  // -------------------------------------------------------------------------
  // Pump and event handling
  // -------------------------------------------------------------------------

  private async pumpLoop(session: LiveSession): Promise<void> {
    try {
      for await (const raw of session.transport.events()) {
        // The kernel is authoritative for envelope facts: transport-provided
        // seq/occurred_at cannot be trusted across restarts or providers.
        const event: SessionEvent = {
          session_id: session.id,
          seq: session.ring.nextSeq,
          transport: session.transport.id,
          occurred_at: this.now().toISOString(),
          body: raw.body,
        };
        const published = session.ring.push(event);
        for (const sub of session.subscribers) {
          sub.emit(published.event);
        }
        await this.handleEvent(session, published.event);
      }
      // Stream ended without the session being closed: treat as a crash,
      // never as success.
      if (!session.closing && !session.finished) {
        await this.crashSafely(session, {
          code: "TRANSPORT_EXHAUSTED",
          message: "the provider event stream ended without the session reaching a terminal status",
        });
      }
    } catch (error) {
      if (!session.closing && !session.finished) {
        await this.crashSafely(
          session,
          asHubError(error),
        );
      }
    } finally {
      session.pump.resolve();
    }
  }

  /**
   * The crash terminal path must never reject into the host process: a
   * failure inside it degrades the session (in-memory terminal, every
   * unsettled turn settled once as failed, queue failed not dispatched)
   * and the process stays alive to serve close/resume.
   */
  private async crashSafely(
    session: LiveSession,
    error: { code: string; message: string },
  ): Promise<void> {
    try {
      await this.handleCrash(session, error);
    } catch (inner) {
      const failure = asHubError(inner);
      session.closing = true;
      session.status = "error";
      session.degrade ??= {
        code: failure.code,
        message: `${failure.message}; the crash path itself failed, so this session is terminal in memory only — ownership facts are retained for recovery`,
        stage: "shutdown",
        retryable: false,
        provider: session.record.provider,
      };
      const turn = session.turn;
      if (turn !== null) {
        session.turn = null;
        turn.error_seen ??= session.degrade;
        this.settleTurnResult(session, turn, "failed");
      }
      await this.failQueued(session, failure.code, "the crash handling failed; queued follow-ups are failed, never dispatched").catch(
        () => undefined,
      );
      this.finishTerminal(session);
    }
  }

  /**
   * Provider died while the kernel watched it. Termination must be PROVEN
   * before anything is finalized: a crash whose `stop("terminate")` cannot
   * show a closed report leaves the session `orphaned` — never assumed
   * closed. The in-flight turn still settles honestly, as failed.
   */
  private async handleCrash(
    session: LiveSession,
    error: { code: string; message: string },
  ): Promise<void> {
    session.closing = true;
    let stop: StopReport;
    try {
      stop = await session.transport.stop("terminate");
    } catch {
      stop = { status: "orphaned", exit_code: null, exit_signal: null, waited_ms: 0 };
    }
    await this.options.observePhase?.("transport-stopped");
    const proven = stop.status === "closed";
    const status: SessionStatus = proven ? "error" : "orphaned";
    session.status = status;
    const lastError = kernelError(
      error.code,
      proven
        ? error.message
        : `${error.message}; shutdown could not be proven, so the provider group may still be running and its ownership facts are retained for recovery`,
      "provider",
      false,
      session.record.provider,
    );

    const turn = session.turn;
    if (turn !== null) {
      session.turn = null;
      turn.error_seen ??= lastError;
      this.settleTurnResult(session, turn, "failed");
    }
    await this.failQueued(
      session,
      error.code,
      "the session crashed before the queued follow-up ran; it was never dispatched",
    );
    try {
      await this.commitStatus(session, status, lastError);
    } catch {
      // The durable mirror itself is broken: the in-memory terminal status
      // stands and resources are retained; recovery re-derives everything
      // from the last committed record.
    }
    this.finishTerminal(session);
  }

  private async handleEvent(session: LiveSession, event: SessionEvent): Promise<void> {
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
          !session.finished &&
          !isTerminalStatus(seen) &&
          seen !== session.status
        ) {
          session.status = seen;
          try {
            await this.commitStatus(session, seen);
          } catch (durableError) {
            // A mirror write that cannot land means the durable chain is
            // no longer trustworthy: degrade now instead of advancing on an
            // unprovable record.
            await this.degradeOnDurableFailure(session, durableError, "error");
          }
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
        if (!session.closing && !session.finished) {
          await this.crashSafely(session, {
            code: "PROVIDER_EXITED",
            message: `the provider process exited with ${
              body.exit_signal
                ? `signal ${body.exit_signal}`
                : `code ${body.exit_code ?? "unknown"}`
            } while the session was live`,
          });
        }
        break;
      }
      default:
        break;
    }
  }

  // -------------------------------------------------------------------------
  // Turn settlement (exactly once per accepted turn)
  // -------------------------------------------------------------------------

  private async dispatchTurn(
    session: LiveSession,
    command: PromptCommand | FollowUpCommand,
    result: Deferred<TurnResult> = deferred<TurnResult>(),
    alreadyDelivered = false,
  ): Promise<TurnResult> {
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
      const failure = asHubError(error);
      throw new AgentHubError(failure.code, `command dispatch failed: ${failure.message}`);
    }
    return result.promise;
  }

  private settleTurn(session: LiveSession): Promise<TurnResult> {
    const turn = session.turn;
    if (turn === null) {
      return Promise.reject(new AgentHubError("INTERNAL_ERROR", "settleTurn without a turn"));
    }
    session.turn = null;
    const outcome: TurnResult["outcome"] = turn.cancel_requested
      ? "cancelled"
      : turn.error_seen !== null
        ? "failed"
        : "succeeded";
    return this.finalizeTurn(session, turn, outcome);
  }

  /**
   * The turn-end boundary. The durable mirror is advanced FIRST (status,
   * resume cursor, last error); only a transition that LANDED may be
   * reported as success. A mirror write that fails settles the turn
   * `failed` with the durable failure recorded and degrades the session —
   * never a false success, never a hang.
   */
  private async finalizeTurn(
    session: LiveSession,
    turn: ActiveTurn,
    outcome: TurnResult["outcome"],
  ): Promise<TurnResult> {
    try {
      await this.commitStatus(session, session.status, turn.error_seen ?? undefined);
    } catch (error) {
      turn.error_seen ??= this.durableWriteError(
        session,
        error,
        "the session turn could not be committed to the durable mirror",
      );
      const failed = this.settleTurnResult(session, turn, "failed");
      await this.degradeOnDurableFailure(session, error, "error");
      return failed;
    }
    const result = this.settleTurnResult(session, turn, outcome);

    // The queue drains only toward another turn. A provider-queued
    // follow-up was ALREADY delivered (the native claim promised immediate
    // delivery): it becomes the tracked next turn without re-sending;
    // hub-queued items are delivered now that the boundary has arrived.
    if (
      !session.closing &&
      !session.finished &&
      session.degrade === null &&
      session.turn === null &&
      session.provider_queued.length + session.queue.length > 0
    ) {
      const fromProviderQueue = session.provider_queued.length > 0;
      const nextItem = (fromProviderQueue ? session.provider_queued : session.queue)
        .shift() as QueuedFollowUp;
      session.queue_bytes -= nextItem.bytes;
      void this.dispatchTurn(session, nextItem.command, nextItem.result, fromProviderQueue).catch(
        (error: unknown) => {
          const failure = asHubError(error);
          nextItem.result.resolve(
            this.failedResult(session, nextItem.command, failure.code, failure.message),
          );
        },
      );
    }
    return result;
  }

  /** Assembles and resolves the caller-visible turn result, exactly once. */
  private settleTurnResult(
    session: LiveSession,
    turn: ActiveTurn,
    outcome: TurnResult["outcome"],
  ): TurnResult {
    let finalText: TurnResult["final_text"] = null;
    for (const stream of turn.streams.values()) {
      if (stream.final) {
        const joined = stream.chunks.join("");
        const bounded = truncateUtf8(joined, session.record.max_text_bytes);
        finalText = { text: bounded, truncated: stream.truncated || bounded !== joined };
      }
    }

    const finishedAt = this.now();
    const result: TurnResult = {
      session_id: session.id,
      command_id: turn.command.command_id,
      kind: turn.command.kind,
      outcome,
      final_text: finalText,
      usage: turn.usage,
      started_at: turn.started_at,
      finished_at: finishedAt.toISOString(),
      duration_ms: finishedAt.getTime() - turn.started_at_ms,
      error: turn.error_seen,
    };
    turn.result.resolve(result);
    return result;
  }

  private failedResult(
    session: LiveSession,
    command: PromptCommand | FollowUpCommand,
    code: string,
    message: string,
  ): TurnResult {
    const at = this.now().toISOString();
    return {
      session_id: session.id,
      command_id: command.command_id,
      kind: command.kind,
      outcome: "failed",
      final_text: null,
      usage: null,
      started_at: at,
      finished_at: at,
      duration_ms: 0,
      error: { code, message, stage: "transport", retryable: false, provider: session.record.provider },
    };
  }

  // -------------------------------------------------------------------------
  // Durable mirror writes (Git-free; degrade on failure)
  // -------------------------------------------------------------------------

  /** Advances the durable mirror exactly one revision with the given status. */
  private async commitStatus(
    session: LiveSession,
    status: SessionStatus,
    lastError?: KernelError | null,
  ): Promise<void> {
    const current = session.record;
    const next: SessionRecord = {
      ...current,
      status,
      revision: current.revision + 1,
      last_error: lastError === undefined ? current.last_error : lastError,
      resume:
        current.resume === null
          ? null
          : { ...current.resume, last_event_seq: session.ring.latest },
      updated_at: this.now().toISOString(),
    };
    await this.durableWrite(session, next);
    session.status = status;
  }

  /** Serialized mirror writes: one committed record at a time, in order. */
  private durableWrite(session: LiveSession, next: SessionRecord): Promise<void> {
    const write = async (): Promise<void> => {
      if (this.options.durable !== undefined) {
        await this.options.durable.commit(structuredClone(next));
      }
      session.record = next;
      await this.options.observePhase?.("record-committed");
    };
    const tail = session.record_tail.then(write, write);
    session.record_tail = tail.catch(() => undefined);
    return tail;
  }

  /**
   * Maps a failed mirror write to a structured kernel error. An
   * `AgentHubError` keeps its own code; anything else becomes
   * `MIRROR_WRITE_FAILED`. Raw provider/filesystem content never echoes.
   */
  private durableWriteError(session: LiveSession, cause: unknown, what: string): KernelError {
    const failure = asHubError(cause);
    return {
      code: cause instanceof AgentHubError ? failure.code : "MIRROR_WRITE_FAILED",
      message: `${what}: ${failure.message}`,
      stage: "state",
      retryable: false,
      provider: session.record.provider,
    };
  }

  /**
   * The durable chain failed mid-flight, so the session's durable history
   * can no longer be trusted. Exactly once, and never throwing:
   *
   *   - a still-owned in-flight turn settles `failed` (a write that never
   *     landed pins nothing — an uncommitted boundary is never success);
   *   - queued and provider-queued follow-ups settle `failed`, never
   *     dispatched — a broken chain dispatches nothing further;
   *   - the session goes terminal in memory and records the first durable
   *     failure on `degrade`, which terminal paths read as the "safety
   *     unproven" flag;
   *   - a best-effort terminal mirror rewrite is attempted; if it fails
   *     through the same broken path the kernel stays alive and the
   *     in-memory terminal status stands;
   *   - nothing is torn down: ownership facts stay retained for recovery.
   */
  private async degradeOnDurableFailure(
    session: LiveSession,
    cause: unknown,
    status: SessionStatus,
  ): Promise<void> {
    const lastError = this.durableWriteError(
      session,
      cause,
      "the durable mirror could not be written, so this session is pinned nowhere — queued work is failed, never dispatched, and ownership facts are retained for recovery",
    );
    session.degrade ??= lastError;

    const turn = session.turn;
    if (turn !== null) {
      session.turn = null;
      turn.error_seen ??= lastError;
      this.settleTurnResult(session, turn, "failed");
    }
    session.status = status;

    await this.failQueued(
      session,
      lastError.code,
      "the durable mirror write failed; queued follow-ups are failed, never dispatched",
    );

    try {
      const current = session.record;
      await this.durableWrite(session, {
        ...current,
        status,
        revision: current.revision + 1,
        last_error: lastError,
        resume:
          current.resume === null
            ? null
            : { ...current.resume, last_event_seq: session.ring.latest },
        updated_at: this.now().toISOString(),
      });
    } catch {
      // The best-effort rewrite failed through the same broken path. The
      // in-memory terminal status stands, resources stay retained, and the
      // kernel process remains alive; recovery re-derives everything from
      // the last committed record.
    }
    this.finishTerminal(session);
  }

  // -------------------------------------------------------------------------
  // Terminal paths: stop → prove → durable commit → finish
  // -------------------------------------------------------------------------

  async close(id: SessionId, mode: StopMode = "graceful"): Promise<CloseResult> {
    const session = this.sessions.get(id);
    if (!session) {
      throw new AgentHubError("SESSION_NOT_FOUND", `no session "${id}" in this kernel process`);
    }
    const inFlight = session.close_in_flight;
    if (inFlight !== null && inFlight.mode === mode) {
      // A concurrent same-mode close joins the single running pipeline; it
      // never re-runs signals or durable writes on its own.
      return inFlight.promise;
    }
    const attempt = (): Promise<CloseResult> => {
      if (session.finished) {
        if (session.record.status === "orphaned" && mode === "terminate") {
          return this.retryTerminate(session);
        }
        return Promise.resolve({
          session_id: id,
          record: structuredClone(session.record),
          stop: null,
        });
      }
      return this.runCloseAttempt(session, mode);
    };
    const promise = session.close_tail.then(attempt, attempt);
    session.close_in_flight = { mode, promise };
    session.close_tail = promise.then(
      () => undefined,
      () => undefined,
    );
    const clear = (): void => {
      if (session.close_in_flight !== null && session.close_in_flight.promise === promise) {
        session.close_in_flight = null;
      }
    };
    void promise.then(clear, clear);
    return promise;
  }

  /** The one terminal pipeline for a single close attempt. */
  private async runCloseAttempt(session: LiveSession, mode: StopMode): Promise<CloseResult> {
    session.closing = true;
    session.status = "closing";
    await this.failQueued(
      session,
      "SESSION_CLOSING",
      "the session was closed before the queued follow-up ran; it was never dispatched",
    );
    let stop: StopReport;
    try {
      stop = await session.transport.stop(mode);
    } catch {
      stop = { status: "orphaned", exit_code: null, exit_signal: null, waited_ms: 0 };
    }
    await this.options.observePhase?.("transport-stopped");

    if (stop.status !== "closed") {
      // Honest orphan: the provider may still be mutating its workspace;
      // no assumptions, ownership stays retained, and a terminate-authorized
      // close may finish the job.
      const orphanError = kernelError(
        "STOP_UNPROVEN",
        `shutdown (${mode}) could not prove the provider process group is gone; the session is orphaned, not closed; ownership facts are retained and a terminate-authorized close may retry the shutdown`,
        "shutdown",
        false,
        session.record.provider,
      );
      session.status = "orphaned";
      const turn = session.turn;
      if (turn !== null) {
        session.turn = null;
        turn.cancel_requested = true;
        turn.error_seen ??= orphanError;
        this.settleTurnResult(session, turn, "failed");
      }
      try {
        await this.commitStatus(session, "orphaned", orphanError);
      } catch (durableError) {
        await this.degradeOnDurableFailure(session, durableError, "orphaned");
      }
      this.finishTerminal(session);
      return {
        session_id: session.id,
        record: structuredClone(session.record),
        stop,
      };
    }

    // Turn in flight when close was called: its work is cancelled — after
    // the reap proved the process is gone, before the closed record lands.
    const turn = session.turn;
    if (turn !== null) {
      session.turn = null;
      this.settleTurnResult(session, turn, "cancelled");
    }
    session.status = "closed";
    try {
      await this.commitStatus(session, "closed");
    } catch (durableError) {
      // The shutdown is PROVEN but the close commit is not. Report the
      // failure honestly instead of committing a closed lie.
      await this.degradeOnDurableFailure(session, durableError, "error");
      return {
        session_id: session.id,
        record: structuredClone(session.record),
        stop,
      };
    }
    this.finishTerminal(session);
    return {
      session_id: session.id,
      record: structuredClone(session.record),
      stop,
    };
  }

  /** An authorized terminate may re-attempt shutdown for an orphan. */
  private async retryTerminate(session: LiveSession): Promise<CloseResult> {
    let stop: StopReport;
    try {
      stop = await session.transport.stop("terminate");
    } catch {
      stop = { status: "orphaned", exit_code: null, exit_signal: null, waited_ms: 0 };
    }
    await this.options.observePhase?.("transport-stopped");
    if (stop.status === "closed") {
      try {
        await this.commitStatus(session, "closed", null);
      } catch {
        // Still orphaned on the mirror; the retry is reported honestly.
      }
    }
    return {
      session_id: session.id,
      record: structuredClone(session.record),
      stop,
    };
  }

  async closeAll(): Promise<CloseResult[]> {
    const results: CloseResult[] = [];
    for (const id of [...this.sessions.keys()]) {
      const session = this.sessions.get(id);
      if (session === undefined) {
        continue;
      }
      try {
        results.push(await this.close(id));
      } catch (error) {
        const failure = asKernelError(error, {
          stage: "shutdown",
          provider: session.record.provider,
        });
        results.push({
          session_id: id,
          record: { ...structuredClone(session.record), last_error: failure },
          stop: null,
        });
      }
    }
    return results;
  }

  private async failQueued(session: LiveSession, code: string, message: string): Promise<void> {
    const queued = session.queue.splice(0, session.queue.length);
    const providerQueued = session.provider_queued.splice(0, session.provider_queued.length);
    session.queue_bytes = 0;
    for (const item of [...queued, ...providerQueued]) {
      // Provider-queued follow-ups reached the provider but will never see
      // a terminal boundary here; their caller-visible future still settles.
      item.result.resolve(this.failedResult(session, item.command, code, message));
    }
  }

  private finishTerminal(session: LiveSession): void {
    session.finished = true;
    for (const sub of session.subscribers) {
      sub.complete();
    }
    session.subscribers.clear();
  }
}

