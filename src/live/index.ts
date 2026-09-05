import { randomUUID } from "node:crypto";

import { AgentHubError, asDelegateError } from "../errors.js";
import {
  assertLiveSessionState,
  getLiveResumeSource,
  isLiveRecord,
  liveTransportRegistry,
  LiveTransportRegistry,
} from "./provider-registry.js";
import type { LiveResumeSource } from "./provider-registry.js";
import type {
  CapabilitySupport,
  LiveBoundedText,
  LiveCapabilities,
  LiveCheckpoint,
  LiveCommand,
  LiveCommandKind,
  LiveCommandOutcome,
  LiveError,
  LiveErrorStage,
  LiveEvent,
  LiveLaunchReport,
  LiveLaunchRequest,
  LivePermissionDecision,
  LiveProviderId,
  LiveSessionState,
  LiveStatus,
  LiveStopMode,
  LiveStopReport,
  LiveTransport,
  LiveTransportId,
  LiveTurnResult,
  LiveUsage,
} from "./types.js";

export {
  assertLiveSessionState,
  getLiveResumeSource,
  isLiveProvider,
  isLiveRecord,
  LIVE_TRANSPORT_PAIRINGS,
  liveTransportRegistry,
  LiveTransportRegistry,
  probeLiveAgent,
  registerLiveTransport,
  setLiveResumeSource,
  supportedLiveAgents,
  unwiredLiveResumeSource,
} from "./provider-registry.js";
export type { LiveProbeDocument, LiveResumeSource } from "./provider-registry.js";
export type {
  CapabilitySupport,
  CheckpointReason,
  LiveBoundedText,
  LiveCapabilities,
  LiveCapabilityClaim,
  LiveCapabilityName,
  LiveCheckpoint,
  LiveCommand,
  LiveCommandKind,
  LiveCommandOutcome,
  LiveError,
  LiveErrorStage,
  LiveEvent,
  LiveEventBody,
  LiveEventKind,
  LiveLaunchReport,
  LiveLaunchRequest,
  LivePermissionDecision,
  LiveProbeResult,
  LiveProviderFactory,
  LiveProviderId,
  LiveSessionState,
  LiveStatus,
  LiveStopMode,
  LiveStopReport,
  LiveTransport,
  LiveTransportDescriptor,
  LiveTransportFactory,
  LiveTransportId,
  LiveTurnResult,
  LiveUsage,
} from "./types.js";

/** Default byte bound stamped into every `LiveLaunchRequest`. */
export const LIVE_DEFAULT_MAX_TEXT_BYTES = 32_768;
/** Events kept per session for cursor polling before the oldest are evicted. */
export const LIVE_DEFAULT_EVENT_BUFFER = 256;
/** Wall-clock bound on one delivered turn before it is failed honestly. */
export const LIVE_DEFAULT_TURN_TIMEOUT_MS = 600_000;
/** How long a command may wait out the `starting` status after launch. */
export const LIVE_LAUNCH_SETTLE_MS = 1_000;
/** Hard bound on live sessions (open or closed) per process-local manager. */
export const LIVE_MAX_SESSIONS = 32;
/** How long in-flight commands may finish before the runner stops the provider. */
export const LIVE_CLOSE_DRAIN_MS = 1_000;

const LIVE_CAPABILITY_NAMES = [
  "prompt",
  "follow_up",
  "steer",
  "cancel",
  "status",
  "permission_response",
  "resume",
  "checkpoint",
  "usage_reporting",
] as const;

const SUPPORT_LEVELS: readonly string[] = ["native", "hub-queued", "derived", "signal", "unsupported"];

const TURN_COMMAND_KINDS: readonly string[] = [
  "prompt",
  "follow_up",
  "steer",
  "cancel",
  "status",
  "permission_response",
];

const PERMISSION_DECISIONS: readonly string[] = ["allow_once", "allow_session", "deny"];

const LIVE_ERROR_STAGES: readonly string[] = [
  "probe",
  "launch",
  "transport",
  "provider",
  "protocol",
  "capability",
  "checkpoint",
  "state",
  "shutdown",
];

/** Support levels that make a command kind deliverable at all. */
const DELIVERABLE: Record<LiveCommandKind, readonly CapabilitySupport[]> = {
  prompt: ["native", "hub-queued"],
  follow_up: ["native", "hub-queued"],
  steer: ["native", "hub-queued"],
  cancel: ["native", "signal", "hub-queued"],
  // `derived` status is answered by the hub from stream evidence and must NOT
  // be forwarded (seed contract); `native` status is forwarded and answered.
  status: ["native", "derived"],
  permission_response: ["native", "hub-queued"],
};

export function isTerminalLiveStatus(status: LiveStatus): boolean {
  return status === "closed" || status === "error" || status === "orphaned";
}

function nowIso(): string {
  return new Date().toISOString();
}

export function liveError(
  code: string,
  message: string,
  stage: LiveErrorStage,
  retryable: boolean,
  provider: LiveProviderId | null,
): LiveError {
  return { code, message, stage, retryable, provider };
}

/** Normalizes anything thrown across a seam into the seed's error shape. */
export function toLiveError(
  error: unknown,
  context: { stage: LiveErrorStage; provider: LiveProviderId | null },
): LiveError {
  if (isLiveRecord(error) && typeof error.code === "string" && typeof error.message === "string") {
    const stage =
      typeof error.stage === "string" && LIVE_ERROR_STAGES.includes(error.stage)
        ? (error.stage as LiveErrorStage)
        : context.stage;
    const provider =
      error.provider === null || typeof error.provider === "string"
        ? (error.provider as LiveProviderId | null)
        : context.provider;
    return {
      code: error.code,
      message: error.message,
      stage,
      retryable: error.retryable === true,
      provider,
    };
  }
  const { code, message } = asDelegateError(error);
  return liveError(code, message, context.stage, false, context.provider);
}

/** True when the last code unit is a dangling high surrogate. */
function endsWithHalfSurrogate(text: string): boolean {
  const last = text.charCodeAt(text.length - 1);
  return last >= 0xd800 && last <= 0xdbff;
}

/** Re-bounds hub-assembled text, never above the session's own byte bound. */
function boundLiveText(parts: readonly string[], maxBytes: number): LiveBoundedText {
  const joined = parts.join("");
  if (Buffer.byteLength(joined, "utf8") <= maxBytes) {
    return { text: joined, truncated: false };
  }
  const buffer = Buffer.from(joined, "utf8");
  let low = 0;
  let high = buffer.length;
  while (low < high) {
    const mid = Math.ceil((low + high) / 2);
    let candidate = buffer.subarray(0, mid).toString("utf8");
    if (endsWithHalfSurrogate(candidate)) {
      candidate = candidate.slice(0, -1);
    }
    if (Buffer.byteLength(candidate, "utf8") <= maxBytes) {
      low = mid + 1;
    } else {
      high = mid - 1;
    }
  }
  let text = buffer.subarray(0, Math.max(0, Math.min(low - 1, buffer.length))).toString("utf8");
  if (endsWithHalfSurrogate(text)) {
    text = text.slice(0, -1);
  }
  return { text, truncated: true };
}

/**
 * Runtime gate on a transport's capability snapshot. The seed's `Record` type
 * makes a missing claim a compile error; this gate enforces the honesty rule
 * at the boundary: every non-`unsupported` claim must carry evidence, and
 * only the nine contract names may carry claims at all.
 */
export function validateLiveCapabilities(value: unknown): LiveCapabilities {
  if (!isLiveRecord(value)) {
    throw new AgentHubError(
      "LIVE_CAPABILITY_EVIDENCE_INVALID",
      "capability snapshot must be an object",
    );
  }
  for (const name of LIVE_CAPABILITY_NAMES) {
    const claim = value[name];
    if (
      !isLiveRecord(claim) ||
      typeof claim.support !== "string" ||
      !SUPPORT_LEVELS.includes(claim.support)
    ) {
      throw new AgentHubError(
        "LIVE_CAPABILITY_EVIDENCE_INVALID",
        `capability claim "${name}" is missing or malformed`,
      );
    }
    if (claim.support === "unsupported") {
      if (claim.evidence !== null) {
        throw new AgentHubError(
          "LIVE_CAPABILITY_EVIDENCE_INVALID",
          `capability claim "${name}" must carry null evidence when unsupported`,
        );
      }
      continue;
    }
    if (typeof claim.evidence !== "string" || claim.evidence.trim().length === 0) {
      throw new AgentHubError(
        "LIVE_CAPABILITY_EVIDENCE_INVALID",
        `capability claim "${name}" (${claim.support}) must carry non-empty evidence`,
      );
    }
  }
  return value as unknown as LiveCapabilities;
}

// ---------------------------------------------------------------------------
// Surface-facing request/result shapes
// ---------------------------------------------------------------------------

/** One injectable hub intent as the CLI/MCP surfaces express it. */
export interface LiveTurnCommand {
  action: LiveCommandKind;
  text?: string | null;
  reason?: string | null;
  request_id?: string | null;
  decision?: LivePermissionDecision | null;
  note?: string | null;
}

export interface LiveStartRequest {
  provider: LiveProviderId;
  workspace: string;
  /** v2 durable session this live session continues or feeds, when any. */
  sessionId?: string | null;
  maxTextBytes?: number;
}

export interface LiveResumeRequest {
  /** The durable record, validated again by `assertLiveSessionState`. */
  state: LiveSessionState;
  workspace: string;
  maxTextBytes?: number;
}

export interface LiveSessionSummary {
  live_session_id: string;
  session_id: string | null;
  provider: LiveProviderId;
  transport: LiveTransportId;
  status: LiveStatus;
  capabilities: LiveCapabilities;
  pid: number | null;
  provider_session_id: string | null;
  launched_at: string;
  last_error: LiveError | null;
  /** Events evicted by ring overflow (never transport-side seq gaps). */
  dropped_events: number;
  /** Non-conforming `seq` transitions observed from the transport. */
  seq_anomalies: number;
  closed: boolean;
}

/** One cursor poll: events strictly after `cursor`, plus eviction honesty. */
export interface LiveEventPage {
  events: LiveEvent[];
  next_cursor: number;
  /** Oldest `seq` still in the buffer; null when nothing has been recorded. */
  earliest_seq: number | null;
  /** True when `cursor` points before the oldest retained event. */
  dropped: boolean;
}

export interface LiveCloseReport {
  live_session_id: string;
  status: LiveStatus;
  stop: LiveStopReport;
}

export interface LiveSessionManagerOptions {
  registry?: LiveTransportRegistry;
  maxTextBytes?: number;
  eventBufferSize?: number;
  turnTimeoutMs?: number;
  launchSettleMs?: number;
  maxSessions?: number;
}

// ---------------------------------------------------------------------------
// Process-local manager
// ---------------------------------------------------------------------------

class WakeSignal {
  private readonly waiters = new Set<() => void>();

  notify(): void {
    for (const wake of [...this.waiters]) {
      this.waiters.delete(wake);
      wake();
    }
  }

  /** Resolves true on the next notify, false on timeout. */
  async wait(timeoutMs: number): Promise<boolean> {
    let wakeFn = (): void => {};
    const woken = new Promise<"wake">((resolve) => {
      wakeFn = () => resolve("wake");
      this.waiters.add(wakeFn);
    });
    let timerHandle: ReturnType<typeof setTimeout> | undefined;
    const timedOut = new Promise<"timeout">((resolve) => {
      timerHandle = setTimeout(() => resolve("timeout"), timeoutMs);
      // Deliberately ref'd: a grace wait can be the process's only pending
      // work while a live session closes, and an unref'd timer here would
      // let Node exit mid-shutdown with the close report unwritten.
    });
    const winner = await Promise.race([woken, timedOut]);
    this.waiters.delete(wakeFn);
    clearTimeout(timerHandle);
    return winner === "wake";
  }
}

interface ActiveTurn {
  command: LiveCommand;
  kind: LiveCommandKind;
  startedMs: number;
  startedAt: string;
  assistantParts: string[];
  usage: LiveUsage | null;
  sawError: LiveError | null;
  exit: { intentional: boolean; exit_code: number | null; exit_signal: string | null } | null;
  cancelRequested: boolean;
  /** Error the hub imposes on the turn (timeout, delivery, pump end). */
  imposedError: LiveError | null;
  outcome: LiveCommandOutcome | null;
  settle: (outcome: LiveCommandOutcome) => void;
  settled: Promise<LiveCommandOutcome>;
}

function createActiveTurn(command: LiveCommand): ActiveTurn {
  let settle = (_outcome: LiveCommandOutcome): void => {};
  const settled = new Promise<LiveCommandOutcome>((resolve) => {
    settle = resolve;
  });
  return {
    command,
    kind: command.kind,
    startedMs: Date.now(),
    startedAt: nowIso(),
    assistantParts: [],
    usage: null,
    sawError: null,
    exit: null,
    cancelRequested: false,
    imposedError: null,
    outcome: null,
    settle,
    settled,
  };
}

/** Launch-frozen identity facts of one managed live session. */
interface ManagedSessionCore {
  live_session_id: string;
  session_id: string | null;
  provider: LiveProviderId;
  transport: LiveTransportId;
  capabilities: LiveCapabilities;
  pid: number | null;
  provider_session_id: string | null;
  launched_at: string;
  promptAccepted: boolean;
}

/**
 * One live session driven by one transport. Owns the event pump, the ring
 * buffer, the capability gate, and turn bookkeeping. Durable persistence is
 * deliberately absent here (Package 2's job): everything on this handle is
 * process-local and transient, exactly as the seed requires.
 */
class ManagedLiveSession {
  readonly wake = new WakeSignal();
  readonly sentCommands: LiveCommand[] = [];
  readonly pendingPermissions = new Set<string>();
  queuedFollowUps: LiveTurnCommand[] = [];
  events: LiveEvent[] = [];
  droppedEvents = 0;
  seqAnomalies = 0;
  lastSeq = 0;
  status: LiveStatus = "starting";
  active: ActiveTurn | null = null;
  lastError: LiveError | null = null;
  closeReport: LiveCloseReport | null = null;
  deliveryTail: Promise<void> = Promise.resolve();
  pumpEnded: Promise<void> = Promise.resolve();
  private pumpResolve: () => void = () => {};

  constructor(
    readonly core: ManagedSessionCore,
    readonly transport: LiveTransport,
    readonly maxTextBytes: number,
    readonly eventBufferSize: number,
    readonly turnTimeoutMs: number,
    readonly launchSettleMs: number,
  ) {}

  get id(): string {
    return this.core.live_session_id;
  }

  get provider(): LiveProviderId {
    return this.core.provider;
  }

  toSummary(): LiveSessionSummary {
    return {
      live_session_id: this.core.live_session_id,
      session_id: this.core.session_id,
      provider: this.core.provider,
      transport: this.core.transport,
      status: this.status,
      capabilities: this.core.capabilities,
      pid: this.core.pid,
      provider_session_id: this.core.provider_session_id,
      launched_at: this.core.launched_at,
      last_error: this.lastError,
      dropped_events: this.droppedEvents,
      seq_anomalies: this.seqAnomalies,
      closed: this.closeReport !== null,
    };
  }

  startPump(): void {
    this.pumpEnded = new Promise<void>((resolve) => {
      this.pumpResolve = resolve;
    });
    void this.pump();
  }

  private async pump(): Promise<void> {
    try {
      for await (const event of this.transport.events()) {
        this.recordEvent(event);
      }
    } catch (error) {
      this.lastError = liveError(
        "LIVE_TRANSPORT_PUMP_FAILED",
        `live transport pump failed for session "${this.id}": ${asDelegateError(error).message}`,
        "transport",
        true,
        this.provider,
      );
      this.setStatus("error");
    } finally {
      if (this.active !== null && this.active.outcome === null) {
        this.active.imposedError ??= liveError(
          "LIVE_TRANSPORT_ENDED",
          `live transport ended before the in-flight ${this.active.kind} command settled`,
          "transport",
          true,
          this.provider,
        );
        this.settleActive();
      }
      if (!isTerminalLiveStatus(this.status)) {
        this.lastError ??= liveError(
          "LIVE_TRANSPORT_ENDED",
          "live transport event pump ended before a terminal status was observed",
          "transport",
          false,
          this.provider,
        );
        this.setStatus("error");
      }
      this.pumpResolve();
      this.wake.notify();
    }
  }

  private recordEvent(event: LiveEvent): void {
    if (event.seq === this.lastSeq + 1) {
      this.lastSeq = event.seq;
    } else {
      this.seqAnomalies += 1;
      if (event.seq > this.lastSeq) {
        this.lastSeq = event.seq;
      }
    }
    this.events.push(event);
    while (this.events.length > this.eventBufferSize) {
      this.events.shift();
      this.droppedEvents += 1;
    }

    const body = event.body;
    switch (body.kind) {
      case "status":
        this.status = body.status;
        this.settleIfTurnOver();
        break;
      case "text":
        if (body.role === "assistant" && this.active !== null) {
          this.active.assistantParts.push(body.text.text);
        }
        break;
      case "permission_request":
        this.pendingPermissions.add(body.request_id);
        break;
      case "usage":
        if (this.active !== null) {
          this.active.usage = body.usage;
        }
        break;
      case "error":
        this.lastError = body.error;
        if (this.active !== null) {
          this.active.sawError = body.error;
        }
        break;
      case "exit":
        this.status = body.intentional ? "closed" : "error";
        if (!body.intentional && this.lastError === null) {
          this.lastError = liveError(
            "LIVE_PROVIDER_EXITED",
            "the provider process exited without hub-authorized shutdown",
            "transport",
            true,
            this.provider,
          );
        }
        if (this.active !== null) {
          this.active.exit = {
            intentional: body.intentional,
            exit_code: body.exit_code,
            exit_signal: body.exit_signal,
          };
        }
        this.settleIfTurnOver();
        break;
      default:
        break;
    }
    this.wake.notify();
  }

  /** Settles the active turn once stream evidence proves the turn ended. */
  private settleIfTurnOver(): void {
    if (this.active === null || this.active.outcome !== null) {
      return;
    }
    if (this.status === "idle" || isTerminalLiveStatus(this.status)) {
      this.settleActive();
    }
  }

  private settleActive(): void {
    const turn = this.active;
    if (turn === null || turn.outcome !== null) {
      return;
    }
    let outcome: LiveCommandOutcome;
    if (turn.imposedError !== null) {
      outcome = "failed";
    } else if (turn.sawError !== null || (turn.exit !== null && !turn.exit.intentional)) {
      outcome = "failed";
    } else if (turn.cancelRequested) {
      outcome = "cancelled";
    } else if (this.status === "idle") {
      // Turn-ended evidence: the stream returned to idle.
      outcome = "succeeded";
    } else {
      // The session hit a terminal status with the turn in flight. Per the
      // seed, an intentional exit means the hub itself shut the provider
      // down — the turn did not conclude, and the honest outcome is
      // "cancelled", never "succeeded".
      outcome = "cancelled";
    }
    turn.outcome = outcome;
    this.active = null;
    turn.settle(outcome);
    // A queued follow-up rides the first idle boundary after this settle.
    if (this.status === "idle" && this.queuedFollowUps.length > 0) {
      const queued = this.queuedFollowUps.shift() as LiveTurnCommand;
      void this.deliverQueued(queued);
    }
  }

  /** Serializes every `transport.send` onto one tail; never drops the queue. */
  deliver<T>(work: () => Promise<T>): Promise<T> {
    const run = this.deliveryTail.then(work, work);
    this.deliveryTail = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  /**
   * Delivers a previously queued follow-up as a real tracked turn (so steer
   * and cancel can attach to it). Its receipt was already returned when the
   * command was queued; delivery failure lands in `last_error` honestly.
   */
  private async deliverQueued(request: LiveTurnCommand): Promise<void> {
    const command = this.buildCommand("follow_up", request);
    const turn = createActiveTurn(command);
    this.active = turn;
    try {
      await this.deliver(() => this.transport.send(command));
      this.sentCommands.push(command);
    } catch (error) {
      if (this.active === turn) {
        this.active = null;
      }
      this.lastError = toLiveError(error, { stage: "transport", provider: this.provider });
      this.wake.notify();
    }
  }

  async waitUntil(matches: () => boolean, timeoutMs: number): Promise<boolean> {
    while (!matches()) {
      if (!(await this.wake.wait(timeoutMs))) {
        return false;
      }
    }
    return true;
  }

  private setStatus(status: LiveStatus): void {
    this.status = status;
    this.settleIfTurnOver();
    this.wake.notify();
  }

  buildCommand(kind: LiveCommandKind, request: LiveTurnCommand): LiveCommand {
    const base = {
      command_id: randomUUID(),
      live_session_id: this.id,
      issued_at: nowIso(),
    };
    switch (kind) {
      case "prompt":
      case "follow_up":
      case "steer":
        return { ...base, kind, text: String(request.text ?? "") };
      case "cancel":
        return { ...base, kind, reason: typeof request.reason === "string" ? request.reason : null };
      case "status":
        return { ...base, kind };
      case "permission_response":
        return {
          ...base,
          kind,
          request_id: String(request.request_id ?? ""),
          decision: (request.decision ?? "deny") as LivePermissionDecision,
          note: typeof request.note === "string" ? request.note : null,
        };
    }
  }

  private resultFrom(
    kind: LiveCommandKind,
    commandId: string | null,
    finalParts: readonly string[] | null,
    usage: LiveUsage | null,
    exit: { exit_code: number | null; exit_signal: string | null } | null,
    outcome: LiveCommandOutcome,
    error: LiveError | null,
    startedAt: string,
    startedMs: number,
    finishedMs: number,
  ): LiveTurnResult {
    // Checkpoint pinning is Package 2's durable concern; the process-local
    // manager honestly reports that it pinned nothing.
    const checkpoint: LiveCheckpoint | null = null;
    return {
      live_session_id: this.id,
      command_id: commandId ?? "",
      kind,
      outcome,
      final_text: finalParts === null ? null : boundLiveText(finalParts, this.maxTextBytes),
      usage,
      checkpoint,
      exit_code: exit?.exit_code ?? null,
      exit_signal: exit?.exit_signal ?? null,
      started_at: startedAt,
      finished_at: new Date(finishedMs).toISOString(),
      duration_ms: Math.max(0, finishedMs - startedMs),
      error,
    };
  }

  /** A receipt for a command that did not run a turn (or was refused). */
  private receipt(
    kind: LiveCommandKind,
    outcome: LiveCommandOutcome,
    error: LiveError | null,
  ): LiveTurnResult {
    const at = Date.now();
    return this.resultFrom(kind, null, null, null, null, outcome, error, nowIso(), at, at);
  }

  // ---------------------------------------------------------------------
  // The capability gate plus state machine. Every refused path returns a
  // `LiveTurnResult` — nothing that fails the gate is ever delivered.
  // ---------------------------------------------------------------------

  async runCommand(request: LiveTurnCommand): Promise<LiveTurnResult> {
    const kind = request.action;
    if (!TURN_COMMAND_KINDS.includes(kind)) {
      return this.invalid(kind, `unknown live command action "${String(kind)}"`);
    }
    const claim = this.core.capabilities[kind];

    if (isTerminalLiveStatus(this.status) || this.closeReport !== null) {
      return this.receipt(
        kind,
        "failed",
        liveError(
          "LIVE_SESSION_CLOSED",
          `live session "${this.id}" is ${this.status}; commands are no longer accepted`,
          "shutdown",
          false,
          this.provider,
        ),
      );
    }

    if (!DELIVERABLE[kind].includes(claim.support)) {
      return this.receipt(
        kind,
        "unsupported",
        liveError(
          "LIVE_CAPABILITY_UNSUPPORTED",
          `command "${kind}" is refused pre-dispatch: provider claim is "${claim.support}"`,
          "capability",
          false,
          this.provider,
        ),
      );
    }

    if ((kind === "prompt" || kind === "follow_up" || kind === "steer") && typeof request.text !== "string") {
      return this.invalid(kind, `command "${kind}" requires a "text" string`);
    }
    if (kind === "permission_response") {
      if (typeof request.request_id !== "string" || request.request_id.length === 0) {
        return this.invalid(kind, 'command "permission_response" requires "request_id"');
      }
      if (typeof request.decision !== "string" || !PERMISSION_DECISIONS.includes(request.decision)) {
        return this.invalid(
          kind,
          'command "permission_response" requires decision allow_once, allow_session, or deny',
        );
      }
    }

    // The `starting` status is a launch race, not a caller error: wait it
    // out bounded, then report honestly.
    if (this.status === "starting") {
      const left = await this.waitUntil(
        () => this.status !== "starting" || isTerminalLiveStatus(this.status),
        this.launchSettleMs,
      );
      if (!left) {
        return this.rejected(kind, `live session "${this.id}" never left the "starting" status`, true);
      }
      if (isTerminalLiveStatus(this.status)) {
        return this.receipt(
          kind,
          "failed",
          liveError(
            "LIVE_SESSION_CLOSED",
            `live session "${this.id}" reached "${this.status}" before the command was delivered`,
            "shutdown",
            false,
            this.provider,
          ),
        );
      }
    }

    switch (kind) {
      case "prompt": {
        if (this.core.promptAccepted) {
          return this.rejected(kind, "prompt is accepted exactly once per live session", false);
        }
        if (this.status !== "idle" || this.active !== null) {
          return this.rejected(
            kind,
            `prompt requires the "idle" status before the first turn, got "${this.status}"`,
            true,
          );
        }
        return this.runTurn(request);
      }
      case "follow_up": {
        if (this.status === "running" && claim.support === "hub-queued" && this.active === null) {
          this.queuedFollowUps.push(request);
          return this.receipt(kind, "succeeded", null);
        }
        if (this.status !== "idle" || this.active !== null) {
          return this.rejected(
            kind,
            `follow_up needs "idle" for a native claim (status "${this.status}"${
              claim.support === "hub-queued" ? "; hub-queued: a queued follow-up is already pending" : ""
            })`,
            true,
          );
        }
        return this.runTurn(request);
      }
      case "steer": {
        if (this.active === null) {
          return this.rejected(kind, "steer needs an in-flight turn", true);
        }
        return this.deliverReceipt(request);
      }
      case "cancel": {
        if (this.active === null && this.queuedFollowUps.length === 0) {
          return this.rejected(kind, "cancel needs an in-flight turn", false);
        }
        if (this.active !== null) {
          this.active.cancelRequested = true;
        } else {
          this.queuedFollowUps = [];
        }
        return this.deliverReceipt(request);
      }
      case "status": {
        // `derived` must never be forwarded (seed contract); `native` rides
        // the transport, and the hub still answers from stream evidence.
        if (claim.support === "native") {
          return this.deliverReceipt(request);
        }
        return this.receipt(kind, "succeeded", null);
      }
      case "permission_response": {
        const requestId = String(request.request_id);
        if (!this.pendingPermissions.has(requestId)) {
          // The answer can overtake its request in the in-flight event pump:
          // give the pump a bounded window to land it before calling it unknown.
          const settled = await this.waitUntil(
            () => this.pendingPermissions.has(requestId) || isTerminalLiveStatus(this.status),
            this.launchSettleMs,
          );
          if (!settled && (isTerminalLiveStatus(this.status) || this.closeReport !== null)) {
            return this.receipt(
              kind,
              "failed",
              liveError(
                "LIVE_SESSION_CLOSED",
                `live session "${this.id}" reached "${this.status}" before the permission answer was delivered`,
                "shutdown",
                false,
                this.provider,
              ),
            );
          }
        }
        if (!this.pendingPermissions.has(requestId)) {
          return this.receipt(
            kind,
            "failed",
            liveError(
              "LIVE_PERMISSION_REQUEST_UNKNOWN",
              `no pending permission request "${requestId}" was observed from this session's stream`,
              "protocol",
              false,
              this.provider,
            ),
          );
        }
        return this.deliverReceipt(request, (command) => {
          if (command.kind === "permission_response") {
            this.pendingPermissions.delete(command.request_id);
          }
        });
      }
    }
  }

  private invalid(kind: LiveCommandKind, message: string): LiveTurnResult {
    return this.receipt(
      kind,
      "failed",
      liveError("LIVE_COMMAND_INVALID", message, "protocol", false, this.provider),
    );
  }

  private rejected(kind: LiveCommandKind, message: string, retryable: boolean): LiveTurnResult {
    return this.receipt(
      kind,
      "failed",
      liveError("LIVE_STATE_REJECTED", message, "protocol", retryable, this.provider),
    );
  }

  /** Delivers a non-turn command; delivery itself is the result. */
  private async deliverReceipt(
    request: LiveTurnCommand,
    after?: (command: LiveCommand) => void,
  ): Promise<LiveTurnResult> {
    const command = this.buildCommand(request.action, request);
    try {
      await this.deliver(() => this.transport.send(command));
    } catch (error) {
      return this.receipt(
        request.action,
        "failed",
        toLiveError(error, { stage: "transport", provider: this.provider }),
      );
    }
    this.sentCommands.push(command);
    after?.(command);
    return this.receipt(request.action, "succeeded", null);
  }

  private async runTurn(request: LiveTurnCommand): Promise<LiveTurnResult> {
    const command = this.buildCommand(request.action, request);
    const turn = createActiveTurn(command);
    this.active = turn;
    try {
      await this.deliver(() => this.transport.send(command));
    } catch (error) {
      this.active = null;
      return this.resultFrom(
        turn.kind,
        command.command_id,
        null,
        null,
        null,
        "failed",
        toLiveError(error, { stage: "transport", provider: this.provider }),
        turn.startedAt,
        turn.startedMs,
        Date.now(),
      );
    }
    this.sentCommands.push(command);
    if (request.action === "prompt") {
      this.core.promptAccepted = true;
    }

    let timerHandle: ReturnType<typeof setTimeout> | undefined;
    const timeout = new Promise<"timeout">((resolve) => {
      timerHandle = setTimeout(() => resolve("timeout"), this.turnTimeoutMs);
      timerHandle.unref?.();
    });
    const winner = await Promise.race([turn.settled, timeout]);
    clearTimeout(timerHandle);

    let outcome: LiveCommandOutcome;
    if (winner === "timeout" && turn.outcome === null) {
      turn.imposedError = liveError(
        "LIVE_TURN_TIMEOUT",
        `the in-flight ${turn.kind} turn did not settle within ${this.turnTimeoutMs} ms`,
        "transport",
        true,
        this.provider,
      );
      if (this.active === turn) {
        this.active = null;
      }
      outcome = "failed";
    } else {
      outcome = turn.outcome ?? (turn.cancelRequested ? "cancelled" : "failed");
    }
    const error =
      turn.imposedError ?? (turn.sawError !== null && outcome === "failed" ? turn.sawError : null);

    return this.resultFrom(
      turn.kind,
      command.command_id,
      turn.assistantParts,
      turn.usage,
      turn.exit,
      outcome,
      error,
      turn.startedAt,
      turn.startedMs,
      Date.now(),
    );
  }

  async close(mode: LiveStopMode): Promise<LiveCloseReport> {
    if (this.closeReport !== null) {
      return this.closeReport;
    }
    this.setStatus("closing");
    let report: LiveStopReport;
    try {
      report = await this.transport.stop(mode);
    } catch (error) {
      // Stop trouble never proves the process is gone, so it is an orphan.
      report = { status: "orphaned", exit_code: null, exit_signal: null, waited_ms: 0 };
      this.lastError = toLiveError(error, { stage: "shutdown", provider: this.provider });
    }
    // Give the pump a grace window to deliver stream evidence before
    // force-concluding an in-flight turn: the turn should settle on what
    // actually happened, not on a hub guess about the closing session.
    const turnSettled = await this.waitUntil(() => this.active === null, this.launchSettleMs);
    if (!turnSettled && this.active !== null) {
      const turn = this.active;
      turn.exit ??= {
        intentional: report.status === "closed",
        exit_code: report.exit_code,
        exit_signal: report.exit_signal,
      };
      turn.outcome = "cancelled";
      this.active = null;
      turn.settle("cancelled");
    }
    this.status = report.status === "closed" ? "closed" : "orphaned";
    this.closeReport = { live_session_id: this.id, status: this.status, stop: report };
    // The transport contract says the pump ends on a terminal status; bound
    // the join anyway so a wedged generator cannot hang a close forever.
    await Promise.race([
      this.pumpEnded,
      new Promise<void>((resolve) => {
        // Ref'd: the grace bound must be able to fire while it is the only
        // pending work left in a closing process.
        setTimeout(resolve, 5_000);
      }),
    ]);
    this.wake.notify();
    return this.closeReport;
  }

  page(cursor: number, limit: number): LiveEventPage {
    const fresh = this.events.filter((event) => event.seq > cursor).slice(0, limit);
    const earliest = this.events[0]?.seq ?? null;
    return {
      events: fresh,
      next_cursor: fresh.length > 0 ? fresh[fresh.length - 1].seq : cursor,
      earliest_seq: earliest,
      dropped: earliest !== null && cursor < earliest - 1,
    };
  }
}

/**
 * The process-local live session manager behind the CLI and MCP surfaces.
 * It opens transports from the registry, gates commands against the launch
 * capability snapshot, relays normalized events with cursors, and stops
 * processes honestly. It persists nothing: durable live state and
 * checkpoints are Package 2, wired through the seams in
 * `./provider-registry.js`.
 */
export class LiveSessionManager {
  private readonly sessions = new Map<string, ManagedLiveSession>();
  private readonly registry: LiveTransportRegistry;
  private readonly maxTextBytes: number;
  private readonly eventBufferSize: number;
  private readonly turnTimeoutMs: number;
  private readonly launchSettleMs: number;
  private readonly maxSessions: number;

  constructor(options: LiveSessionManagerOptions = {}) {
    this.registry = options.registry ?? liveTransportRegistry;
    this.maxTextBytes = options.maxTextBytes ?? LIVE_DEFAULT_MAX_TEXT_BYTES;
    this.eventBufferSize = options.eventBufferSize ?? LIVE_DEFAULT_EVENT_BUFFER;
    this.turnTimeoutMs = options.turnTimeoutMs ?? LIVE_DEFAULT_TURN_TIMEOUT_MS;
    this.launchSettleMs = options.launchSettleMs ?? LIVE_LAUNCH_SETTLE_MS;
    this.maxSessions = options.maxSessions ?? LIVE_MAX_SESSIONS;
  }

  async start(request: LiveStartRequest): Promise<LiveSessionSummary> {
    const factory = this.registry.require(request.provider);
    return this.launch({
      transport: factory.create(),
      liveSessionId: randomUUID(),
      provider: request.provider,
      sessionId: request.sessionId ?? null,
      workspace: request.workspace,
      maxTextBytes: this.requestedTextBytes(request.maxTextBytes),
      resume: null,
    });
  }

  async resume(request: LiveResumeRequest): Promise<LiveSessionSummary> {
    const { state } = request;
    assertLiveSessionState(state);
    const factory = this.registry.require(state.provider);
    if (factory.transport !== state.transport) {
      throw new AgentHubError(
        "LIVE_STATE_INVALID",
        `durable live session names transport "${state.transport}" but the registered factory is "${factory.transport}"`,
      );
    }
    return this.launch({
      transport: factory.create(),
      liveSessionId: state.live_session_id,
      provider: state.provider,
      sessionId: state.session_id,
      workspace: request.workspace,
      maxTextBytes: this.requestedTextBytes(request.maxTextBytes),
      resume: state.resume,
    });
  }

  private async launch(input: {
    transport: LiveTransport;
    liveSessionId: string;
    provider: LiveProviderId;
    sessionId: string | null;
    workspace: string;
    maxTextBytes: number;
    resume: LiveSessionState["resume"];
  }): Promise<LiveSessionSummary> {
    if (this.sessions.size >= this.maxSessions) {
      throw new AgentHubError(
        "LIVE_SESSION_LIMIT",
        `this hub process already runs its ${this.maxSessions}-session live limit`,
      );
    }
    if (this.sessions.has(input.liveSessionId)) {
      throw new AgentHubError(
        "LIVE_SESSION_CONFLICT",
        `live session "${input.liveSessionId}" is already open in this hub process`,
      );
    }

    const descriptor = await input.transport.describe();
    if (descriptor.provider !== input.provider || descriptor.transport !== input.transport.id) {
      throw new AgentHubError(
        "LIVE_TRANSPORT_PAIRING_INVALID",
        `transport "${input.transport.id}" described itself as "${descriptor.transport}" for provider "${descriptor.provider}"`,
      );
    }
    const capabilities = validateLiveCapabilities(descriptor.capabilities);

    const launchRequest: LiveLaunchRequest = {
      live_session_id: input.liveSessionId,
      workspace: input.workspace,
      max_text_bytes: input.maxTextBytes,
      resume: input.resume,
    };

    let report: LiveLaunchReport;
    try {
      report = await input.transport.open(launchRequest);
    } catch (error) {
      throw new AgentHubError(
        "LIVE_LAUNCH_FAILED",
        `live session launch failed for provider "${input.provider}": ${asDelegateError(error).message}`,
      );
    }

    const handle = new ManagedLiveSession(
      {
        live_session_id: input.liveSessionId,
        session_id: input.sessionId,
        provider: input.provider,
        transport: descriptor.transport,
        capabilities,
        pid: report.pid,
        provider_session_id: report.provider_session_id,
        launched_at: report.launched_at,
        promptAccepted: false,
      },
      input.transport,
      input.maxTextBytes,
      this.eventBufferSize,
      this.turnTimeoutMs,
      this.launchSettleMs,
    );
    this.sessions.set(handle.id, handle);
    handle.startPump();
    return handle.toSummary();
  }

  private requestedTextBytes(value: number | undefined): number {
    if (value === undefined) {
      return this.maxTextBytes;
    }
    if (!Number.isInteger(value) || value < 1) {
      throw new AgentHubError("LIVE_COMMAND_INVALID", "max_text_bytes must be a positive integer");
    }
    return value;
  }

  private require(id: string): ManagedLiveSession {
    const session = this.sessions.get(id);
    if (session === undefined) {
      throw new AgentHubError(
        "LIVE_SESSION_NOT_FOUND",
        `no live session "${id}" is managed by this hub process`,
      );
    }
    return session;
  }

  get(id: string): LiveSessionSummary | null {
    return this.sessions.get(id)?.toSummary() ?? null;
  }

  list(): LiveSessionSummary[] {
    return [...this.sessions.values()].map((session) => session.toSummary());
  }

  /** Gate and validation live in `ManagedLiveSession.runCommand`. */
  command(id: string, request: LiveTurnCommand): Promise<LiveTurnResult> {
    return this.require(id).runCommand(request);
  }

  events(id: string, cursor = 0, limit = 200): LiveEventPage {
    const boundedLimit = Math.max(1, Math.min(1000, Math.trunc(limit) || 1));
    return this.require(id).page(Math.max(0, Math.trunc(cursor) || 0), boundedLimit);
  }

  /**
   * Stops the provider and reports what shutdown proved. Awaited only after
   * the pump finalized, so an in-flight turn's result is always observable
   * before a caller sees the close report.
   */
  async close(id: string, mode: LiveStopMode = "graceful"): Promise<LiveCloseReport> {
    const session = this.require(id);
    const report = await session.close(mode);
    await Promise.race([
      session.pumpEnded,
      new Promise<void>((resolve) => {
        setTimeout(resolve, 1_000);
      }),
    ]);
    return report;
  }
}

/** The build-wide manager shared by the CLI `live` command and MCP tools. */
export const liveSessionManager = new LiveSessionManager();

// ---------------------------------------------------------------------------
// CLI runner: long-lived stdin NDJSON ↔ stdout normalized documents
// ---------------------------------------------------------------------------

/** One `agent-hub live …` run: exactly one of provider/resumeId is set. */
export interface LiveLaunchInvocation {
  provider: LiveProviderId | null;
  resumeId: string | null;
  workspace: string;
  maxTextBytes?: number;
}

export interface LiveIo {
  /** The command stream, already split into lines. */
  stdin: AsyncIterable<string>;
  /** One complete NDJSON document per call (the newline is added by CLI). */
  stdout: (document: Record<string, unknown>) => void;
  stderr: (line: string) => void;
}

export interface LiveRunnerDependencies {
  manager?: LiveSessionManager;
  resumeSource?: LiveResumeSource;
}

/** Splits an arbitrary chunk stream into lines (final partial line included). */
export async function* iterateLiveCommands(chunks: AsyncIterable<unknown>): AsyncGenerator<string> {
  let pending = "";
  for await (const chunk of chunks) {
    pending +=
      typeof chunk === "string"
        ? chunk
        : Buffer.isBuffer(chunk)
          ? chunk.toString("utf8")
          : String(chunk);
    let newline = pending.indexOf("\n");
    while (newline >= 0) {
      yield pending.slice(0, newline).replace(/\r$/, "");
      pending = pending.slice(newline + 1);
      newline = pending.indexOf("\n");
    }
  }
  if (pending.length > 0) {
    yield pending.replace(/\r$/, "");
  }
}

type LiveWireAction = LiveCommandKind | "close";

const LIVE_WIRE_ACTIONS: readonly LiveWireAction[] = [
  "prompt",
  "follow_up",
  "steer",
  "cancel",
  "status",
  "permission_response",
  "close",
];

interface LiveWireCommand {
  action: LiveWireAction;
  text: string | null;
  reason: string | null;
  request_id: string | null;
  decision: LivePermissionDecision | null;
  note: string | null;
  terminate: boolean;
}

function parseLiveWireLine(
  line: string,
): { ok: true; command: LiveWireCommand } | { ok: false; message: string } {
  let document: unknown;
  try {
    document = JSON.parse(line);
  } catch {
    return { ok: false, message: "stdin line is not valid JSON" };
  }
  if (!isLiveRecord(document)) {
    return { ok: false, message: "stdin line must be a JSON object" };
  }
  const action = document.action;
  if (typeof action !== "string" || !(LIVE_WIRE_ACTIONS as readonly string[]).includes(action)) {
    return {
      ok: false,
      message: `unknown live command action ${JSON.stringify(action)}; expected one of ${LIVE_WIRE_ACTIONS.join(", ")}`,
    };
  }
  for (const field of ["text", "reason", "note", "request_id"] as const) {
    const value = document[field];
    if (value !== undefined && value !== null && typeof value !== "string") {
      return { ok: false, message: `"${field}" must be a string or null` };
    }
  }
  if (
    document.decision !== undefined &&
    document.decision !== null &&
    (typeof document.decision !== "string" || !PERMISSION_DECISIONS.includes(document.decision))
  ) {
    return { ok: false, message: '"decision" must be allow_once, allow_session, or deny' };
  }
  const terminate = document.terminate ?? false;
  if (typeof terminate !== "boolean") {
    return { ok: false, message: '"terminate" must be a boolean' };
  }
  return {
    ok: true,
    command: {
      action: action as LiveWireAction,
      text: (document.text as string | null | undefined) ?? null,
      reason: (document.reason as string | null | undefined) ?? null,
      request_id: (document.request_id as string | null | undefined) ?? null,
      decision: (document.decision as LivePermissionDecision | null | undefined) ?? null,
      note: (document.note as string | null | undefined) ?? null,
      terminate,
    },
  };
}

/**
 * Runs one long-lived live session over the hub wire: stdin carries one
 * NDJSON command per line, stdout receives `{type:"session"|"event"|"result"|
 * "error"|"close"}` documents, stderr carries human diagnostics. Exit: 0
 * clean, 1 structured failure (launch refusal, failed, or orphaned end).
 */
export async function runLiveSession(
  invocation: LiveLaunchInvocation,
  io: LiveIo,
  dependencies: LiveRunnerDependencies = {},
): Promise<number> {
  const manager = dependencies.manager ?? liveSessionManager;
  const context: { stage: LiveErrorStage; provider: LiveProviderId | null } = {
    stage: "launch",
    provider: invocation.provider,
  };

  let summary: LiveSessionSummary;
  try {
    if (invocation.resumeId !== null) {
      const source = dependencies.resumeSource ?? getLiveResumeSource();
      const state = await source.load(invocation.workspace, invocation.resumeId);
      assertLiveSessionState(state);
      context.provider = state.provider;
      summary = await manager.resume({
        state,
        workspace: invocation.workspace,
        maxTextBytes: invocation.maxTextBytes,
      });
    } else {
      if (invocation.provider === null) {
        throw new AgentHubError("LIVE_COMMAND_INVALID", "a live session needs a provider or a resume id");
      }
      summary = await manager.start({
        provider: invocation.provider,
        workspace: invocation.workspace,
        maxTextBytes: invocation.maxTextBytes,
      });
    }
  } catch (error) {
    io.stdout({ type: "error", error: toLiveError(error, context) });
    return 1;
  }

  const sessionId = summary.live_session_id;
  io.stderr(
    `agent-hub live: session=${sessionId} provider=${summary.provider} transport=${summary.transport} pid=${summary.pid ?? "none"}`,
  );
  io.stdout({ type: "session", session: summary });

  let cursor = 0;
  let relayStopped = false;
  let idleSpins = 0;
  const emitEvents = (): boolean => {
    const page = manager.events(sessionId, cursor, 100);
    for (const event of page.events) {
      cursor = event.seq;
      io.stdout({ type: "event", event });
    }
    return page.events.length > 0;
  };
  const relay = (async () => {
    while (!relayStopped) {
      if (emitEvents()) {
        idleSpins = 0;
        continue;
      }
      const status = manager.get(sessionId)?.status ?? "orphaned";
      if (isTerminalLiveStatus(status)) {
        break;
      }
      idleSpins += 1;
      // No progress: re-check after a brief wait so a wedged pump cannot
      // spin; after 400 quiet polls (≈10 s) stop relaying, not the session.
      if (idleSpins > 400) {
        break;
      }
      // Ref'd: the relay poll keeps a closing runner's event loop alive
      // until the close report lands.
      await new Promise<void>((resolve) => setTimeout(resolve, 25));
    }
    // Final drain: no recorded event may fail to reach stdout before close.
    while (emitEvents()) {
      // drained
    }
  })();

  const pendingResults: Promise<void>[] = [];
  let failedSeen = false;
  let terminate = false;
  for await (const line of io.stdin) {
    const trimmed = line.trim();
    if (trimmed.length === 0) {
      continue;
    }
    const parsed = parseLiveWireLine(trimmed);
    if (!parsed.ok) {
      io.stdout({
        type: "error",
        error: liveError("LIVE_COMMAND_INVALID", parsed.message, "protocol", false, summary.provider),
      });
      continue;
    }
    if (parsed.command.action === "close") {
      terminate = parsed.command.terminate;
      break;
    }
    const { action, terminate: _terminate, ...fields } = parsed.command;
    const pending = manager
      .command(sessionId, { action: action as LiveCommandKind, ...fields })
      .then(
        (result: LiveTurnResult) => {
          if (result.outcome === "failed") {
            failedSeen = true;
          }
          io.stdout({ type: "result", result, status: manager.get(sessionId)?.status ?? result.outcome });
        },
        (error: unknown) => {
          io.stdout({
            type: "error",
            error: toLiveError(error, { stage: "protocol", provider: summary.provider }),
          });
        },
      );
    pendingResults.push(pending);
  }

  // Terminal: stop accepting input, let in-flight commands finish (a
  // permission answer mid-grace must reach the provider before it dies),
  // then stop the provider and report the close honestly. Commands still
  // in flight after the bounded drain settle through the close path.
  await Promise.race([
    Promise.allSettled(pendingResults),
    new Promise<void>((resolve) => {
      // Ref'd: this grace is the process's only pending work while closing.
      setTimeout(resolve, LIVE_CLOSE_DRAIN_MS);
    }),
  ]);
  let closeReport: LiveCloseReport;
  try {
    closeReport = await manager.close(sessionId, terminate ? "terminate" : "graceful");
  } catch (error) {
    closeReport = {
      live_session_id: sessionId,
      status: "orphaned",
      stop: { status: "orphaned", exit_code: null, exit_signal: null, waited_ms: 0 },
    };
    io.stderr(`agent-hub live: close failed: ${asDelegateError(error).message}`);
  }
  await Promise.allSettled(pendingResults);
  relayStopped = true;
  await relay;

  io.stdout({ type: "close", close: closeReport });
  const finalStatus = manager.get(sessionId)?.status ?? closeReport.status;
  io.stderr(`agent-hub live: session=${sessionId} ended status=${finalStatus} stop=${closeReport.stop.status}`);
  return (
    failedSeen ||
    closeReport.stop.status === "orphaned" ||
    finalStatus === "error" ||
    finalStatus === "orphaned"
  )
    ? 1
    : 0;
}
