import { AgentHubError } from "../errors.js";

/**
 * Gate 0 — the provider-neutral interaction contract (rewrite, P1).
 *
 * This module is the public-neutral surface between callers, the
 * `InteractionKernel`, and injected provider transports. It fixes five
 * binding invariants:
 *
 *   1. Provider neutrality. `ProviderId` and `TransportId` are opaque
 *      strings; the contract enumerates no providers. Provider-specific
 *      facts ride only inside `ResumeState.data`, which the kernel treats
 *      as an opaque transport-owned payload and never interprets. The
 *      OMP RPC v2-only assumptions of the rewritten hub (probe evidence,
 *      dialect verification) live inside the injected OMP transport
 *      factory's probe/descriptor honesty — never in a kernel branch.
 *   2. Capability honesty. `Capabilities` is a total record: silence is a
 *      type error, and any claim short of `unsupported` must carry
 *      human-readable evidence naming what was verified. `derived` is
 *      meaningful only for observation-style names and a `status` claim —
 *      it is never injectable, and `validateCapabilities` refuses it on
 *      any command kind a caller can fire.
 *   3. Durability discipline. `SessionRecord` is the ONLY shape the kernel
 *      ever commits to the durable mirror, and its type is the guarantee:
 *      no command text, no event bodies, no transcripts, no permission
 *      summaries can fit. Commands, events, and turn results are
 *      transient — in-memory or on the wire only.
 *   4. No Git, no workspace internals. The workspace is an opaque root
 *      string handed to the transport; commit lineages, checkpoint pins,
 *      and ownership leases belong to `WorkspaceLifecycle` (P2) behind the
 *      `SessionRecord` / spawn-report boundaries — never to this contract.
 *   5. No wire payloads. Transports normalize their traffic into
 *      `EventBody`; anything they cannot map crosses as `unrecognized`
 *      (transport discriminator + byte count only).
 */

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

/** Opaque provider key. Selection enforces transport↔provider pairing at runtime; the contract never enumerates providers. */
export type ProviderId = string;

/** Opaque transport key. One transport implementation pairs with exactly one provider. */
export type TransportId = string;

/** Hub-generated session id (UUID by default). Never derived from user text. */
export type SessionId = string;

/** The frozen durable-record schema id. Records carrying anything else are refused. */
export const SESSION_SCHEMA_VERSION = "agent-hub-interaction/v1" as const;

// ---------------------------------------------------------------------------
// Capabilities (explicit honesty)
// ---------------------------------------------------------------------------

/**
 * *How* a capability is delivered — the mechanism, not merely a yes/no:
 *
 * - `native`: the provider protocol itself; commands are delivered directly.
 * - `hub-queued`: the kernel accepts the command and delivers it at the
 *   next safe boundary (a follow-up between turns). Transient queue only.
 * - `derived`: read-only observability synthesized by the kernel from
 *   stream evidence. Only meaningful for `status` and observation names;
 *   never injectable (enforced by `validateCapabilities`).
 * - `signal`: delivered through the OS signal path instead of the provider
 *   protocol (e.g. cancel via SIGTERM with bounded escalation).
 * - `unsupported`: honest absence. Commands of that kind are refused
 *   pre-dispatch with `stage: "capability"` errors, never silently dropped.
 */
export type CapabilitySupport = "native" | "hub-queued" | "derived" | "signal" | "unsupported";

/**
 * One capability claim. The union is the honesty gate: a claim short of
 * `unsupported` cannot exist without evidence naming what was actually
 * verified (installed command version, observed stream event, ...).
 */
export type CapabilityClaim =
  | { support: "unsupported"; evidence: null }
  | { support: Exclude<CapabilitySupport, "unsupported">; evidence: string };

/** Command kinds double as capability names, so no command can go unclaimed. */
export type CommandKind =
  | "prompt"
  | "follow_up"
  | "steer"
  | "cancel"
  | "status"
  | "permission_response";

/** Observation-only capability names beyond the command kinds. */
export type ObservationName = "resume" | "usage_reporting";

export type CapabilityName = CommandKind | ObservationName;

/**
 * A complete capability snapshot. `Record` is total: omitting a name is a
 * type error. Snapshots are captured per launch; `resume()` MUST refresh
 * the snapshot from the live transport — claims are launch-scoped.
 */
export type Capabilities = Record<CapabilityName, CapabilityClaim>;

export const COMMAND_KINDS: readonly CommandKind[] = [
  "prompt",
  "follow_up",
  "steer",
  "cancel",
  "status",
  "permission_response",
];

export const OBSERVATION_NAMES: readonly ObservationName[] = ["resume", "usage_reporting"];

export const CAPABILITY_NAMES: readonly CapabilityName[] = [
  ...COMMAND_KINDS,
  ...OBSERVATION_NAMES,
];

const CAPABILITY_SUPPORTS: readonly string[] = [
  "native",
  "hub-queued",
  "derived",
  "signal",
  "unsupported",
];

/** Names for which `derived` is a legal claim. */
const DERIVABLE_NAMES: readonly CapabilityName[] = ["status", "resume", "usage_reporting"];

export function isCommandKind(value: unknown): value is CommandKind {
  return typeof value === "string" && (COMMAND_KINDS as readonly string[]).includes(value);
}

/**
 * Runtime gate on any capability snapshot crossing a boundary (transport
 * descriptor, parsed durable record). The seed's `Record` type cannot
 * enforce totality across a JSON round-trip, so this function does:
 * every name present, no extras, `derived` only on derivable names, and
 * the evidence rule enforced per claim. Throws `CAPABILITY_SNAPSHOT_INVALID`.
 */
export function validateCapabilities(value: unknown): Capabilities {
  const issues: string[] = [];
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw invalidCapabilities(["capability snapshot must be a plain object"]);
  }
  const record = value as Record<string, unknown>;
  for (const key of Object.keys(record)) {
    if (!(CAPABILITY_NAMES as readonly string[]).includes(key)) {
      issues.push(`unknown capability "${key}"`);
    }
  }
  const out = {} as Record<CapabilityName, CapabilityClaim>;
  for (const name of CAPABILITY_NAMES) {
    const claim = record[name];
    if (typeof claim !== "object" || claim === null) {
      issues.push(`capability "${name}" is missing or not a claim object`);
      continue;
    }
    const { support, evidence } = claim as { support?: unknown; evidence?: unknown };
    if (typeof support !== "string" || !CAPABILITY_SUPPORTS.includes(support)) {
      issues.push(`capability "${name}" has support ${JSON.stringify(support) ?? "undefined"}`);
      continue;
    }
    if (support === "unsupported") {
      if (evidence !== null) {
        issues.push(`capability "${name}" claims unsupported but carries evidence; unsupported requires null`);
        continue;
      }
      out[name] = { support: "unsupported", evidence: null };
      continue;
    }
    if (typeof evidence !== "string" || evidence.length === 0) {
      issues.push(`capability "${name}" claims ${support} without an evidence string`);
      continue;
    }
    if (support === "derived" && !DERIVABLE_NAMES.includes(name)) {
      issues.push(`capability "${name}" is injectable; a "derived" claim would promise a command the kernel must never forward`);
      continue;
    }
    out[name] = { support: support as Exclude<CapabilitySupport, "unsupported">, evidence };
  }
  if (issues.length > 0) {
    throw invalidCapabilities(issues);
  }
  return out;
}

function invalidCapabilities(issues: readonly string[]): AgentHubError {
  return new AgentHubError(
    "CAPABILITY_SNAPSHOT_INVALID",
    `capability snapshot is not a valid honesty record: ${issues.join("; ")}`,
  );
}

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/**
 * Interaction session status.
 *
 * - `starting`/`idle`/`running`/`cancelling`/`closing`: the live lifecycle.
 * - `closed`: orderly shutdown with proof the provider is gone.
 * - `error`: the session ended abnormally (crash, degraded durable chain)
 *   with shutdown proven.
 * - `orphaned`: shutdown could NOT be proven — the provider group may
 *   still be running. Terminal for the kernel (no further commands), and
 *   the honest marker a recovery pass downstream re-derives from.
 */
export type SessionStatus =
  | "starting"
  | "idle"
  | "running"
  | "cancelling"
  | "closing"
  | "closed"
  | "error"
  | "orphaned";

export const SESSION_STATUSES: readonly SessionStatus[] = [
  "starting",
  "idle",
  "running",
  "cancelling",
  "closing",
  "closed",
  "error",
  "orphaned",
];

export const TERMINAL_STATUSES: readonly SessionStatus[] = ["closed", "error", "orphaned"];

export function isTerminalStatus(status: SessionStatus): boolean {
  return TERMINAL_STATUSES.includes(status);
}

// ---------------------------------------------------------------------------
// Commands (transient; the only place user text legitimately lives)
// ---------------------------------------------------------------------------

/** Permission verdicts the wire accepts — exactly two. Anything else is a caller error, never converted. */
export type PermissionDecision = "allow_once" | "deny";

export function isPermissionDecision(value: unknown): value is PermissionDecision {
  return value === "allow_once" || value === "deny";
}

interface CommandBase {
  /** Kernel-generated UUID; echoed back in the matching `TurnResult`. */
  command_id: string;
  session_id: SessionId;
  /** ISO-8601 issuance time. */
  issued_at: string;
}

/** Initial user task. Accepted exactly once per session, while `idle` before the first turn. */
export interface PromptCommand extends CommandBase {
  kind: "prompt";
  text: string;
}

/** Next-turn input; may be `hub-queued` while `running`. */
export interface FollowUpCommand extends CommandBase {
  kind: "follow_up";
  text: string;
}

/** Mid-turn guidance; valid only when the `steer` claim is `native` or `hub-queued`. */
export interface SteerCommand extends CommandBase {
  kind: "steer";
  text: string;
}

/** Abort the in-flight turn; delivered natively or via the `signal` path. */
export interface CancelCommand extends CommandBase {
  kind: "cancel";
  /** Kernel-side human note; never provider content. */
  reason: string | null;
}

/** Ask the provider for authoritative progress. Forwarded only when the `status` claim is `native`; `derived` means the kernel answers from stream evidence and MUST NOT forward. */
export interface StatusCommand extends CommandBase {
  kind: "status";
}

/** Answer a permission request observed from the event stream. */
export interface PermissionResponseCommand extends CommandBase {
  kind: "permission_response";
  /** The `request_id` from the observed `permission_request` body. */
  request_id: string;
  decision: PermissionDecision;
  /** Optional human note accompanying the decision. */
  note: string | null;
}

/**
 * All injectable kernel intents. The kernel validates each command against
 * the session's launch-scoped `Capabilities` before dispatch; an
 * unsupported kind is refused as a caller error, never delivered and never
 * silently queued.
 */
export type Command =
  | PromptCommand
  | FollowUpCommand
  | SteerCommand
  | CancelCommand
  | StatusCommand
  | PermissionResponseCommand;

// ---------------------------------------------------------------------------
// Events (normalized, byte-bounded, transient)
// ---------------------------------------------------------------------------

/**
 * Text that passed the event byte bound. `truncated: true` means content
 * was cut; the bound is enforced by both transport and kernel, so nothing
 * unbounded crosses this boundary.
 */
export interface BoundedText {
  text: string;
  truncated: boolean;
}

/** Usage counters. Every field is null when the provider did not report it — reported-zero and unreported are never conflated. */
export interface Usage {
  input_tokens: number | null;
  output_tokens: number | null;
  cached_tokens: number | null;
  cost_usd: number | null;
}

export type EventBody =
  | { kind: "status"; status: SessionStatus; note: string | null }
  | {
      kind: "text";
      role: "assistant" | "reasoning" | "system";
      /** Stable per-message id across chunks so consumers can concatenate. */
      stream_id: string;
      text: BoundedText;
      /** True on the last chunk of this `stream_id`. */
      final: boolean;
    }
  | {
      kind: "tool_start";
      call_id: string;
      tool: string;
      input_preview: BoundedText | null;
    }
  | {
      kind: "tool_end";
      call_id: string;
      tool: string;
      ok: boolean;
      output_preview: BoundedText | null;
    }
  | {
      kind: "permission_request";
      request_id: string;
      tool: string;
      summary: BoundedText;
    }
  | { kind: "usage"; usage: Usage }
  | { kind: "log"; level: "info" | "warn" | "error"; text: BoundedText }
  | { kind: "error"; error: KernelError }
  | {
      kind: "exit";
      /** False when the provider process died other than by kernel shutdown. */
      intentional: boolean;
      exit_code: number | null;
      exit_signal: string | null;
    }
  | {
      /**
       * Normalization fallback: the transport saw traffic it could not map
       * to a body above. Only the transport-side discriminator and size
       * cross — raw payloads must never be placed anywhere in this union.
       */
      kind: "unrecognized";
      transport_kind: string | null;
      bytes: number;
    };

/** Discriminants of `EventBody["kind"]`. */
export type EventKind = EventBody["kind"];

/**
 * One normalized stream event. Transient by contract: durable state never
 * stores event bodies. `seq` is per-session, gapless, and kernel-stamped;
 * across a `resume()` it continues from the durable resume cursor, so a
 * replay never re-consumes events.
 */
export interface SessionEvent {
  session_id: SessionId;
  seq: number;
  transport: TransportId;
  /** ISO-8601 observation time (kernel clock). */
  occurred_at: string;
  body: EventBody;
}

// ---------------------------------------------------------------------------
// Resume state (durable; opaque payload only, never wire data)
// ---------------------------------------------------------------------------

/**
 * Resume honesty marker: `verified` may be true only after a native resume
 * actually round-tripped with this handle, and `verified_via` records the
 * observable basis (never inferred from the provider merely accepting a
 * flag). Deeper provider-specific verification (e.g. the OMP RPC v2
 * switch_session + get_state locator echo) is reported BY the transport on
 * the launch report; the kernel only ever upgrades on an observed id
 * round-trip.
 */
export type ResumeVerification =
  | { verified: true; verified_via: string }
  | { verified: false; verified_via: null };

/**
 * Provider-neutral resume handle.
 *
 * - `data` is a transport-owned opaque payload for `provider_session_id`
 *   material beyond the id itself (tokens, dialect flags). The kernel
 *   round-trips it verbatim into the next `open()` and never reads inside.
 *   Treat its contents as secret material: never logged.
 * - `last_event_seq` is kernel-owned: the highest event seq durably
 *   consumed. The kernel overwrites any transport-claimed cursor — replay
 *   and resume boundaries are kernel facts, not provider claims.
 */
export type ResumeState = {
  provider: ProviderId;
  /** The provider's own conversation/session handle when it surfaced one. */
  provider_session_id: string | null;
  data: Readonly<Record<string, unknown>>;
  last_event_seq: number;
} & ResumeVerification;

/** Rebuild/validate a resume handle crossing a boundary (disk, IPC). Throws `SESSION_RECORD_INVALID`. */
export function parseResumeState(value: unknown): ResumeState {
  const invalid = (detail: string): AgentHubError =>
    new AgentHubError("SESSION_RECORD_INVALID", `resume state is invalid: ${detail}`);
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw invalid("must be a plain object");
  }
  const v = value as Record<string, unknown>;
  if (typeof v.provider !== "string" || v.provider.length === 0) {
    throw invalid("provider must be a non-empty string");
  }
  if (v.provider_session_id !== null && typeof v.provider_session_id !== "string") {
    throw invalid("provider_session_id must be a string or null");
  }
  if (typeof v.data !== "object" || v.data === null || Array.isArray(v.data)) {
    throw invalid("data must be a plain object (possibly empty)");
  }
  if (typeof v.last_event_seq !== "number" || !Number.isInteger(v.last_event_seq) || v.last_event_seq < 0) {
    throw invalid("last_event_seq must be a non-negative integer");
  }
  if (v.verified === true) {
    if (typeof v.verified_via !== "string" || v.verified_via.length === 0) {
      throw invalid("verified handles require a non-empty verified_via basis");
    }
    return {
      provider: v.provider,
      provider_session_id: v.provider_session_id as string | null,
      data: { ...(v.data as Record<string, unknown>) },
      last_event_seq: v.last_event_seq,
      verified: true,
      verified_via: v.verified_via,
    };
  }
  if (v.verified === false) {
    if (v.verified_via !== null) {
      throw invalid("unverified handles require verified_via: null");
    }
    return {
      provider: v.provider,
      provider_session_id: v.provider_session_id as string | null,
      data: { ...(v.data as Record<string, unknown>) },
      last_event_seq: v.last_event_seq,
      verified: false,
      verified_via: null,
    };
  }
  throw invalid("verified must be exactly true or false");
}

// ---------------------------------------------------------------------------
// Structured errors
// ---------------------------------------------------------------------------

/** Where a kernel error originated. Each stage carries its own retry semantics downstream. `checkpoint`-style durable pins are not a kernel stage — they belong to WorkspaceLifecycle (P2). */
export type ErrorStage =
  | "probe"
  | "launch"
  | "transport"
  | "provider"
  | "protocol"
  | "capability"
  | "state"
  | "shutdown";

/** Structured kernel error. Messages are kernel-generated only — provider stderr and transcripts never become error messages. */
export interface KernelError {
  code: string;
  message: string;
  stage: ErrorStage;
  /** Safe to retry with the same inputs (transient transport/provider trouble). */
  retryable: boolean;
  /** Provider in effect, or null for provider-neutral failures. */
  provider: ProviderId | null;
}

const ERROR_STAGES: readonly ErrorStage[] = [
  "probe",
  "launch",
  "transport",
  "provider",
  "protocol",
  "capability",
  "state",
  "shutdown",
];

export function kernelError(
  code: string,
  message: string,
  stage: ErrorStage,
  retryable: boolean,
  provider: ProviderId | null,
): KernelError {
  return { code, message, stage, retryable, provider };
}

/** Normalizes anything thrown across a seam into the kernel error shape. */
export function asKernelError(
  error: unknown,
  context: { stage: ErrorStage; provider: ProviderId | null },
): KernelError {
  if (error instanceof AgentHubError) {
    return { code: error.code, message: error.message, stage: context.stage, retryable: false, provider: context.provider };
  }
  if (error instanceof Error) {
    return { code: "INTERNAL_ERROR", message: error.message, stage: context.stage, retryable: false, provider: context.provider };
  }
  return { code: "INTERNAL_ERROR", message: String(error), stage: context.stage, retryable: false, provider: context.provider };
}

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

/**
 * Outcome of one delivered command.
 * - `succeeded` / `failed` / `cancelled`: delivered and concluded.
 * - `unsupported`: refused pre-dispatch by the capability gate (caller
 *   error; `error.stage` is `capability`); nothing was delivered.
 */
export type CommandOutcome = "succeeded" | "failed" | "cancelled" | "unsupported";

/**
 * Transient per-command result. Exit facts ride the `exit` event body; the
 * durable record stores pointers, never text, so nothing here is ever
 * persisted by the kernel.
 */
export interface TurnResult {
  session_id: SessionId;
  command_id: string;
  kind: CommandKind;
  outcome: CommandOutcome;
  /** Final assistant text of the turn, byte-bounded; null when none was produced. */
  final_text: BoundedText | null;
  usage: Usage | null;
  started_at: string;
  finished_at: string;
  duration_ms: number;
  error: KernelError | null;
}

/** Immediate acknowledgement for a command submitted through the async API. */
export interface CommandAccepted {
  session_id: SessionId;
  command_id: string;
  kind: "prompt" | "follow_up";
  accepted_at: string;
}

// ---------------------------------------------------------------------------
// The durable session record (Git-free by construction)
// ---------------------------------------------------------------------------

/**
 * The durable session mirror — the ONLY shape the kernel commits. Its
 * type is the guarantee: no task text, no event bodies, no transcripts, no
 * permission summaries can fit. It carries NO Git fields: commit lineage
 * and checkpoint pins are WorkspaceLifecycle facts (P2), correlated by
 * `session_id` outside this kernel. `workspace` is an opaque root string
 * the transport was launched in — a handle, never an interpretation.
 *
 * `status` is last-known with the crash rule: recovery must re-prove
 * liveness or rewrite it to `orphaned`.
 */
export interface SessionRecord {
  schema: typeof SESSION_SCHEMA_VERSION;
  session_id: SessionId;
  provider: ProviderId;
  transport: TransportId;
  /** Capability claims from the CURRENT launch's descriptor; `resume()` refreshes this snapshot. */
  capabilities: Capabilities;
  /** Opaque workspace root handed to the transport at launch. */
  workspace: string;
  /** The byte bound every `BoundedText` in this session respects. */
  max_text_bytes: number;
  /** Provider resume handle, or null before the provider surfaced any. */
  resume: ResumeState | null;
  status: SessionStatus;
  /** Increments by exactly one per committed durable transition. */
  revision: number;
  /** Last structured error, if any; message is kernel-generated. */
  last_error: KernelError | null;
  created_at: string;
  updated_at: string;
}

/**
 * Rebuild-and-validate a durable record crossing a boundary (disk via the
 * P2 store, IPC). Field-by-field reconstruction: unknown keys are dropped
 * and any structural lie throws `SESSION_RECORD_INVALID`.
 */
export function parseSessionRecord(value: unknown): SessionRecord {
  const invalid = (detail: string): AgentHubError =>
    new AgentHubError("SESSION_RECORD_INVALID", `session record is invalid: ${detail}`);
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw invalid("must be a plain object");
  }
  const v = value as Record<string, unknown>;
  if (v.schema !== SESSION_SCHEMA_VERSION) {
    throw invalid(`schema must be "${SESSION_SCHEMA_VERSION}", got ${JSON.stringify(v.schema) ?? "undefined"}`);
  }
  for (const key of ["session_id", "provider", "transport", "workspace", "created_at", "updated_at"] as const) {
    if (typeof v[key] !== "string" || v[key].length === 0) {
      throw invalid(`${key} must be a non-empty string`);
    }
  }
  if (
    typeof v.max_text_bytes !== "number" ||
    !Number.isInteger(v.max_text_bytes) ||
    v.max_text_bytes <= 0
  ) {
    throw invalid("max_text_bytes must be a positive integer");
  }
  if (typeof v.revision !== "number" || !Number.isInteger(v.revision) || v.revision < 0) {
    throw invalid("revision must be a non-negative integer");
  }
  if (typeof v.status !== "string" || !(SESSION_STATUSES as readonly string[]).includes(v.status)) {
    throw invalid(`status ${JSON.stringify(v.status) ?? "undefined"} is not a kernel status`);
  }
  if (v.resume !== null && typeof v.resume !== "object") {
    throw invalid("resume must be an object or null");
  }
  if (v.last_error !== null) {
    if (typeof v.last_error !== "object") {
      throw invalid("last_error must be an object or null");
    }
    const e = v.last_error as Record<string, unknown>;
    if (typeof e.code !== "string" || typeof e.message !== "string" || !ERROR_STAGES.includes(e.stage as ErrorStage) || typeof e.retryable !== "boolean") {
      throw invalid("last_error must carry code, message, a kernel stage, and retryable");
    }
    if (e.provider !== null && typeof e.provider !== "string") {
      throw invalid("last_error.provider must be a string or null");
    }
  }
  return {
    schema: SESSION_SCHEMA_VERSION,
    session_id: v.session_id as string,
    provider: v.provider as string,
    transport: v.transport as string,
    capabilities: validateCapabilities(v.capabilities),
    workspace: v.workspace as string,
    max_text_bytes: v.max_text_bytes as number,
    resume: v.resume === null ? null : parseResumeState(v.resume),
    status: v.status as SessionStatus,
    revision: v.revision as number,
    last_error: (v.last_error as KernelError | null) ?? null,
    created_at: v.created_at as string,
    updated_at: v.updated_at as string,
  };
}

// ---------------------------------------------------------------------------
// Transport seam (narrow; implemented by the provider transports, P3)
// ---------------------------------------------------------------------------

/** Static identity + capability declaration of a transport implementation. */
export interface TransportDescriptor {
  transport: TransportId;
  provider: ProviderId;
  capabilities: Capabilities;
}

/** Process identity facts proven at spawn time. `pgid` equals `pid` for detached group leaders on POSIX. */
export interface ProcessFacts {
  pid: number;
  pgid: number;
}

/**
 * Explicit permission policy for sessions whose provider surfaces permission
 * decisions. `deny` (the contract default): the session may NOT escalate a
 * decision to a human; kernel-side handling answers it without interaction.
 * `interactive`: observed requests are surfaced verbatim and answered only
 * through `permission_response` commands. A transport MUST NOT flip, soften,
 * or drop a capability claim because of the chosen policy.
 */
export type PermissionPolicy = "deny" | "interactive";

/** Everything a transport needs to start a session. Contains no task text — prompts arrive only via `send`. */
export interface LaunchRequest {
  session_id: SessionId;
  /** Opaque workspace root the provider runs in. */
  workspace: string;
  /** Byte bound every `BoundedText` this session emits must respect. */
  max_text_bytes: number;
  /** Durable resume hint; a transport that cannot honor it must fail `open`, never silently start fresh. */
  resume: ResumeState | null;
  /** Permission policy; omitting it means the contract default `deny`. */
  permission_policy?: PermissionPolicy;
  /**
   * Durable ownership boundary: a transport that spawned a local provider
   * process MUST await this callback immediately after the spawn succeeds
   * and BEFORE any protocol handshake can fail, so a later handshake
   * failure can never lose the process.
   */
  report_process?: (facts: ProcessFacts) => Promise<void>;
}

/** What a launch produced. */
export interface LaunchReport {
  /** Local provider process pid, or null when the transport has no local process. */
  pid: number | null;
  /** Provider session handle observed at startup. */
  provider_session_id: string | null;
  launched_at: string;
  /**
   * Post-handshake resume state built by the transport from what it
   * actually observed (locator echo, init-envelope identity, round-trip).
   * `verified` here is the transport's own evidence, not a kernel
   * inference; a launch under a resume hint that produced no resume state
   * is a contract violation the kernel must reject.
   */
  resume_state?: ResumeState | null;
}

/** How shutdown was requested. `terminate` means bounded SIGKILL escalation is authorized. */
export type StopMode = "graceful" | "terminate";

/** What shutdown proved. */
export interface StopReport {
  /** `closed` only with proof the process is gone; otherwise `orphaned`, never assumed. */
  status: "closed" | "orphaned";
  exit_code: number | null;
  exit_signal: string | null;
  waited_ms: number;
}

/**
 * Transport runtime contract. All provider traffic must cross this boundary
 * already normalized into `EventBody`s and out of `Command`s — wire formats
 * stay inside the implementation. `events()` yields events whose envelope
 * the kernel re-stamps; only `body` is trusted.
 */
export interface Transport {
  readonly id: TransportId;
  readonly provider: ProviderId;
  /** Capability claims for this implementation; `native` claims without verification are contract violations. */
  describe(): Promise<TransportDescriptor>;
  open(request: LaunchRequest): Promise<LaunchReport>;
  /** Delivers a capability-vetted command; transports must not second-guess the kernel's gate, but must never accept text they cannot bound. */
  send(command: Command): Promise<void>;
  /** Single-consumer event pump; ends when the session reaches a terminal status. */
  events(): AsyncIterable<SessionEvent>;
  /** Idempotent; resolves only after the process is reaped or its survival is honestly reported. */
  stop(mode: StopMode): Promise<StopReport>;
}

/** Result of probing the installed provider command; launching nothing is fine, guessing is not. The OMP RPC v2-only dialect bar is expressed here by the OMP factory's honest probe. */
export interface ProbeResult {
  found: boolean;
  /** Provider-reported version string; null when the command does not report one. */
  version: string | null;
  /** Human-readable bounded detail. */
  detail: string | null;
}

/** Creates transports. One factory serves exactly one transport/provider pair; the kernel validates `provider` pairing before `create` may be called. */
export interface TransportFactory {
  readonly transport: TransportId;
  readonly provider: ProviderId;
  /** Detects the installed provider command without launching it. */
  probe(): Promise<ProbeResult>;
  create(): Transport;
}

/**
 * Provider-side factory: knows which transports the provider may run on and
 * picks among kernel-validated candidates. Returning null is honest and
 * preferred over any fallback guess — no transport, no session.
 */
export interface ProviderFactory {
  readonly provider: ProviderId;
  /** Accepted transport ids, preference order first. */
  readonly transports: readonly TransportId[];
  /** Selects from factories whose `provider` already matched; null when none is usable. */
  selectTransport(factories: readonly TransportFactory[]): TransportFactory | null;
}
