import type { DelegateError, RepositoryIdentity } from "../types.js";

/**
 * v3 live Agent Hub shared contract (Gate 0 seed).
 *
 * Types only — this module contains no runtime logic and must never gain any.
 * It is purely additive: no v1 (`src/types.ts`) or v2 (`src/state.ts`,
 * `src/session.ts`) field or wire shape is touched, and everything imported
 * here is imported with `import type`.
 *
 * Contract invariants (C2C plan — binding for every later live package):
 *
 *   1. No task text, stdout/stderr, or raw transcript content may ever be
 *      stored in durable live state. `LiveSessionState` is the *only* durable
 *      record in this module; its allowed content is fixed by the type.
 *      Commands, events, and results are transient — in-memory or on-the-wire
 *      only — which is why they may carry text and durable state may not.
 *   2. Capability honesty is explicit and type-enforced: every capability a
 *      transport claims must carry human-readable evidence unless the claim is
 *      `unsupported`, and a resume handle counts as `verified` only when a
 *      native resume actually round-tripped (`verified_via` records how).
 *   3. Shared fields are provider-neutral. Provider-specific facts live only
 *      inside the matching `ProviderResumeState` variant, never in the shared
 *      command/event/state shapes.
 *   4. No provider wire payloads cross this boundary. Transports normalize
 *      their traffic into `LiveEvent` bodies; anything they cannot normalize
 *      surfaces as the `unrecognized` body (kind + byte count only), never as
 *      raw content.
 *
 * Transport ↔ provider pairing is 1:1:
 *   omp-rpc → omp, agy-stream-json → agy, pi-rpc → pi, hermes-acp → hermes.
 */

// ---------------------------------------------------------------------------
// Identity
// ---------------------------------------------------------------------------

/** Wire transports the hub normalizes for live sessions. One per provider. */
export type LiveTransportId = "omp-rpc" | "agy-stream-json" | "pi-rpc" | "hermes-acp";

/** Providers a live session can run. `pi` and `hermes` are v3-only. */
export type LiveProviderId = "omp" | "agy" | "pi" | "hermes";

// ---------------------------------------------------------------------------
// Capabilities (explicit honesty)
// ---------------------------------------------------------------------------

/**
 * *How* a capability is delivered — the mechanism, not merely a yes/no:
 *
 * - `native`: the provider protocol itself; commands are delivered directly.
 * - `hub-queued`: the hub accepts the command and delivers it at the next
 *   safe boundary (e.g. a follow-up between turns); acceptance before
 *   delivery is honest as long as the queue itself is durable-free (transient).
 * - `derived`: read-only observability synthesized by the hub from other
 *   evidence (e.g. `status` inferred from stream activity). Never injectable;
 *   meaningful only for observation-style names (`status`, `usage_reporting`,
 *   `checkpoint`), never for a command kind a caller can fire mid-turn.
 * - `signal`: delivered through the OS signal path instead of the provider
 *   protocol (e.g. cancel via SIGTERM with bounded SIGKILL escalation).
 * - `unsupported`: honest absence. Commands of that kind are refused up front
 *   with `stage: "capability"` errors and never silently dropped.
 */
export type CapabilitySupport = "native" | "hub-queued" | "derived" | "signal" | "unsupported";

/**
 * One capability claim. The union is the honesty gate: a claim short of
 * `unsupported` cannot exist in this type system without evidence naming what
 * was actually verified (installed command version, observed stream event, ...).
 */
export type LiveCapabilityClaim =
  | { support: "unsupported"; evidence: null }
  | { support: Exclude<CapabilitySupport, "unsupported">; evidence: string };

/** Command kinds double as capability names, so no command can go unclaimed. */
export type LiveCommandKind = "prompt" | "follow_up" | "steer" | "cancel" | "status" | "permission_response";

/** Observation-only capability names beyond the command kinds. */
export type LiveObservationName = "resume" | "checkpoint" | "usage_reporting";

export type LiveCapabilityName = LiveCommandKind | LiveObservationName;

/**
 * A complete capability snapshot. `Record` is total: omitting a name is a
 * type error, so silence can never masquerade as support. Snapshots are
 * captured at launch and treated as immutable for the session's lifetime.
 */
export type LiveCapabilities = Record<LiveCapabilityName, LiveCapabilityClaim>;

// ---------------------------------------------------------------------------
// Status
// ---------------------------------------------------------------------------

/**
 * Live session status.
 *
 * - `starting`: transport launching, capabilities being claimed.
 * - `idle`: transport up, no turn in flight.
 * - `running`: a turn is in flight; follow-ups/steer may be accepted per
 *   their capability claims.
 * - `cancelling`: cancel accepted, awaiting the provider's turn-end.
 * - `closing`: shutdown requested; events still drain.
 * - `closed`: terminal, transport stopped and reaped.
 * - `error`: terminal failure recorded via a `LiveError`.
 * - `orphaned`: the hub can no longer prove it owns the underlying process or
 *   session (crash recovery, unverifiable liveness). Mirrors the lock-recovery
 *   stance in `src/locks.ts`: unprovable is never treated as ours.
 *
 * Durable persistence rule for `LiveSessionState.status`: on hub crash
 * recovery, a persisted `starting`/`idle`/`running`/`cancelling`/`closing`
 * must be re-validated and either proven alive or rewritten to `orphaned` —
 * never trusted as-is.
 */
export type LiveStatus =
  | "starting"
  | "idle"
  | "running"
  | "cancelling"
  | "closing"
  | "closed"
  | "error"
  | "orphaned";

// ---------------------------------------------------------------------------
// Commands (transient; the only place user text legitimately lives in
// shared shapes — durable state never stores them)
// ---------------------------------------------------------------------------

/** Permission verdict returned for an observed `permission_request` event. v3
 * accepts exactly two verdicts; anything else (including any "session-wide
 * allow" wording) is rejected at the surface boundary, never converted. */
export type LivePermissionDecision = "allow_once" | "deny";

interface LiveCommandBase {
  /** Hub-generated UUID; echoed back in the matching `LiveTurnResult`. */
  command_id: string;
  live_session_id: string;
  /** ISO-8601 issuance time. */
  issued_at: string;
}

/** Initial user task. Accepted exactly once per live session, while `idle` before the first turn. */
export interface LivePromptCommand extends LiveCommandBase {
  kind: "prompt";
  text: string;
}

/** Next-turn input; may be `hub-queued` while `running`. */
export interface LiveFollowUpCommand extends LiveCommandBase {
  kind: "follow_up";
  text: string;
}

/** Mid-turn guidance; valid only when the `steer` claim is `native` or `hub-queued`. */
export interface LiveSteerCommand extends LiveCommandBase {
  kind: "steer";
  text: string;
}

/** Abort the in-flight turn; delivered natively or via the `signal` path. */
export interface LiveCancelCommand extends LiveCommandBase {
  kind: "cancel";
  /** Hub-side human note; never provider content. */
  reason: string | null;
}

/** Ask the provider for authoritative progress. Valid when `status` is `native`; `derived` means the hub answers from stream evidence and must NOT forward this command. */
export interface LiveStatusCommand extends LiveCommandBase {
  kind: "status";
}

/** Answer a permission request observed from the event stream. */
export interface LivePermissionResponseCommand extends LiveCommandBase {
  kind: "permission_response";
  /** The `request_id` from the observed `permission_request` body. */
  request_id: string;
  decision: LivePermissionDecision;
  /** Optional human note accompanying the decision. */
  note: string | null;
}

/**
 * All injectable hub intents. The hub validates each command against the
 * session's `LiveCapabilities` before dispatch; an unsupported kind is
 * rejected as a caller error, never delivered and never silently queued.
 */
export type LiveCommand =
  | LivePromptCommand
  | LiveFollowUpCommand
  | LiveSteerCommand
  | LiveCancelCommand
  | LiveStatusCommand
  | LivePermissionResponseCommand;

// ---------------------------------------------------------------------------
// Events (normalized, byte-bounded, transient)
// ---------------------------------------------------------------------------

/**
 * Text that has passed the transport's byte bound. `truncated: true` means
 * content was cut to stay within `LiveLaunchRequest.max_text_bytes`; the
 * bound is enforced by the transport, and nothing unbounded crosses here.
 */
export interface LiveBoundedText {
  text: string;
  truncated: boolean;
}

/** Usage counters. Every field is null when the provider did not report it — reported-zero and unreported are never conflated. */
export interface LiveUsage {
  input_tokens: number | null;
  output_tokens: number | null;
  cached_tokens: number | null;
  cost_usd: number | null;
}

export type LiveEventBody =
  | { kind: "status"; status: LiveStatus; note: string | null }
  | {
      kind: "text";
      role: "assistant" | "reasoning" | "system";
      /** Stable per-message id across chunks so consumers can concatenate. */
      stream_id: string;
      text: LiveBoundedText;
      /** True on the last chunk of this `stream_id`. */
      final: boolean;
    }
  | {
      kind: "tool_start";
      call_id: string;
      tool: string;
      input_preview: LiveBoundedText | null;
    }
  | {
      kind: "tool_end";
      call_id: string;
      tool: string;
      ok: boolean;
      output_preview: LiveBoundedText | null;
    }
  | {
      kind: "permission_request";
      request_id: string;
      tool: string;
      summary: LiveBoundedText;
    }
  | { kind: "usage"; usage: LiveUsage }
  | { kind: "log"; level: "info" | "warn" | "error"; text: LiveBoundedText }
  | { kind: "error"; error: LiveError }
  | {
      kind: "exit";
      /** False when the provider process died other than by hub shutdown. */
      intentional: boolean;
      exit_code: number | null;
      exit_signal: string | null;
    }
  | {
      /**
       * Normalization fallback: the transport saw traffic it could not map to
       * a body above. Only the transport-side discriminator and size cross —
       * raw payloads must never be placed anywhere in this union.
       */
      kind: "unrecognized";
      transport_kind: string | null;
      bytes: number;
    };

/** Discriminants of `LiveEventBody["kind"]`. */
export type LiveEventKind = LiveEventBody["kind"];

/**
 * One normalized stream event. Transient by contract: durable live state never
 * stores event bodies or their text. `seq` is per-session, starts at 1, and
 * has no gaps — a consumer that saw a gap is seeing a bug, not provider noise.
 */
export interface LiveEvent {
  live_session_id: string;
  seq: number;
  transport: LiveTransportId;
  /** ISO-8601 observation time (hub clock). */
  occurred_at: string;
  body: LiveEventBody;
}

// ---------------------------------------------------------------------------
// Checkpoints
// ---------------------------------------------------------------------------

/**
 * Why a checkpoint was taken.
 *
 * - `turn_end`: an accepted turn finished (any outcome) and its work is worth
 *   pinning.
 * - `requested`: the caller explicitly asked mid-session.
 * - `cancel`: the turn was cancelled and partial work is pinned.
 * - `close`: orderly shutdown pinned the final state.
 * - `error`: the session entered `error` and work up to that point is pinned.
 * - `crash_recovery`: the hub recovered an orphaned session and pinned the
 *   surviving worktree state before rewriting its status.
 */
export type CheckpointReason =
  | "turn_end"
  | "requested"
  | "cancel"
  | "close"
  | "error"
  | "crash_recovery";

/**
 * One hook-free artifact commit in the live checkpoint chain, using the same
 * full-width-SHA and commit-capture discipline as the v2 session lineage
 * (`SessionArtifact`), but on the live session's own private ref. Diffs and
 * changed-file content stay out; this is a commit pointer record.
 */
export interface LiveCheckpoint {
  /** 1-based position in this live session's checkpoint chain. */
  seq: number;
  /** Checkpoint commit (40- or 64-hex, full object name). */
  commit: string;
  /** Previous chain head; the base commit for seq 1. */
  parent: string;
  /** Tree recorded by `commit`. */
  tree: string;
  /** True when `tree` matches the parent tree (no changes since last checkpoint). */
  empty: boolean;
  reason: CheckpointReason;
  taken_at: string;
}

// ---------------------------------------------------------------------------
// Provider resume state (durable; opaque handles only, never wire payloads)
// ---------------------------------------------------------------------------

interface ProviderResumeBase {
  /** The provider's own conversation/session handle when it surfaced one. */
  provider_session_id: string | null;
}

/**
 * Resume honesty marker: `verified` may be true only after a native resume
 * actually round-tripped with this handle, and `verified_via` then records the
 * observable basis (never inferred from the provider merely accepting a flag).
 */
export type ResumeVerification =
  | { verified: true; verified_via: string }
  | { verified: false; verified_via: null };

/** omp / omp-rpc: session id plus an event cursor so replays never re-consume events after a hub restart. */
export type OmpResumeState = ProviderResumeBase &
  ResumeVerification & {
    provider: "omp";
    /** Highest `LiveEvent.seq` the hub durably consumed; replay starts after it. */
    last_event_seq: number;
  };

/**
 * agy / agy-stream-json: the provider session id is observed from the stream,
 * but native resume is only real once the installed binary is *proven* to
 * accept the resume argv — the same verification bar as v2's
 * `NativeResumeCapability.verify()` (see `src/adapters/types.ts`). Until then
 * continuation is filesystem-only through the bound v2 session.
 */
export type AgyResumeState = ProviderResumeBase &
  ResumeVerification & {
    provider: "agy";
    resume_argv_verified: boolean;
  };

/** pi / pi-rpc: pi-issued opaque resume token. Treat as secret material: never logged, never in durable records beyond this field. */
export type PiResumeState = ProviderResumeBase &
  ResumeVerification & {
    provider: "pi";
    resume_token: string | null;
  };

/** hermes / hermes-acp: resume rides ACP `session/load`; the flag is set only when the initialize handshake actually advertised it. */
export type HermesResumeState = ProviderResumeBase &
  ResumeVerification & {
    provider: "hermes";
    session_load_advertised: boolean;
  };

/** Discriminated on `provider`; a transport may only carry its own variant. */
export type ProviderResumeState =
  | OmpResumeState
  | AgyResumeState
  | PiResumeState
  | HermesResumeState;

// ---------------------------------------------------------------------------
// Structured errors and results
// ---------------------------------------------------------------------------

/** Where a live error originated. Each stage has its own retry semantics downstream. */
export type LiveErrorStage =
  | "probe"
  | "launch"
  | "transport"
  | "provider"
  | "protocol"
  | "capability"
  | "checkpoint"
  | "state"
  | "shutdown";

/**
 * Structured live error. Extends the v1 `DelegateError` shape so v2-era
 * consumers keep reading `code`/`message`; hub-generated messages only —
 * provider stderr and transcripts never become error messages.
 */
export interface LiveError extends DelegateError {
  stage: LiveErrorStage;
  /** Safe to retry with the same inputs (transient transport/provider trouble). */
  retryable: boolean;
  /** Provider in effect, or null for provider-neutral failures (state, probe). */
  provider: LiveProviderId | null;
}

/**
 * Outcome of one delivered command.
 * - `succeeded` / `failed` / `cancelled`: delivered and concluded.
 * - `unsupported`: refused pre-dispatch by the capability gate (caller error;
 *   `error.stage` is `capability`), nothing was delivered.
 */
export type LiveCommandOutcome = "succeeded" | "failed" | "cancelled" | "unsupported";

/**
 * Transient per-command result (in-memory / on-the-wire only). This is where
 * bounded final output lives — durable state records only the pointers.
 */
export interface LiveTurnResult {
  live_session_id: string;
  command_id: string;
  kind: LiveCommandKind;
  outcome: LiveCommandOutcome;
  /** Final assistant text of the turn, byte-bounded; null when none was produced. */
  final_text: LiveBoundedText | null;
  usage: LiveUsage | null;
  /** Checkpoint pinned for this command, if any. */
  checkpoint: LiveCheckpoint | null;
  exit_code: number | null;
  exit_signal: string | null;
  started_at: string;
  finished_at: string;
  duration_ms: number;
  error: LiveError | null;
}

// ---------------------------------------------------------------------------
// Durable live state metadata
// ---------------------------------------------------------------------------

/**
 * The durable live-session record — the *only* type in this module whose
 * values may be persisted. Its shape is the guarantee: no task text, no
 * stdout/stderr, no event bodies, no transcripts, no permission summaries can
 * fit in. It mirrors the minimalism of v2 `SessionState` (identity, commit
 * lineage, revision, timestamps) and adds the v3 live facts (status,
 * capability snapshot taken at launch, resume state, last error).
 *
 * `status` is last-known status with the crash rule from `LiveStatus`:
 * recovery must re-prove liveness or rewrite it to `orphaned`.
 */
export interface LiveSessionState {
  /** Live-record schema version: the non-ambiguous frozen schema id. */
  schema: "agent-hub-live/v1";
  /** Hub-generated UUID; never derived from raw user text. */
  live_session_id: string;
  /**
   * v2 durable session this live session continues or feeds, or null when the
   * live session stands alone. The live checkpoint chain lives on its own
   * private ref and never shares the v2 session ref.
   */
  session_id: string | null;
  provider: LiveProviderId;
  transport: LiveTransportId;
  /** Capability claims from the CURRENT launch's transport descriptor; a successful resume refreshes this snapshot from the live transport. */
  capabilities: LiveCapabilities;
  /** Repository identity captured at launch (same rule as v2 fan-out/session). */
  identity: RepositoryIdentity;
  /** Head the live worktree started from. */
  base_commit: string;
  /** Live checkpoint chain head; equals `base_commit` before the first checkpoint. */
  current_commit: string;
  /** Number of checkpoints taken (0 before the first); matches `LiveCheckpoint.seq` of the head. */
  checkpoint_seq: number;
  /** Reason recorded by the checkpoint at the chain head; null before the first checkpoint. */
  last_checkpoint_reason: CheckpointReason | null;
  /** Absolute path of the hub-owned live worktree backing this record. */
  worktree_path: string;
  /** Absolute path of that worktree's hub temp parent directory. */
  worktree_parent: string;
  /** Resume state for the provider, or null before the provider surfaced any handle. */
  resume: ProviderResumeState | null;
  status: LiveStatus;
  /** Increments by exactly one per committed durable transition. */
  revision: number;
  /** Last structured error, if any; message is hub-generated (see `LiveError`). */
  last_error: LiveError | null;
  created_at: string;
  updated_at: string;
}

// ---------------------------------------------------------------------------
// Transport contract and factories
// ---------------------------------------------------------------------------

/** Static identity + capability declaration of a transport implementation. */
export interface LiveTransportDescriptor {
  transport: LiveTransportId;
  provider: LiveProviderId;
  capabilities: LiveCapabilities;
}

/** Process identity facts proven at spawn time. `pgid` equals `pid` for the
 * detached group leaders the hub launches on POSIX. */
export interface LiveProviderProcessFacts {
  pid: number;
  pgid: number;
}

/**
 * Explicit permission policy for sessions whose provider surfaces permission
 * decisions (Hermes/ACP is the named consumer; the field itself is
 * provider-neutral so no wire shape ever crosses this boundary).
 *
 * - `deny` (the contract default when a request omits it): the session may
 *   NOT escalate a permission decision to a human; hub-side handling answers
 *   it without user interaction. Capability truth stays fully representable
 *   under either policy: the `permission_response` claim in the launch
 *   capability snapshot keeps describing what the transport *actually* does
 *   (with its evidence), and a transport MUST NOT flip, soften, or drop a
 *   claim because of the chosen policy — `unsupported` remains the honest
 *   claim when nothing is forwarded, `native` stays when verdicts really do
 *   reach the provider.
 * - `interactive`: observed permission requests are surfaced to the caller
 *   verbatim and answered only through `permission_response` commands.
 *
 * This is the binding contract; the MCP/CLI surface and the Hermes transport
 * wiring are later packages and may not reinterpret it.
 */
export type LivePermissionPolicy = "deny" | "interactive";

/** Everything a transport needs to start a live session. Contains no task text — prompts arrive only via `send`. */
export interface LiveLaunchRequest {
  live_session_id: string;
  /** Isolated worktree root the provider runs in (hub-owned, never the caller checkout). */
  workspace: string;
  /** Byte bound every `LiveBoundedText` this session emits must respect. */
  max_text_bytes: number;
  /** Durable resume hint; a transport that cannot honor it must fail launch with a `LiveError`, never silently start fresh. */
  resume: ProviderResumeState | null;
  /** Permission policy for this launch/resume; omitting it means the contract default `deny`. */
  permission_policy?: LivePermissionPolicy;
  /**
   * Durable ownership boundary: a transport that spawned a local provider
   * process MUST await this callback with the spawn facts immediately after
   * the spawn succeeds and BEFORE any protocol handshake can fail. The hub
   * records the ownership durably at that point, so a later handshake failure
   * can never lose the process. Absent only when the caller cannot record
   * ownership (no lease); such callers may not spawn group-owned children.
   */
  report_process?: (facts: LiveProviderProcessFacts) => Promise<void>;
}

/** What a launch produced. */
export interface LiveLaunchReport {
  /** Local provider process pid, or null when the transport has no local process. */
  pid: number | null;
  /** Provider session handle observed at startup (also lands in the resume state). */
  provider_session_id: string | null;
  launched_at: string;
  /**
   * The full post-handshake resume state, built by the transport from what it
   * actually observed (locator echo, init-envelope identity, session/load
   * round-trip, argv verification). `verified` here is the transport's own
   * evidence, not a hub inference; a launch under a resume hint that produced
   * no resume state is a contract violation the hub must reject.
   */
  resume_state?: ProviderResumeState | null;
}

/** How shutdown was requested. `terminate` means bounded SIGKILL escalation is authorized. */
export type LiveStopMode = "graceful" | "terminate";

/** What shutdown proved. */
export interface LiveStopReport {
  /** `closed` only with proof the process is gone; otherwise `orphaned`, never assumed. */
  status: "closed" | "orphaned";
  exit_code: number | null;
  exit_signal: string | null;
  waited_ms: number;
}

/**
 * Live transport runtime contract. All provider traffic must cross the
 * boundary already normalized into `LiveEvent`s and out of `LiveCommand`s —
 * wire formats stay inside the implementation.
 */
export interface LiveTransport {
  readonly id: LiveTransportId;
  readonly provider: LiveProviderId;
  /** Capability claims for this implementation; `native` claims without verification are contract violations. */
  describe(): Promise<LiveTransportDescriptor>;
  open(request: LiveLaunchRequest): Promise<LiveLaunchReport>;
  /** Delivers a capability-vetted command; transports must not second-guess the hub's gate, but must never accept text they cannot bound. */
  send(command: LiveCommand): Promise<void>;
  /** Single-consumer event pump; ends when the session reaches a terminal status. */
  events(): AsyncIterable<LiveEvent>;
  /** Idempotent; resolves only after the process is reaped or its survival is honestly reported. */
  stop(mode: LiveStopMode): Promise<LiveStopReport>;
}

/** Result of probing the installed provider command; launching nothing is fine, guessing is not. */
export interface LiveProbeResult {
  found: boolean;
  /** Provider-reported version string; null when the command does not report one. */
  version: string | null;
  /** Human-readable bounded detail. */
  detail: string | null;
}

/**
 * Creates live transports. One factory serves exactly one transport/provider
 * pair; the hub validates `provider` pairing before `create` may be called.
 */
export interface LiveTransportFactory {
  readonly transport: LiveTransportId;
  readonly provider: LiveProviderId;
  /** Detects the installed provider command without launching it. */
  probe(): Promise<LiveProbeResult>;
  create(): LiveTransport;
}

/**
 * Provider-side factory: knows which transports the provider may run on and
 * picks among hub-validated candidates. Returning null is honest and
 * preferred over any fallback guess — no transport, no live session.
 */
export interface LiveProviderFactory {
  readonly provider: LiveProviderId;
  /** Accepted transport ids, preference order first. */
  readonly transports: readonly LiveTransportId[];
  /** Selects from factories whose `provider` already matched; null when none is usable. */
  selectTransport(factories: readonly LiveTransportFactory[]): LiveTransportFactory | null;
}
