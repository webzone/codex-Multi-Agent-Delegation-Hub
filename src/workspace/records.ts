import { AgentHubError } from "../errors.js";
import type { RepositoryIdentity } from "../types.js";
import {
  parseSessionRecord,
  type BoundedText,
  type CommandKind,
  type CommandOutcome,
  type KernelError,
  type SessionRecord,
  type SessionStatus,
  type Usage,
} from "../kernel/contracts.js";
import { isWorkspaceSessionId } from "./home.js";

/**
 * Durable record shapes for P2 workspace custody.
 *
 * Three persisted objects plus one sidecar:
 *
 *   - the custody record: which worktree/ref/head this session owns, its
 *     custody state (`live` → `closed`), and — only after an explicit exact
 *     handoff — the decision that arms the retention clock;
 *   - one result record per terminal turn, always: its `seq` is the turn's
 *     position in the result sequence, and `commit`/`tree`/`ref` are the
 *     exact identity of what the workspace looked like when the turn
 *     settled, whether or not the tree changed;
 *   - a runtime mirror file, the last `SessionRecord` the kernel committed,
 *     kept separate from custody so mirror traffic and custody transitions
 *     never contend on one file;
 *   - a transaction sidecar written before any ref move, so a hub that dies
 *     mid-transition leaves a provable intent that recovery can land or
 *     abandon — never guess.
 *
 * Every parser rebuilds from known keys: unknown keys are dropped, any
 * structural lie throws `WORKSPACE_RECORD_INVALID`.
 */

export const WORKSPACE_SCHEMA_VERSION = "agent-hub-workspace/v1" as const;
export const WORKSPACE_RESULT_SCHEMA_VERSION = "agent-hub-workspace-result/v1" as const;
export const WORKSPACE_RUNTIME_SCHEMA_VERSION = "agent-hub-workspace-runtime/v1" as const;
export const WORKSPACE_TRANSACTION_SCHEMA_VERSION = "agent-hub-workspace-transaction/v1" as const;

/** Default retention after an accepted handoff decision: 24 hours. */
export const DEFAULT_RETENTION_MS = 24 * 60 * 60 * 1000;

export const WORKSPACE_REF_NAMESPACE = "refs/agent-hub/workspace";

/** Full commit object name: exactly one SHA-1 (40) or SHA-256 (64) hex width. */
const COMMIT_PATTERN = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;

const SESSION_STATUSES: readonly SessionStatus[] = [
  "starting",
  "idle",
  "running",
  "cancelling",
  "closing",
  "closed",
  "error",
  "orphaned",
];

const TERMINAL_SESSION_STATUSES: readonly SessionStatus[] = ["closed", "error", "orphaned"];

export function isTerminalRuntimeStatus(status: SessionStatus | null): boolean {
  return status === null || TERMINAL_SESSION_STATUSES.includes(status);
}

/** Local mirror of the kernel's `ErrorStage` union for validating stored TurnResult errors. */
const KERNEL_ERROR_STAGES: readonly string[] = [
  "probe",
  "launch",
  "transport",
  "provider",
  "protocol",
  "capability",
  "state",
  "shutdown",
];

const COMMAND_KINDS: readonly string[] = ["prompt", "follow_up", "steer", "cancel", "status", "permission_response"];
const COMMAND_OUTCOMES: readonly string[] = ["succeeded", "failed", "cancelled", "unsupported"];
const CAPTURE_REASONS: readonly string[] = ["turn_end", "close"];

export function workspaceRefFor(sessionId: string): string {
  if (!isWorkspaceSessionId(sessionId)) {
    throw new AgentHubError(
      "WORKSPACE_ID_INVALID",
      `workspace session id "${sessionId}" must be a hub-generated UUID`,
    );
  }
  return `${WORKSPACE_REF_NAMESPACE}/${sessionId}`;
}

// ---------------------------------------------------------------------------
// Custody record
// ---------------------------------------------------------------------------

export type CustodyStatus = "live" | "closed";
export type HandoffDecision = "accepted" | "discarded";

export interface WorkspaceHandoff {
  decision: HandoffDecision;
  /** The exact result sequence position the consumer decided on. */
  result_seq: number;
  /** The exact head commit the consumer decided on. */
  commit: string;
  decided_at: string;
  consumer: string | null;
}

export interface WorkspaceRecord {
  schema: typeof WORKSPACE_SCHEMA_VERSION;
  session_id: string;
  /** Caller-facing agent label; display only, never a path or ref segment. */
  agent: string;
  provider: string | null;
  transport: string | null;
  /** Absolute path of the caller checkout the workspace was provisioned against. */
  repository_cwd: string;
  identity: RepositoryIdentity;
  base_commit: string;
  /** Absolute path of the hub-provisioned isolated worktree. */
  worktree_path: string;
  ref: string;
  /** Custody state. `closed` only via an explicit finalize; never implied by close-of-process. */
  custody: CustodyStatus;
  /** Last kernel status recorded by a custody transition (mirrors are kept separately). */
  runtime_status: SessionStatus | null;
  /** Commit the custody ref points (or would point) at; `base_commit` until first advance. */
  head_commit: string;
  head_tree: string;
  /** Highest published result sequence; 0 when no terminal turn has settled yet. */
  last_result_seq: number;
  handoff: WorkspaceHandoff | null;
  /** Set exactly when `handoff` is set; GC may act only at or after this instant. */
  retention_until: string | null;
  closed_at: string | null;
  /** Honest note on how closure was reached (kernel stop report or recovery proof). */
  close_evidence: string | null;
  last_error: { code: string; message: string } | null;
  /** Increments by exactly one per committed custody transition. */
  revision: number;
  created_at: string;
  updated_at: string;
}

// ---------------------------------------------------------------------------
// Result record — one per terminal turn, changed tree or not
// ---------------------------------------------------------------------------

export type CaptureReason = "turn_end" | "close";

export interface WorkspaceResultRecord {
  schema: typeof WORKSPACE_RESULT_SCHEMA_VERSION;
  session_id: string;
  /** 1-based, gapless result sequence for this session. */
  seq: number;
  command_id: string;
  kind: CommandKind;
  outcome: CommandOutcome;
  /** The commit this result's identity is: head after the turn, unchanged head when the tree did not change. */
  commit: string;
  parent: string;
  tree: string;
  /** The custody ref holding `commit` reachable. */
  ref: string;
  tree_changed: boolean;
  final_text: BoundedText | null;
  usage: Usage | null;
  started_at: string;
  finished_at: string;
  duration_ms: number;
  error: KernelError | null;
  recorded_at: string;
}

// ---------------------------------------------------------------------------
// Runtime mirror file (kernel's committed SessionRecord, custody-adjacent)
// ---------------------------------------------------------------------------

export interface WorkspaceRuntimeMirror {
  schema: typeof WORKSPACE_RUNTIME_SCHEMA_VERSION;
  mirrored_at: string;
  /** True when recovery rewrote the status (a dead hub cannot rewrite its own mirror). */
  rewritten_by_recovery: boolean;
  record: SessionRecord;
}

// ---------------------------------------------------------------------------
// Transaction sidecar (git-ref-visible transitions only)
// ---------------------------------------------------------------------------

export type TransactionKind = "result" | "close-capture";
export type TransactionPhase = "intent" | "captured";

export interface WorkspaceTransaction {
  schema: typeof WORKSPACE_TRANSACTION_SCHEMA_VERSION;
  session_id: string;
  kind: TransactionKind;
  reason: CaptureReason;
  /** Result sequence being published; null for close captures. */
  seq: number | null;
  command_id: string | null;
  ref: string;
  /** Ref value the plan was built against (null = ref absent). */
  expected_ref: string | null;
  expected_revision: number;
  capture_phase: TransactionPhase;
  /** Only set at `captured`: the commit produced before the CAS. */
  new_commit: string | null;
  tree: string | null;
  next_record: WorkspaceRecord | null;
  result: WorkspaceResultRecord | null;
  prepared_at: string;
}

// ---------------------------------------------------------------------------
// Parsers
// ---------------------------------------------------------------------------

function invalid(detail: string): never {
  throw new AgentHubError("WORKSPACE_RECORD_INVALID", `workspace record is invalid: ${detail}`);
}

function str(value: unknown, field: string): string {
  if (typeof value !== "string" || value.length === 0) {
    invalid(`${field} must be a non-empty string`);
  }
  return value;
}

function absolute(value: unknown, field: string): string {
  const s = str(value, field);
  if (!s.startsWith("/")) {
    invalid(`${field} must be an absolute path`);
  }
  return s;
}

function commit(value: unknown, field: string): string {
  const s = str(value, field);
  if (!COMMIT_PATTERN.test(s)) {
    invalid(`${field} must be a full-width commit object name`);
  }
  return s;
}

function nullableCommit(value: unknown, field: string): string | null {
  return value === null ? null : commit(value, field);
}

function int(value: unknown, field: string, min: number): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < min) {
    invalid(`${field} must be an integer >= ${min}`);
  }
  return value;
}

function oneOf<T extends string>(value: unknown, field: string, allowed: readonly T[]): T {
  if (typeof value !== "string" || !(allowed as readonly string[]).includes(value)) {
    invalid(`${field} ${JSON.stringify(value) ?? "undefined"} is not one of ${allowed.join(", ")}`);
  }
  return value as T;
}

function nullableString(value: unknown, field: string): string | null {
  if (value === null || value === undefined) {
    return null;
  }
  if (typeof value !== "string") {
    invalid(`${field} must be a string or null`);
  }
  return value;
}

function plainObject(value: unknown, field: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    invalid(`${field} must be a plain object`);
  }
  return value as Record<string, unknown>;
}

function parseIdentity(value: unknown): RepositoryIdentity {
  const v = plainObject(value, "identity");
  return {
    common_dir: absolute(v.common_dir, "identity.common_dir"),
    worktree_root: absolute(v.worktree_root, "identity.worktree_root"),
    branch: nullableString(v.branch, "identity.branch"),
    head: commit(v.head, "identity.head"),
  };
}

function parseBoundedText(value: unknown, field: string): BoundedText {
  const v = plainObject(value, field);
  if (typeof v.text !== "string") {
    invalid(`${field}.text must be a string`);
  }
  if (typeof v.truncated !== "boolean") {
    invalid(`${field}.truncated must be a boolean`);
  }
  return { text: v.text, truncated: v.truncated };
}

function parseUsage(value: unknown): Usage | null {
  if (value === null || value === undefined) {
    return null;
  }
  const v = plainObject(value, "usage");
  const counter = (key: string): number | null => {
    const c = v[key];
    if (c === null) {
      return null;
    }
    if (typeof c !== "number" || !Number.isFinite(c)) {
      invalid(`usage.${key} must be a finite number or null`);
    }
    return c;
  };
  return {
    input_tokens: counter("input_tokens"),
    output_tokens: counter("output_tokens"),
    cached_tokens: counter("cached_tokens"),
    cost_usd: counter("cost_usd"),
  };
}

function parseKernelError(value: unknown): KernelError | null {
  if (value === null || value === undefined) {
    return null;
  }
  const v = plainObject(value, "error");
  const provider = nullableString(v.provider, "error.provider");
  if (provider === null && v.provider !== null && v.provider !== undefined) {
    invalid("error.provider must be a string or null");
  }
  return {
    code: str(v.code, "error.code"),
    message: str(v.message, "error.message"),
    stage: oneOf(v.stage, "error.stage", KERNEL_ERROR_STAGES) as KernelError["stage"],
    retryable: typeof v.retryable === "boolean" ? v.retryable : invalid("error.retryable must be a boolean"),
    provider,
  };
}

function parseHandoff(value: unknown): WorkspaceHandoff | null {
  if (value === null || value === undefined) {
    return null;
  }
  const v = plainObject(value, "handoff");
  return {
    decision: oneOf(v.decision, "handoff.decision", ["accepted", "discarded"] as const),
    result_seq: int(v.result_seq, "handoff.result_seq", 0),
    commit: commit(v.commit, "handoff.commit"),
    decided_at: str(v.decided_at, "handoff.decided_at"),
    consumer: nullableString(v.consumer, "handoff.consumer"),
  };
}

export function parseWorkspaceRecord(value: unknown): WorkspaceRecord {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    invalid("custody record must be a plain object");
  }
  const v = value as Record<string, unknown>;
  if (v.schema !== WORKSPACE_SCHEMA_VERSION) {
    invalid(`schema must be "${WORKSPACE_SCHEMA_VERSION}"`);
  }
  const session = str(v.session_id, "session_id");
  if (!isWorkspaceSessionId(session)) {
    invalid("session_id must be a hub-generated UUID");
  }
  const custody = oneOf(v.custody, "custody", ["live", "closed"] as const);
  const runtimeStatus =
    v.runtime_status === null || v.runtime_status === undefined
      ? null
      : oneOf(v.runtime_status, "runtime_status", SESSION_STATUSES);
  const handoff = parseHandoff(v.handoff);
  const retentionUntil = nullableString(v.retention_until, "retention_until");
  if (handoff !== null && (custody !== "closed" || retentionUntil === null)) {
    invalid("a handoff decision requires closed custody and an armed retention deadline");
  }
  let lastError: { code: string; message: string } | null = null;
  if (v.last_error !== null && v.last_error !== undefined) {
    const e = plainObject(v.last_error, "last_error");
    lastError = { code: str(e.code, "last_error.code"), message: str(e.message, "last_error.message") };
  }
  return {
    schema: WORKSPACE_SCHEMA_VERSION,
    session_id: session,
    agent: str(v.agent, "agent"),
    provider: nullableString(v.provider, "provider"),
    transport: nullableString(v.transport, "transport"),
    repository_cwd: absolute(v.repository_cwd, "repository_cwd"),
    identity: parseIdentity(v.identity),
    base_commit: commit(v.base_commit, "base_commit"),
    worktree_path: absolute(v.worktree_path, "worktree_path"),
    ref: workspaceRefFor(session),
    custody,
    runtime_status: runtimeStatus,
    head_commit: commit(v.head_commit, "head_commit"),
    head_tree: commit(v.head_tree, "head_tree"),
    last_result_seq: int(v.last_result_seq, "last_result_seq", 0),
    handoff,
    retention_until: retentionUntil,
    closed_at: nullableString(v.closed_at, "closed_at"),
    close_evidence: nullableString(v.close_evidence, "close_evidence"),
    last_error: lastError,
    revision: int(v.revision, "revision", 0),
    created_at: str(v.created_at, "created_at"),
    updated_at: str(v.updated_at, "updated_at"),
  };
}

export function parseResultRecord(value: unknown): WorkspaceResultRecord {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    invalid("result record must be a plain object");
  }
  const v = value as Record<string, unknown>;
  if (v.schema !== WORKSPACE_RESULT_SCHEMA_VERSION) {
    invalid(`schema must be "${WORKSPACE_RESULT_SCHEMA_VERSION}"`);
  }
  const session = str(v.session_id, "session_id");
  if (!isWorkspaceSessionId(session)) {
    invalid("session_id must be a hub-generated UUID");
  }
  return {
    schema: WORKSPACE_RESULT_SCHEMA_VERSION,
    session_id: session,
    seq: int(v.seq, "seq", 1),
    command_id: str(v.command_id, "command_id"),
    kind: oneOf(v.kind, "kind", COMMAND_KINDS) as CommandKind,
    outcome: oneOf(v.outcome, "outcome", COMMAND_OUTCOMES) as CommandOutcome,
    commit: commit(v.commit, "commit"),
    parent: commit(v.parent, "parent"),
    tree: commit(v.tree, "tree"),
    ref: str(v.ref, "ref"),
    tree_changed: typeof v.tree_changed === "boolean" ? v.tree_changed : invalid("tree_changed must be a boolean"),
    final_text: v.final_text === null || v.final_text === undefined ? null : parseBoundedText(v.final_text, "final_text"),
    usage: parseUsage(v.usage),
    started_at: str(v.started_at, "started_at"),
    finished_at: str(v.finished_at, "finished_at"),
    duration_ms: int(v.duration_ms, "duration_ms", 0),
    error: parseKernelError(v.error),
    recorded_at: str(v.recorded_at, "recorded_at"),
  };
}

export function parseRuntimeMirror(value: unknown): WorkspaceRuntimeMirror {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    invalid("runtime mirror must be a plain object");
  }
  const v = value as Record<string, unknown>;
  if (v.schema !== WORKSPACE_RUNTIME_SCHEMA_VERSION) {
    invalid(`schema must be "${WORKSPACE_RUNTIME_SCHEMA_VERSION}"`);
  }
  return {
    schema: WORKSPACE_RUNTIME_SCHEMA_VERSION,
    mirrored_at: str(v.mirrored_at, "mirrored_at"),
    rewritten_by_recovery:
      typeof v.rewritten_by_recovery === "boolean" ? v.rewritten_by_recovery : false,
    record: parseSessionRecord(v.record),
  };
}

export function parseWorkspaceTransaction(value: unknown): WorkspaceTransaction {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    invalid("transaction sidecar must be a plain object");
  }
  const v = value as Record<string, unknown>;
  if (v.schema !== WORKSPACE_TRANSACTION_SCHEMA_VERSION) {
    invalid(`schema must be "${WORKSPACE_TRANSACTION_SCHEMA_VERSION}"`);
  }
  const session = str(v.session_id, "session_id");
  const kind = oneOf(v.kind, "kind", ["result", "close-capture"] as const);
  const phase = oneOf(v.phase === undefined ? v.capture_phase : v.phase, "capture_phase", ["intent", "captured"] as const);
  const seq = v.seq === null || v.seq === undefined ? null : int(v.seq, "seq", 1);
  if (kind === "result" && seq === null) {
    invalid("a result transaction must carry its result sequence");
  }
  if (kind === "close-capture" && seq !== null) {
    invalid("a close-capture transaction must not carry a result sequence");
  }
  const tx: WorkspaceTransaction = {
    schema: WORKSPACE_TRANSACTION_SCHEMA_VERSION,
    session_id: session,
    kind,
    reason: oneOf(v.reason, "reason", CAPTURE_REASONS) as CaptureReason,
    seq,
    command_id: nullableString(v.command_id, "command_id"),
    ref: workspaceRefFor(session),
    expected_ref: nullableCommit(v.expected_ref, "expected_ref"),
    expected_revision: int(v.expected_revision, "expected_revision", 0),
    capture_phase: phase,
    new_commit: nullableCommit(v.new_commit, "new_commit"),
    tree: nullableCommit(v.tree, "tree"),
    next_record: v.next_record === null || v.next_record === undefined ? null : parseWorkspaceRecord(v.next_record),
    result: v.result === null || v.result === undefined ? null : parseResultRecord(v.result),
    prepared_at: str(v.prepared_at, "prepared_at"),
  };
  if (tx.ref !== workspaceRefFor(tx.session_id)) {
    invalid("transaction ref does not belong to its session");
  }
  if (phase === "captured") {
    if (tx.new_commit === null || tx.tree === null || tx.next_record === null) {
      invalid("a captured transaction must carry its commit, tree, and next record");
    }
    if (tx.kind === "result" && tx.result === null) {
      invalid("a captured result transaction must carry its result record");
    }
    if (tx.kind === "close-capture" && tx.result !== null) {
      invalid("a close-capture transaction must not carry a result record");
    }
    if (tx.next_record!.session_id !== tx.session_id) {
      invalid("next_record belongs to a different session");
    }
    if (tx.next_record!.revision !== tx.expected_revision + 1) {
      invalid("next_record revision must be exactly one past the expected revision");
    }
    if (tx.next_record!.head_commit !== tx.new_commit || tx.next_record!.head_tree !== tx.tree) {
      invalid("next_record head must be the captured commit and tree");
    }
    if (tx.kind === "result" && (tx.result!.seq !== tx.seq || tx.result!.commit !== tx.new_commit)) {
      invalid("the result record must name the transaction's sequence and commit");
    }
  } else if (tx.new_commit !== null || tx.next_record !== null || tx.result !== null) {
    invalid("an intent-phase transaction must not carry captured artifacts");
  }
  return tx;
}
