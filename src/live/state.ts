import { randomUUID } from "node:crypto";
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { isAbsolute, join } from "node:path";

import { asDelegateError, AgentHubError } from "../errors.js";
import { acquireRepositoryLock, type RepositoryLock } from "../locks.js";
import { runGit } from "../git.js";
import { runProcess } from "../process.js";
import { zeroOidFor } from "../state.js";
import type { RepositoryIdentity } from "../types.js";
import type {
  CheckpointReason,
  LiveCapabilities,
  LiveCapabilityClaim,
  LiveCapabilityName,
  LiveError,
  LiveErrorStage,
  LiveProviderId,
  LiveSessionState,
  LiveStatus,
  LiveTransportId,
  ProviderResumeState,
} from "./types.js";

/**
 * Durable live-session state (v3 live, Package 1).
 *
 * Layout — under the *absolute* Git common dir, in namespaces no v1/v2 code
 * ever reads (v1 fan-out refs, v2 `refs/agent-hub/sessions` and
 * `agent-hub/sessions` stay untouched and unread by this module):
 *
 *   <common>/agent-hub/live/sessions/<id>.json         state record (schema agent-hub-live/v1)
 *   <common>/agent-hub/live/sessions/<id>.pending.json pending transaction sidecar
 *   refs/agent-hub/live/<id>                     live checkpoint chain ref
 *   <common>/agent-hub/locks/live-<id>.lock      per-session short-op lock
 *   <common>/agent-hub/locks/live-admin.lock     create/launch worktree lock
 *   <common>/agent-hub/live/leases/…             lifetime leases (lease.ts)
 *
 * The record type is the containment guarantee: `LiveSessionState` has no
 * field that can hold task text, provider stdout, event bodies, or permission
 * summaries, and the parser below *rebuilds from known keys only* — unknown
 * keys in a file are dropped, and anything this hub writes was built by the
 * same rebuild, so string equality doubles as record equality during
 * recovery.
 *
 * The transition protocol is the v2-proven one, re-keyed onto the live ref:
 *
 *   1. the checkpoint commit already exists as a Git object,
 *   2. write the pending sidecar recording the full intended post-state,
 *   3. CAS `git update-ref <live ref> <new> <expected-old>`
 *      (all-zero OID at the repository's own hash width = "must not exist"),
 *   4. atomic JSON write of the state record,
 *   5. remove the sidecar.
 *
 * A surviving sidecar is replayed only when ref + state prove one of the two
 * outcomes (landed / never landed); anything else raises
 * LIVE_STATE_INCONSISTENT instead of guessing. All load/recovery/transition
 * work must run under the per-session live lock.
 */

export const LIVE_STATE_SUBDIR = join("agent-hub", "live");
export const LIVE_SESSIONS_SUBDIR = join("agent-hub", "live", "sessions");
export const LIVE_REF_NAMESPACE = "refs/agent-hub/live";
export const LIVE_SCHEMA_VERSION = "agent-hub-live/v1" as const;
export const LIVE_ADMIN_LOCK_NAME = "live-admin";

const LIVE_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
/** Full object name: exactly one SHA-1 (40) or SHA-256 (64) hex width. */
const COMMIT_PATTERN = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;
const STATE_SUFFIX = ".json";
const PENDING_SUFFIX = ".pending.json";

const PROVIDER_IDS: readonly LiveProviderId[] = ["omp", "agy", "pi", "hermes"];
const TRANSPORT_IDS: readonly LiveTransportId[] = ["omp-rpc", "agy-stream-json", "pi-rpc", "hermes-acp"];
/** 1:1 transport ↔ provider pairing from the Gate 0 contract. */
const TRANSPORT_PROVIDER: Record<LiveTransportId, LiveProviderId> = {
  "omp-rpc": "omp",
  "agy-stream-json": "agy",
  "pi-rpc": "pi",
  "hermes-acp": "hermes",
};
const STATUS_VALUES: readonly LiveStatus[] = [
  "starting",
  "idle",
  "running",
  "cancelling",
  "closing",
  "closed",
  "error",
  "orphaned",
];
const ERROR_STAGES: readonly LiveErrorStage[] = [
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
const CAPABILITY_NAMES: readonly LiveCapabilityName[] = [
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

const SUPPORT_VALUES = ["native", "hub-queued", "derived", "signal", "unsupported"] as const;

const CHECKPOINT_REASONS: readonly CheckpointReason[] = [
  "turn_end",
  "requested",
  "cancel",
  "close",
  "error",
  "crash_recovery",
];

export type LiveTransitionKind = "create" | "advance";

/** Intent record written as the sidecar before the live-ref CAS. */
export interface LiveTransaction {
  kind: LiveTransitionKind;
  live_session_id: string;
  ref: string;
  /** Commit the live ref must currently hold; null means "must not exist". */
  expected_ref: string | null;
  new_commit: string;
  /** Full state record that must hold once the ref points at `new_commit`. */
  next_state: LiveSessionState;
}

export type LiveTransitionPhase = "sidecar-written" | "ref-updated";

export interface LiveTransitionContext {
  commonDir: string;
  repositoryCwd: string;
  /** Test seam: throws simulate a crash at exactly this durable boundary. */
  observePhase?: (phase: LiveTransitionPhase) => Promise<void>;
}

export interface LiveLoadContext {
  commonDir: string;
  repositoryCwd: string;
  liveSessionId: string;
}

export interface LiveLockContext {
  commonDir: string;
  liveSessionId: string;
  /** Total busy-wait window; default 0 so contention fails fast. */
  waitMs?: number;
  acquireLock?: typeof acquireRepositoryLock;
}

// ---------------------------------------------------------------------------
// Paths, ids, refs
// ---------------------------------------------------------------------------

export function liveStateRoot(commonDir: string): string {
  if (!isAbsolute(commonDir)) {
    throw new AgentHubError(
      "LIVE_STATE_ROOT_INVALID",
      `Live state requires an absolute Git common dir, got "${commonDir}"`,
    );
  }
  return join(commonDir, LIVE_STATE_SUBDIR);
}

/** Live ids are hub-generated UUIDs; raw user text can never reach a path/ref. */
export function liveRefFor(liveSessionId: string): string {
  if (!LIVE_ID_PATTERN.test(liveSessionId)) {
    throw new AgentHubError(
      "LIVE_SESSION_ID_INVALID",
      `Live session id "${liveSessionId}" is not a generated UUID; refusing to derive a ref path from it`,
    );
  }
  return `${LIVE_REF_NAMESPACE}/${liveSessionId}`;
}

export function newLiveSessionId(): string {
  return randomUUID();
}

function liveFileBase(commonDir: string, liveSessionId: string): string {
  liveRefFor(liveSessionId); // Shape gate for every path derived below.
  return join(liveStateRoot(commonDir), "sessions", liveSessionId);
}

export function liveStatePath(commonDir: string, liveSessionId: string): string {
  return `${liveFileBase(commonDir, liveSessionId)}${STATE_SUFFIX}`;
}

export function livePendingPath(commonDir: string, liveSessionId: string): string {
  return `${liveFileBase(commonDir, liveSessionId)}${PENDING_SUFFIX}`;
}

export function liveLockName(liveSessionId: string): string {
  liveRefFor(liveSessionId);
  return `live-${liveSessionId}`;
}

// ---------------------------------------------------------------------------
// Per-session exclusive lock (short operation; the lease in lease.ts is the
// lifetime ownership record — deliberately two different mechanisms)
// ---------------------------------------------------------------------------

export interface LiveLockOutcome<T> {
  value: T;
  releaseError: { code: string; message: string } | null;
}

export async function withLiveLock<T>(
  context: LiveLockContext,
  operation: () => Promise<T>,
): Promise<LiveLockOutcome<T>> {
  const acquire = context.acquireLock ?? acquireRepositoryLock;
  const name = liveLockName(context.liveSessionId);

  let lock: RepositoryLock;
  try {
    lock = await acquire({
      commonDir: context.commonDir,
      name,
      waitMs: context.waitMs ?? 0,
    });
  } catch (error) {
    throw new AgentHubError(
      "LIVE_SESSION_BUSY",
      `Live session "${context.liveSessionId}" cannot be used right now: ${
        error instanceof Error ? error.message : String(error)
      }`,
    );
  }

  let value: T;
  try {
    value = await operation();
  } catch (error) {
    try {
      await lock.release();
    } catch (releaseError) {
      const failure = asDelegateError(error);
      throw new AgentHubError(
        failure.code,
        `${failure.message} (and the live session lock could not be released either: ${
          asDelegateError(releaseError).message
        })`,
      );
    }
    throw error;
  }

  try {
    await lock.release();
    return { value, releaseError: null };
  } catch (releaseError) {
    const failure = asDelegateError(releaseError);
    return {
      value,
      releaseError: new AgentHubError(
        failure.code === "INTERNAL_ERROR" ? "LOCK_RELEASE_FAILED" : failure.code,
        `${failure.message}; the live session lock record still exists and needs recovery`,
      ),
    };
  }
}

// ---------------------------------------------------------------------------
// Validation (rebuild-from-known-keys normalization)
// ---------------------------------------------------------------------------

function isPlainObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function nonEmptyString(value: unknown): string | null {
  return typeof value === "string" && value.trim() ? value : null;
}

function commitString(value: unknown): string | null {
  return typeof value === "string" && COMMIT_PATTERN.test(value) ? value : null;
}

function nullableString(value: unknown): string | null | undefined {
  if (value === null) {
    return null;
  }
  return typeof value === "string" && value.trim() ? value : undefined;
}

function parseIdentity(value: unknown): RepositoryIdentity | null {
  if (!isPlainObject(value)) {
    return null;
  }
  const commonDir = nonEmptyString(value.common_dir);
  const worktreeRoot = nonEmptyString(value.worktree_root);
  const head = commitString(value.head);
  const branch = value.branch === null ? null : nonEmptyString(value.branch);
  if (!commonDir || !worktreeRoot || !head || (value.branch !== null && branch === null)) {
    return null;
  }
  return { common_dir: commonDir, worktree_root: worktreeRoot, branch, head };
}

/**
 * The honesty gate from the contract, enforced at the durability boundary:
 * a claim short of `unsupported` cannot be stored without evidence naming
 * what was verified, and an `unsupported` claim cannot smuggle text.
 */
function parseCapabilityClaim(value: unknown): LiveCapabilityClaim | null {
  if (!isPlainObject(value)) {
    return null;
  }
  if (!SUPPORT_VALUES.includes(value.support as (typeof SUPPORT_VALUES)[number])) {
    return null;
  }
  if (value.support === "unsupported") {
    return value.evidence === null ? { support: "unsupported", evidence: null } : null;
  }
  const evidence = nonEmptyString(value.evidence);
  if (evidence === null) {
    return null;
  }
  return {
    support: value.support as Exclude<(typeof SUPPORT_VALUES)[number], "unsupported">,
    evidence,
  };
}

function parseCapabilities(value: unknown): LiveCapabilities | null {
  if (!isPlainObject(value)) {
    return null;
  }
  const keys = Object.keys(value);
  if (keys.length !== CAPABILITY_NAMES.length) {
    return null;
  }
  const out: Partial<LiveCapabilities> = {};
  for (const name of CAPABILITY_NAMES) {
    if (!Object.hasOwn(value, name)) {
      return null;
    }
    const claim = parseCapabilityClaim(value[name]);
    if (claim === null) {
      return null;
    }
    out[name] = claim;
  }
  return out as LiveCapabilities;
}

function parseResume(value: unknown): ProviderResumeState | null | undefined {
  if (value === null) {
    return null;
  }
  if (!isPlainObject(value)) {
    return undefined;
  }
  const providerSessionId = nullableString(value.provider_session_id);
  if (providerSessionId === undefined) {
    return undefined;
  }
  if (typeof value.verified !== "boolean") {
    return undefined;
  }
  if (value.verified === true) {
    if (typeof value.verified_via !== "string" || !value.verified_via.trim()) {
      return undefined;
    }
  } else if (value.verified_via !== null) {
    return undefined;
  }
  const verification =
    value.verified === true
      ? ({ verified: true, verified_via: value.verified_via as string } as const)
      : ({ verified: false, verified_via: null } as const);

  switch (value.provider) {
    case "omp": {
      if (!Number.isInteger(value.last_event_seq) || (value.last_event_seq as number) < 0) {
        return undefined;
      }
      return {
        provider: "omp",
        provider_session_id: providerSessionId,
        ...verification,
        last_event_seq: value.last_event_seq as number,
      };
    }
    case "agy": {
      if (typeof value.resume_argv_verified !== "boolean") {
        return undefined;
      }
      return {
        provider: "agy",
        provider_session_id: providerSessionId,
        ...verification,
        resume_argv_verified: value.resume_argv_verified,
      };
    }
    case "pi": {
      const token = nullableString(value.resume_token);
      if (token === undefined) {
        return undefined;
      }
      return {
        provider: "pi",
        provider_session_id: providerSessionId,
        ...verification,
        resume_token: token,
      };
    }
    case "hermes": {
      if (typeof value.session_load_advertised !== "boolean") {
        return undefined;
      }
      return {
        provider: "hermes",
        provider_session_id: providerSessionId,
        ...verification,
        session_load_advertised: value.session_load_advertised,
      };
    }
    default:
      return undefined;
  }
}

function parseLiveError(value: unknown): LiveError | null | undefined {
  if (value === null) {
    return null;
  }
  if (!isPlainObject(value)) {
    return undefined;
  }
  const code = nonEmptyString(value.code);
  const message = nonEmptyString(value.message);
  const stage = nonEmptyString(value.stage);
  if (!code || !message || !stage || !ERROR_STAGES.includes(stage as LiveErrorStage)) {
    return undefined;
  }
  if (typeof value.retryable !== "boolean") {
    return undefined;
  }
  if (value.provider !== null && !PROVIDER_IDS.includes(value.provider as LiveProviderId)) {
    return undefined;
  }
  return {
    code,
    message,
    stage: stage as LiveErrorStage,
    retryable: value.retryable,
    provider: value.provider as LiveProviderId | null,
  };
}

/**
 * Validate and normalize a durable live record. The rebuild drops unknown
 * keys and rewrites in fixed order; a stored record that survived a round
 * trip through here is byte-comparable with a freshly built one.
 */
export function parseLiveSessionState(value: unknown): LiveSessionState | null {
  if (!isPlainObject(value) || value.schema !== LIVE_SCHEMA_VERSION) {
    return null;
  }
  const liveSessionId = nonEmptyString(value.live_session_id);
  if (!liveSessionId || !LIVE_ID_PATTERN.test(liveSessionId)) {
    return null;
  }
  const sessionId = nullableString(value.session_id);
  if (sessionId === undefined || (sessionId !== null && !LIVE_ID_PATTERN.test(sessionId))) {
    return null;
  }
  if (!PROVIDER_IDS.includes(value.provider as LiveProviderId)) {
    return null;
  }
  if (!TRANSPORT_IDS.includes(value.transport as LiveTransportId)) {
    return null;
  }
  const provider = value.provider as LiveProviderId;
  const transport = value.transport as LiveTransportId;
  if (TRANSPORT_PROVIDER[transport] !== provider) {
    return null;
  }
  const capabilities = parseCapabilities(value.capabilities);
  if (capabilities === null) {
    return null;
  }
  const identity = parseIdentity(value.identity);
  const baseCommit = commitString(value.base_commit);
  const currentCommit = commitString(value.current_commit);
  if (!identity || !baseCommit || !currentCommit) {
    return null;
  }
  if (!Number.isInteger(value.checkpoint_seq) || (value.checkpoint_seq as number) < 0) {
    return null;
  }
  if ((value.checkpoint_seq as number) === 0 && currentCommit !== baseCommit) {
    return null;
  }
  const lastCheckpointReason =
    value.last_checkpoint_reason === null
      ? null
      : CHECKPOINT_REASONS.includes(value.last_checkpoint_reason as CheckpointReason)
        ? (value.last_checkpoint_reason as CheckpointReason)
        : undefined;
  if (lastCheckpointReason === undefined) {
    return null;
  }
  if ((value.checkpoint_seq as number) === 0 ? lastCheckpointReason !== null : lastCheckpointReason === null) {
    return null;
  }
  const worktreePath = nonEmptyString(value.worktree_path);
  const worktreeParent = nonEmptyString(value.worktree_parent);
  if (
    !worktreePath ||
    !worktreeParent ||
    !isAbsolute(worktreePath) ||
    !isAbsolute(worktreeParent) ||
    worktreePath === worktreeParent ||
    !worktreePath.startsWith(`${worktreeParent}/`)
  ) {
    return null;
  }
  const resume = parseResume(value.resume);
  if (resume === undefined) {
    return null;
  }
  if (!STATUS_VALUES.includes(value.status as LiveStatus)) {
    return null;
  }
  if (!Number.isInteger(value.revision) || (value.revision as number) < 1) {
    return null;
  }
  const lastError = parseLiveError(value.last_error);
  if (lastError === undefined) {
    return null;
  }
  const createdAt = nonEmptyString(value.created_at);
  const updatedAt = nonEmptyString(value.updated_at);
  if (!createdAt || !updatedAt) {
    return null;
  }

  return {
    schema: LIVE_SCHEMA_VERSION,
    live_session_id: liveSessionId,
    session_id: sessionId,
    provider,
    transport,
    capabilities,
    identity,
    base_commit: baseCommit,
    current_commit: currentCommit,
    checkpoint_seq: value.checkpoint_seq as number,
    last_checkpoint_reason: lastCheckpointReason,
    worktree_path: worktreePath,
    worktree_parent: worktreeParent,
    resume,
    status: value.status as LiveStatus,
    revision: value.revision as number,
    last_error: lastError,
    created_at: createdAt,
    updated_at: updatedAt,
  };
}

function parseLiveTransaction(value: unknown): LiveTransaction | null {
  if (!isPlainObject(value)) {
    return null;
  }
  if (value.kind !== "create" && value.kind !== "advance") {
    return null;
  }
  const liveSessionId = nonEmptyString(value.live_session_id);
  const ref = nonEmptyString(value.ref);
  const expected = value.expected_ref === null ? null : commitString(value.expected_ref);
  const newCommit = commitString(value.new_commit);
  const nextState = parseLiveSessionState(value.next_state);
  if (!liveSessionId || !ref || !newCommit || !nextState) {
    return null;
  }
  if (value.kind === "advance" && !expected) {
    return null;
  }
  if (value.kind === "create" && expected !== null) {
    return null;
  }
  return {
    kind: value.kind,
    live_session_id: liveSessionId,
    ref,
    expected_ref: expected,
    new_commit: newCommit,
    next_state: nextState,
  };
}

function validateLivePlan(plan: LiveTransaction): void {
  const transaction = parseLiveTransaction(plan);
  if (!transaction) {
    throw new AgentHubError("LIVE_STATE_INCONSISTENT", "live transition plan is not well-formed");
  }
  if (
    transaction.live_session_id !== plan.live_session_id
    || transaction.ref !== plan.ref
    || transaction.next_state.live_session_id !== plan.live_session_id
    || transaction.new_commit !== transaction.next_state.current_commit
    || transaction.next_state.revision < 1
  ) {
    throw new AgentHubError(
      "LIVE_STATE_INCONSISTENT",
      "live transition plan disagrees with its own session identity or state record",
    );
  }
}

// ---------------------------------------------------------------------------
// Filesystem primitives
// ---------------------------------------------------------------------------

/** undefined = file absent; null = file present but unreadable as JSON. */
async function readJson(path: string): Promise<unknown> {
  try {
    return JSON.parse(await readFile(path, "utf8"));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return undefined;
    }
    return null;
  }
}

async function writeJsonAtomic(path: string, value: unknown): Promise<void> {
  await mkdir(join(path, ".."), { recursive: true });
  const tempPath = `${path}.${process.pid}.${randomUUID()}.tmp`;
  await writeFile(tempPath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
  await rename(tempPath, path);
}

type LiveStateRecord =
  | { status: "absent" }
  | { status: "corrupt" }
  | { status: "present"; state: LiveSessionState };

async function readLiveStateRecord(commonDir: string, liveSessionId: string): Promise<LiveStateRecord> {
  const raw = await readJson(liveStatePath(commonDir, liveSessionId));
  if (raw === undefined) {
    return { status: "absent" };
  }
  if (raw === null) {
    return { status: "corrupt" };
  }
  const state = parseLiveSessionState(raw);
  return state ? { status: "present", state } : { status: "corrupt" };
}

async function probeRef(repositoryCwd: string, ref: string): Promise<string | null> {
  const result = await runProcess(
    "git",
    ["rev-parse", "--verify", "--quiet", ref],
    { cwd: repositoryCwd, maxOutputBytes: 1000 },
  );
  if (result.error) {
    throw new AgentHubError("GIT_COMMAND_FAILED", `git rev-parse failed: ${result.error}`);
  }
  const sha = result.stdout.trim();
  return result.exitCode === 0 && COMMIT_PATTERN.test(sha) ? sha : null;
}

function inconsistent(message: string): never {
  throw new AgentHubError("LIVE_STATE_INCONSISTENT", message);
}

// ---------------------------------------------------------------------------
// Loading with sidecar recovery
// ---------------------------------------------------------------------------

/**
 * Replay a surviving sidecar: returns the post-commit state when the ref
 * proves the transition landed, null when the ref proves it never landed.
 * Every other combination is an explicit inconsistency. Runs under lock.
 */
async function recoverPendingLiveTransaction(
  commonDir: string,
  repositoryCwd: string,
  liveSessionId: string,
): Promise<LiveSessionState | null> {
  const pendingPath = livePendingPath(commonDir, liveSessionId);
  const raw = await readJson(pendingPath);
  if (raw === undefined) {
    return null;
  }
  const transaction = raw === null ? null : parseLiveTransaction(raw);
  if (!transaction) {
    inconsistent(
      `live session "${liveSessionId}" has a pending transaction sidecar that is corrupt or does not describe a provable transition`,
    );
  }
  if (transaction.live_session_id !== liveSessionId || transaction.ref !== liveRefFor(liveSessionId)) {
    inconsistent(`pending transaction sidecar does not belong to live session "${liveSessionId}"`);
  }

  const ref = await probeRef(repositoryCwd, transaction.ref);
  const record = await readLiveStateRecord(commonDir, liveSessionId);

  if (ref === transaction.new_commit) {
    const settled =
      record.status === "present"
      && JSON.stringify(record.state) === JSON.stringify(transaction.next_state);
    if (!settled) {
      await writeJsonAtomic(liveStatePath(commonDir, liveSessionId), transaction.next_state);
    }
    await rm(pendingPath, { force: true });
    return transaction.next_state;
  }

  const refMatchesOld = transaction.expected_ref === null ? ref === null : ref === transaction.expected_ref;
  if (refMatchesOld) {
    const stateMatchesOld = transaction.kind === "create"
      ? record.status === "absent"
      : record.status === "present" && record.state.current_commit === transaction.expected_ref;
    if (!stateMatchesOld) {
      inconsistent(
        `pending transaction for live session "${liveSessionId}" was aborted but the state record is not the pre-transition one`,
      );
    }
    await rm(pendingPath, { force: true });
    return null;
  }

  inconsistent(
    `live session "${liveSessionId}" has a pending transaction expecting ref ${
      transaction.expected_ref ?? "(absent)"
    } -> ${transaction.new_commit}, but the ref points at ${ref ?? "(absent)"}`,
  );
}

/**
 * Load the authoritative live state, replaying a provable pending transaction
 * first. LIVE_SESSION_NOT_FOUND for unknown ids; LIVE_STATE_INCONSISTENT for
 * any unprovable divergence.
 */
export async function loadLiveState(context: LiveLoadContext): Promise<LiveSessionState> {
  const { commonDir, repositoryCwd, liveSessionId } = context;
  const ref = liveRefFor(liveSessionId);

  await recoverPendingLiveTransaction(commonDir, repositoryCwd, liveSessionId);

  const record = await readLiveStateRecord(commonDir, liveSessionId);
  if (record.status === "corrupt") {
    inconsistent(`live session record for "${liveSessionId}" exists but is corrupt`);
  }
  if (record.status === "absent") {
    throw new AgentHubError(
      "LIVE_SESSION_NOT_FOUND",
      `No live session "${liveSessionId}" in this repository`,
    );
  }

  const actual = await probeRef(repositoryCwd, ref);
  if (actual !== record.state.current_commit) {
    inconsistent(
      `live session "${liveSessionId}" ref points at ${actual ?? "(nothing)"} but state expects ${record.state.current_commit}`,
    );
  }
  return record.state;
}

// ---------------------------------------------------------------------------
// Transactional transition
// ---------------------------------------------------------------------------

/**
 * Commit a live transition: sidecar → live-ref CAS → state write → sidecar
 * removal. The caller must hold the per-session live lock and must have
 * loaded (and thereby recovered) the current state first.
 */
export async function applyLiveTransition(
  context: LiveTransitionContext,
  plan: LiveTransaction,
): Promise<void> {
  const { commonDir, repositoryCwd } = context;
  validateLivePlan(plan);

  const record = await readLiveStateRecord(commonDir, plan.live_session_id);
  if (record.status === "corrupt") {
    inconsistent(`live session record for "${plan.live_session_id}" is corrupt`);
  }

  if (plan.kind === "create") {
    if (plan.expected_ref !== null) {
      inconsistent("a live create transition must expect an absent ref");
    }
    if (record.status === "present") {
      inconsistent(`cannot create live session "${plan.live_session_id}": a state record already exists`);
    }
  } else {
    if (record.status !== "present") {
      inconsistent(`cannot advance unknown live session "${plan.live_session_id}"`);
    }
    const current = record.state;
    if (
      current.current_commit !== plan.expected_ref
      || current.live_session_id !== plan.live_session_id
      || current.provider !== plan.next_state.provider
      || current.transport !== plan.next_state.transport
      || current.base_commit !== plan.next_state.base_commit
      || current.created_at !== plan.next_state.created_at
      || current.session_id !== plan.next_state.session_id
      || JSON.stringify(current.identity) !== JSON.stringify(plan.next_state.identity)
      // Launch-captured capabilities are immutable for the record's lifetime.
      || JSON.stringify(current.capabilities) !== JSON.stringify(plan.next_state.capabilities)
      || plan.next_state.revision !== current.revision + 1
      || plan.next_state.checkpoint_seq < current.checkpoint_seq
      || (plan.next_state.checkpoint_seq === current.checkpoint_seq
        && plan.next_state.current_commit !== current.current_commit)
    ) {
      inconsistent(`advance plan for live session "${plan.live_session_id}" contradicts the stored state`);
    }
  }

  if ((await readJson(livePendingPath(commonDir, plan.live_session_id))) !== undefined) {
    inconsistent(`a pending transaction already exists for live session "${plan.live_session_id}"`);
  }

  const observed = await probeRef(repositoryCwd, plan.ref);
  if (observed !== plan.expected_ref) {
    inconsistent(
      `live session ref ${plan.ref} is at ${observed ?? "(nothing)"} but the plan expects ${
        plan.expected_ref ?? "(nothing)"
      }`,
    );
  }

  await writeJsonAtomic(livePendingPath(commonDir, plan.live_session_id), plan);
  if (context.observePhase) {
    await context.observePhase("sidecar-written");
  }

  const cas = plan.expected_ref ?? zeroOidFor(plan.new_commit);
  try {
    await runGit(repositoryCwd, ["update-ref", plan.ref, plan.new_commit, cas], 1000);
  } catch {
    // CAS rejection proves the ref diverged after our probe. Remove only the
    // sidecar we just wrote; the divergence itself stays on disk for audit.
    await rm(livePendingPath(commonDir, plan.live_session_id), { force: true });
    inconsistent(
      `live session ref ${plan.ref} moved concurrently; refusing to advance from ${
        plan.expected_ref ?? "(nothing)"
      }`,
    );
  }
  if (context.observePhase) {
    await context.observePhase("ref-updated");
  }

  await writeJsonAtomic(liveStatePath(commonDir, plan.live_session_id), plan.next_state);
  await rm(livePendingPath(commonDir, plan.live_session_id), { force: true });
}
