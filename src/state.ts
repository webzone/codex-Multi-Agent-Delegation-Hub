import { randomUUID } from "node:crypto";
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { isAbsolute, join } from "node:path";

import { AgentHubError, asDelegateError } from "./errors.js";
import { runGit } from "./git.js";
import { acquireRepositoryLock, type RepositoryLock } from "./locks.js";
import { runProcess } from "./process.js";
import type { DelegateError, RepositoryIdentity } from "./types.js";

/**
 * Durable Hub-session state (Package 3).
 *
 * Layout — everything lives under `<absolute-git-common-dir>/agent-hub/sessions`
 * (inside `.git/`), so state is repository-local metadata shared by every
 * linked worktree and can never appear as an untracked file in any checkout.
 * For each generated session id (a UUID; never raw user text):
 *
 *   <common>/agent-hub/sessions/<id>.json           state record
 *   <common>/agent-hub/sessions/<id>.pending.json   pending transaction sidecar
 *   refs/agent-hub/sessions/<id>                    private artifact ref
 *   <common>/agent-hub/locks/session-<id>.lock      per-session exclusive lock
 *
 * The state record holds only minimal serializable metadata — repository
 * identity, agent, base/current artifact commits, provider session id (when
 * the adapter reported one), timestamps, revision and continuation mode. Task
 * text, stdout/stderr and diffs stay in transient results and are never
 * persisted.
 *
 * A state transition (create, or advance on resume) is committed with a
 * sidecar-guarded, ordered protocol:
 *
 *   1. the new artifact commit already exists as a Git object (commit-tree is durable),
 *   2. write the pending sidecar recording the full intended post-state,
 *   3. CAS `git update-ref <ref> <new> <expected-old>` (an all-zero OID of the
 *      repository's own hash width = "must not exist yet"),
 *   4. atomic JSON write of the state record,
 *   5. remove the sidecar.
 *
 * On load, a surviving sidecar is replayed only when ref + state files match
 * one of the two provable outcomes (transition landed, or never landed);
 * anything else raises SESSION_STATE_INCONSISTENT instead of guessing. All
 * load/recovery/transition work must run under the per-session lock.
 */

export const SESSIONS_SUBDIR = join("agent-hub", "sessions");
export const SESSION_REF_NAMESPACE = "refs/agent-hub/sessions";
export const SESSION_SCHEMA_VERSION = 1;

const SESSION_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
/** A full object name: exactly one SHA-1 (40) or SHA-256 (64) hex width —
 *  nothing in between is a commit id, and accepting it would let a truncated
 *  or padded id through into a CAS comparison. */
const COMMIT_PATTERN = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;
const STATE_SUFFIX = ".json";
const PENDING_SUFFIX = ".pending.json";

export type ContinuationMode = "filesystem" | "native";

export interface SessionState {
  schema: 1;
  session_id: string;
  agent: string;
  /** Private ref derived from the generated id. */
  ref: string;
  /** Repository identity captured when the session was created. */
  identity: RepositoryIdentity;
  /** Head the lineage started from. */
  base_commit: string;
  /** Artifact commit the session ref currently points at. */
  current_commit: string;
  provider_session_id: string | null;
  /** Continuation actually used by the most recent run. */
  continuation_mode: ContinuationMode;
  /** Increments by exactly one per committed transition. */
  revision: number;
  created_at: string;
  updated_at: string;
}

export type TransitionKind = "create" | "advance";

/** Intent record written as the sidecar before the ref CAS. */
export interface SessionTransaction {
  kind: TransitionKind;
  session_id: string;
  ref: string;
  /** SHA the ref must currently hold; null means "must not exist" (create). */
  expected_ref: string | null;
  new_commit: string;
  /** Full state record that must hold once the ref points at `new_commit`. */
  next_state: SessionState;
}

export type TransitionPhase = "sidecar-written" | "ref-updated";

export interface SessionTransitionContext {
  /** Absolute Git common dir (see `sessionsRoot`). */
  commonDir: string;
  /** Any path where git commands may run for this repository. */
  repositoryCwd: string;
  /**
   * Test seam: invoked at durable boundaries; a throw here simulates a crash
   * at exactly that point without touching git or the filesystem directly.
   */
  observePhase?: (phase: TransitionPhase) => Promise<void>;
}

export interface SessionLoadContext {
  commonDir: string;
  repositoryCwd: string;
  sessionId: string;
}

export interface SessionLockContext {
  commonDir: string;
  sessionId: string;
  /** Total busy-wait window; default 0 so contention fails fast. */
  waitMs?: number;
  acquireLock?: typeof acquireRepositoryLock;
}

// ---------------------------------------------------------------------------
// Paths, ids, refs
// ---------------------------------------------------------------------------

/**
 * Resolve the state root from the *absolute* Git common dir reported by
 * `resolveRepositoryIdentity`. A relative common dir would let state follow
 * whichever cwd happened to ask, so it is rejected outright.
 */
export function sessionsRoot(commonDir: string): string {
  if (!isAbsolute(commonDir)) {
    throw new AgentHubError(
      "SESSION_STATE_ROOT_INVALID",
      `Session state requires an absolute Git common dir, got "${commonDir}"`,
    );
  }
  return join(commonDir, SESSIONS_SUBDIR);
}

/**
 * Session ids are UUIDs generated by the hub (or injected equivalents). Raw
 * user text never becomes a ref segment: any id that is not a generated-shape
 * UUID is refused before it can reach a path or ref.
 */
export function sessionRefFor(sessionId: string): string {
  if (!SESSION_ID_PATTERN.test(sessionId)) {
    throw new AgentHubError(
      "SESSION_ID_INVALID",
      `Session id "${sessionId}" is not a generated UUID; refusing to derive a ref path from it`,
    );
  }
  return `${SESSION_REF_NAMESPACE}/${sessionId}`;
}

export function newSessionId(): string {
  return randomUUID();
}

/**
 * The all-zero OID Git compares against when a ref must not exist yet. Its
 * width is the object-name width of the repository's hash algorithm, derived
 * from the commit being written — that commit's OID came from this very
 * repository — rather than a hard-coded 40: in a SHA-256 repository Git rejects
 * a 40-zero old value outright ("not a valid old SHA1"), so a fixed width
 * breaks every session create there.
 */
export function zeroOidFor(oid: string): string {
  if (!COMMIT_PATTERN.test(oid)) {
    throw new AgentHubError(
      "SESSION_STATE_INCONSISTENT",
      `"${oid}" is not a full commit id, so its zero OID width cannot be derived`,
    );
  }
  return "0".repeat(oid.length);
}

function sessionFileBase(commonDir: string, sessionId: string): string {
  sessionRefFor(sessionId); // Shape gate for every path derived below.
  return join(sessionsRoot(commonDir), sessionId);
}

export function sessionStatePath(commonDir: string, sessionId: string): string {
  return `${sessionFileBase(commonDir, sessionId)}${STATE_SUFFIX}`;
}

export function sessionPendingPath(commonDir: string, sessionId: string): string {
  return `${sessionFileBase(commonDir, sessionId)}${PENDING_SUFFIX}`;
}

export function sessionLockName(sessionId: string): string {
  sessionRefFor(sessionId);
  return `session-${sessionId}`;
}

// ---------------------------------------------------------------------------
// Per-session exclusive lock
// ---------------------------------------------------------------------------

/** Outcome of {@link withSessionLock}. */
export interface SessionLockOutcome<T> {
  /** Whatever the operation produced; the caller owns it even if release failed. */
  value: T;
  /** Set when the operation succeeded but the per-session lock could not be released. */
  releaseError: DelegateError | null;
}

/**
 * Run `operation` while holding the per-session lock (Package 1 lock record:
 * random token, pid, hostname, timestamp, dead-same-host-owner recovery — all
 * unchanged). Contention surfaces as SESSION_BUSY; nothing about the
 * underlying lock semantics is relaxed here.
 *
 * Same deliberate asymmetry as `claimUnderLock`: when the operation has
 * already durably committed (the session ref/state transition landed) and the
 * release then fails, the completed value is returned together with the
 * release error — converting that into a throw would orphan a committed
 * transition from its result while leaving the lock record wedged. An
 * operation failure still propagates, and a release failure on that path is
 * appended to the thrown error rather than dropped, because an unreleased
 * lock record is its own operational problem.
 */
export async function withSessionLock<T>(
  context: SessionLockContext,
  operation: () => Promise<T>,
): Promise<SessionLockOutcome<T>> {
  const acquire = context.acquireLock ?? acquireRepositoryLock;
  const name = sessionLockName(context.sessionId);

  let lock: RepositoryLock;
  try {
    lock = await acquire({ commonDir: context.commonDir, name, waitMs: context.waitMs ?? 0 });
  } catch (error) {
    throw new AgentHubError(
      "SESSION_BUSY",
      `Session "${context.sessionId}" cannot be used right now: ${
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
        `${failure.message} (and the session lock could not be released either: ${
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
        `${failure.message}; the session lock record still exists and needs recovery`,
      ),
    };
  }
}

// ---------------------------------------------------------------------------
// Records
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
 * Validate and normalize a candidate state record. Normalization rebuilds the
 * object in a fixed key order so that string equality doubles as record
 * equality during recovery.
 */
function parseSessionState(value: unknown): SessionState | null {
  if (!isPlainObject(value) || value.schema !== SESSION_SCHEMA_VERSION) {
    return null;
  }
  const sessionId = nonEmptyString(value.session_id);
  const agent = nonEmptyString(value.agent);
  const ref = nonEmptyString(value.ref);
  const identity = parseIdentity(value.identity);
  const baseCommit = commitString(value.base_commit);
  const currentCommit = commitString(value.current_commit);
  const provider =
    value.provider_session_id === null ? null : nonEmptyString(value.provider_session_id);
  const createdAt = nonEmptyString(value.created_at);
  const updatedAt = nonEmptyString(value.updated_at);
  const revision = value.revision;

  if (!sessionId || !agent || !ref || !identity || !baseCommit || !currentCommit) {
    return null;
  }
  if (!createdAt || !updatedAt) {
    return null;
  }
  if (!Number.isInteger(revision) || (revision as number) < 1) {
    return null;
  }
  if (value.continuation_mode !== "filesystem" && value.continuation_mode !== "native") {
    return null;
  }
  try {
    if (ref !== sessionRefFor(sessionId)) {
      return null;
    }
  } catch {
    return null;
  }

  return {
    schema: SESSION_SCHEMA_VERSION,
    session_id: sessionId,
    agent,
    ref,
    identity,
    base_commit: baseCommit,
    current_commit: currentCommit,
    provider_session_id: provider,
    continuation_mode: value.continuation_mode,
    revision: revision as number,
    created_at: createdAt,
    updated_at: updatedAt,
  };
}

function parseTransaction(value: unknown): SessionTransaction | null {
  if (!isPlainObject(value)) {
    return null;
  }
  if (value.kind !== "create" && value.kind !== "advance") {
    return null;
  }
  const sessionId = nonEmptyString(value.session_id);
  const ref = nonEmptyString(value.ref);
  const expected = value.expected_ref === null ? null : commitString(value.expected_ref);
  const newCommit = commitString(value.new_commit);
  const nextState = parseSessionState(value.next_state);
  if (!sessionId || !ref || !newCommit || !nextState) {
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
    session_id: sessionId,
    ref,
    expected_ref: expected,
    new_commit: newCommit,
    next_state: nextState,
  };
}

function validatePlan(plan: SessionTransaction): void {
  const transaction = parseTransaction(plan);
  if (!transaction) {
    throw new AgentHubError("SESSION_STATE_INCONSISTENT", "transition plan is not well-formed");
  }
  if (
    transaction.session_id !== plan.session_id
    || transaction.ref !== plan.ref
    || transaction.next_state.session_id !== plan.session_id
    || transaction.next_state.ref !== plan.ref
    || transaction.new_commit !== transaction.next_state.current_commit
  ) {
    throw new AgentHubError(
      "SESSION_STATE_INCONSISTENT",
      "transition plan disagrees with its own session identity or state record",
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

type StateRecord =
  | { status: "absent" }
  | { status: "corrupt" }
  | { status: "present"; state: SessionState };

async function readStateRecord(commonDir: string, sessionId: string): Promise<StateRecord> {
  const raw = await readJson(sessionStatePath(commonDir, sessionId));
  if (raw === undefined) {
    return { status: "absent" };
  }
  if (raw === null) {
    return { status: "corrupt" };
  }
  const state = parseSessionState(raw);
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
  throw new AgentHubError("SESSION_STATE_INCONSISTENT", message);
}

// ---------------------------------------------------------------------------
// Loading with sidecar recovery
// ---------------------------------------------------------------------------

/**
 * Replay a surviving sidecar. Returns the recovered (post-commit) state when
 * the ref proves the transition landed, or null when the ref proves it never
 * landed. Every other combination is an explicit inconsistency.
 *
 * Must run under the per-session lock.
 */
async function recoverPendingTransaction(
  commonDir: string,
  repositoryCwd: string,
  sessionId: string,
): Promise<SessionState | null> {
  const pendingPath = sessionPendingPath(commonDir, sessionId);
  const raw = await readJson(pendingPath);
  if (raw === undefined) {
    return null; // No transaction in flight.
  }
  const transaction = raw === null ? null : parseTransaction(raw);
  if (!transaction) {
    inconsistent(
      `session "${sessionId}" has a pending transaction sidecar that is corrupt or does not describe a provable transition`,
    );
  }
  if (transaction.session_id !== sessionId || transaction.ref !== sessionRefFor(sessionId)) {
    inconsistent(`pending transaction sidecar does not belong to session "${sessionId}"`);
  }

  const ref = await probeRef(repositoryCwd, transaction.ref);
  const record = await readStateRecord(commonDir, sessionId);

  if (ref === transaction.new_commit) {
    // The ref CAS succeeded: the ref plus this sidecar prove the intended
    // post-state. Anything else at the state path (absent, half-written,
    // corrupt) is non-authoritative and gets replaced by the proof.
    const settled = record.status === "present"
      && JSON.stringify(record.state) === JSON.stringify(transaction.next_state);
    if (!settled) {
      await writeJsonAtomic(sessionStatePath(commonDir, sessionId), transaction.next_state);
    }
    await rm(pendingPath, { force: true });
    return transaction.next_state;
  }

  const refMatchesOld = transaction.expected_ref === null
    ? ref === null
    : ref === transaction.expected_ref;
  if (refMatchesOld) {
    // The CAS never landed: state must still describe the old world exactly.
    const stateMatchesOld = transaction.kind === "create"
      ? record.status === "absent"
      : record.status === "present" && record.state.current_commit === transaction.expected_ref;
    if (!stateMatchesOld) {
      inconsistent(
        `pending transaction for session "${sessionId}" was aborted but the state record is not the pre-transition one`,
      );
    }
    await rm(pendingPath, { force: true });
    return null;
  }

  inconsistent(
    `session "${sessionId}" has a pending transaction expecting ref ${
      transaction.expected_ref ?? "(absent)"
    } -> ${transaction.new_commit}, but the ref points at ${ref ?? "(absent)"}`,
  );
}

/**
 * Load the authoritative state for a session, replaying a provable pending
 * transaction first. Throws SESSION_NOT_FOUND for unknown ids and
 * SESSION_STATE_INCONSISTENT for any unprovable divergence.
 *
 * Must run under the per-session lock.
 */
export async function loadSessionState(context: SessionLoadContext): Promise<SessionState> {
  const { commonDir, repositoryCwd, sessionId } = context;
  const ref = sessionRefFor(sessionId);

  // Provable pending transactions are settled first; unprovable ones throw.
  await recoverPendingTransaction(commonDir, repositoryCwd, sessionId);

  const record = await readStateRecord(commonDir, sessionId);
  if (record.status === "corrupt") {
    inconsistent(`session record for "${sessionId}" exists but is corrupt`);
  }
  if (record.status === "absent") {
    throw new AgentHubError("SESSION_NOT_FOUND", `No session "${sessionId}" in this repository`);
  }

  const actual = await probeRef(repositoryCwd, ref);
  if (actual !== record.state.current_commit) {
    inconsistent(
      `session "${sessionId}" ref points at ${actual ?? "(nothing)"} but state expects ${record.state.current_commit}`,
    );
  }
  return record.state;
}

// ---------------------------------------------------------------------------
// Transactional transition
// ---------------------------------------------------------------------------

/**
 * Commit a session transition under the protocol documented at the top of
 * this file. The caller must hold the per-session lock and must have loaded
 * (and thereby recovered) the current state first.
 */
export async function applySessionTransition(
  context: SessionTransitionContext,
  plan: SessionTransaction,
): Promise<void> {
  const { commonDir, repositoryCwd } = context;
  validatePlan(plan);

  const record = await readStateRecord(commonDir, plan.session_id);
  if (record.status === "corrupt") {
    inconsistent(`session record for "${plan.session_id}" is corrupt`);
  }

  if (plan.kind === "create") {
    if (plan.expected_ref !== null) {
      inconsistent("a create transition must expect an absent ref");
    }
    if (record.status === "present") {
      inconsistent(`cannot create session "${plan.session_id}": a state record already exists`);
    }
  } else {
    if (record.status !== "present") {
      inconsistent(`cannot advance unknown session "${plan.session_id}"`);
    }
    const current = record.state;
    if (
      current.current_commit !== plan.expected_ref
      || current.session_id !== plan.session_id
      || current.agent !== plan.next_state.agent
      || current.base_commit !== plan.next_state.base_commit
      || current.created_at !== plan.next_state.created_at
      || JSON.stringify(current.identity) !== JSON.stringify(plan.next_state.identity)
      || plan.next_state.revision !== current.revision + 1
    ) {
      inconsistent(`advance plan for session "${plan.session_id}" contradicts the stored state`);
    }
  }

  if ((await readJson(sessionPendingPath(commonDir, plan.session_id))) !== undefined) {
    inconsistent(`a pending transaction already exists for session "${plan.session_id}"`);
  }

  const observed = await probeRef(repositoryCwd, plan.ref);
  if (observed !== plan.expected_ref) {
    inconsistent(
      `session ref ${plan.ref} is at ${observed ?? "(nothing)"} but the plan expects ${
        plan.expected_ref ?? "(nothing)"
      }`,
    );
  }

  await writeJsonAtomic(sessionPendingPath(commonDir, plan.session_id), plan);
  if (context.observePhase) {
    await context.observePhase("sidecar-written");
  }

  // "Must not exist yet" is expressed at the repository's own hash width.
  const cas = plan.expected_ref ?? zeroOidFor(plan.new_commit);
  try {
    await runGit(repositoryCwd, ["update-ref", plan.ref, plan.new_commit, cas], 1000);
  } catch {
    // CAS rejection proves the ref diverged after our probe. Remove only the
    // sidecar we just wrote; the divergence itself stays on disk for audit.
    await rm(sessionPendingPath(commonDir, plan.session_id), { force: true });
    inconsistent(
      `session ref ${plan.ref} moved concurrently; refusing to advance from ${
        plan.expected_ref ?? "(nothing)"
      }`,
    );
  }
  if (context.observePhase) {
    await context.observePhase("ref-updated");
  }

  await writeJsonAtomic(sessionStatePath(commonDir, plan.session_id), plan.next_state);
  await rm(sessionPendingPath(commonDir, plan.session_id), { force: true });
}
