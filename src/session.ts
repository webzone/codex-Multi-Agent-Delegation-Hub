import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { resolveAdapter } from "./adapters/index.js";
import { asNativeResumeCapableAdapter, PROVIDER_SESSION_METADATA_KEY } from "./adapters/types.js";
import { ARTIFACT_IDENTITY_EMAIL, ARTIFACT_IDENTITY_NAME } from "./artifacts.js";
import { AgentHubError, asDelegateError } from "./errors.js";
import {
  createDelegateResult,
  DEFAULT_MAX_OUTPUT_BYTES,
  assertCleanUnlessAllowed,
  emptyAdapterResult,
} from "./execution.js";
import {
  createWorktreeAtBase,
  pruneWorktrees,
  removeWorktree,
  resolveRepositoryIdentity,
  runGit,
  type BaseWorktree,
} from "./git.js";
import { acquireRepositoryLock, claimUnderLock, type RepositoryLock } from "./locks.js";
import { WORKTREE_ADMIN_LOCK_NAME } from "./fanout.js";
import {
  applySessionTransition,
  loadSessionState,
  newSessionId,
  sessionRefFor,
  withSessionLock,
  type ContinuationMode,
  type SessionState,
} from "./state.js";
import type {
  AdapterExecutionResult,
  AdapterMetadata,
  AgentAdapter,
  DelegateError,
  DelegateRequest,
  DelegateResult,
  RepositoryIdentity,
} from "./types.js";

/**
 * Durable Hub sessions (Package 3) — not workflow persistence.
 *
 * A session is a linear lineage of hook-free artifact commits on a private
 * ref (`refs/agent-hub/sessions/<generated-id>`), plus a minimal metadata
 * record under the repository's Git common dir. Every create or resume:
 *
 *   1. takes the per-session exclusive lock (SESSION_BUSY on contention),
 *   2. materializes a *fresh* isolated worktree at the session's current
 *      artifact commit (never reusing a previous run's path),
 *   3. runs the adapter there,
 *   4. captures the resulting tree as the next artifact commit (parent = the
 *      current one, even when the agent changed nothing, so the lineage is
 *      always a pure chain of artifact commits),
 *   5. commits the state transition through the sidecar/CAS protocol in
 *      state.ts, then tears the worktree down.
 *
 * The filesystem continuation is guaranteed for every adapter; the agent's
 * file edits survive regardless of its exit status because they were already
 * committed into the artifact before any run-level error is reported.
 * Provider-native conversational resume is an optional capability — it is
 * only exercised when an adapter exposes `nativeResumeCapability` whose
 * `verify()` proves the installed command syntax (see adapters/types.ts).
 *
 * Lock ordering (never inverted): per-session lock → worktree-admin lock.
 */

/** Fixed constant message: never contains the task text or agent output. */
export const SESSION_ARTIFACT_COMMIT_MESSAGE = "Agent Hub session artifact";

const ADMIN_LOCK_WAIT_MS = 30_000;
const ADMIN_LOCK_RETRY_MS = 20;

export interface CreateSessionRequest {
  workspace: string;
  agent: string;
  task: string;
  allowDirty?: boolean;
  maxOutputBytes?: number;
}

export interface ResumeSessionRequest {
  workspace: string;
  /** Generated session id; anything else is refused before touching paths. */
  session_id: string;
  task: string;
  maxOutputBytes?: number;
}

export type NativeResumeStatus =
  | "used"
  | "adapter-incapable"
  | "no-provider-session"
  | "not-verified";

/**
 * Filesystem and native continuation are reported separately: `filesystem`
 * is the always-available worktree-at-artifact-commit path, `native` only
 * says whether provider conversation state was carried over.
 */
export interface SessionContinuation {
  filesystem: true;
  native: boolean;
  native_status: NativeResumeStatus;
}

export interface SessionArtifact {
  parent: string;
  commit: string;
  tree: string;
  changed_files: string[];
  diff: string;
  diff_truncated: boolean;
}

export interface SessionRunResult {
  /** The persisted state after the committed transition. */
  session: SessionState;
  /** This run mapped onto the v1 DelegateResult wire shape. */
  run: DelegateResult;
  continuation: SessionContinuation;
  artifact: SessionArtifact;
  /** The fresh worktree used for this run (already removed). */
  execution_workspace: string;
  /** Teardown trouble never masks a committed transition; it lands here. */
  cleanup_error: DelegateError | null;
}

export interface SessionDependencies {
  resolveAdapter?: (agent: string) => AgentAdapter;
  createWorktree?: (workspace: string, base: string) => Promise<BaseWorktree>;
  removeWorktree?: (workspace: string, worktree: BaseWorktree) => Promise<void>;
  pruneWorktrees?: (workspace: string) => Promise<void>;
  acquireAdminLock?: (commonDir: string) => Promise<RepositoryLock>;
  newSessionId?: () => string;
  now?: () => Date;
}

interface TurnContext {
  kind: "create" | "advance";
  workspace: string;
  commonDir: string;
  sessionId: string;
  agent: string;
  task: string;
  /** What the fresh worktree is pinned to: create base head or current artifact. */
  baseCommit: string;
  maxOutputBytes: number;
  identity: RepositoryIdentity;
  /** Stored state for advance runs; null for create. */
  prior: SessionState | null;
  now: () => Date;
  deps: SessionDependencies;
}

function assertMaxOutputBytes(value: number | undefined): void {
  if (value !== undefined && (!Number.isInteger(value) || value < 1)) {
    throw new AgentHubError("INVALID_CONFIGURATION", "maxOutputBytes must be a positive integer");
  }
}

function validateShared(workspace: string, task: string, maxOutputBytes?: number): void {
  if (!workspace?.trim()) {
    throw new AgentHubError("INVALID_WORKSPACE", "workspace must not be empty");
  }
  if (!task?.trim()) {
    throw new AgentHubError("INVALID_TASK", "task must not be empty");
  }
  assertMaxOutputBytes(maxOutputBytes);
}

/**
 * Create a durable session: identity capture, first agent run at HEAD, first
 * artifact commit, ref + state record creation under the session lock.
 */
export async function createSession(
  request: CreateSessionRequest,
  dependencies: SessionDependencies = {},
): Promise<SessionRunResult> {
  validateShared(request.workspace, request.task);
  if (!request.agent?.trim()) {
    throw new AgentHubError("INVALID_AGENT", "agent must not be empty");
  }

  await assertCleanUnlessAllowed(request.workspace, request.allowDirty);
  const identity = await resolveRepositoryIdentity(request.workspace);

  const newId = dependencies.newSessionId ?? newSessionId;
  const sessionId = newId();
  const ref = sessionRefFor(sessionId); // rejects any id that is not generated-shape

  return withSessionLock({ commonDir: identity.common_dir, sessionId }, async () => {
    return runSessionTurn({
      kind: "create",
      workspace: request.workspace,
      commonDir: identity.common_dir,
      sessionId,
      agent: request.agent,
      task: request.task,
      baseCommit: identity.head,
      maxOutputBytes: request.maxOutputBytes ?? DEFAULT_MAX_OUTPUT_BYTES,
      identity,
      prior: null,
      now: dependencies.now ?? (() => new Date()),
      deps: dependencies,
    }, ref);
  });
}

/**
 * Resume a session: load (with sidecar recovery) under the session lock, run
 * a fresh worktree at the *current* artifact commit, advance the ref by one
 * linear artifact commit.
 */
export async function resumeSession(
  request: ResumeSessionRequest,
  dependencies: SessionDependencies = {},
): Promise<SessionRunResult> {
  validateShared(request.workspace, request.task);
  const ref = sessionRefFor(request.session_id);

  // The caller checkout's dirtiness is irrelevant here: the session ref, not
  // the checkout, defines what the run starts from.
  const identity = await resolveRepositoryIdentity(request.workspace);

  return withSessionLock({ commonDir: identity.common_dir, sessionId: request.session_id }, async () => {
    const prior = await loadSessionState({
      commonDir: identity.common_dir,
      repositoryCwd: request.workspace,
      sessionId: request.session_id,
    });

    return runSessionTurn({
      kind: "advance",
      workspace: request.workspace,
      commonDir: identity.common_dir,
      sessionId: request.session_id,
      agent: prior.agent,
      task: request.task,
      baseCommit: prior.current_commit,
      maxOutputBytes: request.maxOutputBytes ?? DEFAULT_MAX_OUTPUT_BYTES,
      // Identity is refreshed per operation (paths may move); the lineage
      // keeps the creation-time identity stored in the state record.
      identity: prior.identity,
      prior,
      now: dependencies.now ?? (() => new Date()),
      deps: dependencies,
    }, ref);
  });
}

async function runSessionTurn(
  turn: TurnContext,
  ref: string,
): Promise<SessionRunResult> {
  const { deps, workspace, commonDir } = turn;
  const resolveAdapterFn = deps.resolveAdapter ?? resolveAdapter;
  const createWorktree = deps.createWorktree ?? createWorktreeAtBase;
  const removeWorktreeFn = deps.removeWorktree ?? removeWorktree;
  const pruneFn = deps.pruneWorktrees ?? pruneWorktrees;
  const acquireAdminLock =
    deps.acquireAdminLock ??
    ((dir: string) =>
      acquireRepositoryLock({
        commonDir: dir,
        name: WORKTREE_ADMIN_LOCK_NAME,
        waitMs: ADMIN_LOCK_WAIT_MS,
        retryDelayMs: ADMIN_LOCK_RETRY_MS,
      }));

  async function withAdminLock<T>(operation: () => Promise<T>): Promise<T> {
    const lock = await acquireAdminLock(commonDir);
    try {
      return await operation();
    } finally {
      await lock.release();
    }
  }

  const adapter = resolveAdapterFn(turn.agent);
  const continuation = await decideContinuation(adapter, turn.prior);

  let adapterResult = emptyAdapterResult();
  let runError: DelegateError | null = null;
  let worktree: BaseWorktree | undefined;
  let result: SessionRunResult | undefined;
  /** Session worktree that was created but whose admin lock could not be released. */
  let adminReleaseError: DelegateError | null = null;

  const delegateRequest: DelegateRequest = {
    task: turn.task,
    agent: turn.agent,
    mode: "isolated",
    workspace,
    allowDirty: true, // The session ref, not the checkout, is the baseline.
    maxOutputBytes: turn.maxOutputBytes,
  };

  const startedAt = turn.now();
  try {
    // Fresh worktree at the session artifact commit, every single time.
    // Claiming it and releasing the admin lock are separate facts: a release
    // failure must not cost the session its handle, because without the handle
    // the `finally` teardown below cannot even be attempted, and the reported
    // cleanup error would not know which worktree it is talking about.
    const claim = await claimUnderLock(
      () => acquireAdminLock(commonDir),
      () => createWorktree(workspace, turn.baseCommit),
      (worktree) => worktree.path,
    );
    worktree = claim.value;
    adminReleaseError = claim.releaseError ? asDelegateError(claim.releaseError) : null;
    try {
      adapterResult = await adapter.execute({
        task: turn.task,
        cwd: worktree.path,
        maxOutputBytes: turn.maxOutputBytes,
        metadata: continuation.metadata,
      });
    } catch (error) {
      // Contained: partial filesystem edits are still captured below.
      runError = asDelegateError(error);
    }
    const finishedAt = turn.now();

    // Capture and commit the artifact *before* teardown and regardless of the
    // adapter's exit status: the filesystem continuation must survive a failed
    // run, so the artifact is durable even when the run reports an error.
    const artifact = await captureSessionArtifact(worktree.path, turn.baseCommit, turn.maxOutputBytes);

    const providerId = reportedProviderSessionId(adapterResult);
    const state = buildNextState(turn, ref, artifact.commit, providerId, continuation.native);
    await applySessionTransition(
      { commonDir, repositoryCwd: workspace },
      {
        kind: turn.kind,
        session_id: turn.sessionId,
        ref,
        expected_ref: turn.kind === "create" ? null : turn.prior?.current_commit ?? null,
        new_commit: artifact.commit,
        next_state: state,
      },
    );

    const run = createDelegateResult(
      delegateRequest,
      startedAt,
      finishedAt,
      {
        adapterResult,
        executionWorkspace: worktree.path,
        snapshot: {
          changedFiles: artifact.changed_files,
          diff: artifact.diff,
          diffTruncated: artifact.diff_truncated,
        },
      },
      runError,
    );

    result = {
      session: state,
      run,
      continuation: {
        filesystem: true,
        native: continuation.native,
        native_status: continuation.status,
      },
      artifact,
      execution_workspace: worktree.path,
      cleanup_error: adminReleaseError,
    };
  } finally {
    if (worktree) {
      try {
        // Serialize worktree administration with everything else in this
        // repository (fan-out included), prune once, then drop the directory.
        await withAdminLock(async () => {
          await removeWorktreeFn(workspace, worktree as BaseWorktree);
          await pruneFn(workspace);
        });
      } catch (error) {
        if (result) {
          const trouble = asDelegateError(error);
          // Never displace an earlier cleanup problem; report both.
          result.cleanup_error = result.cleanup_error
            ? {
                code: result.cleanup_error.code,
                message: `${result.cleanup_error.message} Teardown also failed: ${trouble.message}`,
              }
            : trouble;
        }
        // Pre-transition failures must not be masked by teardown noise.
      }
    }
  }

  // Unreachable: every pre-assignment failure propagates through `finally`.
  if (!result) {
    throw new AgentHubError("INTERNAL_ERROR", "session turn ended without a result");
  }
  return result;
}

interface ContinuationDecision {
  metadata: AdapterMetadata | null;
  native: boolean;
  status: NativeResumeStatus;
}

/**
 * Filesystem continuation is unconditional. Native resume is only taken when
 * the adapter exposes the optional capability AND its verify() proves the
 * installed syntax AND a provider session id was reported earlier — otherwise
 * the request metadata stays empty and argv is untouched.
 */
async function decideContinuation(
  adapter: AgentAdapter,
  prior: SessionState | null,
): Promise<ContinuationDecision> {
  const incapable: ContinuationDecision = {
    metadata: null,
    native: false,
    status: "adapter-incapable",
  };

  if (!prior || !prior.provider_session_id) {
    return { metadata: null, native: false, status: "no-provider-session" };
  }
  const capable = asNativeResumeCapableAdapter(adapter);
  if (!capable) {
    return incapable;
  }
  const verified = await capable.nativeResumeCapability.verify().then(
    (value) => value === true,
    () => false,
  );
  if (!verified) {
    return { metadata: null, native: false, status: "not-verified" };
  }
  return {
    metadata: { [PROVIDER_SESSION_METADATA_KEY]: prior.provider_session_id },
    native: true,
    status: "used",
  };
}

function reportedProviderSessionId(result: AdapterExecutionResult): string | null {
  const direct = typeof result.session_id === "string" && result.session_id.trim()
    ? result.session_id.trim()
    : null;
  if (direct) {
    return direct;
  }
  const reported = result.metadata?.[PROVIDER_SESSION_METADATA_KEY];
  return typeof reported === "string" && reported.trim() ? reported.trim() : null;
}

function buildNextState(
  turn: TurnContext,
  ref: string,
  artifactCommit: string,
  providerId: string | null,
  native: boolean,
): SessionState {
  const timestamp = turn.now().toISOString();
  const continuationMode: ContinuationMode = native ? "native" : "filesystem";
  if (turn.kind === "create") {
    return {
      schema: 1,
      session_id: turn.sessionId,
      agent: turn.agent,
      ref,
      identity: turn.identity,
      base_commit: turn.identity.head,
      current_commit: artifactCommit,
      provider_session_id: providerId,
      continuation_mode: continuationMode,
      revision: 1,
      created_at: timestamp,
      updated_at: timestamp,
    };
  }
  const prior = turn.prior as SessionState;
  return {
    schema: 1,
    session_id: turn.sessionId,
    agent: prior.agent,
    ref,
    identity: prior.identity,
    base_commit: prior.base_commit,
    current_commit: artifactCommit,
    // Provider ids stick once reported; a later run that reports a new one wins.
    provider_session_id: providerId ?? prior.provider_session_id,
    continuation_mode: continuationMode,
    revision: prior.revision + 1,
    created_at: prior.created_at,
    updated_at: timestamp,
  };
}

/**
 * Capture the worktree state as the next linear artifact commit over `parent`
 * using only plumbing (`read-tree` / `add` / `write-tree` / `commit-tree`)
 * through a private temporary index — no hooks, caller index untouched. An
 * unchanged tree still produces a commit (empty diff, same tree) so the
 * session lineage stays a pure chain: `ref -> artifact -> ... -> base`.
 */
async function captureSessionArtifact(
  worktreePath: string,
  parent: string,
  maxOutputBytes: number,
): Promise<SessionArtifact> {
  const indexDirectory = await mkdtemp(join(tmpdir(), "agent-hub-session-artifact-"));
  const indexEnv: NodeJS.ProcessEnv = {
    ...process.env,
    GIT_INDEX_FILE: join(indexDirectory, "index"),
    GIT_AUTHOR_NAME: ARTIFACT_IDENTITY_NAME,
    GIT_AUTHOR_EMAIL: ARTIFACT_IDENTITY_EMAIL,
    GIT_COMMITTER_NAME: ARTIFACT_IDENTITY_NAME,
    GIT_COMMITTER_EMAIL: ARTIFACT_IDENTITY_EMAIL,
  };

  try {
    await runGit(worktreePath, ["read-tree", parent], maxOutputBytes, indexEnv);
    await runGit(worktreePath, ["add", "-A", "--", "."], maxOutputBytes, indexEnv);
    const tree = (await runGit(worktreePath, ["write-tree"], 1000, indexEnv)).stdout.trim();
    const names = await runGit(
      worktreePath,
      ["diff", "--cached", "--name-only", "-z", "--"],
      maxOutputBytes,
      indexEnv,
    );
    const diff = await runGit(
      worktreePath,
      ["diff", "--cached", "--binary", "--no-ext-diff", "--"],
      maxOutputBytes,
      indexEnv,
    );
    const commit = (
      await runGit(worktreePath, ["commit-tree", tree, "-p", parent, "-m", SESSION_ARTIFACT_COMMIT_MESSAGE], 1000, indexEnv)
    ).stdout.trim();

    return {
      parent,
      commit,
      tree,
      changed_files: names.stdout.split("\0").filter(Boolean),
      diff: diff.stdout,
      diff_truncated: diff.stdoutTruncated,
    };
  } catch (error) {
    throw new AgentHubError(
      "SESSION_ARTIFACT_FAILED",
      `session artifact capture failed: ${error instanceof Error ? error.message : String(error)}`,
    );
  } finally {
    await rm(indexDirectory, { recursive: true, force: true });
  }
}
