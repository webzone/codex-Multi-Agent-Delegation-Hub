import { randomUUID } from "node:crypto";

import { resolveAdapter } from "./adapters/index.js";
import { captureCandidateArtifact } from "./artifacts.js";
import { asDelegateError, AgentHubError } from "./errors.js";
import {
  assertCleanUnlessAllowed,
  createDelegateResult,
  DEFAULT_MAX_OUTPUT_BYTES,
  emptyAdapterResult,
  runAdapter,
} from "./execution.js";
import {
  createWorktreeAtBase,
  ensureGitRepository,
  pruneWorktrees,
  removeWorktree,
  resolveRepositoryIdentity,
  type BaseWorktree,
} from "./git.js";
import { acquireRepositoryLock, type RepositoryLock } from "./locks.js";
import type {
  AdapterExecutionResult,
  AgentAdapter,
  CandidateArtifact,
  DelegateRequest,
  FanOutCandidateResult,
  FanOutCandidateSpec,
  FanOutRequest,
  FanOutResult,
  FanOutStatus,
  RepositoryIdentity,
} from "./types.js";
/**
 * Bounded fan-out over one shared base. Isolated-only: every candidate runs in
 * its own worktree pinned to the base captured before dispatch. Only Git
 * worktree administration (add / remove / the single trailing prune) is
 * serialized under a repository-local lock; adapter execution and artifact
 * capture run fully concurrently.
 */

export const FANOUT_MAX_CONCURRENCY_LIMIT = 8;
export const FANOUT_DEFAULT_CONCURRENCY_CAP = 4;
export const WORKTREE_ADMIN_LOCK_NAME = "worktree-admin";

const ADMIN_LOCK_WAIT_MS = 30_000;
const ADMIN_LOCK_RETRY_MS = 20;

export interface FanOutDependencies {
  resolveAdapter?: (agent: string) => AgentAdapter;
  createWorktree?: (workspace: string, base: string) => Promise<BaseWorktree>;
  removeWorktree?: (workspace: string, worktree: BaseWorktree) => Promise<void>;
  pruneWorktrees?: (workspace: string) => Promise<void>;
  acquireAdminLock?: (commonDir: string) => Promise<RepositoryLock>;
  captureArtifact?: (options: {
    worktreePath: string;
    base: string;
    candidateKey: string;
    maxOutputBytes?: number;
  }) => Promise<CandidateArtifact>;
  newCandidateId?: () => string;
  now?: () => Date;
}

function validateFanOutRequest(request: FanOutRequest): void {
  if (!request.workspace?.trim()) {
    throw new AgentHubError("INVALID_WORKSPACE", "workspace must not be empty");
  }
  if (!Array.isArray(request.candidates) || request.candidates.length === 0) {
    throw new AgentHubError("INVALID_CANDIDATES", "at least one candidate is required");
  }
  request.candidates.forEach((candidate, index) => {
    if (!candidate.task?.trim()) {
      throw new AgentHubError("INVALID_TASK", `candidate ${index}: task must not be empty`);
    }
    if (!candidate.agent?.trim()) {
      throw new AgentHubError("INVALID_AGENT", `candidate ${index}: agent must not be empty`);
    }
  });
  if (
    request.maxConcurrency !== undefined &&
    (!Number.isInteger(request.maxConcurrency) ||
      request.maxConcurrency < 1 ||
      request.maxConcurrency > FANOUT_MAX_CONCURRENCY_LIMIT)
  ) {
    throw new AgentHubError(
      "INVALID_CONCURRENCY",
      `maxConcurrency must be an integer between 1 and ${FANOUT_MAX_CONCURRENCY_LIMIT}`,
    );
  }
  if (
    request.maxOutputBytes !== undefined &&
    (!Number.isInteger(request.maxOutputBytes) || request.maxOutputBytes < 1)
  ) {
    throw new AgentHubError("INVALID_CONFIGURATION", "maxOutputBytes must be a positive integer");
  }
}

/**
 * Run `worker` over `items` with at most `limit` in flight. Results are
 * written to a pre-sized array by index, so the returned order always matches
 * input order; a rejecting worker entry is contained by the worker itself and
 * can never cancel siblings.
 */
async function runBounded<T>(
  items: T[],
  limit: number,
  worker: (item: T, index: number) => Promise<void>,
): Promise<void> {
  let cursor = 0;
  const lanes = Array.from({ length: Math.min(limit, items.length) }, async () => {
    for (;;) {
      const index = cursor;
      cursor += 1;
      if (index >= items.length) {
        return;
      }
      await worker(items[index], index);
    }
  });
  await Promise.all(lanes);
}

interface CandidateOutcome {
  adapterResult: AdapterExecutionResult;
  executionWorkspace: string;
  artifact: CandidateArtifact | null;
  failure: { code: string; message: string } | null;
}

export async function fanOut(
  request: FanOutRequest,
  dependencies: FanOutDependencies = {},
): Promise<FanOutResult> {
  validateFanOutRequest(request);

  const {
    resolveAdapter: resolveAdapterFn = resolveAdapter,
    createWorktree = createWorktreeAtBase,
    removeWorktree: removeWorktreeFn = removeWorktree,
    pruneWorktrees: pruneWorktreesFn = pruneWorktrees,
    captureArtifact = captureCandidateArtifact,
    newCandidateId = () => randomUUID(),
    now = () => new Date(),
  } = dependencies;

  await ensureGitRepository(request.workspace);
  await assertCleanUnlessAllowed(request.workspace, request.allowDirty);
  // Identity (including HEAD) is captured exactly once: every candidate below
  // pins to this same base even if the caller commits mid-flight.
  const base: RepositoryIdentity = await resolveRepositoryIdentity(request.workspace);

  const maxConcurrency =
    request.maxConcurrency ?? Math.min(request.candidates.length, FANOUT_DEFAULT_CONCURRENCY_CAP);

  const acquireAdminLock =
    dependencies.acquireAdminLock ??
    ((commonDir: string) =>
      acquireRepositoryLock({
        commonDir,
        name: WORKTREE_ADMIN_LOCK_NAME,
        waitMs: ADMIN_LOCK_WAIT_MS,
        retryDelayMs: ADMIN_LOCK_RETRY_MS,
      }));

  async function withAdminLock<T>(operation: () => Promise<T>): Promise<T> {
    const lock = await acquireAdminLock(base.common_dir);
    try {
      return await operation();
    } finally {
      await lock.release();
    }
  }

  async function runCandidate(spec: FanOutCandidateSpec, index: number): Promise<FanOutCandidateResult> {
    const startedAt = now();
    // Internal id only; the artifact ref hashes this value instead of using
    // the raw string.
    const candidateId = newCandidateId();
    const delegateRequest: DelegateRequest = {
      task: spec.task,
      agent: spec.agent,
      mode: "isolated",
      workspace: request.workspace,
      allowDirty: true, // Pre-flight already judged the caller checkout.
      maxOutputBytes: request.maxOutputBytes,
    };

    let adapterResult = emptyAdapterResult();
    let executionWorkspace = request.workspace;
    let artifact: CandidateArtifact | null = null;
    let failure: { code: string; message: string } | null = null;
    let worktree: BaseWorktree | undefined;

    try {
      const adapter = resolveAdapterFn(spec.agent);
      worktree = await withAdminLock(() => createWorktree(request.workspace, base.head));
      executionWorkspace = worktree.path;
      adapterResult = await runAdapter(delegateRequest, adapter, worktree.path);
    } catch (error) {
      failure = asDelegateError(error);
    }

    // Capture before teardown even when the agent failed, so judges can still
    // inspect partial work; an artifact failure must not mask an agent failure.
    if (worktree) {
      try {
        artifact = await captureArtifact({
          worktreePath: worktree.path,
          base: base.head,
          candidateKey: candidateId,
          maxOutputBytes: request.maxOutputBytes ?? DEFAULT_MAX_OUTPUT_BYTES,
        });
      } catch (error) {
        failure ??= asDelegateError(error);
      }

      try {
        await withAdminLock(() => removeWorktreeFn(request.workspace, worktree as BaseWorktree));
      } catch (error) {
        failure ??= asDelegateError(error);
      }
    }

    const finishedAt = now();
    const result = createDelegateResult(
      delegateRequest,
      startedAt,
      finishedAt,
      {
        adapterResult,
        executionWorkspace,
        snapshot: {
          changedFiles: artifact?.changed_files ?? [],
          diff: artifact?.diff ?? "",
          diffTruncated: artifact?.diff_truncated ?? false,
        },
      },
      failure,
    );

    return {
      ...result,
      index,
      label: spec.label ?? "",
      task: spec.task,
      candidate_id: candidateId,
      artifact,
    };
  }

  const startedAt = now();
  const candidates = new Array<FanOutCandidateResult>(request.candidates.length);
  await runBounded(request.candidates, maxConcurrency, async (spec, index) => {
    // runCandidate contains every per-candidate failure mode; this catch only
    // guards against bugs in the harness itself and keeps siblings alive.
    try {
      candidates[index] = await runCandidate(spec, index);
    } catch (error) {
      const finishedAt = now();
      candidates[index] = {
        ...createDelegateResult(
          {
            task: spec.task,
            agent: spec.agent,
            mode: "isolated",
            workspace: request.workspace,
            allowDirty: true,
            maxOutputBytes: request.maxOutputBytes,
          },
          startedAt,
          finishedAt,
          {
            adapterResult: emptyAdapterResult(),
            executionWorkspace: request.workspace,
            snapshot: { changedFiles: [], diff: "", diffTruncated: false },
          },
          asDelegateError(error),
        ),
        index,
        label: spec.label ?? "",
        task: spec.task,
        candidate_id: "",
        artifact: null,
      };
    }
  });

  let fanOutError: FanOutResult["error"] = null;
  try {
    // Exactly one prune for the whole fan-out, under the same admin lock.
    await withAdminLock(() => pruneWorktreesFn(request.workspace));
  } catch (error) {
    fanOutError = asDelegateError(error);
  }

  const finishedAt = now();
  // Aggregate view: a fan-out-level error or zero successes is a failure;
  // mixed candidate outcomes are a partial. Per-candidate errors stay on the
  // candidates themselves.
  const successes = candidates.filter((candidate) => candidate.status === "success").length;
  const status: FanOutStatus =
    fanOutError !== null || successes === 0
      ? "failure"
      : successes === candidates.length
        ? "success"
        : "partial";
  return {
    base,
    max_concurrency: maxConcurrency,
    candidates,
    status,
    started_at: startedAt.toISOString(),
    finished_at: finishedAt.toISOString(),
    duration_ms: finishedAt.getTime() - startedAt.getTime(),
    error: fanOutError,
  };
}

export type {
  FanOutCandidateSpec,
  FanOutRequest,
  FanOutResult,
  FanOutStatus,
} from "./types.js";
