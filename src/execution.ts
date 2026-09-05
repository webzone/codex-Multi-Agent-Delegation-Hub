import { AgentHubError } from "./errors.js";
import {
  createTemporaryWorktree,
  ensureGitRepository,
  gitSnapshot,
  isDirty,
  removeTemporaryWorktree,
  type GitSnapshot,
  type TemporaryWorktree,
} from "./git.js";
import type {
  AdapterExecutionResult,
  AdapterRequest,
  AgentAdapter,
  DelegateRequest,
  DelegateResult,
} from "./types.js";

export const DEFAULT_MAX_OUTPUT_BYTES = 200_000;

export interface ExecutionOutcome {
  adapterResult: AdapterExecutionResult;
  executionWorkspace: string;
  snapshot: GitSnapshot;
}

export async function runAdapter(
  request: DelegateRequest,
  adapter: AgentAdapter,
  executionWorkspace: string,
): Promise<AdapterExecutionResult> {
  const adapterRequest: AdapterRequest = {
    task: request.task,
    cwd: executionWorkspace,
    maxOutputBytes: request.maxOutputBytes ?? DEFAULT_MAX_OUTPUT_BYTES,
  };
  return adapter.execute(adapterRequest);
}

export async function assertCleanUnlessAllowed(workspace: string, allowDirty?: boolean): Promise<void> {
  if (!allowDirty && (await isDirty(workspace))) {
    throw new AgentHubError(
      "DIRTY_WORKTREE",
      "The workspace has uncommitted or untracked changes. Use allowDirty only when this is intentional.",
    );
  }
}

export async function runDirect(request: DelegateRequest, adapter: AgentAdapter): Promise<ExecutionOutcome> {
  await ensureGitRepository(request.workspace);
  await assertCleanUnlessAllowed(request.workspace, request.allowDirty);

  const adapterResult = await runAdapter(request, adapter, request.workspace);
  const snapshot = await gitSnapshot(request.workspace, request.maxOutputBytes ?? DEFAULT_MAX_OUTPUT_BYTES);

  return {
    adapterResult,
    executionWorkspace: request.workspace,
    snapshot,
  };
}

export async function runIsolated(request: DelegateRequest, adapter: AgentAdapter): Promise<ExecutionOutcome> {
  await ensureGitRepository(request.workspace);
  await assertCleanUnlessAllowed(request.workspace, request.allowDirty);

  let worktree: TemporaryWorktree | undefined;
  try {
    worktree = await createTemporaryWorktree(request.workspace);
    const adapterResult = await runAdapter(request, adapter, worktree.path);
    const snapshot = await gitSnapshot(worktree.path, request.maxOutputBytes ?? DEFAULT_MAX_OUTPUT_BYTES);

    return {
      adapterResult,
      executionWorkspace: worktree.path,
      snapshot,
    };
  } finally {
    if (worktree) {
      await removeTemporaryWorktree(request.workspace, worktree);
    }
  }
}

export function emptyAdapterResult(): AdapterExecutionResult {
  return {
    exit_code: null,
    stdout: "",
    stderr: "",
    session_id: null,
    stdout_truncated: false,
    stderr_truncated: false,
    error: null,
  };
}

/**
 * Map an execution outcome onto the v1 DelegateResult wire shape. Kept in
 * lockstep with delegate.ts's private createResult so fan-out candidates are
 * byte-compatible with v1 results (same codes: PROCESS_ERROR, AGENT_FAILED).
 */
export function createDelegateResult(
  request: DelegateRequest,
  startedAt: Date,
  finishedAt: Date,
  outcome: ExecutionOutcome,
  error: { code: string; message: string } | null,
): DelegateResult {
  const adapterResult = outcome.adapterResult;
  const finalError = error ?? (adapterResult.error
    ? { code: "PROCESS_ERROR", message: adapterResult.error }
    : adapterResult.exit_code === 0
      ? null
      : {
          code: "AGENT_FAILED",
          message: `Agent exited with code ${adapterResult.exit_code ?? "unknown"}`,
        });
  const status = finalError ? "failure" : "success";

  return {
    agent: request.agent,
    mode: request.mode,
    status,
    exit_code: adapterResult.exit_code,
    session_id: adapterResult.session_id,
    summary: status === "success" ? `${request.agent} completed successfully` : finalError?.message ?? "Agent failed",
    stdout: adapterResult.stdout,
    stderr: adapterResult.stderr,
    started_at: startedAt.toISOString(),
    finished_at: finishedAt.toISOString(),
    duration_ms: finishedAt.getTime() - startedAt.getTime(),
    workspace: request.workspace,
    execution_workspace: outcome.executionWorkspace,
    changed_files: outcome.snapshot.changedFiles,
    diff: outcome.snapshot.diff,
    output_truncated: {
      stdout: adapterResult.stdout_truncated,
      stderr: adapterResult.stderr_truncated,
      diff: outcome.snapshot.diffTruncated,
    },
    error: finalError,
  };
}
