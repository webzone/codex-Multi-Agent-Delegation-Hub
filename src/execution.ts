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
import type { AdapterExecutionResult, AdapterRequest, AgentAdapter, DelegateRequest } from "./types.js";

export const DEFAULT_MAX_OUTPUT_BYTES = 200_000;

export interface ExecutionOutcome {
  adapterResult: AdapterExecutionResult;
  executionWorkspace: string;
  snapshot: GitSnapshot;
}

async function runAdapter(
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

async function assertCleanUnlessAllowed(request: DelegateRequest): Promise<void> {
  if (!request.allowDirty && (await isDirty(request.workspace))) {
    throw new AgentHubError(
      "DIRTY_WORKTREE",
      "The workspace has uncommitted or untracked changes. Use allowDirty only when this is intentional.",
    );
  }
}

export async function runDirect(request: DelegateRequest, adapter: AgentAdapter): Promise<ExecutionOutcome> {
  await ensureGitRepository(request.workspace);
  await assertCleanUnlessAllowed(request);

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
  await assertCleanUnlessAllowed(request);

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
