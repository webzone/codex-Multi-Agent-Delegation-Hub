import { resolveAdapter } from "./adapters/index.js";
import { asDelegateError, AgentHubError } from "./errors.js";
import { runDirect, runIsolated, type ExecutionOutcome } from "./execution.js";
import type { AgentAdapter, DelegateRequest, DelegateResult, ExecutionMode } from "./types.js";

export interface DelegateDependencies {
  resolveAdapter?: (agent: string) => AgentAdapter;
}

function validateRequest(request: DelegateRequest): void {
  if (!request.task.trim()) {
    throw new AgentHubError("INVALID_TASK", "task must not be empty");
  }
  if (!request.agent.trim()) {
    throw new AgentHubError("INVALID_AGENT", "agent must not be empty");
  }
  if (request.mode !== "direct" && request.mode !== "isolated") {
    throw new AgentHubError("INVALID_MODE", `Unsupported execution mode "${request.mode}"`);
  }
  if (!request.workspace.trim()) {
    throw new AgentHubError("INVALID_WORKSPACE", "workspace must not be empty");
  }
  if (request.maxOutputBytes !== undefined && (!Number.isInteger(request.maxOutputBytes) || request.maxOutputBytes < 1)) {
    throw new AgentHubError("INVALID_CONFIGURATION", "maxOutputBytes must be a positive integer");
  }
}

function emptyOutcome(workspace: string): ExecutionOutcome {
  return {
    adapterResult: {
      exit_code: null,
      stdout: "",
      stderr: "",
      session_id: null,
      stdout_truncated: false,
      stderr_truncated: false,
      error: null,
    },
    executionWorkspace: workspace,
    snapshot: { changedFiles: [], diff: "", diffTruncated: false },
  };
}

function createResult(
  request: DelegateRequest,
  startedAt: Date,
  finishedAt: Date,
  outcome: ReturnType<typeof emptyOutcome>,
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

export async function delegate(
  request: DelegateRequest,
  dependencies: DelegateDependencies = {},
): Promise<DelegateResult> {
  const startedAt = new Date();
  const outcome = emptyOutcome(request.workspace);

  try {
    validateRequest(request);
    const adapter = (dependencies.resolveAdapter ?? ((agent: string) => resolveAdapter(agent)))(request.agent);
    const execution = request.mode === "direct"
      ? await runDirect(request, adapter)
      : await runIsolated(request, adapter);
    const finishedAt = new Date();
    return createResult(request, startedAt, finishedAt, execution, null);
  } catch (error) {
    const finishedAt = new Date();
    return createResult(request, startedAt, finishedAt, outcome, asDelegateError(error));
  }
}

export type { DelegateRequest, DelegateResult, ExecutionMode } from "./types.js";
