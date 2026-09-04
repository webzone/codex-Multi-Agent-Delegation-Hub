export type ExecutionMode = "direct" | "isolated";

export type DelegateStatus = "success" | "failure";

export interface DelegateRequest {
  task: string;
  agent: string;
  mode: ExecutionMode;
  workspace: string;
  allowDirty?: boolean;
  maxOutputBytes?: number;
}

export interface AdapterRequest {
  task: string;
  cwd: string;
  maxOutputBytes: number;
}

export interface AdapterExecutionResult {
  exit_code: number | null;
  stdout: string;
  stderr: string;
  session_id: string | null;
  stdout_truncated: boolean;
  stderr_truncated: boolean;
  error: string | null;
}

export interface AgentAdapter {
  readonly id: string;
  execute(request: AdapterRequest): Promise<AdapterExecutionResult>;
}

export interface DelegateError {
  code: string;
  message: string;
}

export interface DelegateResult {
  agent: string;
  mode: ExecutionMode;
  status: DelegateStatus;
  exit_code: number | null;
  session_id: string | null;
  summary: string;
  stdout: string;
  stderr: string;
  started_at: string;
  finished_at: string;
  duration_ms: number;
  workspace: string;
  execution_workspace: string;
  changed_files: string[];
  diff: string;
  output_truncated: {
    stdout: boolean;
    stderr: boolean;
    diff: boolean;
  };
  error: DelegateError | null;
}
