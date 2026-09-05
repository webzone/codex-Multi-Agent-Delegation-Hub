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
  /**
   * v2 additive: optional pass-through metadata supplied by the hub for
   * provider-specific behaviour (e.g. resume hints). Never required in v1.
   */
  metadata?: AdapterMetadata | null;
}

export interface AdapterExecutionResult {
  exit_code: number | null;
  stdout: string;
  stderr: string;
  session_id: string | null;
  stdout_truncated: boolean;
  stderr_truncated: boolean;
  error: string | null;
  /**
   * v2 additive: optional metadata reported by the adapter (e.g. provider
   * session details used for later resume/competition packages).
   */
  metadata?: AdapterMetadata | null;
}

export interface AgentAdapter {
  readonly id: string;
  execute(request: AdapterRequest): Promise<AdapterExecutionResult>;
}

export type AdapterMetadata = Record<string, string | number | boolean | null>;

// ---------------------------------------------------------------------------
// v2 additive contracts (Package 1: fan-out foundation).
// Everything below is additive: no v1 field or wire shape above is changed.
// Later packages (competition, sessions, merge, CLI/MCP) consume these types.
// ---------------------------------------------------------------------------

/**
 * Repository state captured exactly once before a fan-out begins. Every
 * candidate in the fan-out is pinned to `head`; `branch` records the attached
 * branch of the caller checkout at capture time (null when detached).
 */
export interface RepositoryIdentity {
  /** Absolute path of the Git common dir; where Agent Hub repository-local state lives. */
  common_dir: string;
  /** Absolute path of the caller's worktree root. */
  worktree_root: string;
  /** Attached branch short name, or null for a detached HEAD. */
  branch: string | null;
  /** Full commit SHA that all candidates branch from. */
  head: string;
}

export interface FanOutCandidateSpec {
  /** Optional caller-facing label; display only, never used in ref names. */
  label?: string;
  task: string;
  agent: string;
}

/** Fan-out is isolated-only: candidates never execute in the caller checkout. */
export interface FanOutRequest {
  workspace: string;
  candidates: FanOutCandidateSpec[];
  /** Bounded concurrency, 1..8. Defaults to min(candidates.length, 4). */
  maxConcurrency?: number;
  allowDirty?: boolean;
  maxOutputBytes?: number;
}

/**
 * Hook-free Git commit produced from a candidate worktree via a temporary
 * index (write-tree/commit-tree). Fixed Agent Hub identity; parent is the
 * captured base. A no-op candidate yields `empty: true` with no commit and no
 * ref. `ref` is an internally generated private ref (never derived from raw
 * candidate IDs) and is only created after the commit exists.
 */
export interface CandidateArtifact {
  /** Captured base SHA; the sole parent of `commit`. */
  parent: string;
  /** Artifact commit SHA, or null when the candidate produced no changes. */
  commit: string | null;
  /** Tree recorded by the artifact commit, or null for a no-op. */
  tree: string | null;
  /** Fully-qualified private ref, or null when no commit was created. */
  ref: string | null;
  empty: boolean;
  changed_files: string[];
  diff: string;
  diff_truncated: boolean;
}

export interface FanOutCandidateResult extends DelegateResult {
  index: number;
  /** Echo of the caller's display label ("" when unset). */
  label: string;
  task: string;
  /** Internally generated id (random UUID); never interpolated into ref names. */
  candidate_id: string;
  artifact: CandidateArtifact | null;
}

export interface FanOutResult {
  /** Identity captured once before fan-out; the shared base for all candidates. */
  base: RepositoryIdentity;
  max_concurrency: number;
  /** Same order as the input specs, independent of completion order. */
  candidates: FanOutCandidateResult[];
  started_at: string;
  finished_at: string;
  duration_ms: number;
  /** Fan-out level error (e.g. worktree prune); per-candidate errors stay on the candidate. */
  error: DelegateError | null;
}

export type JudgementVerdict = "accepted" | "rejected" | "inconclusive";

/** Contract consumed by the later competition/judge package. */
export interface CandidateJudgement {
  candidate_id: string;
  index: number;
  verdict: JudgementVerdict;
  score: number;
  rationale: string;
}

export interface CompetitionOutcome {
  judge_agent: string;
  /** Candidate ids ordered best-first as parsed from the judge output. */
  ranking: string[];
  winner_id: string | null;
  judgements: CandidateJudgement[];
  raw_output: string;
  truncated: boolean;
  error: DelegateError | null;
}

export type MergeStrategy = "none" | "fast-forward" | "cherry-pick" | "patch";

/** Contract consumed by the later auto-merge package. */
export interface MergeOutcome {
  strategy: MergeStrategy;
  candidate_id: string | null;
  artifact_commit: string | null;
  target_ref: string | null;
  clean: boolean;
  applied_commit: string | null;
  notes: string[];
  error: DelegateError | null;
}

/** Contract consumed by the later session/state package. */
export interface DelegationSession {
  session_id: string;
  created_at: string;
  updated_at: string;
  workspace: string;
  base: RepositoryIdentity;
  fan_out: FanOutResult;
  competition: CompetitionOutcome | null;
  merge: MergeOutcome | null;
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
