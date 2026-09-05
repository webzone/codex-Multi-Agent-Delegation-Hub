export { delegate } from "./delegate.js";
export { resolveAdapter, supportedAgents } from "./adapters/index.js";

// v2 — Package 1: isolated fan-out foundation.
export {
  fanOut,
  FANOUT_DEFAULT_CONCURRENCY_CAP,
  FANOUT_MAX_CONCURRENCY_LIMIT,
  WORKTREE_ADMIN_LOCK_NAME,
} from "./fanout.js";
export type { FanOutDependencies } from "./fanout.js";

// v2 — Package 4: opt-in auto-merge (verified fast-forward only).
export { autoMerge, mergeCandidate, MERGE_LOCK_NAME } from "./merge.js";
export type { AutoMergeInput, MergeCandidateRequest, MergeDependencies } from "./merge.js";

// v2 — competition and durable agent sessions.
export * from "./competition.js";
export * from "./session.js";

export type {
  AdapterExecutionResult,
  AdapterRequest,
  AgentAdapter,
  CandidateArtifact,
  CandidateJudgement,
  CompetitionOutcome,
  DelegateError,
  DelegateRequest,
  DelegateResult,
  DelegateStatus,
  DelegationSession,
  ExecutionMode,
  FanOutCandidateResult,
  FanOutCandidateSpec,
  FanOutRequest,
  FanOutResult,
  JudgementVerdict,
  MergeOutcome,
  MergeStrategy,
  RepositoryIdentity,
} from "./types.js";
