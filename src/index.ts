export { delegate } from "./delegate.js";
export { resolveAdapter, supportedAgents } from "./adapters/index.js";

// v2 — Package 1: isolated fan-out foundation.
export {
  fanOut,
  FANOUT_DEFAULT_CONCURRENCY_CAP,
  FANOUT_MAX_CANDIDATES,
  FANOUT_MAX_CONCURRENCY_LIMIT,
  WORKTREE_ADMIN_LOCK_NAME,
} from "./fanout.js";
export type { FanOutDependencies } from "./fanout.js";

// v2 — artifact ref ownership: terminal callers release retained refs here.
export { releaseCandidateRef, releaseFanOutArtifactRefs } from "./artifacts.js";

// v2 — Package 4: opt-in auto-merge (verified fast-forward only).
// `mergeCandidate` is internal on purpose: adoption may only follow a real
// `runCompetition()` result through `autoMerge`, never a hand-supplied artifact.
export { autoMerge, MERGE_LOCK_NAME } from "./merge.js";
export type { AutoMergeInput, MergeDependencies } from "./merge.js";

// v2 — competition, durable agent sessions, and the session state contracts.
export * from "./competition.js";
export * from "./session.js";
export * from "./state.js";

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
  ExecutionMode,
  FanOutCandidateResult,
  FanOutCandidateSpec,
  FanOutRequest,
  FanOutResult,
  FanOutStatus,
  JudgementVerdict,
  MergeOutcome,
  MergeStrategy,
  RepositoryIdentity,
} from "./types.js";
