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

// v3 — Package 4: live surfaces (CLI `agent-hub live`, MCP `live_session_*`)
// on the Gate 0 live contract. The live vocabulary is separate: legacy
// `supportedAgents` stays `omp, agy, grok`; `supportedLiveAgents` adds
// `pi`/`hermes` for live sessions only.
export {
  assertLiveSessionState,
  getLiveResumeSource,
  isLiveProvider,
  isTerminalLiveStatus,
  LIVE_DEFAULT_EVENT_BUFFER,
  LIVE_DEFAULT_MAX_TEXT_BYTES,
  LIVE_MAX_SESSIONS,
  LIVE_TRANSPORT_PAIRINGS,
  liveSessionManager,
  liveTransportRegistry,
  LiveSessionManager,
  LiveTransportRegistry,
  probeLiveAgent,
  registerLiveTransport,
  runLiveSession,
  setLiveResumeSource,
  supportedLiveAgents,
  unwiredLiveResumeSource,
  validateLiveCapabilities,
} from "./live/index.js";
export type {
  LiveCloseReport,
  LiveEventPage,
  LiveIo,
  LiveLaunchInvocation,
  LiveProbeDocument,
  LiveResumeRequest,
  LiveResumeSource,
  LiveRunnerDependencies,
  LiveSessionManagerOptions,
  LiveSessionSummary,
  LiveStartRequest,
  LiveTurnCommand,
} from "./live/index.js";

// v3 — the Gate 0 live contract types (identity, commands, events, durable
// state, transport/factory interfaces).
export type {
  CapabilitySupport,
  LiveBoundedText,
  LiveCapabilities,
  LiveCapabilityClaim,
  LiveCapabilityName,
  LiveCheckpoint,
  LiveCommand,
  LiveCommandKind,
  LiveCommandOutcome,
  LiveError,
  LiveErrorStage,
  LiveEvent,
  LiveEventBody,
  LiveEventKind,
  LiveLaunchReport,
  LiveLaunchRequest,
  LivePermissionDecision,
  LiveProbeResult,
  LiveProviderFactory,
  LiveProviderId,
  LiveSessionState,
  LiveStatus,
  LiveStopMode,
  LiveStopReport,
  LiveTransport,
  LiveTransportDescriptor,
  LiveTransportFactory,
  LiveTransportId,
  LiveTurnResult,
  LiveUsage,
} from "./live/types.js";

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
