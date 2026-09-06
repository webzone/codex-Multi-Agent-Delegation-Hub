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

// v3 — live surfaces (CLI `agent-hub live`, MCP `live_session_*`). The live
// vocabulary is separate: legacy `supportedAgents` stays `omp, agy, grok`;
// `supportedLiveAgents` adds `pi`/`hermes` for live sessions only. The
// authoritative production manager is the core `LiveSessionManager`; the
// surfaces build it per workspace through `createLiveManager`, which registers
// all four real transports and wires the durable live-state reader.
export {
  assertLiveSessionState,
  createLiveManager,
  durableLiveResumeSource,
  getLiveResumeSource,
  isLiveProvider,
  isTerminalLiveStatus,
  LIVE_DEFAULT_MAX_TEXT_BYTES,
  LIVE_TRANSPORT_PAIRINGS,
  liveTransportRegistry,
  LiveSessionManager,
  LiveTransportRegistry,
  probeLiveAgent,
  productionTransportFactories,
  registerLiveTransport,
  registerProductionLiveTransports,
  runLiveSession,
  setLiveResumeSource,
  supportedLiveAgents,
  validateLiveCapabilities,
  wireDurableLiveResumeSource,
} from "./live/index.js";
export type {
  CreateLiveManagerOptions,
  LiveCloseResult,
  LiveIo,
  LiveLaunchInvocation,
  LiveManagerOptions,
  LiveProbeDocument,
  LiveResumeFromStateRequest,
  LiveResumeSource,
  LiveRunnerDependencies,
  LiveStartRequest,
  LiveStartResult,
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
  LiveProviderProcessFacts,
  LiveSessionState,
  CheckpointReason,
  LiveStatus,
  LiveStopMode,
  LiveStopReport,
  ProviderResumeState,
  ResumeVerification,
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
