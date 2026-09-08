/**
 * agent-hub — the public library surface of the rewrite.
 *
 * Provider-neutral by contract: one hub object (`AgentHub`) composing the
 * Git-free interaction core (`InteractionKernel`, P1) and the durable
 * workspace core (`WorkspaceLifecycle`). Interaction crosses ONLY the
 * shipped provider transports, auto-selected per provider:
 *
 *   - omp    → omp-rpc          (RPC v2 dialect only; no v1 fallback)
 *   - pi     → pi-rpc
 *   - agy    → agy-stream-json
 *   - hermes → hermes-acp
 *
 * There is deliberately no delegate / fanout / session / live / competition
 * / judge / auto-merge vocabulary here, and no compatibility aliases for it.
 */

export { AgentHubError, asDelegateError } from "./errors.js";

export {
  AgentHub,
  AgentHubSupervisor,
  processHubSupervisor,
  WorkspaceLifecycle,
  HUB_PROVIDERS,
  HUB_TRANSPORT_BY_PROVIDER,
  HUB_PROCESS_SESSION_QUOTA,
  HUB_SESSION_QUOTA,
  HUB_REF_NAMESPACE,
  bridgeTransportFactory,
  kernelCapabilities,
  kernelizeResume,
  liveResumeOf,
  productionBridgedFactories,
  productionBridgedProviderFactories,
  AttachInputPump,
  ATTACH_QUEUE_MAX_COMMANDS,
  ATTACH_QUEUE_MAX_BYTES,
  ATTACH_CLOSE_DRAIN_DEFAULT_MS,
  type AttachInputEvent,
  type AgentHubOptions,
  type BridgedTransportFactory,
  type HandoffDocument,
  type HubCloseDocument,
  type HubOpen,
  type HubStartDocument,
  type HubStatusDocument,
  type LaunchFacts,
  type LifecyclePhase,
  type PreparedLaunch,
  type ProviderFactoryLike,
  type ReconcileReport,
  type ReconcileSessionReport,
  type ResumeOptions,
  type StartOptions,
  type TurnDocument,
  type WorkspaceLifecycleOptions,
} from "./hub/index.js";

// The interaction core stays directly constructible for embedders that bring
// their own durable/lifecycle layer; its seams (DurableMirror,
// onProviderSpawn, attached) are the supported extension points.
export {
  InteractionKernel,
  DEFAULT_MAX_TEXT_BYTES,
  DEFAULT_SESSION_QUOTA,
  FOLLOW_UP_MAX_MESSAGE_BYTES,
  FOLLOW_UP_QUEUE_MAX_BYTES,
  FOLLOW_UP_QUEUE_MAX_MESSAGES,
  SESSION_SCHEMA_VERSION,
  asKernelError,
  isCommandKind,
  isPermissionDecision,
  isTerminalStatus,
  kernelError,
  parseResumeState,
  parseSessionRecord,
  validateCapabilities,
  CAPABILITY_NAMES,
  COMMAND_KINDS,
  OBSERVATION_NAMES,
  SESSION_STATUSES,
  TERMINAL_STATUSES,
  boundEvent,
  eventBytes,
  EventRing,
  EventSubscription,
  truncateUtf8,
  EVENT_MAX_BYTES,
  RING_MAX_BYTES,
  RING_MAX_EVENTS,
  type BoundedText,
  type CancelCommand,
  type Capabilities,
  type CapabilityClaim,
  type CapabilityName,
  type CapabilitySupport,
  type CloseResult,
  type Command,
  type CommandKind,
  type CommandOutcome,
  type DurableMirror,
  type ErrorStage,
  type EventBody,
  type EventKind,
  type FollowUpCommand,
  type KernelError,
  type KernelOptions,
  type KernelPhase,
  type LaunchReport,
  type LaunchRequest,
  type ObservationName,
  type PermissionDecision,
  type PermissionPolicy,
  type PermissionResponseCommand,
  type ProcessFacts,
  type ProbeResult,
  type PromptCommand,
  type ProviderFactory,
  type ProviderId,
  type ProbeDocument,
  type ResumeState,
  type ResumeVerification,
  type SessionEvent,
  type SessionId,
  type SessionRecord,
  type SessionStatus,
  type StartRequest,
  type StartResult,
  type StatusCommand,
  type SteerCommand,
  type StopMode,
  type StopReport,
  type Transport,
  type TransportDescriptor,
  type TransportFactory,
  type TransportId,
  type TransportSelection,
  type TurnResult,
  type Usage,
} from "./kernel/index.js";

// Durable lifecycle vocabulary the hub documents carry (types only; the
// underlying primitives are internal integration surface, not public API).
export type {
  CheckpointReason,
  LiveCheckpoint,
  LiveSessionState,
  LiveStatus,
} from "./live/types.js";
