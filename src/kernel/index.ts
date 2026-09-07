/**
 * InteractionKernel package façade (rewrite, P1).
 *
 * Public-neutral contracts (Gate 0) plus the Git-free interaction core:
 * request correlation, event streaming, capability-gated cancel/steer/
 * status, recovery/resume boundaries, and injected provider transport
 * selection. This module exports ONLY that surface — no delegate, fanout,
 * competition/judge/merge vocabulary, no workflow modes, no compatibility
 * wrappers. Durable persistence and workspace lifecycle (P2) and provider
 * transports (P3) integrate through the narrow seams declared here:
 * `DurableMirror`, `onProviderSpawn`, `attached()`, and the `Transport` /
 * `TransportFactory` / `ProviderFactory` contracts.
 */

export {
  InteractionKernel,
  DEFAULT_MAX_TEXT_BYTES,
  DEFAULT_SESSION_QUOTA,
  FOLLOW_UP_MAX_MESSAGE_BYTES,
  FOLLOW_UP_QUEUE_MAX_BYTES,
  FOLLOW_UP_QUEUE_MAX_MESSAGES,
  type CloseResult,
  type DurableMirror,
  type KernelOptions,
  type KernelPhase,
  type ProbeDocument,
  type ResumeOptions,
  type StartRequest,
  type StartResult,
  type TransportSelection,
} from "./interaction-kernel.js";

export {
  asKernelError,
  isCommandKind,
  isPermissionDecision,
  isTerminalStatus,
  kernelError,
  parseResumeState,
  parseSessionRecord,
  SESSION_SCHEMA_VERSION,
  validateCapabilities,
  CAPABILITY_NAMES,
  COMMAND_KINDS,
  OBSERVATION_NAMES,
  SESSION_STATUSES,
  TERMINAL_STATUSES,
  type BoundedText,
  type CancelCommand,
  type Capabilities,
  type CapabilityClaim,
  type CapabilityName,
  type CapabilitySupport,
  type Command,
  type CommandKind,
  type CommandOutcome,
  type ErrorStage,
  type EventBody,
  type EventKind,
  type FollowUpCommand,
  type KernelError,
  type LaunchReport,
  type LaunchRequest,
  type ObservationName,
  type PermissionDecision,
  type PermissionPolicy,
  type PermissionResponseCommand,
  type ProcessFacts,
  type PromptCommand,
  type ProbeResult,
  type ProviderFactory,
  type ProviderId,
  type ResumeState,
  type ResumeVerification,
  type SessionEvent,
  type SessionId,
  type SessionRecord,
  type SessionStatus,
  type StatusCommand,
  type SteerCommand,
  type StopMode,
  type StopReport,
  type Transport,
  type TransportDescriptor,
  type TransportFactory,
  type TransportId,
  type TurnResult,
  type Usage,
} from "./contracts.js";

export {
  boundEvent,
  eventBytes,
  EventRing,
  EventSubscription,
  truncateUtf8,
  EVENT_MAX_BYTES,
  RING_MAX_BYTES,
  RING_MAX_EVENTS,
  type EventPublishResult,
  type EventReplay,
  type EventRingOptions,
} from "./events.js";
