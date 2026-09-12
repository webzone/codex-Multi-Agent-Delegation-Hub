/**
 * Agent Hub public package façade (rewrite, P4).
 *
 * The integration layer only: `AgentHub` (kernel + P2 custody), the host
 * supervisor, the attach-wire input pump, and the transport bridges to the
 * shipped providers. Durable custody vocabulary comes from the canonical
 * `src/workspace` module — the hub composes it and never duplicates it.
 * No delegate/fanout/competition/session/live vocabulary, no workflow modes,
 * no compatibility wrappers, no transport pinning at public boundaries.
 */

export {
  AgentHub,
  HUB_PROVIDERS,
  HUB_PROCESS_SESSION_QUOTA,
  HUB_GC_SWEEP_INTERVAL_MS,
  type AgentHubOptions,
  type HandoffDecisionInput,
  type HubCleanupDocument,
  type HubCloseDocument,
  type HubStartDocument,
  type HubWaitDocument,
  type HubStatusDocument,
  type ResumeOptions,
  type StartOptions,
  type TurnDocument,
} from "./agent-hub.js";

// The canonical durable custody surface (P2) that the hub's documents carry.
export { WorkspaceLifecycle } from "../workspace/lifecycle.js";
export type {
  CloseInput,
  FinalizeReport,
  GcReport,
  GcRetainCode,
  HandoffInput,
  PublishedTurn,
  RecoveryReport,
  WorkspaceInspection,
  WorkspaceLifecycleOptions,
} from "../workspace/lifecycle.js";
export type {
  CustodyStatus,
  HandoffDecision,
  WorkspaceHandoff,
  WorkspaceRecord,
  WorkspaceResultRecord,
} from "../workspace/records.js";

export {
  AttachInputPump,
  ATTACH_QUEUE_MAX_COMMANDS,
  ATTACH_QUEUE_MAX_BYTES,
  ATTACH_LINE_MAX_BYTES,
  ATTACH_CHUNK_MAX_BYTES,
  type AttachInputEvent,
} from "./attach-io.js";

export {
  AgentHubSupervisor,
  processHubSupervisor,
  type HubOpen,
} from "./supervisor.js";

export {
  bridgeTransportFactory,
  productionBridgedFactories,
  productionBridgedProviderFactories,
  type BridgedTransportFactory,
} from "./transport-adapter.js";
