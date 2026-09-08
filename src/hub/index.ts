/**
 * Agent Hub public package façade (rewrite, P4).
 *
 * The integration layer only: `AgentHub` (kernel + lifecycle), the
 * `WorkspaceLifecycle` it composes, the host supervisor, and the transport
 * bridges to the shipped providers. No delegate/fanout/competition/session/
 * live vocabulary, no workflow modes, no compatibility wrappers.
 */

export {
  AgentHub,
  HUB_PROVIDERS,
  HUB_TRANSPORT_BY_PROVIDER,
  HUB_PROCESS_SESSION_QUOTA,
  type AgentHubOptions,
  type HubCloseDocument,
  type HubStartDocument,
  type HubStatusDocument,
  type ProviderFactoryLike,
  type ResumeOptions,
  type StartOptions,
  type TurnDocument,
} from "./agent-hub.js";

export {
  WorkspaceLifecycle,
  HUB_SESSION_QUOTA,
  HUB_REF_NAMESPACE,
  mergeKernelResume,
  kernelResumeFromState,
  projectRecordFromState,
  type HandoffDocument,
  type LifecyclePhase,
  type PreparedLaunch,
  type ReconcileReport,
  type ReconcileSessionReport,
  type WorkspaceLifecycleOptions,
} from "./workspace-lifecycle.js";

export {
  AttachInputPump,
  ATTACH_QUEUE_MAX_COMMANDS,
  ATTACH_QUEUE_MAX_BYTES,
  ATTACH_CLOSE_DRAIN_DEFAULT_MS,
  type AttachInputEvent,
} from "./attach-io.js";

export {
  AgentHubSupervisor,
  processHubSupervisor,
  type HubOpen,
} from "./supervisor.js";

export {
  bridgeTransportFactory,
  kernelCapabilities,
  kernelizeResume,
  liveResumeOf,
  productionBridgedFactories,
  productionBridgedProviderFactories,
  type BridgedTransportFactory,
  type LaunchFacts,
} from "./transport-adapter.js";
