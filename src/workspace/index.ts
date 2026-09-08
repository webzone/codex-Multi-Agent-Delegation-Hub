/**
 * WorkspaceLifecycle package façade (rewrite, P2).
 *
 * The durable custody layer that sits beside the InteractionKernel (P1) and
 * in front of the provider transports (P3): AGENT_HUB_HOME-anchored metadata,
 * one isolated worktree per agent, exact result identity per terminal turn,
 * provider process leases, close/handoff/retention custody, crash-and-orphan
 * recovery, and a GC that deletes only what a consumer has explicitly taken
 * over (or discarded) and that every precondition proves quiescent.
 *
 * The kernel is wired to this module through its own published seams only:
 * `mirror` as the `DurableMirror`, `onProviderSpawn` as the spawn hook, and
 * `kernel.attached()` as the input to `recover`/`gc`.
 */

export { WorkspaceLifecycle, orphanSeedRecord } from "./lifecycle.js";
export type {
  CloseInput,
  FinalizeReport,
  GcReport,
  GcRetainCode,
  HandoffInput,
  ProvisionInput,
  PublishedTurn,
  RecoveryReport,
  WorkspaceInspection,
  WorkspaceLifecycleOptions,
} from "./lifecycle.js";

export { resolveHubHome, worktreePath, leasePath, resultPath, workspaceRecordPath, tombstonePath } from "./home.js";
export type { LeaseProbes } from "./leases.js";

export {
  WORKSPACE_SCHEMA_VERSION,
  WORKSPACE_RESULT_SCHEMA_VERSION,
  WORKSPACE_RUNTIME_SCHEMA_VERSION,
  WORKSPACE_TRANSACTION_SCHEMA_VERSION,
  WORKSPACE_REF_NAMESPACE,
  DEFAULT_RETENTION_MS,
  workspaceRefFor,
  parseWorkspaceRecord,
  parseResultRecord,
  parseRuntimeMirror,
  parseWorkspaceTransaction,
} from "./records.js";
export type {
  CustodyStatus,
  HandoffDecision,
  WorkspaceHandoff,
  WorkspaceRecord,
  WorkspaceResultRecord,
  WorkspaceRuntimeMirror,
  WorkspaceTransaction,
  CaptureReason,
} from "./records.js";

export {
  WORKSPACE_LEASE_SCHEMA,
  classifyLease,
  defaultLeaseProbes,
  readLease,
  reapProviderLease,
  recordWorkspaceLease,
} from "./leases.js";
export type { WorkspaceLeaseRecord, LeaseClassification, ProviderFate, ReapOutcome } from "./leases.js";

export {
  WORKSPACE_ADMIN_LOCK,
  loadWorkspace,
  scanHubHome,
  withAdminLock,
  withWorkspaceLock,
} from "./store.js";
export type { StoreContext, WorkspacePhase, PhaseObserver } from "./store.js";
