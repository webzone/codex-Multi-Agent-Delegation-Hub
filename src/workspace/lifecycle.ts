import { AgentHubError } from "../errors.js";
import { ensureGitRepository, resolveRepositoryIdentity } from "../git.js";
import { acquireRepositoryLock } from "../locks.js";
import type { RepositoryLock } from "../locks.js";
import {
  parseSessionRecord,
  type ProcessFacts,
  type SessionRecord,
  type TurnResult,
} from "../kernel/contracts.js";
import type { InteractionKernel } from "../kernel/index.js";
import {
  custodialWorktreeRoot,
  isInside,
  isWorkspaceSessionId,
  readJsonFile,
  removeFile,
  removeTree,
  resolveHubHome,
  tombstonePath,
  worktreePath,
  writeJsonAtomic,
} from "./home.js";
import {
  addWorktree,
  casDeleteRef,
  captureWorkspaceState,
  commitExists,
  inspectWorktree,
  probeRef,
  pruneWorktrees,
  removeWorktree,
} from "./gitops.js";
import type { WorktreeInspection } from "./gitops.js";
import {
  classifyLease,
  defaultLeaseProbes,
  deleteLeaseFile,
  hubProcessStartToken,
  readLease,
  recordWorkspaceLease,
  reapProviderLease,
} from "./leases.js";
import type { LeaseClassification, LeaseProbes } from "./leases.js";
import {
  DEFAULT_RETENTION_MS,
  isTerminalRuntimeStatus,
  workspaceRefFor,
  WORKSPACE_RESULT_SCHEMA_VERSION,
  WORKSPACE_SCHEMA_VERSION,
  WORKSPACE_TRANSACTION_SCHEMA_VERSION,
} from "./records.js";
import type {
  HandoffDecision,
  WorkspaceRecord,
  WorkspaceResultRecord,
  WorkspaceTransaction,
} from "./records.js";
import {
  ensureLayout,
  loadWorkspace,
  readRecordRaw,
  readResult,
  readRuntimeMirror,
  removeCustodyTree,
  scanHubHome,
  updateRecordCas,
  withAdminLock,
  withWorkspaceLock,
  writeIntentSidecar,
  applyRefTransitionCaptured,
  writeRuntimeMirrorAtomic,
  writeRecordAtomic,
} from "./store.js";
import type { LoadedWorkspace, PhaseObserver, StoreContext } from "./store.js";

/**
 * WorkspaceLifecycle — P2 custody over workspaces and results.
 *
 * Boundaries (fixed by the P1 contract): the kernel owns interaction and
 * commits `SessionRecord` through the injected `DurableMirror`; transports
 * report spawn facts through `onProviderSpawn`; everything Git — worktree
 * provisioning, checkpoint lineage, result identity, ownership leases,
 * handoff, retention, and GC — lives here and only here.
 *
 * Custody rules, in one breath:
 *   - every agent gets its own isolated worktree under `AGENT_HUB_HOME`;
 *   - every terminal turn publishes an exact result identity (sequence,
 *     commit, tree, ref), even when the tree did not change;
 *   - `close` never deletes a worktree, ref, or result;
 *   - deletion requires an explicit exact handoff (`accepted` or
 *     `discarded`), an expired retention window (default 24 h from the
 *     decision), no active runtime/provider lease, no pending transaction,
 *     and a state whose ref, record, commits, and results all agree;
 *   - abnormal, orphaned, foreign, unverifiable, or racing work is always
 *     retained and reported. Results cannot vanish before consumer takeover.
 */

export interface WorkspaceLifecycleOptions {
  /** Explicit hub home; otherwise `AGENT_HUB_HOME`, otherwise homedir. */
  home?: string;
  env?: NodeJS.ProcessEnv;
  retentionMs?: number;
  now?: () => Date;
  probes?: LeaseProbes;
  /** Wait budget for custody/admin locks; 0 means single attempt (busy → retained). */
  lockWaitMs?: number;
  observePhase?: PhaseObserver;
  probePid?: (pid: number) => "live" | "dead";
}

export interface ProvisionInput {
  /** Hub-generated session id (UUID). */
  session_id: string;
  repository_cwd: string;
  /** Display label for the agent owning this workspace. */
  agent?: string;
  /** Base commit; defaults to the repository HEAD at provision time. */
  base?: string;
}

export interface PublishedTurn {
  turn: TurnResult;
  /** Null exactly when the command was never delivered or is not a turn. */
  result: WorkspaceResultRecord | null;
  publish_skipped_reason: string | null;
}

export interface CloseInput {
  session_id: string;
  /** Honest note on what authorized the closure (kernel stop report, recovery proof). */
  evidence: string;
  runtime_status?: WorkspaceRecord["runtime_status"];
}

export interface HandoffInput {
  session_id: string;
  decision: HandoffDecision;
  /** Exact identity takeover: must equal the workspace's current published head. */
  result_seq: number;
  commit: string;
  consumer?: string | null;
}

export interface FinalizeReport {
  workspace: WorkspaceRecord;
  /** True when the close capture advanced the lineage. */
  advanced: boolean;
  capture_commit: string | null;
  skipped_capture_reason: string | null;
}

export interface RecoveryReport {
  reconciled: { session_id: string; outcome: string; detail: string }[];
  live: string[];
  orphans: {
    session_id: string;
    classification: string;
    reapable: boolean;
    closed_by_recovery: boolean;
    detail: string;
  }[];
  inconsistencies: { session_id: string; detail: string }[];
  unclaimed: {
    worktrees: string[];
    leases: string[];
    /** Runtime mirrors whose custody record is gone (retained for audit). */
    runtimes: string[];
    /** Namespace entries that name no hub session; retained, never touched. */
    unknown_segments: string[];
  };
}

export type GcRetainCode =
  | "handoff-undecided"
  | "retention-active"
  | "runtime-attached"
  | "runtime-live"
  | "lease-live"
  | "lease-uncertain"
  | "lease-foreign"
  | "hub-live"
  | "locked-by-peer"
  | "admin-locked"
  | "state-inconsistent"
  | "results-missing"
  | "commit-unverifiable"
  | "ref-diverged"
  | "worktree-foreign"
  | "worktree-not-custodial"
  | "worktree-remove-failed"
  | "repository-unreachable";

export interface GcReport {
  deleted: {
    session_id: string;
    last_result_seq: number;
    head_commit: string;
    decision: HandoffDecision;
    /** True when a tombstone proved the deletion was already decided before a crash. */
    continued: boolean;
  }[];
  retained: { session_id: string; code: GcRetainCode; detail: string }[];
  unclaimed: {
    /** Custody-namespace paths with no record and no tombstone: retained for inspection. */
    worktrees: string[];
    leases: string[];
    /** Namespace entries that name no hub session; retained, never touched. */
    unknown_segments: string[];
    cleanup_errors: { path: string; reason: string }[];
  };
  pruned_repositories: string[];
}

interface Tombstone {
  schema: typeof TOMBSTONE_SCHEMA;
  session_id: string;
  /** The hub home that owned the deleted custody; continuation paths resolve from it. */
  hub_home: string;
  repository_cwd: string;
  worktree_path: string;
  head_commit: string;
  last_result_seq: number;
  decision: HandoffDecision;
  deleted_at: string;
}

const TOMBSTONE_SCHEMA = "agent-hub-workspace-deleted/v1";

function fail(code: string, message: string): never {
  throw new AgentHubError(code, message);
}

const ALL_CAPABILITY_NAMES = [
  "prompt",
  "follow_up",
  "steer",
  "cancel",
  "status",
  "permission_response",
  "resume",
  "usage_reporting",
] as const;

/** Everything `inspect` proves about one session, as a named contract. */
export interface WorkspaceInspection {
  workspace: WorkspaceRecord;
  runtime:
    | { state: "absent" }
    | { state: "corrupt" }
    | { state: "present"; record: SessionRecord; rewritten_by_recovery: boolean };
  lease: LeaseClassification | { state: "absent" } | { state: "corrupt" };
  worktree: WorktreeInspection;
  results: { seq: number; result: WorkspaceResultRecord | "corrupt" }[];
}

export class WorkspaceLifecycle {
  readonly home: string;
  private readonly retentionMs: number;
  private readonly now: () => Date;
  private readonly probes: LeaseProbes;
  private readonly lockWaitMs: number;
  private readonly observePhase: PhaseObserver;
  private readonly probePid: ((pid: number) => "live" | "dead") | undefined;

  constructor(options: WorkspaceLifecycleOptions = {}) {
    this.home = resolveHubHome(options.home, options.env ?? process.env);
    this.retentionMs = options.retentionMs ?? DEFAULT_RETENTION_MS;
    this.now = options.now ?? (() => new Date());
    this.probes = options.probes ?? defaultLeaseProbes;
    this.lockWaitMs = options.lockWaitMs ?? 0;
    this.observePhase = options.observePhase ?? (() => undefined);
    this.probePid = options.probePid;
  }

  /** The DurableMirror to inject into the InteractionKernel. */
  readonly mirror = {
    commit: async (record: SessionRecord): Promise<void> => {
      // Rebuild-and-validate at the boundary; the store refuses structural lies.
      parseSessionRecord(record);
      await writeRuntimeMirrorAtomic(
        this.home,
        record.session_id,
        structuredClone(record),
        this.now().toISOString(),
        false,
      );
    },
  };

  /** The onProviderSpawn hook to inject into the InteractionKernel. */
  readonly onProviderSpawn = async (sessionId: string, facts: ProcessFacts): Promise<void> => {
    if (!isWorkspaceSessionId(sessionId)) {
      fail("WORKSPACE_ID_INVALID", `cannot record a lease for non-UUID session id "${sessionId}"`);
    }
    await ensureLayout(this.home);
    const runtime = await readRuntimeMirror(this.home, sessionId);
    await recordWorkspaceLease({
      home: this.home,
      session_id: sessionId,
      facts,
      provider: runtime.status === "present" ? runtime.mirror.record.provider : null,
      transport: runtime.status === "present" ? runtime.mirror.record.transport : null,
      hubStartToken: await hubProcessStartToken(this.probes),
      now: this.now,
      probes: this.probes,
    });
  };

  // -------------------------------------------------------------------------
  // Provisioning
  // -------------------------------------------------------------------------

  /**
   * Create the agent's isolated worktree and custody record. One agent, one
   * worktree: paths are keyed by session id, created detached at `base`
   * under the hub-owned worktree root. Serialized through the admin lock —
   * git worktree administration is not concurrency-safe.
   */
  async provision(input: ProvisionInput): Promise<WorkspaceRecord> {
    const { session_id: sessionId } = input;
    if (!isWorkspaceSessionId(sessionId)) {
      fail("WORKSPACE_ID_INVALID", `session id "${sessionId}" must be a hub-generated UUID`);
    }
    await ensureGitRepository(input.repository_cwd);
    const identity = await resolveRepositoryIdentity(input.repository_cwd);
    const base = input.base ?? identity.head;
    if (!(await commitExists(input.repository_cwd, base))) {
      fail("WORKSPACE_BASE_UNVERIFIABLE", `base commit ${base} does not exist in ${input.repository_cwd}`);
    }
    const ctx = this.context(input.repository_cwd);
    return withAdminLock(
      ctx,
      async () => {
        await ensureLayout(this.home);
        const existing = await readRecordRaw(this.home, sessionId);
        if (existing.status !== "absent") {
          fail("WORKSPACE_EXISTS", `session "${sessionId}" is already under custody (record ${existing.status})`);
        }
        const stray = await readJsonFile(tombstonePath(this.home, sessionId));
        if (stray !== undefined && stray !== null) {
          // A previous workspace under this (UUID-reused) id was GC-deleted;
          // its tombstone is history, not custody.
          await removeFile(tombstonePath(this.home, sessionId));
        }
        const wtPath = worktreePath(this.home, sessionId);
        await addWorktree(input.repository_cwd, wtPath, base);
        const headTree = (await captureWorkspaceState(wtPath, base, "turn_end", null, this.now)).tree;
        const at = this.now().toISOString();
        const record: WorkspaceRecord = {
          schema: WORKSPACE_SCHEMA_VERSION,
          session_id: sessionId,
          agent: input.agent?.trim() || sessionId,
          provider: null,
          transport: null,
          repository_cwd: identity.worktree_root,
          identity,
          base_commit: base,
          worktree_path: wtPath,
          ref: workspaceRefFor(sessionId),
          custody: "live",
          runtime_status: null,
          head_commit: base,
          head_tree: headTree,
          last_result_seq: 0,
          handoff: null,
          retention_until: null,
          closed_at: null,
          close_evidence: null,
          last_error: null,
          revision: 0,
          created_at: at,
          updated_at: at,
        };
        // Crash before this single atomic write leaves an unclaimed worktree
        // directory with no record: recovery reports it unclaimed and retains
        // it. Provision is the one step with no git-visible transition to
        // recover through — and nothing is ever assumed from an unclaimed dir.
        await writeRecordAtomic(this.home, record);
        return record;
      },
      { waitMs: Math.max(this.lockWaitMs, 2_000) },
    );
  }

  // -------------------------------------------------------------------------
  // Kernel orchestration (never mutates the kernel; uses its published seams)
  // -------------------------------------------------------------------------

  /** Provision + `kernel.start` in one step, wiring workspace and session id. */
  async startWorkspace(
    kernel: InteractionKernel,
    input: ProvisionInput & {
      provider: string;
      max_text_bytes?: number;
      permission_policy?: "deny" | "interactive";
    },
  ): Promise<{ workspace: WorkspaceRecord; start: Awaited<ReturnType<InteractionKernel["start"]>> }> {
    const workspace = await this.provision(input);
    const start = await kernel.start({
      provider: input.provider,
      workspace: workspace.worktree_path,
      session_id: workspace.session_id,
      ...(input.max_text_bytes ? { max_text_bytes: input.max_text_bytes } : {}),
      ...(input.permission_policy ? { permission_policy: input.permission_policy } : {}),
    });
    return { workspace, start };
  }

  /** Deliver a prompt/follow-up and publish the turn's exact result identity. */
  async turn(
    kernel: InteractionKernel,
    sessionId: string,
    kind: "prompt" | "follow_up",
    text: string,
  ): Promise<PublishedTurn> {
    const turnResult =
      kind === "prompt"
        ? await kernel.prompt(sessionId, text)
        : await kernel.followUp(sessionId, text);
    return this.publishTurnResult(sessionId, turnResult);
  }

  /** Kernel close + custody finalize. Nothing is deleted by either. */
  async closeSession(
    kernel: InteractionKernel,
    sessionId: string,
    mode: "graceful" | "terminate" = "graceful",
    beforeFinalize?: () => Promise<void>,
  ): Promise<{ close: Awaited<ReturnType<InteractionKernel["close"]>>; finalize: FinalizeReport }> {
    const close = await kernel.close(sessionId, mode);
    // A host may be publishing the terminal turn settled by kernel.close.
    // Let that publication finish while custody is still live; otherwise a
    // concurrent explicit close could finalize first and erase the result's
    // exact identity from the public response.
    await beforeFinalize?.();
    const stop = close.stop;
    const evidence = stop
      ? `kernel ${mode} close: stop=${stop.status} exit_code=${stop.exit_code ?? "null"} exit_signal=${stop.exit_signal ?? "null"}`
      : `kernel ${mode} close: no stop report`;
    const finalize = await this.finalizeClosure({
      session_id: sessionId,
      evidence,
      runtime_status: close.record.status,
    });
    return { close, finalize };
  }

  // -------------------------------------------------------------------------
  // Result publication (every terminal turn, changed tree or not)
  // -------------------------------------------------------------------------

  /**
   * Publish the exact identity of one terminal turn. A changed tree advances
   * the custody chain via sidecar + ref CAS; an unchanged tree still writes
   * a result record naming the current commit, tree, and ref. Commands the
   * capability gate never delivered (`unsupported`) have no turn to record.
   */
  async publishTurnResult(sessionId: string, turn: TurnResult): Promise<PublishedTurn> {
    if (turn.kind !== "prompt" && turn.kind !== "follow_up") {
      return { turn, result: null, publish_skipped_reason: `command kind "${turn.kind}" is not a turn` };
    }
    if (turn.outcome === "unsupported") {
      return { turn, result: null, publish_skipped_reason: "the capability gate refused the command; it was never delivered" };
    }
    const pre = await readRecordRaw(this.home, sessionId);
    if (pre.status === "absent") {
      fail("WORKSPACE_NOT_FOUND", `session "${sessionId}" is not under workspace custody`);
    }
    if (pre.status === "corrupt") {
      fail("WORKSPACE_STATE_INCONSISTENT", `custody record for "${sessionId}" is corrupt`);
    }
    const ctx = this.context(pre.record.repository_cwd);
    const result = await withWorkspaceLock(ctx, sessionId, async () => {
      const loaded = await loadWorkspace(ctx, sessionId);
      if (loaded.status === "inconsistent") {
        fail("WORKSPACE_STATE_INCONSISTENT", loaded.detail);
      }
      if (loaded.status === "absent") {
        fail("WORKSPACE_NOT_FOUND", `session "${sessionId}" is not under workspace custody`);
      }
      return this.publishUnderLock(ctx, loaded.record, turn);
    });
    return { turn, result, publish_skipped_reason: null };
  }

  private async publishUnderLock(
    ctx: StoreContext,
    rec: WorkspaceRecord,
    turn: TurnResult,
  ): Promise<WorkspaceResultRecord> {
    const sessionId = rec.session_id;
    if (rec.custody !== "live") {
      fail(
        "WORKSPACE_CLOSED",
        `session "${sessionId}" custody is ${rec.custody}; terminal turns after closure are not published`,
      );
    }
    const inspection = await inspectWorktree(rec.repository_cwd, rec.worktree_path);
    if (inspection.state !== "present") {
      fail(
        "WORKSPACE_WORKTREE_UNAVAILABLE",
        `cannot capture a result for "${sessionId}": worktree ${rec.worktree_path} is ${inspection.state}`,
      );
    }

    const seq = rec.last_result_seq + 1;
    const expectedRef = rec.head_commit === rec.base_commit ? null : rec.head_commit;
    const ref = workspaceRefFor(sessionId);
    const intent: WorkspaceTransaction = {
      schema: WORKSPACE_TRANSACTION_SCHEMA_VERSION,
      session_id: sessionId,
      kind: "result",
      reason: "turn_end",
      seq,
      command_id: turn.command_id,
      ref,
      expected_ref: expectedRef,
      expected_revision: rec.revision,
      capture_phase: "intent",
      new_commit: null,
      tree: null,
      next_record: null,
      result: null,
      prepared_at: this.now().toISOString(),
    };
    await writeIntentSidecar(ctx, intent);
    await this.observePhase("sidecar-written", sessionId);

    const capture = await captureWorkspaceState(rec.worktree_path, rec.head_commit, "turn_end", seq, this.now);
    const runtime = await readRuntimeMirror(this.home, sessionId);
    const next: WorkspaceRecord = {
      ...structuredClone(rec),
      provider: rec.provider ?? (runtime.status === "present" ? runtime.mirror.record.provider : null),
      transport: rec.transport ?? (runtime.status === "present" ? runtime.mirror.record.transport : null),
      head_commit: capture.commit,
      head_tree: capture.tree,
      last_result_seq: seq,
      revision: rec.revision + 1,
      updated_at: this.now().toISOString(),
    };
    const result: WorkspaceResultRecord = {
      schema: WORKSPACE_RESULT_SCHEMA_VERSION,
      session_id: sessionId,
      seq,
      command_id: turn.command_id,
      kind: turn.kind,
      outcome: turn.outcome,
      commit: capture.commit,
      parent: rec.head_commit,
      tree: capture.tree,
      ref,
      tree_changed: capture.advanced,
      final_text: turn.final_text,
      usage: turn.usage,
      started_at: turn.started_at,
      finished_at: turn.finished_at,
      duration_ms: turn.duration_ms,
      error: turn.error,
      recorded_at: this.now().toISOString(),
    };
    const captured: WorkspaceTransaction = {
      ...intent,
      capture_phase: "captured",
      new_commit: capture.commit,
      tree: capture.tree,
      next_record: next,
      result,
    };
    await applyRefTransitionCaptured(ctx, { sessionId, tx: captured, observe: this.observePhase });
    return result;
  }

  // -------------------------------------------------------------------------
  // Closure (never deletes; captures the final state)
  // -------------------------------------------------------------------------

  /**
   * Close custody: capture any final working state (advancing the lineage if
   * the provider wrote after the last turn), mark the record closed, and
   * retain every worktree, ref, and result. Idempotent.
   */
  async finalizeClosure(input: CloseInput): Promise<FinalizeReport> {
    const { session_id: sessionId } = input;
    const pre = await readRecordRaw(this.home, sessionId);
    if (pre.status === "absent") {
      fail("WORKSPACE_NOT_FOUND", `no custody record for session "${sessionId}"`);
    }
    if (pre.status === "corrupt") {
      fail("WORKSPACE_STATE_INCONSISTENT", `custody record for "${sessionId}" is corrupt`);
    }
    const ctx = this.context(pre.record.repository_cwd);
    return withWorkspaceLock(ctx, sessionId, async () => {
      const loaded = await loadWorkspace(ctx, sessionId);
      if (loaded.status === "inconsistent") {
        fail("WORKSPACE_STATE_INCONSISTENT", loaded.detail);
      }
      if (loaded.status === "absent") {
        fail("WORKSPACE_NOT_FOUND", `no custody record for session "${sessionId}"`);
      }
      const rec = loaded.record;
      if (rec.custody === "closed") {
        return {
          workspace: rec,
          advanced: false,
          capture_commit: null,
          skipped_capture_reason: "custody was already closed; closure is idempotent",
        };
      }
      const inspection = await inspectWorktree(rec.repository_cwd, rec.worktree_path);
      const at = this.now().toISOString();
      if (inspection.state !== "present") {
        const closed = await updateRecordCas(ctx, sessionId, rec.revision, (r) => ({
          ...r,
          custody: "closed" as const,
          runtime_status: input.runtime_status ?? r.runtime_status,
          closed_at: at,
          close_evidence: input.evidence,
          last_error: {
            code: "WORKSPACE_WORKTREE_UNAVAILABLE",
            message: `close capture skipped: worktree ${rec.worktree_path} is ${inspection.state}`,
          },
          revision: r.revision + 1,
          updated_at: at,
        }));
        return {
          workspace: closed,
          advanced: false,
          capture_commit: null,
          skipped_capture_reason: `worktree is ${inspection.state}; the ref pins the last result identity`,
        };
      }

      const expectedRef = rec.head_commit === rec.base_commit ? null : rec.head_commit;
      const ref = workspaceRefFor(sessionId);
      const intent: WorkspaceTransaction = {
        schema: WORKSPACE_TRANSACTION_SCHEMA_VERSION,
        session_id: sessionId,
        kind: "close-capture",
        reason: "close",
        seq: null,
        command_id: null,
        ref,
        expected_ref: expectedRef,
        expected_revision: rec.revision,
        capture_phase: "intent",
        new_commit: null,
        tree: null,
        next_record: null,
        result: null,
        prepared_at: at,
      };
      await writeIntentSidecar(ctx, intent);
      await this.observePhase("sidecar-written", sessionId);
      const capture = await captureWorkspaceState(rec.worktree_path, rec.head_commit, "close", null, this.now);
      const next: WorkspaceRecord = {
        ...structuredClone(rec),
        head_commit: capture.commit,
        head_tree: capture.tree,
        custody: "closed" as const,
        runtime_status: input.runtime_status ?? rec.runtime_status,
        closed_at: at,
        close_evidence: input.evidence,
        last_error: null,
        revision: rec.revision + 1,
        updated_at: at,
      };
      const captured: WorkspaceTransaction = {
        ...intent,
        capture_phase: "captured",
        new_commit: capture.commit,
        tree: capture.tree,
        next_record: next,
        result: null,
      };
      await applyRefTransitionCaptured(ctx, { sessionId, tx: captured, observe: this.observePhase });
      return {
        workspace: next,
        advanced: capture.advanced,
        capture_commit: capture.advanced ? capture.commit : null,
        skipped_capture_reason: null,
      };
    });
  }

  // -------------------------------------------------------------------------
  // Handoff (the only path that arms retention)
  // -------------------------------------------------------------------------

  /**
   * The consumer's exact decision on the workspace's current published head.
   * Only `accepted` or `discarded` exist; the identity must match the exact
   * `result_seq`/`commit` pair currently published or the handoff is refused
   * without touching anything. The retention clock starts at the decision.
   */
  async handoff(input: HandoffInput): Promise<WorkspaceRecord> {
    const { session_id: sessionId } = input;
    const pre = await readRecordRaw(this.home, sessionId);
    if (pre.status === "absent") {
      fail("WORKSPACE_NOT_FOUND", `no custody record for session "${sessionId}"`);
    }
    if (pre.status === "corrupt") {
      fail("WORKSPACE_STATE_INCONSISTENT", `custody record for "${sessionId}" is corrupt`);
    }
    const ctx = this.context(pre.record.repository_cwd);
    return withWorkspaceLock(ctx, sessionId, async () => {
      const loaded = await loadWorkspace(ctx, sessionId);
      if (loaded.status === "inconsistent") {
        fail("WORKSPACE_STATE_INCONSISTENT", loaded.detail);
      }
      if (loaded.status === "absent") {
        fail("WORKSPACE_NOT_FOUND", `no custody record for session "${sessionId}"`);
      }
      const rec = loaded.record;
      if (rec.custody !== "closed") {
        fail(
          "WORKSPACE_NOT_CLOSED",
          `session "${sessionId}" custody is still live; handoff applies only after closure`,
        );
      }
      if (rec.handoff !== null) {
        if (
          rec.handoff.decision === input.decision &&
          rec.handoff.result_seq === input.result_seq &&
          rec.handoff.commit === input.commit
        ) {
          return rec; // Idempotent replay of the same exact decision.
        }
        fail(
          "WORKSPACE_HANDOFF_CONFLICT",
          `session "${sessionId}" already decided ${rec.handoff.decision} on result ${rec.handoff.result_seq} @ ${rec.handoff.commit}; decisions are not revisable`,
        );
      }
      if (input.result_seq !== rec.last_result_seq || input.commit !== rec.head_commit) {
        fail(
          "WORKSPACE_HANDOFF_MISMATCH",
          `handoff names result ${input.result_seq} @ ${input.commit} but the exact published head is result ${rec.last_result_seq} @ ${rec.head_commit}`,
        );
      }
      const at = this.now().toISOString();
      return updateRecordCas(ctx, sessionId, rec.revision, (r) => ({
        ...r,
        handoff: {
          decision: input.decision,
          result_seq: input.result_seq,
          commit: input.commit,
          decided_at: at,
          consumer: input.consumer ?? null,
        },
        retention_until: new Date(this.now().getTime() + this.retentionMs).toISOString(),
        revision: r.revision + 1,
        updated_at: at,
      }));
    });
  }

  /**
   * The resume boundary for custody (the exact-identity guard for the
   * kernel's own terminal-record check lives in P1). A closed workspace may
   * reopen ONLY while no consumer has decided its head: a `handoff` decision
   * arms retention, and a workspace that may be collected must never accept
   * new work. Any still-owned lease (live hub, live provider, uncertain or
   * corrupt proof) refuses the reopen — never a silent takeover.
   */
  async reopenForResume(sessionId: string): Promise<WorkspaceRecord> {
    const pre = await readRecordRaw(this.home, sessionId);
    if (pre.status === "absent") {
      fail("WORKSPACE_NOT_FOUND", `no custody record for session "${sessionId}"`);
    }
    if (pre.status === "corrupt") {
      fail("WORKSPACE_STATE_INCONSISTENT", `custody record for "${sessionId}" is corrupt`);
    }
    const ctx = this.context(pre.record.repository_cwd);
    return withWorkspaceLock(ctx, sessionId, async () => {
      const loaded = await loadWorkspace(ctx, sessionId);
      if (loaded.status === "inconsistent") {
        fail("WORKSPACE_STATE_INCONSISTENT", loaded.detail);
      }
      if (loaded.status === "absent") {
        fail("WORKSPACE_NOT_FOUND", `no custody record for session "${sessionId}"`);
      }
      const rec = loaded.record;
      if (rec.custody === "live") {
        fail("WORKSPACE_LIVE", `session "${sessionId}" custody is still live; there is nothing to resume`);
      }
      if (rec.handoff !== null) {
        fail(
          "WORKSPACE_HANDOFF_DECIDED",
          `session "${sessionId}" head result ${rec.handoff.result_seq} @ ${rec.handoff.commit} was decided ${rec.handoff.decision}; a decided workspace is consumer-owned and cannot resume`,
        );
      }
      const runtime = await readRuntimeMirror(this.home, sessionId);
      if (runtime.status === "absent") {
        fail("WORKSPACE_RUNTIME_MISSING", `session "${sessionId}" has no runtime mirror to resume from`);
      }
      if (runtime.status === "corrupt") {
        fail("WORKSPACE_STATE_INCONSISTENT", `runtime mirror for "${sessionId}" is corrupt`);
      }
      if (!isTerminalRuntimeStatus(runtime.mirror.record.status)) {
        fail(
          "WORKSPACE_RUNTIME_LIVE",
          `runtime mirror for "${sessionId}" still says "${runtime.mirror.record.status}"; recovery must settle it first`,
        );
      }
      const leaseRead = await readLease(this.home, sessionId);
      if (leaseRead.status === "corrupt") {
        fail(
          "WORKSPACE_LEASE_UNCERTAIN",
          `lease file for "${sessionId}" is corrupt; ownership is unverifiable and takeover is refused`,
        );
      }
      if (leaseRead.status === "present") {
        const classification = await classifyLease(leaseRead.lease, this.probes);
        if (classification.state === "foreign-host") {
          fail(
            "WORKSPACE_LEASE_FOREIGN",
            `lease for "${sessionId}" belongs to host ${classification.owner_hostname}; takeover is refused`,
          );
        }
        if (classification.state === "hub-live") {
          fail("WORKSPACE_LIVE", `a live hub process still owns session "${sessionId}"`);
        }
        if (classification.provider.state !== "dead") {
          fail(
            "WORKSPACE_LEASE_LIVE",
            classification.provider.state === "alive"
              ? `the leased provider process for "${sessionId}" is still alive; recover or reap it before resuming`
              : `provider ownership for "${sessionId}" is uncertain (${classification.provider.reason})`,
          );
        }
      }
      const at = this.now().toISOString();
      return updateRecordCas(ctx, sessionId, rec.revision, (r) => ({
        ...r,
        custody: "live" as const,
        runtime_status: null,
        closed_at: null,
        close_evidence: null,
        last_error: null,
        revision: r.revision + 1,
        updated_at: at,
      }));
    });
  }

  /**
   * Release THIS process's lease once closure is proven by the kernel
   * mirror (`status: "closed"`, i.e. the provider shutdown was proved). A
   * released lease is what lets the owning hub resume the session or let
   * GC collect it; a lease whose provider death was NOT proven (orphaned)
   * is never released here — only recovery/GC re-proves and releases it.
   */
  async releaseClosedLease(sessionId: string): Promise<boolean> {
    const pre = await readRecordRaw(this.home, sessionId);
    if (pre.status !== "present") {
      return false;
    }
    const ctx = this.context(pre.record.repository_cwd);
    return withWorkspaceLock(ctx, sessionId, async () => {
      const lease = await readLease(this.home, sessionId);
      if (lease.status !== "present" || lease.lease.hub_pid !== process.pid) {
        return false;
      }
      const runtime = await readRuntimeMirror(this.home, sessionId);
      if (runtime.status !== "present" || runtime.mirror.record.status !== "closed") {
        return false;
      }
      await deleteLeaseFile(this.home, sessionId);
      return true;
    });
  }

  // -------------------------------------------------------------------------
  // Recovery (crash/orphan reconciliation)
  // -------------------------------------------------------------------------

  /**
   * Reconcile durable custody against what this process can prove. Pass the
   * kernel's `attached()` output: durable-but-not-attached work is orphaned
   * by hub loss and gets classified through its lease — never assumed dead.
   * Work whose provider is PROVABLY dead (or whose kernel mirror already
   * proved closure) is closed so consumers can take it over; everything else
   * is retained and reported. Recovery never deletes.
   */
  async recover(
    attached: readonly SessionRecord[] = [],
    options: { reapOrphans?: boolean } = {},
  ): Promise<RecoveryReport> {
    const attachedIds = new Set(attached.map((r) => r.session_id));
    const inventory = await scanHubHome(this.home);
    const report: RecoveryReport = {
      reconciled: [],
      live: [],
      orphans: [],
      inconsistencies: [],
      unclaimed: { worktrees: [], leases: [], runtimes: [], unknown_segments: [] },
    };
    const custody = new Set(inventory.custodyIds);
    for (const id of inventory.custodyIds) {
      const raw = await readRecordRaw(this.home, id);
      if (raw.status === "corrupt") {
        report.inconsistencies.push({ session_id: id, detail: "custody record is corrupt" });
        continue;
      }
      if (raw.status === "absent") {
        report.inconsistencies.push({ session_id: id, detail: "custody directory exists without a record" });
        const runtime = await readRuntimeMirror(this.home, id);
        if (runtime.status !== "absent") {
          report.unclaimed.runtimes.push(id);
        }
        continue;
      }
      const ctx = this.context(raw.record.repository_cwd);
      let loaded: LoadedWorkspace;
      try {
        loaded = await withWorkspaceLock(ctx, id, () => loadWorkspace(ctx, id));
      } catch (error) {
        report.inconsistencies.push({
          session_id: id,
          detail: `custody lock busy or load failed: ${error instanceof AgentHubError ? error.message : String(error)}`,
        });
        continue;
      }
      if (loaded.status === "inconsistent") {
        report.inconsistencies.push({ session_id: id, detail: loaded.detail });
        continue;
      }
      if (loaded.status === "absent") {
        continue;
      }
      if (loaded.recovery.outcome !== "none") {
        report.reconciled.push({ session_id: id, outcome: loaded.recovery.outcome, detail: loaded.recovery.detail });
      }
      const rec = loaded.record;
      const runtime = await readRuntimeMirror(this.home, id);
      if (runtime.status === "corrupt") {
        report.inconsistencies.push({ session_id: id, detail: "runtime mirror file is corrupt" });
      }
      if (rec.custody === "closed") {
        continue;
      }
      if (attachedIds.has(id)) {
        report.live.push(id);
        continue;
      }
      const runtimeRecord = runtime.status === "present" ? runtime.mirror.record : null;
      if (runtimeRecord !== null && runtimeRecord.status === "closed") {
        // The kernel proved closure (stop proven, record committed) but the
        // hub died before finalizing custody. The closure is provable; land it.
        const finalize = await this.finalizeClosure({
          session_id: id,
          evidence: "recovery: kernel mirror committed a closed record; custody finalization was never applied",
          runtime_status: "closed",
        });
        report.reconciled.push({
          session_id: id,
          outcome: "finalized",
          detail: `custody closed from proven kernel closure with ${finalize.advanced ? "a final capture" : "no final capture needed"}`,
        });
        continue;
      }

      // Live-or-unknown: classify through the lease. Nothing is assumed.
      const leaseRead = await readLease(this.home, id);
      if (leaseRead.status === "corrupt") {
        report.inconsistencies.push({
          session_id: id,
          detail: "lease file exists but is corrupt; provider ownership is unverifiable",
        });
        continue;
      }
      if (leaseRead.status === "absent") {
        report.orphans.push(
          runtime.status === "present"
            ? {
                session_id: id,
                classification: "hub-gone",
                reapable: false,
                closed_by_recovery: false,
                detail: "runtime evidence exists but no process lease was ever recorded; nothing may be assumed about a provider that was never owned",
              }
            : {
                session_id: id,
                classification: "provisioned-never-started",
                reapable: false,
                closed_by_recovery: false,
                detail: "custody was provisioned but the session never started; close explicitly when the start attempt is abandoned",
              },
        );
        continue;
      }
      const classification = await classifyLease(leaseRead.lease, this.probes);
      if (classification.state === "hub-live") {
        report.live.push(id);
        continue;
      }
      if (classification.state === "foreign-host") {
        report.orphans.push({
          session_id: id,
          classification: "foreign-host",
          reapable: false,
          closed_by_recovery: false,
          detail: `lease belongs to host ${classification.owner_hostname}; hands-off`,
        });
        continue;
      }
      const fate = classification.provider;
      if (fate.state === "dead") {
        const finalize = await this.finalizeClosure({
          session_id: id,
          evidence: `recovery: hub gone and provider ${leaseRead.lease.provider_pid} proven dead (group ${leaseRead.lease.provider_pgid} gone)`,
          runtime_status: "orphaned",
        });
        if (runtimeRecord !== null) {
          await writeRuntimeMirrorAtomic(
            this.home,
            id,
            {
              ...structuredClone(runtimeRecord),
              status: "orphaned",
              last_error: {
                code: "HUB_LOSS_ORPHAN",
                message: "recovery marked this session orphaned: the owning hub died and the provider process group is proven gone",
                stage: "state",
                retryable: false,
                provider: runtimeRecord.provider,
              },
            },
            this.now().toISOString(),
            true,
          );
        }
        report.orphans.push({
          session_id: id,
          classification: "hub-gone",
          reapable: false,
          closed_by_recovery: true,
          detail: `provider proven dead; custody closed by recovery with ${finalize.advanced ? "a final capture" : "no final capture needed"}`,
        });
        continue;
      }
      let detail = fate.state === "alive" ? "provider alive with an exact-identity lease" : fate.reason;
      let reapable = fate.state === "alive" && fate.reapable;
      if (options.reapOrphans && reapable && fate.state === "alive") {
        const reaped = await reapProviderLease(leaseRead.lease, fate, this.probes, { graceMs: 2_000, pollMs: 50 });
        detail = `reap attempted: ${reaped.status}`;
        if (reaped.status === "reaped") {
          const finalize = await this.finalizeClosure({
            session_id: id,
            evidence: `recovery: provider group reaped under exact-identity lease proof (pid ${leaseRead.lease.provider_pid})`,
            runtime_status: "orphaned",
          });
          report.orphans.push({
            session_id: id,
            classification: "hub-gone",
            reapable: false,
            closed_by_recovery: true,
            detail: `${detail}; custody closed by recovery with ${finalize.advanced ? "a final capture" : "no final capture needed"}`,
          });
          continue;
        }
        reapable = reaped.status === "survived";
      }
      report.orphans.push({
        session_id: id,
        classification: "hub-gone",
        reapable,
        closed_by_recovery: false,
        detail,
      });
    }
    for (const id of inventory.worktreeIds) {
      if (!custody.has(id)) {
        report.unclaimed.worktrees.push(worktreePath(this.home, id));
      }
    }
    for (const id of inventory.leaseIds) {
      if (!custody.has(id)) {
        report.unclaimed.leases.push(id);
      }
    }
    report.unclaimed.unknown_segments.push(...inventory.unknownSegments);
    return report;
  }

  // -------------------------------------------------------------------------
  // GC (the only deletion path)
  // -------------------------------------------------------------------------

  /**
   * Delete only fully-decided, fully-expired, fully-consistent workspaces.
   * Every retained entry names the failed precondition. Racing work (locks
   * held elsewhere, refs moved mid-pass) is retained by definition.
   */
  async gc(attached: readonly SessionRecord[] = []): Promise<GcReport> {
    const report: GcReport = {
      deleted: [],
      retained: [],
      unclaimed: { worktrees: [], leases: [], unknown_segments: [], cleanup_errors: [] },
      pruned_repositories: [],
    };
    const inventory = await scanHubHome(this.home);
    const attachedIds = new Set(attached.map((r) => r.session_id));
    const custody = new Set(inventory.custodyIds);
    const ctx = this.context(process.cwd());
    try {
      await withAdminLock(
        ctx,
        async () => {
          const pruned = new Set<string>();
          for (const id of inventory.custodyIds) {
            await this.gcOne(id, attachedIds, report, pruned);
          }
          report.pruned_repositories = [...pruned];
        },
        { waitMs: 0 },
      );
    } catch {
      report.retained.push({
        session_id: "*",
        code: "admin-locked",
        detail: "workspace-admin lock held by a concurrent admin pass; the whole GC pass was skipped",
      });
    }
    // Unclaimed custody-namespace entries: retained by default. A matching
    // tombstone proves the hub decided this deletion before a crash, so its
    // leftovers are completed; anything else is foreign and untouched.
    for (const id of inventory.worktreeIds) {
      if (custody.has(id)) {
        continue;
      }
      const tomb = await readTombstone(this.home, id);
      const path = worktreePath(this.home, id);
      if (tomb !== null && tomb.worktree_path === path) {
        await removeTree(path).catch((error) => {
          report.unclaimed.cleanup_errors.push({ path, reason: String(error) });
        });
      } else {
        report.unclaimed.worktrees.push(path);
      }
    }
    for (const id of inventory.leaseIds) {
      if (custody.has(id)) {
        continue;
      }
      const tomb = await readTombstone(this.home, id);
      if (tomb !== null) {
        await deleteLeaseFile(this.home, id).catch((error) => {
          report.unclaimed.cleanup_errors.push({ path: id, reason: String(error) });
        });
      } else {
        report.unclaimed.leases.push(id);
      }
    }
    report.unclaimed.unknown_segments.push(...inventory.unknownSegments);
    return report;
  }

  private async gcOne(
    id: string,
    attachedIds: Set<string>,
    report: GcReport,
    pruned: Set<string>,
  ): Promise<void> {
    const retain = (code: GcRetainCode, detail: string): void => {
      report.retained.push({ session_id: id, code, detail });
    };
    const tomb = await readTombstone(this.home, id);
    const raw = await readRecordRaw(this.home, id);
    if (raw.status === "absent") {
      if (tomb !== null) {
        // Interrupted deletion whose record tree removal never landed.
        await removeCustodyTree(this.home, id);
        await deleteLeaseFile(this.home, id).catch(() => undefined);
        report.deleted.push({
          session_id: id,
          last_result_seq: tomb.last_result_seq,
          head_commit: tomb.head_commit,
          decision: tomb.decision,
          continued: true,
        });
      }
      return;
    }
    if (raw.status === "corrupt") {
      retain("state-inconsistent", "custody record is corrupt and unverifiable");
      return;
    }
    let lock: RepositoryLock;
    try {
      lock = await acquireRepositoryLock({
        commonDir: this.home,
        name: `ws-${id}`,
        waitMs: this.lockWaitMs,
        retryDelayMs: 25,
        ...(this.probePid ? { probePid: this.probePid } : {}),
        now: this.now,
      });
    } catch {
      retain("locked-by-peer", "custody lock is held by a concurrent writer (racing); retained untouched");
      return;
    }
    try {
      const rec0 = raw.record;
      const ctx = this.context(rec0.repository_cwd);
      const loaded = await loadWorkspace(ctx, id);
      if (loaded.status === "inconsistent") {
        // Tombstone continuation: a decided deletion whose ref already moved
        // on is finished, not stranded.
        if (
          tomb !== null
          && tomb.head_commit === rec0.head_commit
          && (await probeRef(rec0.repository_cwd, rec0.ref)) === null
        ) {
          await completeDeletion(id, rec0, tomb, pruned);
          report.deleted.push({
            session_id: id,
            last_result_seq: tomb.last_result_seq,
            head_commit: tomb.head_commit,
            decision: tomb.decision,
            continued: true,
          });
          return;
        }
        retain("state-inconsistent", loaded.detail);
        return;
      }
      if (loaded.status === "absent") {
        return;
      }
      const rec = loaded.record;
      if (attachedIds.has(rec.session_id)) {
        retain("runtime-attached", "the kernel still runs this session in this process");
        return;
      }
      const runtime = await readRuntimeMirror(this.home, id);
      if (runtime.status === "corrupt") {
        retain("state-inconsistent", "runtime mirror file exists but is corrupt");
        return;
      }
      if (runtime.status === "present" && !isTerminalRuntimeStatus(runtime.mirror.record.status)) {
        retain("runtime-live", `kernel mirror still says "${runtime.mirror.record.status}"`);
        return;
      }
      if (rec.handoff === null) {
        retain("handoff-undecided", "no accepted/discarded handoff decision: results cannot disappear before consumer takeover");
        return;
      }
      const until = Date.parse(rec.retention_until ?? "");
      if (!Number.isFinite(until) || this.now().getTime() < until) {
        retain("retention-active", `retention runs until ${rec.retention_until ?? "(unset)"}`);
        return;
      }
      const lease = await readLease(this.home, id);
      if (lease.status === "corrupt") {
        retain("lease-uncertain", "lease file exists but is corrupt; ownership is unverifiable");
        return;
      }
      if (lease.status === "present") {
        const classification = await classifyLease(lease.lease, this.probes);
        if (classification.state === "foreign-host") {
          retain("lease-foreign", `lease names host ${classification.owner_hostname}`);
          return;
        }
        if (classification.state === "hub-live") {
          retain("hub-live", "another hub process is still live for this workspace");
          return;
        }
        const fate = classification.provider;
        if (fate.state !== "dead") {
          retain(
            fate.state === "alive" ? "lease-live" : "lease-uncertain",
            fate.state === "alive" ? "the leased provider process is still alive" : fate.reason,
          );
          return;
        }
      }
      const expectedRef = rec.head_commit === rec.base_commit ? null : rec.head_commit;
      let refNow: string | null;
      try {
        refNow = await probeRef(rec.repository_cwd, rec.ref);
      } catch {
        retain("repository-unreachable", `cannot probe ${rec.repository_cwd}; the repository itself is unreachable`);
        return;
      }
      if (refNow !== expectedRef) {
        retain("ref-diverged", `ref now points at ${refNow ?? "(absent)"} but custody recorded ${rec.head_commit}`);
        return;
      }
      if (!isInside(custodialWorktreeRoot(this.home), rec.worktree_path)
        || rec.worktree_path !== worktreePath(this.home, id)) {
        retain("worktree-not-custodial", `worktree path ${rec.worktree_path} is not the hub-owned custody path for this session`);
        return;
      }
      for (let seq = 1; seq <= rec.last_result_seq; seq += 1) {
        const res = await readResult(this.home, id, seq);
        if (res === "absent" || res === "corrupt") {
          retain("results-missing", `result ${seq} of ${rec.last_result_seq} is ${res}`);
          return;
        }
        const reachable = await commitExists(rec.repository_cwd, res.commit).catch(() => false);
        if (!reachable) {
          retain("commit-unverifiable", `result ${seq} names commit ${res.commit}, which the repository no longer proves`);
          return;
        }
      }
      const worktree = await inspectWorktree(rec.repository_cwd, rec.worktree_path).catch(() => ({
        state: "foreign" as const,
        reason: "inspection failed",
      }));
      if (worktree.state === "foreign") {
        retain("worktree-foreign", "the recorded worktree path is no longer this repository's worktree");
        return;
      }
      // Deletion order, crash-safe by tombstone continuation:
      // tombstone → ref CAS-delete → worktree remove → prune → custody tree → lease.
      await writeTombstone(this.home, rec, this.now);
      if (refNow !== null) {
        const refGone = await casDeleteRef(rec.repository_cwd, rec.ref, rec.head_commit);
        if (!refGone) {
          retain("ref-diverged", "the ref moved between probe and CAS-delete (racing)");
          return;
        }
      }
      const removal = await removeWorktree(rec.repository_cwd, worktreePath(this.home, id));
      if (!removal.removed) {
        retain("worktree-remove-failed", removal.reason);
        return;
      }
      pruned.add(rec.repository_cwd);
      await pruneWorktrees(rec.repository_cwd).catch(() => undefined);
      await removeCustodyTree(this.home, id);
      await deleteLeaseFile(this.home, id);
      report.deleted.push({
        session_id: id,
        last_result_seq: rec.last_result_seq,
        head_commit: rec.head_commit,
        decision: rec.handoff.decision,
        continued: false,
      });
    } catch (error) {
      retain(
        "state-inconsistent",
        error instanceof AgentHubError ? `${error.code}: ${error.message}` : String(error),
      );
    } finally {
      await lock.release();
    }
  }

  // -------------------------------------------------------------------------
  // Reads
  // -------------------------------------------------------------------------

  async list(): Promise<WorkspaceRecord[]> {
    const inventory = await scanHubHome(this.home);
    const out: WorkspaceRecord[] = [];
    for (const id of inventory.custodyIds) {
      const raw = await readRecordRaw(this.home, id);
      if (raw.status === "present") {
        out.push(raw.record);
      }
    }
    return out;
  }

  async inspect(sessionId: string): Promise<WorkspaceInspection> {
    const raw = await readRecordRaw(this.home, sessionId);
    if (raw.status === "absent") {
      fail("WORKSPACE_NOT_FOUND", `no custody record for session "${sessionId}"`);
    }
    if (raw.status === "corrupt") {
      fail("WORKSPACE_STATE_INCONSISTENT", `custody record for "${sessionId}" is corrupt`);
    }
    const rec = raw.record;
    const runtime = await readRuntimeMirror(this.home, sessionId);
    const leaseRead = await readLease(this.home, sessionId);
    const lease =
      leaseRead.status === "absent"
        ? ({ state: "absent" } as const)
        : leaseRead.status === "corrupt"
          ? ({ state: "corrupt" } as const)
          : await classifyLease(leaseRead.lease, this.probes);
    const worktree = await inspectWorktree(rec.repository_cwd, rec.worktree_path).catch(
      () => ({ state: "foreign" as const, reason: "inspection failed" }) as const,
    );
    const results: { seq: number; result: WorkspaceResultRecord | "corrupt" }[] = [];
    for (let seq = 1; seq <= rec.last_result_seq; seq += 1) {
      const res = await readResult(this.home, sessionId, seq);
      results.push({ seq, result: res === "absent" || res === "corrupt" ? "corrupt" : res });
    }
    return {
      workspace: rec,
      runtime:
        runtime.status === "absent"
          ? { state: "absent" as const }
          : runtime.status === "corrupt"
            ? { state: "corrupt" as const }
            : {
                state: "present" as const,
                record: runtime.mirror.record,
                rewritten_by_recovery: runtime.mirror.rewritten_by_recovery,
              },
      lease,
      worktree,
      results,
    };
  }

  async result(sessionId: string, seq: number): Promise<WorkspaceResultRecord> {
    const res = await readResult(this.home, sessionId, seq);
    if (res === "absent") {
      fail("WORKSPACE_RESULT_NOT_FOUND", `session "${sessionId}" has no result ${seq}`);
    }
    if (res === "corrupt") {
      fail("WORKSPACE_STATE_INCONSISTENT", `result ${seq} for "${sessionId}" is corrupt`);
    }
    return res;
  }

  // -------------------------------------------------------------------------
  // Internals
  // -------------------------------------------------------------------------

  private context(repositoryCwd: string): StoreContext {
    return {
      home: this.home,
      repositoryCwd,
      now: this.now,
      lockWaitMs: this.lockWaitMs,
      ...(this.probePid ? { probePid: this.probePid } : {}),
    };
  }
}

// ---------------------------------------------------------------------------
// Module-local helpers
// ---------------------------------------------------------------------------

async function readTombstone(home: string, sessionId: string): Promise<Tombstone | null> {
  const raw = await readJsonFile(tombstonePath(home, sessionId));
  if (typeof raw !== "object" || raw === null) {
    return null;
  }
  const v = raw as Record<string, unknown>;
  if (v.schema !== TOMBSTONE_SCHEMA) {
    return null;
  }
  if (
    typeof v.session_id !== "string"
    || !isWorkspaceSessionId(v.session_id)
    || typeof v.worktree_path !== "string"
    || typeof v.head_commit !== "string"
  ) {
    return null;
  }
  return {
    schema: TOMBSTONE_SCHEMA,
    session_id: v.session_id,
    hub_home: typeof v.hub_home === "string" && v.hub_home.startsWith("/") ? v.hub_home : "",
    repository_cwd: typeof v.repository_cwd === "string" ? v.repository_cwd : "",
    worktree_path: v.worktree_path,
    head_commit: v.head_commit,
    last_result_seq: typeof v.last_result_seq === "number" ? v.last_result_seq : 0,
    decision: v.decision === "accepted" || v.decision === "discarded" ? v.decision : "discarded",
    deleted_at: typeof v.deleted_at === "string" ? v.deleted_at : "",
  };
}

async function writeTombstone(
  home: string,
  rec: WorkspaceRecord,
  now: () => Date,
): Promise<void> {
  const tombstone: Tombstone = {
    schema: TOMBSTONE_SCHEMA,
    session_id: rec.session_id,
    hub_home: home,
    repository_cwd: rec.repository_cwd,
    worktree_path: rec.worktree_path,
    head_commit: rec.head_commit,
    last_result_seq: rec.last_result_seq,
    decision: rec.handoff!.decision,
    deleted_at: now().toISOString(),
  };
  await writeJsonAtomic(tombstonePath(home, rec.session_id), tombstone);
}

/**
 * Finish a tombstoned deletion whose custody record still reads inconsistent
 * (typically ref-gone-but-record-survives). The tombstone is the hub's own
 * prior decision — its `hub_home` and `worktree_path` are the only facts
 * trusted for where deletion may continue.
 */
async function completeDeletion(
  sessionId: string,
  rec: WorkspaceRecord,
  tomb: Tombstone,
  pruned: Set<string>,
): Promise<void> {
  if (tomb.hub_home === "") {
    fail("WORKSPACE_PATH_UNSAFE", `tombstone for ${sessionId} names no hub home; nothing may be deleted`);
  }
  if (
    rec.worktree_path !== worktreePath(tomb.hub_home, sessionId)
    || !isInside(custodialWorktreeRoot(tomb.hub_home), rec.worktree_path)
  ) {
    fail("WORKSPACE_PATH_UNSAFE", `refusing to complete deletion of non-custodial path ${rec.worktree_path}`);
  }
  await removeWorktree(rec.repository_cwd, tomb.worktree_path);
  pruned.add(rec.repository_cwd);
  await pruneWorktrees(rec.repository_cwd).catch(() => undefined);
  await removeCustodyTree(tomb.hub_home, sessionId);
  await deleteLeaseFile(tomb.hub_home, sessionId).catch(() => undefined);
}

/** Seed SessionRecord for a dead orphan that never committed a runtime mirror. */
export function orphanSeedRecord(rec: WorkspaceRecord): SessionRecord {
  const capabilities = Object.fromEntries(
    ALL_CAPABILITY_NAMES.map((name) => [name, { support: "unsupported", evidence: null }]),
  ) as SessionRecord["capabilities"];
  return {
    schema: "agent-hub-interaction/v1",
    session_id: rec.session_id,
    provider: rec.provider ?? "unknown",
    transport: rec.transport ?? "unknown",
    capabilities,
    workspace: rec.worktree_path,
    max_text_bytes: 65_536,
    resume: null,
    status: "orphaned",
    revision: 0,
    last_error: null,
    created_at: rec.created_at,
    updated_at: rec.updated_at,
  };
}
