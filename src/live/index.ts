import { AgentHubError, asDelegateError } from "../errors.js";
import { isLiveRecord } from "./provider-registry.js";
import { createLiveManager } from "./bootstrap.js";
import type {
  LiveCapabilities,
  LiveCommandKind,
  LiveError,
  LiveErrorStage,
  LiveEvent,
  LiveProviderId,
  LiveSessionState,
  LiveStatus,
  LiveStopMode,
  LiveStopReport,
  LiveTransportId,
  LiveTurnResult,
} from "./types.js";

/**
 * v3 live Agent Hub — package façade.
 *
 * The ONE authoritative production manager is the safety core in
 * `manager.ts`; it is re-exported here unchanged (plus the production
 * bootstrap in `bootstrap.ts`). This module adds only surface-facing glue:
 * the CLI wire protocol (`runLiveSession`) and shared helpers. It owns no
 * session state, no persistence, and no second lifecycle — everything
 * durable goes through the core manager.
 */

export { LiveSessionManager } from "./manager.js";
export {
  LIVE_COMMON_DIR_SESSION_QUOTA,
  LIVE_DEFAULT_MAX_TEXT_BYTES,
  LIVE_FOLLOW_UP_MAX_MESSAGE_BYTES,
  LIVE_FOLLOW_UP_QUEUE_MAX_BYTES,
  LIVE_FOLLOW_UP_QUEUE_MAX_MESSAGES,
  LIVE_PROCESS_SESSION_QUOTA,
  type LiveCloseResult,
  type LiveManagerOptions,
  type LiveManagerPhase,
  type LiveRecoveryReport,
  type LiveRecoverySessionReport,
  type LiveResumeFromStateRequest,
  type LiveStartRequest,
  type LiveStartResult,
} from "./manager.js";
export {
  createLiveManager,
  durableLiveResumeSource,
  productionProviderFactories,
  productionTransportFactories,
  registerProductionLiveTransports,
  wireDurableLiveResumeSource,
  type CreateLiveManagerOptions,
} from "./bootstrap.js";
export {
  assertLiveSessionState,
  getLiveResumeSource,
  isLiveProvider,
  isLiveRecord,
  LIVE_TRANSPORT_PAIRINGS,
  liveTransportRegistry,
  LiveTransportRegistry,
  probeLiveAgent,
  registerLiveTransport,
  setLiveResumeSource,
  supportedLiveAgents,
  unwiredLiveResumeSource,
} from "./provider-registry.js";
export type { LiveProbeDocument, LiveResumeSource } from "./provider-registry.js";
export {
  LIVE_ADMIN_LOCK_NAME,
  LIVE_REF_NAMESPACE,
  LIVE_SCHEMA_VERSION,
  LIVE_STATE_SUBDIR,
  liveRefFor,
  liveStatePath,
  liveStateRoot,
  livePendingPath,
  loadLiveState,
  parseLiveSessionState,
  withLiveLock,
} from "./state.js";
export {
  classifyLiveLease,
  createLiveLease,
  liveLeasePath,
  listLiveLeases,
  readLiveLease,
  removeLiveLease,
  type LiveLeaseRecord,
} from "./lease.js";
export type {
  CapabilitySupport,
  CheckpointReason,
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
  LiveStatus,
  LiveStopMode,
  LiveStopReport,
  LiveTransport,
  LiveTransportDescriptor,
  LiveTransportFactory,
  LiveTransportId,
  LiveTurnResult,
  LiveUsage,
  ProviderResumeState,
  ResumeVerification,
} from "./types.js";

/** How long in-flight commands may finish before the runner stops the provider. */
export const LIVE_CLOSE_DRAIN_MS = 1_000;

/** Permission verdicts the v3 wire accepts — exactly the durable vocabulary. */
const PERMISSION_DECISIONS: readonly string[] = ["allow_once", "deny"];

export function isTerminalLiveStatus(status: LiveStatus): boolean {
  return status === "closed" || status === "error" || status === "orphaned";
}

export function liveError(
  code: string,
  message: string,
  stage: LiveErrorStage,
  retryable: boolean,
  provider: LiveProviderId | null,
): LiveError {
  return { code, message, stage, retryable, provider };
}

/** Normalizes anything thrown across a seam into the seed's error shape. */
export function toLiveError(
  error: unknown,
  context: { stage: LiveErrorStage; provider: LiveProviderId | null },
): LiveError {
  if (isLiveRecord(error) && typeof error.code === "string" && typeof error.message === "string") {
    const stage =
      typeof error.stage === "string" &&
      ["probe", "launch", "transport", "provider", "protocol", "capability", "checkpoint", "state", "shutdown"].includes(
        error.stage,
      )
        ? (error.stage as LiveErrorStage)
        : context.stage;
    const provider =
      error.provider === null || typeof error.provider === "string"
        ? (error.provider as LiveProviderId | null)
        : context.provider;
    return {
      code: error.code,
      message: error.message,
      stage,
      retryable: error.retryable === true,
      provider,
    };
  }
  const { code, message } = asDelegateError(error);
  return liveError(code, message, context.stage, false, context.provider);
}

/**
 * Runtime gate on a transport's capability snapshot. The seed's `Record` type
 * makes a missing claim a compile error; this gate enforces the honesty rule
 * at the boundary: every non-`unsupported` claim must carry evidence, and
 * only the nine contract names may carry claims at all.
 */
export function validateLiveCapabilities(value: unknown): LiveCapabilities {
  if (!isLiveRecord(value)) {
    throw new AgentHubError(
      "LIVE_CAPABILITY_EVIDENCE_INVALID",
      "capability snapshot must be an object",
    );
  }
  const names: readonly string[] = [
    "prompt",
    "follow_up",
    "steer",
    "cancel",
    "status",
    "permission_response",
    "resume",
    "checkpoint",
    "usage_reporting",
  ];
  const supports: readonly string[] = ["native", "hub-queued", "derived", "signal", "unsupported"];
  for (const name of names) {
    const claim = value[name];
    if (
      !isLiveRecord(claim) ||
      typeof claim.support !== "string" ||
      !supports.includes(claim.support)
    ) {
      throw new AgentHubError(
        "LIVE_CAPABILITY_EVIDENCE_INVALID",
        `capability claim "${name}" is missing or malformed`,
      );
    }
    if (claim.support === "unsupported") {
      if (claim.evidence !== null) {
        throw new AgentHubError(
          "LIVE_CAPABILITY_EVIDENCE_INVALID",
          `capability claim "${name}" must carry null evidence when unsupported`,
        );
      }
      continue;
    }
    if (typeof claim.evidence !== "string" || claim.evidence.trim().length === 0) {
      throw new AgentHubError(
        "LIVE_CAPABILITY_EVIDENCE_INVALID",
        `capability claim "${name}" (${claim.support}) must carry non-empty evidence`,
      );
    }
  }
  return value as unknown as LiveCapabilities;
}

// ---------------------------------------------------------------------------
// CLI runner: long-lived stdin NDJSON ↔ stdout normalized documents
// ---------------------------------------------------------------------------

/** One `agent-hub live …` run: exactly one of provider/resumeId is set. */
export interface LiveLaunchInvocation {
  provider: LiveProviderId | null;
  resumeId: string | null;
  workspace: string;
  maxTextBytes?: number;
}

export interface LiveIo {
  /** The command stream, already split into lines. */
  stdin: AsyncIterable<string>;
  /** One complete NDJSON document per call (the newline is added by CLI). */
  stdout: (document: Record<string, unknown>) => void;
  stderr: (line: string) => void;
}

export interface LiveRunnerDependencies {
  /**
   * Injected CORE manager (focused tests). Production always omits this and
   * lets `createManager` build one from the requested workspace.
   */
  manager?: LiveSessionManagerLike;
  createManager?: (workspace: string) => Promise<LiveSessionManagerLike>;
}

/**
 * The exact core-manager surface the runner drives. Typed structurally so a
 * production `LiveSessionManager` from `manager.ts` satisfies it directly.
 */
interface LiveSessionManagerLike {
  start(request: {
    provider: LiveProviderId;
    transport?: LiveTransportId;
    session_id?: string | null;
    max_text_bytes?: number;
  }): Promise<{
    live_session_id: string;
    state: LiveSessionState;
    workspace: string;
    warnings?: { code: string; message: string }[];
  }>;
  resumeFromState(request: {
    live_session_id: string;
    transport?: LiveTransportId;
    max_text_bytes?: number;
  }): Promise<{
    live_session_id: string;
    state: LiveSessionState;
    workspace: string;
    warnings?: { code: string; message: string }[];
  }>;
  prompt(id: string, text: string): Promise<LiveTurnResult>;
  followUp(id: string, text: string): Promise<LiveTurnResult>;
  steer(id: string, text: string): Promise<LiveTurnResult>;
  cancel(id: string, reason: string | null): Promise<LiveTurnResult>;
  requestStatus(id: string): Promise<LiveTurnResult>;
  respondPermission(
    id: string,
    requestId: string,
    decision: "allow_once" | "deny",
    note: string | null,
  ): Promise<LiveTurnResult>;
  view(id: string): LiveSessionState;
  eventsAfter(id: string, cursor: number): { events: LiveEvent[]; next_cursor: number };
  eventCursor(id: string): number;
  close(
    id: string,
  ): Promise<{
    state: LiveSessionState;
    stop: LiveStopReport | null;
    checkpoint_taken: boolean;
    cleanup_errors: { code: string; message: string }[];
  }>;
}

/** Splits an arbitrary chunk stream into lines (final partial line included). */
export async function* iterateLiveCommands(chunks: AsyncIterable<unknown>): AsyncGenerator<string> {
  let pending = "";
  for await (const chunk of chunks) {
    pending +=
      typeof chunk === "string"
        ? chunk
        : Buffer.isBuffer(chunk)
          ? chunk.toString("utf8")
          : String(chunk);
    let newline = pending.indexOf("\n");
    while (newline >= 0) {
      yield pending.slice(0, newline).replace(/\r$/, "");
      pending = pending.slice(newline + 1);
      newline = pending.indexOf("\n");
    }
  }
  if (pending.length > 0) {
    yield pending.replace(/\r$/, "");
  }
}

type LiveWireAction = LiveCommandKind | "close";

const LIVE_WIRE_ACTIONS: readonly LiveWireAction[] = [
  "prompt",
  "follow_up",
  "steer",
  "cancel",
  "status",
  "permission_response",
  "close",
];

interface LiveWireCommand {
  action: LiveWireAction;
  text: string | null;
  reason: string | null;
  request_id: string | null;
  decision: "allow_once" | "deny" | null;
  note: string | null;
  terminate: boolean;
}

function parseLiveWireLine(
  line: string,
): { ok: true; command: LiveWireCommand } | { ok: false; message: string } {
  let document: unknown;
  try {
    document = JSON.parse(line);
  } catch {
    return { ok: false, message: "stdin line is not valid JSON" };
  }
  if (!isLiveRecord(document)) {
    return { ok: false, message: "stdin line must be a JSON object" };
  }
  const action = document.action;
  if (typeof action !== "string" || !(LIVE_WIRE_ACTIONS as readonly string[]).includes(action)) {
    return {
      ok: false,
      message: `unknown live command action ${JSON.stringify(action)}; expected one of ${LIVE_WIRE_ACTIONS.join(", ")}`,
    };
  }
  for (const field of ["text", "reason", "note", "request_id"] as const) {
    const value = document[field];
    if (value !== undefined && value !== null && typeof value !== "string") {
      return { ok: false, message: `"${field}" must be a string or null` };
    }
  }
  let decision: "allow_once" | "deny" | null = null;
  if (document.decision !== undefined && document.decision !== null) {
    // v3 accepts only allow_once / deny. Anything else (any "session-wide
    // allow" wording included) is a caller error — rejected here, never
    // silently converted into deny.
    if (typeof document.decision !== "string" || !PERMISSION_DECISIONS.includes(document.decision)) {
      return { ok: false, message: '"decision" must be allow_once or deny' };
    }
    decision = document.decision as "allow_once" | "deny";
  }
  const terminate = document.terminate ?? false;
  if (typeof terminate !== "boolean") {
    return { ok: false, message: '"terminate" must be a boolean' };
  }
  return {
    ok: true,
    command: {
      action: action as LiveWireAction,
      text: (document.text as string | null | undefined) ?? null,
      reason: (document.reason as string | null | undefined) ?? null,
      request_id: (document.request_id as string | null | undefined) ?? null,
      decision,
      note: (document.note as string | null | undefined) ?? null,
      terminate,
    },
  };
}

/**
 * Runs one long-lived live session over the hub wire against the CORE
 * manager: stdin carries one NDJSON command per line, stdout receives
 * `{type:"session"|"event"|"result"|"error"|"close"}` documents, stderr
 * carries human diagnostics. Exit: 0 clean, 1 structured failure (launch
 * refusal, failed command, or orphaned end).
 */
export async function runLiveSession(
  invocation: LiveLaunchInvocation,
  io: LiveIo,
  dependencies: LiveRunnerDependencies = {},
): Promise<number> {
  const context: { stage: LiveErrorStage; provider: LiveProviderId | null } = {
    stage: "launch",
    provider: invocation.provider,
  };

  let manager: LiveSessionManagerLike;
  let started: Awaited<ReturnType<LiveSessionManagerLike["start"]>>;
  try {
    manager =
      dependencies.manager ??
      (await (dependencies.createManager ?? ((workspace) => createLiveManager(workspace)))(
        invocation.workspace,
      ));
    if (invocation.resumeId !== null) {
      started = await manager.resumeFromState({
        live_session_id: invocation.resumeId,
        max_text_bytes: invocation.maxTextBytes,
      });
      context.provider = started.state.provider;
    } else {
      if (invocation.provider === null) {
        throw new AgentHubError("LIVE_COMMAND_INVALID", "a live session needs a provider or a resume id");
      }
      started = await manager.start({
        provider: invocation.provider,
        max_text_bytes: invocation.maxTextBytes,
      });
    }
  } catch (error) {
    io.stdout({ type: "error", error: toLiveError(error, context) });
    return 1;
  }

  const sessionId = started.live_session_id;
  io.stderr(
    `agent-hub live: session=${sessionId} provider=${started.state.provider} transport=${started.state.transport} worktree=${started.workspace}`,
  );
  io.stdout({
    type: "session",
    session: {
      live_session_id: started.state.live_session_id,
      session_id: started.state.session_id,
      provider: started.state.provider,
      transport: started.state.transport,
      status: started.state.status,
      workspace: started.workspace,
      base_commit: started.state.base_commit,
      current_commit: started.state.current_commit,
      capabilities: started.state.capabilities,
      warnings: started.warnings ?? [],
    },
  });

  let cursor = 0;
  let relayStopped = false;
  let idleSpins = 0;
  const emitEvents = (): boolean => {
    for (;;) {
      let page: { events: LiveEvent[]; next_cursor: number };
      try {
        page = manager.eventsAfter(sessionId, cursor);
      } catch (error) {
        if (asDelegateError(error).code === "LIVE_SESSION_NOT_FOUND") {
          // The runner's own `close` just tore the session (and its ring)
          // down. That is this runner's terminal path, not ring expiry:
          // stop the relay quietly — never a spurious error document and
          // never a second manager call that would re-throw.
          relayStopped = true;
          return false;
        }
        // Honest ring expiry: the runner never silently drops events; it
        // resynchronizes the cursor from the manager and says so.
        io.stdout({
          type: "error",
          error: toLiveError(error, { stage: "transport", provider: started.state.provider }),
        });
        try {
          cursor = manager.eventCursor(sessionId);
        } catch {
          relayStopped = true;
        }
        return false;
      }
      if (page.events.length === 0) {
        cursor = page.next_cursor;
        return false;
      }
      for (const event of page.events) {
        cursor = event.seq;
        io.stdout({ type: "event", event });
      }
      cursor = page.next_cursor;
      if (page.events.length < 100) {
        return true;
      }
    }
  };
  const relay = (async () => {
    while (!relayStopped) {
      if (emitEvents()) {
        idleSpins = 0;
        continue;
      }
      let status: LiveStatus = "orphaned";
      try {
        status = manager.view(sessionId).status;
      } catch {
        status = "orphaned";
      }
      if (isTerminalLiveStatus(status)) {
        break;
      }
      idleSpins += 1;
      // No progress: re-check after a brief wait so a wedged pump cannot
      // spin; after 400 quiet polls (≈10 s) stop relaying, not the session.
      if (idleSpins > 400) {
        break;
      }
      // Ref'd: the relay poll keeps a closing runner's event loop alive
      // until the close report lands.
      await new Promise<void>((resolve) => setTimeout(resolve, 25));
    }
    // Final drain: no recorded event may fail to reach stdout before close.
    while (emitEvents()) {
      // drained
    }
  })();

  const dispatchCommand = async (command: LiveWireCommand): Promise<LiveTurnResult> => {
    switch (command.action) {
      case "prompt":
        if (typeof command.text !== "string") {
          throw new AgentHubError("LIVE_COMMAND_INVALID", 'command "prompt" requires a "text" string');
        }
        return manager.prompt(sessionId, command.text);
      case "follow_up":
        if (typeof command.text !== "string") {
          throw new AgentHubError(
            "LIVE_COMMAND_INVALID",
            'command "follow_up" requires a "text" string',
          );
        }
        return manager.followUp(sessionId, command.text);
      case "steer":
        if (typeof command.text !== "string") {
          throw new AgentHubError("LIVE_COMMAND_INVALID", 'command "steer" requires a "text" string');
        }
        return manager.steer(sessionId, command.text);
      case "cancel":
        return manager.cancel(sessionId, command.reason);
      case "status":
        return manager.requestStatus(sessionId);
      case "permission_response": {
        if (typeof command.request_id !== "string" || command.request_id.length === 0) {
          throw new AgentHubError(
            "LIVE_COMMAND_INVALID",
            'command "permission_response" requires "request_id"',
          );
        }
        if (command.decision === null) {
          throw new AgentHubError(
            "LIVE_COMMAND_INVALID",
            'command "permission_response" requires decision allow_once or deny',
          );
        }
        return manager.respondPermission(sessionId, command.request_id, command.decision, command.note);
      }
      case "close":
        throw new AgentHubError("LIVE_COMMAND_INVALID", "close is not a deliverable command");
    }
  };

  const pendingResults: Promise<void>[] = [];
  let failedSeen = false;
  let terminate = false;
  for await (const line of io.stdin) {
    const trimmed = line.trim();
    if (trimmed.length === 0) {
      continue;
    }
    const parsed = parseLiveWireLine(trimmed);
    if (!parsed.ok) {
      io.stdout({
        type: "error",
        error: liveError(
          "LIVE_COMMAND_INVALID",
          parsed.message,
          "protocol",
          false,
          started.state.provider,
        ),
      });
      continue;
    }
    if (parsed.command.action === "close") {
      terminate = parsed.command.terminate;
      break;
    }
    const pending = dispatchCommand(parsed.command)
      .then(
        (result: LiveTurnResult) => {
          if (result.outcome === "failed") {
            failedSeen = true;
          }
          let status: LiveStatus | string = result.outcome;
          try {
            status = manager.view(sessionId).status;
          } catch {
            status = "closed";
          }
          io.stdout({ type: "result", result, status });
        },
        (error: unknown) => {
          io.stdout({
            type: "error",
            error: toLiveError(error, { stage: "protocol", provider: started.state.provider }),
          });
        },
      );
    pendingResults.push(pending);
  }

  // Terminal: stop accepting input, let in-flight commands finish (a
  // permission answer mid-grace must reach the provider before it dies),
  // then stop the provider and report the close honestly.
  await Promise.race([
    Promise.allSettled(pendingResults),
    new Promise<void>((resolve) => {
      // Ref'd: this grace is the process's only pending work while closing.
      setTimeout(resolve, LIVE_CLOSE_DRAIN_MS);
    }),
  ]);
  let closeDocument: {
    live_session_id: string;
    status: LiveStatus;
    stop: LiveStopReport | null;
    checkpoint_taken: boolean;
    cleanup_errors: { code: string; message: string }[];
  };
  let finalStatus: LiveStatus;
  try {
    const close = await manager.close(sessionId);
    finalStatus = close.state.status;
    closeDocument = {
      live_session_id: sessionId,
      status: close.state.status,
      stop: close.stop,
      checkpoint_taken: close.checkpoint_taken,
      cleanup_errors: close.cleanup_errors,
    };
  } catch (error) {
    finalStatus = "orphaned";
    closeDocument = {
      live_session_id: sessionId,
      status: "orphaned",
      stop: { status: "orphaned", exit_code: null, exit_signal: null, waited_ms: 0 },
      checkpoint_taken: false,
      cleanup_errors: [asDelegateError(error)],
    };
    io.stderr(`agent-hub live: close failed: ${asDelegateError(error).message}`);
  }
  await Promise.allSettled(pendingResults);
  relayStopped = true;
  await relay;

  io.stdout({ type: "close", close: closeDocument });
  const stopStatus = closeDocument.stop?.status ?? "orphaned";
  io.stderr(`agent-hub live: session=${sessionId} ended status=${finalStatus} stop=${stopStatus}`);
  return failedSeen || stopStatus === "orphaned" || finalStatus === "error" || finalStatus === "orphaned"
    ? 1
    : 0;
}
