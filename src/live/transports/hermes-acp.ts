import { Readable, Writable } from "node:stream";


import { ClientSideConnection, ndJsonStream } from "@zed-industries/agent-client-protocol";
import type {
  Client,
  PermissionOption,
  RequestPermissionRequest,
  RequestPermissionResponse,
  SessionNotification,
} from "@zed-industries/agent-client-protocol";

import { launchLiveChild, type LiveChildHandle } from "../child-process.js";
import { deferred, type Deferred } from "../../deferred.js";
import type {
  HermesResumeState,
  LiveBoundedText,
  LiveCapabilities,
  LiveCommand,
  LiveError,
  LiveEvent,
  LiveEventBody,
  LiveFollowUpCommand,
  LiveLaunchReport,
  LiveLaunchRequest,
  LivePermissionDecision,
  LiveProbeResult,
  LiveStatus,
  LiveStopMode,
  LiveStopReport,
  LiveTransport,
  LiveTransportDescriptor,
  LiveTransportFactory,
  LiveTransportId,
} from "../types.js";
import { HERMES_ACP_ARGS, probeHermes, resolveHermesCommand } from "../probes/hermes.js";

/**
 * `hermes-acp` live transport (Package 3, Hermes 0.20.5 contract).
 *
 * Launches `hermes acp` (argv, `shell: false`) speaking ACP v1 over stdio via
 * the official TypeScript SDK (`@zed-industries/agent-client-protocol@0.4.5`,
 * exact-pinned). The runtime handshake is verified, not assumed:
 * `initialize` must negotiate `protocolVersion: 1`, and resume rides
 * `session/load`, permitted only when the agent's own `initialize` response
 * advertised `loadSession` — otherwise launch fails instead of silently
 * starting fresh.
 * Permission policy (binding):
 *   - headless sessions deny every `session/request_permission`;
 *   - interactive sessions wait for the hub's answer for at most
 *     `permission_timeout_ms` (default 60s), then deny;
 *   - v3 speaks exactly two verdicts: `allow_once` selects the agent's own
 *     `allow_once` option (never another kind), `deny` selects `reject_once`
 *     or cancels; `allow_always` is never selected, and any wider verdict is
 *     rejected at the surface boundary before it can reach this transport;
 *   - a turn cancelled while a permission is pending answers `cancelled`.
 *
 * ACP v1 `session/prompt` responses carry no usage counters, so
 * `usage_reporting` is honestly `unsupported`.
 */

const ACP_PROTOCOL_VERSION = 1;
const HANDSHAKE_TIMEOUT_MS = 15_000;
const GRACEFUL_EXIT_MS = 5_000;
const TERMINATE_STEP_MS = 1_500;
const DEFAULT_PERMISSION_TIMEOUT_MS = 60_000;
const ACP_SDK_VERSION = "0.4.5";

export class LiveTransportError extends Error {
  readonly live_error: LiveError;

  constructor(liveErrorValue: LiveError) {
    super(liveErrorValue.message);
    this.name = "LiveTransportError";
    this.live_error = liveErrorValue;
  }
}

function liveError(
  stage: LiveError["stage"],
  code: string,
  message: string,
  retryable: boolean,
): LiveError {
  return { code, message, stage, retryable, provider: "hermes" };
}

function boundText(text: string, maxBytes: number): LiveBoundedText {
  const buffer = Buffer.from(text, "utf8");
  if (buffer.byteLength <= maxBytes) {
    return { text, truncated: false };
  }
  let cut = new TextDecoder("utf-8", { fatal: false }).decode(buffer.subarray(0, maxBytes));
  if (cut.endsWith("\uFFFD")) {
    cut = cut.slice(0, -1);
  }
  return { text: cut, truncated: true };
}

type LivePromptOrFollowUp = Extract<LiveCommand, { kind: "prompt" | "follow_up" }>;

interface TurnState {
  command: LivePromptOrFollowUp;
  startedAt: string;
  streamId: string;
  sawAssistantText: boolean;
}

interface PendingPermission {
  requestId: string;
  options: PermissionOption[];
  timer: NodeJS.Timeout | undefined;
  resolve: (response: RequestPermissionResponse) => void;
}

/** Single-consumer, gapless event pump (same discipline as agy-stream-json). */
class EventSink {
  private readonly events: LiveEvent[] = [];
  private readonly waiters: Deferred<void>[] = [];
  private done = false;
  private seq = 0;
  private iterator: AsyncGenerator<LiveEvent> | null = null;

  constructor(
    private readonly liveSessionId: string,
    private readonly transportId: LiveTransportId,
  ) {}

  push(body: LiveEventBody): void {
    if (this.done) {
      return;
    }
    this.seq += 1;
    this.events.push({
      live_session_id: this.liveSessionId,
      seq: this.seq,
      transport: this.transportId,
      occurred_at: new Date().toISOString(),
      body,
    });
    this.waiters.shift()?.resolve();
  }

  close(): void {
    if (this.done) {
      return;
    }
    this.done = true;
    for (const waiter of this.waiters.splice(0)) {
      waiter.resolve();
    }
  }

  pump(): AsyncGenerator<LiveEvent> {
    if (!this.iterator) {
      this.iterator = this.iterate();
    }
    return this.iterator;
  }

  private async *iterate(): AsyncGenerator<LiveEvent> {
    for (;;) {
      while (this.events.length) {
        yield this.events.shift()!;
      }
      if (this.done) {
        return;
      }
      const waiter = deferred<void>();
      this.waiters.push(waiter);
      await waiter.promise;
    }
  }
}

export interface HermesTransportOptions {
  command?: string;
  environment?: NodeJS.ProcessEnv;
  /** Interactive sessions may answer permissions; headless defaults to deny. */
  interactive?: boolean;
  /** Cap on how long an interactive session waits for one answer (<= 60s). */
  permission_timeout_ms?: number;
}

export class HermesAcpTransport implements LiveTransport {
  readonly id: LiveTransportId = "hermes-acp";
  readonly provider = "hermes" as const;

  private readonly options: HermesTransportOptions;
  private readonly permissionTimeoutMs: number;
  private request: LiveLaunchRequest | null = null;
  private child: LiveChildHandle | null = null;
  private conn: ClientSideConnection | null = null;
  private sink: EventSink | null = null;
  private sessionId: string | null = null;
  private resumeAdvertised = false;
  private resumeRoundTripped = false;
  private activeTurn: TurnState | null = null;
  private followUpQueue: LiveFollowUpCommand[] = [];
  private pendingPermissions = new Map<string, PendingPermission>();
  private permissionSeq = 0;
  private hubSignalSent = false;
  private stopMode: LiveStopMode | null = null;
  private stopCall: Promise<LiveStopReport> | null = null;
  private exitInfo: { code: number | null; signal: string | null } | null = null;
  private exitDeferred: Deferred<void> = deferred<void>();
  private fatal = false;

  constructor(options: HermesTransportOptions = {}) {
    this.options = options;
    const requested = options.permission_timeout_ms ?? DEFAULT_PERMISSION_TIMEOUT_MS;
    // The contract caps interactive waits at 60 seconds; clamp, never trust.
    this.permissionTimeoutMs = Math.min(Math.max(requested, 1), DEFAULT_PERMISSION_TIMEOUT_MS);
  }

  async describe(): Promise<LiveTransportDescriptor> {
    const capabilities: LiveCapabilities = {
      prompt: {
        support: "native",
        evidence: `ACP v1 session/prompt delivered via @zed-industries/agent-client-protocol@${ACP_SDK_VERSION}; the protocolVersion 1 handshake is verified at session open, never assumed`,
      },
      follow_up: {
        support: "hub-queued",
        evidence: "follow_up accepted mid-turn is held in the transport and sent as the next session/prompt only after the current prompt response resolves (queue is in-memory only)",
      },
      steer: { support: "unsupported", evidence: null },
      cancel: {
        support: "native",
        evidence: "ACP v1 session/cancel notification; the agent concludes the turn with stopReason cancelled and pending permissions answer cancelled",
      },
      status: {
        support: "derived",
        evidence: "status is derived from session/prompt and session/update activity; ACP v1 exposes no status request, so status commands are never forwarded",
      },
      permission_response: {
        support: "native",
        evidence: `ACP v1 session/request_permission round-trip via @zed-industries/agent-client-protocol@${ACP_SDK_VERSION}; headless default deny, interactive answer capped at ${this.permissionTimeoutMs}ms, only allow_once/deny honored, timeout denies, allow_always is never selected`,
      },
      resume: this.resumeAdvertised
        ? {
            support: "native",
            evidence: "ACP initialize response advertised agentCapabilities.loadSession (protocol v1); resume rides session/load with the persisted session id and verified_via records the round-trip",
          }
        : { support: "unsupported", evidence: null },
      checkpoint: { support: "unsupported", evidence: null },
      usage_reporting: { support: "unsupported", evidence: null },
    };
    return { transport: this.id, provider: this.provider, capabilities };
  }

  async open(request: LiveLaunchRequest): Promise<LiveLaunchReport> {
    if (this.request) {
      throw new LiveTransportError(liveError("launch", "LIVE_ACP_ALREADY_OPEN", "transport is already open", false));
    }
    this.request = request;
    this.sink ??= new EventSink(request.live_session_id, this.id);
    this.exitDeferred = deferred<void>();

    if (request.resume && request.resume.provider !== "hermes") {
      throw new LiveTransportError(
        liveError("launch", "LIVE_ACP_RESUME_MISMATCH", "resume state is not a hermes resume state", false),
      );
    }

    let child: LiveChildHandle;
    try {
      child = await launchLiveChild({
        command: resolveHermesCommand(this.options),
        args: [...HERMES_ACP_ARGS],
        cwd: request.workspace,
        env: this.options.environment,
        maxStderrBytes: 64 * 1024,
      });
    } catch (error) {
      throw new LiveTransportError(
        liveError(
          "launch",
          "LIVE_ACP_SPAWN_FAILED",
          `hermes process could not run: ${error instanceof Error ? error.message : String(error)}`,
          true,
        ),
      );
    }
    this.child = child;

    const stream = ndJsonStream(
      Writable.toWeb(child.stdin as Writable) as WritableStream<Uint8Array>,
      Readable.toWeb(child.stdout as Readable) as ReadableStream<Uint8Array>,
    );
    this.conn = new ClientSideConnection(
      (): Client => ({
        requestPermission: (params) => this.handlePermissionRequest(params),
        sessionUpdate: async (params) => {
          this.handleSessionUpdate(params);
        },
      }),
      stream,
    );

    child.onStderr((chunk) => {
      for (const line of chunk.toString("utf8").split("\n")) {
        if (line.trim()) {
          this.emit({ kind: "log", level: "info", text: this.bound(line) });
        }
      }
    });
    // Durable ownership boundary: the hub records the group leader BEFORE
    // the ACP handshake can fail, so a rejected open() can never orphan a
    // detached process whose pid was never written down.
    if (request.report_process) {
      try {
        await request.report_process({ pid: child.pid, pgid: child.pgid });
      } catch (error) {
        const structured = liveError(
          "launch",
          "LIVE_OWNERSHIP_RECORDING_FAILED",
          `the hub could not durably record ownership of the spawned provider (${error instanceof Error ? error.message : String(error)}); refusing to proceed with an unowned process`,
          false,
        );
        this.emit({ kind: "error", error: structured });
        this.fatal = true;
        child.closeStdin();
        void child.stop("terminate");
        throw new LiveTransportError(structured);
      }
    }
    void child.exited().then((info) => this.handleExit(info.exit_code, info.exit_signal));

    const launchedAt = new Date().toISOString();
    try {
      const initResponse = await this.withHandshakeTimeout(
        this.conn.initialize({
          protocolVersion: ACP_PROTOCOL_VERSION,
          clientCapabilities: { fs: { readTextFile: false, writeTextFile: false }, terminal: false },
        }),
        "initialize",
      );
      if (initResponse.protocolVersion !== ACP_PROTOCOL_VERSION) {
        throw new LiveTransportError(
          liveError(
            "protocol",
            "LIVE_ACP_HANDSHAKE_VERSION",
            `hermes negotiated ACP protocolVersion ${initResponse.protocolVersion}, this transport speaks v1 only`,
            false,
          ),
        );
      }
      this.resumeAdvertised = initResponse.agentCapabilities?.loadSession === true;

      if (request.resume) {
        const wantedSessionId = request.resume.provider_session_id;
        if (!wantedSessionId) {
          throw new LiveTransportError(
            liveError("launch", "LIVE_ACP_RESUME_UNVERIFIED", "hermes resume requires a persisted provider session id", false),
          );
        }
        if (!this.resumeAdvertised) {
          throw new LiveTransportError(
            liveError(
              "capability",
              "LIVE_ACP_LOAD_SESSION_NOT_ADVERTISED",
              "hermes did not advertise loadSession in its initialize response; refusing to start a fresh session under a resume request",
              false,
            ),
          );
        }
        // Set identity before load so replayed session/update notifications
        // match the session being resumed.
        this.sessionId = wantedSessionId;
        await this.withHandshakeTimeout(
          this.conn.loadSession({ sessionId: wantedSessionId, cwd: request.workspace, mcpServers: [] }),
          "session/load",
        );
        this.resumeRoundTripped = true;
      } else {
        const newSession = await this.withHandshakeTimeout(
          this.conn.newSession({ cwd: request.workspace, mcpServers: [] }),
          "session/new",
        );
        if (!newSession.sessionId) {
          throw new LiveTransportError(
            liveError("protocol", "LIVE_ACP_SESSION_ID_MISSING", "hermes returned an empty sessionId from session/new", false),
          );
        }
        this.sessionId = newSession.sessionId;
      }
    } catch (error) {
      const structured =
        error instanceof LiveTransportError
          ? error.live_error
          : error instanceof Error && error.message.startsWith("ACP ")
            ? liveError("launch", "LIVE_ACP_HANDSHAKE_TIMEOUT", error.message, true)
            : liveError("protocol", "LIVE_ACP_HANDSHAKE_FAILED", `ACP handshake failed: ${error instanceof Error ? error.message : "unknown cause"}`, false);
      this.emit({ kind: "error", error: structured });
      this.fatal = true;
      this.terminateAfterFailedOpen();
      throw new LiveTransportError(structured);
    }

    this.setStatus("idle");
    return {
      pid: child.pid,
      provider_session_id: this.sessionId,
      launched_at: launchedAt,
      resume_state: this.resumeState(),
    };
  }

  async send(command: LiveCommand): Promise<void> {
    if (!this.request || !this.child || !this.conn || !this.sessionId) {
      throw new LiveTransportError(
        liveError("transport", "LIVE_TRANSPORT_NOT_OPEN", "hermes transport is not open", false),
      );
    }
    switch (command.kind) {
      case "prompt": {
        if (this.activeTurn) {
          throw new LiveTransportError(
            liveError("transport", "LIVE_ACP_PROMPT_DURING_TURN", "a turn is already in flight; next-turn input uses follow_up", false),
          );
        }
        this.startTurn(command);
        return;
      }
      case "follow_up": {
        if (this.activeTurn || this.followUpQueue.length > 0) {
          this.followUpQueue.push(command);
          return;
        }
        this.startTurn(command);
        return;
      }
      case "steer": {
        throw new LiveTransportError(
          liveError("capability", "LIVE_ACP_STEER_UNSUPPORTED", "ACP v1 has no steer channel; queued guidance uses follow_up", false),
        );
      }
      case "cancel": {
        if (!this.activeTurn) {
          return;
        }
        this.setStatus("cancelling", command.reason ? this.bound(command.reason).text : null);
        this.answerPendingPermissions({ outcome: { outcome: "cancelled" } }, "turn cancelled");
        await this.conn.cancel({ sessionId: this.sessionId });
        return;
      }
      case "status": {
        throw new LiveTransportError(
          liveError("capability", "LIVE_ACP_STATUS_DERIVED", "hermes status is derived from the event stream; a status command must never be forwarded", false),
        );
      }
      case "permission_response": {
        this.answerPermission(command.request_id, command.decision);
        return;
      }
    }
  }

  events(): AsyncIterable<LiveEvent> {
    if (!this.sink) {
      this.sink = new EventSink(this.request?.live_session_id ?? "unopened", this.id);
    }
    return this.sink.pump();
  }

  stop(mode: LiveStopMode): Promise<LiveStopReport> {
    if (!this.stopCall) {
      this.stopCall = this.shutdown(mode);
    }
    return this.stopCall;
  }

  /** Resume state for the hub's durable record; null until the transport opened. */
  resumeState(): HermesResumeState | null {
    if (!this.request) {
      return null;
    }
    const base = {
      provider: "hermes" as const,
      provider_session_id: this.sessionId,
      session_load_advertised: this.resumeAdvertised,
    };
    if (this.resumeRoundTripped) {
      return {
        ...base,
        verified: true,
        verified_via: "ACP initialize advertised loadSession and session/load round-tripped successfully for the requested sessionId",
      };
    }
    return { ...base, verified: false, verified_via: null };
  }

  // -- internals ----------------------------------------------------------

  private bound(text: string): LiveBoundedText {
    return boundText(text, this.request?.max_text_bytes ?? 4096);
  }

  private emit(body: LiveEventBody): void {
    this.sink?.push(body);
  }

  private setStatus(status: LiveStatus, note?: string | null): void {
    this.emit({ kind: "status", status, note: note ? this.bound(note).text : null });
  }

  private withHandshakeTimeout<T>(promise: Promise<T>, label: string): Promise<T> {
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(
        () => reject(new Error(`ACP ${label} did not complete within ${HANDSHAKE_TIMEOUT_MS}ms`)),
        HANDSHAKE_TIMEOUT_MS,
      );
      timer.unref?.();
    });
    return Promise.race([promise, timeout]).finally(() => {
      clearTimeout(timer);
    });
  }

  private startTurn(command: LivePromptOrFollowUp): void {
    if (this.fatal || !this.conn || !this.sessionId) {
      return;
    }
    const turn: TurnState = {
      command,
      startedAt: new Date().toISOString(),
      streamId: `acp-message-${this.sessionId}`,
      sawAssistantText: false,
    };
    this.activeTurn = turn;
    this.setStatus("running");
    this.conn
      .prompt({ sessionId: this.sessionId, prompt: [{ type: "text", text: command.text }] })
      .then((response) => {
        if (this.activeTurn !== turn || this.fatal) {
          return;
        }
        this.settleTurn(turn, response.stopReason);
      })
      .catch(() => {
        if (this.activeTurn !== turn || this.fatal) {
          return;
        }
        this.activeTurn = null;
        this.emit({
          kind: "error",
          error: liveError("provider", "LIVE_ACP_PROMPT_FAILED", `ACP session/prompt failed for the turn started at ${turn.startedAt}`, false),
        });
        this.setStatus("idle");
        this.flushFollowUps();
      });
  }

  private settleTurn(turn: TurnState, stopReason: string): void {
    this.activeTurn = null;
    if (turn.sawAssistantText) {
      this.emit({ kind: "text", role: "assistant", stream_id: turn.streamId, text: { text: "", truncated: false }, final: true });
    }
    if (stopReason !== "end_turn" && stopReason !== "cancelled") {
      this.emit({
        kind: "error",
        error: liveError("provider", "LIVE_ACP_TURN_INCOMPLETE", `ACP turn ended with stopReason ${stopReason} (turn started ${turn.startedAt})`, false),
      });
    }
    this.setStatus("idle");
    this.flushFollowUps();
  }

  private flushFollowUps(): void {
    const next = this.followUpQueue.shift();
    if (next) {
      this.startTurn(next);
    }
  }

  private handleSessionUpdate(notification: SessionNotification): void {
    if (this.fatal) {
      return;
    }
    if (notification.sessionId !== this.sessionId) {
      this.fatalProtocolError(
        "LIVE_ACP_SESSION_ID_MISMATCH",
        "hermes emitted a session/update naming a session other than the one opened",
      );
      return;
    }
    const update = notification.update;
    const rawBytes = Buffer.byteLength(JSON.stringify(update), "utf8");

    switch (update.sessionUpdate) {
      case "agent_message_chunk":
      case "agent_thought_chunk": {
        const block = update.content;
        if (block.type !== "text") {
          this.emit({ kind: "unrecognized", transport_kind: block.type, bytes: rawBytes });
          return;
        }
        if (this.activeTurn && update.sessionUpdate === "agent_message_chunk") {
          this.activeTurn.sawAssistantText = true;
        }
        this.emit({
          kind: "text",
          role: update.sessionUpdate === "agent_message_chunk" ? "assistant" : "reasoning",
          stream_id: this.activeTurn ? this.activeTurn.streamId : `acp-message-${notification.sessionId}`,
          text: this.bound(block.text),
          final: false,
        });
        return;
      }
      case "tool_call": {
        this.emit({
          kind: "tool_start",
          call_id: update.toolCallId,
          tool: this.bound(update.title).text,
          input_preview: update.rawInput === undefined ? null : this.bound(JSON.stringify(update.rawInput)),
        });
        return;
      }
      case "tool_call_update": {
        if (update.status !== "completed" && update.status !== "failed") {
          this.emit({ kind: "unrecognized", transport_kind: "tool_call_update", bytes: rawBytes });
          return;
        }
        this.emit({
          kind: "tool_end",
          call_id: update.toolCallId,
          tool: this.bound(update.title ?? "tool").text,
          ok: update.status === "completed",
          output_preview: update.rawOutput === undefined ? null : this.bound(JSON.stringify(update.rawOutput)),
        });
        return;
      }
      default: {
        this.emit({ kind: "unrecognized", transport_kind: update.sessionUpdate, bytes: rawBytes });
      }
    }
  }

  private handlePermissionRequest(params: RequestPermissionRequest): Promise<RequestPermissionResponse> {
    this.permissionSeq += 1;
    const requestId = `perm-${this.permissionSeq}`;
    const kind = params.toolCall.kind ?? "other";
    this.emit({
      kind: "permission_request",
      request_id: requestId,
      tool: this.bound(params.toolCall.title ?? "tool").text,
      summary: this.bound(`${kind}: ${params.toolCall.title ?? "tool"}`),
    });

    if (!this.options.interactive) {
      this.emit({ kind: "log", level: "warn", text: this.bound(`headless session: permission ${requestId} denied by default`) });
      return Promise.resolve(this.denyResponse(params.options));
    }

    return new Promise<RequestPermissionResponse>((resolve) => {
      const entry: PendingPermission = {
        requestId,
        options: params.options,
        timer: undefined,
        resolve,
      };
      entry.timer = setTimeout(() => {
        if (!this.pendingPermissions.delete(requestId)) {
          return;
        }
        this.emit({ kind: "log", level: "warn", text: this.bound(`permission ${requestId} timed out unanswered: denying`) });
        resolve(this.denyResponse(entry.options));
      }, this.permissionTimeoutMs);
      entry.timer.unref?.();
      this.pendingPermissions.set(requestId, entry);
    });
  }

  /** Deny = the agent's own reject_once option when offered, else cancelled. */
  private denyResponse(options: PermissionOption[]): RequestPermissionResponse {
    const reject = options.find((option) => option.kind === "reject_once");
    return reject ? { outcome: { outcome: "selected", optionId: reject.optionId } } : { outcome: { outcome: "cancelled" } };
  }

  private answerPermission(requestId: string, decision: LivePermissionDecision): void {
    const entry = this.pendingPermissions.get(requestId);
    if (!entry) {
      throw new LiveTransportError(
        liveError("transport", "LIVE_ACP_PERMISSION_UNKNOWN", `no pending permission request ${requestId}`, false),
      );
    }
    this.pendingPermissions.delete(requestId);
    clearTimeout(entry.timer);

    let response: RequestPermissionResponse;
    if (decision === "allow_once") {
      const allow = entry.options.find((option) => option.kind === "allow_once");
      // An unoffered allow_once is never mapped onto some other option kind.
      response = allow ? { outcome: { outcome: "selected", optionId: allow.optionId } } : this.denyResponse(entry.options);
    } else {
      response = this.denyResponse(entry.options);
    }
    entry.resolve(response);
  }

  private answerPendingPermissions(response: RequestPermissionResponse, reason: string): void {
    for (const [requestId, entry] of this.pendingPermissions) {
      this.pendingPermissions.delete(requestId);
      clearTimeout(entry.timer);
      this.emit({ kind: "log", level: "info", text: this.bound(`permission ${requestId} answered ${reason}`) });
      entry.resolve(response);
    }
  }

  private fatalProtocolError(code: string, message: string): void {
    if (this.fatal) {
      return;
    }
    this.fatal = true;
    this.activeTurn = null;
    this.answerPendingPermissions({ outcome: { outcome: "cancelled" } }, "protocol failure");
    this.emit({ kind: "error", error: liveError("protocol", code, message, false) });
    this.setStatus("error");
    this.child?.closeStdin();
    if (!this.stopMode) {
      this.stopMode = "terminate";
      this.stopCall ??= this.shutdown("terminate");
    }
  }

  private handleExit(code: number | null, signal: string | null): void {
    if (this.exitInfo) {
      return;
    }
    this.exitInfo = { code, signal };
    this.exitDeferred.resolve();

    const intentional = this.hubSignalSent || this.stopMode !== null;
    if (this.activeTurn && !this.stopMode && !this.fatal) {
      this.emit({
        kind: "error",
        error: liveError(
          "provider",
          "LIVE_ACP_AGENT_EXITED",
          `hermes exited (code ${code ?? "null"}, signal ${signal ?? "null"}) while a session/prompt was in flight`,
          false,
        ),
      });
    }
    this.activeTurn = null;
    this.emit({ kind: "exit", intentional, exit_code: code, exit_signal: signal });

    if (!this.fatal) {
      this.setStatus(intentional ? "closed" : "error");
    }
    this.sink?.close();
  }

  private signalGroup(signal: NodeJS.Signals): void {
    if (!this.child) {
      return;
    }
    this.hubSignalSent = true;
    this.child.signalGroup(signal);
  }

  private terminateAfterFailedOpen(): void {
    this.child?.closeStdin();
    if (!this.stopMode) {
      this.stopMode = "terminate";
      this.stopCall ??= this.shutdown("terminate");
    }
  }

  private async shutdown(mode: LiveStopMode): Promise<LiveStopReport> {
    const started = Date.now();
    this.answerPendingPermissions({ outcome: { outcome: "cancelled" } }, "shutdown");
    if (!this.child) {
      return { status: "closed", exit_code: null, exit_signal: null, waited_ms: 0 };
    }
    if (!this.fatal) {
      this.setStatus("closing");
    }
    const child = this.child;
    child.closeStdin();

    const waitForExit = async (ms: number): Promise<boolean> => {
      if (this.exitInfo) {
        return true;
      }
      return Promise.race([
        this.exitDeferred.promise.then(() => true),
        new Promise<boolean>((resolve) => {
          const timer = setTimeout(() => resolve(false), ms);
          timer.unref?.();
        }),
      ]);
    };

    if (mode === "graceful") {
      this.signalGroup("SIGTERM");
      await waitForExit(GRACEFUL_EXIT_MS);
    } else {
      this.signalGroup("SIGTERM");
      if (!(await waitForExit(TERMINATE_STEP_MS))) {
        this.signalGroup("SIGKILL");
        await waitForExit(GRACEFUL_EXIT_MS);
      }
    }

    if (!this.exitInfo) {
      return { status: "orphaned", exit_code: null, exit_signal: null, waited_ms: Date.now() - started };
    }
    // `closed` requires the leader exit AND proof the owned group is gone;
    // a surviving helper gets one bounded SIGKILL sweep first.
    if (child.groupAlive()) {
      child.signalGroup("SIGKILL");
      await child.proveGroupGone(GRACEFUL_EXIT_MS);
    }
    if (child.groupAlive()) {
      return {
        status: "orphaned",
        exit_code: this.exitInfo.code,
        exit_signal: this.exitInfo.signal,
        waited_ms: Date.now() - started,
      };
    }
    return {
      status: "closed",
      exit_code: this.exitInfo.code,
      exit_signal: this.exitInfo.signal,
      waited_ms: Date.now() - started,
    };
  }
}

export function createHermesAcpFactory(options: HermesTransportOptions = {}): LiveTransportFactory {
  return {
    transport: "hermes-acp",
    provider: "hermes",
    probe: (): Promise<LiveProbeResult> => probeHermes({ command: options.command, environment: options.environment }),
    create: () => new HermesAcpTransport(options),
  };
}
