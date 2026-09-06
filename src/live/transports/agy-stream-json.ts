import { launchLiveChild, SUPPORTS_GROUP_SIGNALS, type LiveChildHandle } from "../child-process.js";

import { deferred, type Deferred } from "../../deferred.js";
import type {
  AgyResumeState,
  LiveBoundedText,
  LiveCancelCommand,
  LiveCapabilities,
  LiveCommand,
  LiveError,
  LiveEvent,
  LiveEventBody,
  LiveFollowUpCommand,
  LiveLaunchReport,
  LiveLaunchRequest,
  LiveProbeResult,
  LiveStatus,
  LiveStopMode,
  LiveStopReport,
  LiveTransport,
  LiveTransportDescriptor,
  LiveTransportFactory,
  LiveTransportId,
  LiveUsage,
} from "../types.js";
import {
  AGY_RESUME_FLAG,
  AGY_STREAM_JSON_ARGS,
  probeAgy,
  resolveAgyCommand,
  type AgyProbeResult,
} from "../probes/agy.js";

/**
 * `agy-stream-json` live transport (Package 3, AGY 1.1.26 contract).
 *
 * Wire contract (binding, from the approved v3 plan):
 *
 *   - Launch argv is exactly `agy --input-format stream-json --output-format
 *     stream-json` (`AGY_STREAM_JSON_ARGS`), plus `--conversation <id>` only
 *     for a resume whose argv was verified and whose identity is then checked
 *     against the `init` envelope. Always argv + `shell: false`.
 *   - The only bytes ever written to stdin are user envelopes: one
 *     newline-delimited JSON line `{"event":"user","message":"..."}`.
 *     There is no steer, no status poll, no cancel control — inventing
 *     control frames is a contract violation, and cancel is SIGINT to the
 *     process group only.
 *   - stdout is a stream of newline-delimited JSON envelopes with an
 *     `event` discriminator: `init`, `step_update`, `result`. `result` is the
 *     turn boundary; a follow-up accepted while running is queued and becomes
 *     the *next* `user` envelope only after `result` lands.
 *   - Any line that is not a well-formed envelope of a known event is
 *     stream-fatal: the session goes to `error`, the child is killed, and the
 *     pump ends. A recognized envelope carrying an unmappable inner kind
 *     surfaces as the `unrecognized` body (kind + bytes only), never raw.
 *
 * Envelope grammar honored (AGY 1.1.26):
 *   {event:"init", conversation_id?: string}
 *   {event:"step_update", step_id?: string, step_type: string,
 *     text?: string, tool?: string, call_id?: string, ok?: boolean,
 *     input_preview?: string, output_preview?: string}
 *   {event:"result", subtype?: string, result?: string,
 *     usage?: {input_tokens?, output_tokens?, cached_tokens?, cost_usd?}}
 */

const MAX_ENVELOPE_BYTES = 1 << 20;
const INIT_TIMEOUT_MS = 15_000;
const GRACEFUL_EXIT_MS = 5_000;
const TERMINATE_STEP_MS = 1_500;

/** One decoded stdout line: JSON object with a string `event` discriminator. */
interface AgyEnvelope {
  event: string;
  [field: string]: unknown;
}

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
  return { code, message, stage, retryable, provider: "agy" };
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

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" ? value : undefined;
}

function optionalFiniteNumber(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

type LivePromptOrFollowUp = Extract<LiveCommand, { kind: "prompt" | "follow_up" }>;

interface TurnState {
  command: LivePromptOrFollowUp;
  startedAt: string;
  streamId: string;
  assistantText: string[];
  sawAssistantText: boolean;
}

/**
 * Single-consumer, gapless event pump. `seq` starts at 1; `close()` is called
 * only after the final (exit/status) event is pushed, so the consumer drains
 * everything before the iterator ends.
 */
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

export interface AgyTransportOptions {
  command?: string;
  environment?: NodeJS.ProcessEnv;
}

interface InitEnvelope {
  conversation_id: string | null;
}

export class AgyStreamJsonTransport implements LiveTransport {
  readonly id: LiveTransportId = "agy-stream-json";
  readonly provider = "agy" as const;

  private readonly options: AgyTransportOptions;
  private request: LiveLaunchRequest | null = null;
  private child: LiveChildHandle | null = null;
  private sink: EventSink | null = null;
  private promptDelivered = false;
  private activeTurn: TurnState | null = null;
  private followUpQueue: LiveFollowUpCommand[] = [];
  private hubSignalSent = false;
  private stopMode: LiveStopMode | null = null;
  private stopCall: Promise<LiveStopReport> | null = null;
  private exitInfo: { code: number | null; signal: string | null } | null = null;
  private exitDeferred: Deferred<void> = deferred<void>();
  private initDeferred: Deferred<InitEnvelope> | null = null;
  private initSeen = false;
  private providerSessionId: string | null = null;
  private resumeIdentityEcho: string | null = null;
  private resumeArgvVerified = false;
  private fatal = false;
  private describeProbe: Promise<AgyProbeResult> | null = null;

  constructor(options: AgyTransportOptions = {}) {
    this.options = options;
  }

  async describe(): Promise<LiveTransportDescriptor> {
    const probe = await this.probeOnce();
    const versionNote = probe.version ? ` ${probe.version}` : "";
    const capabilities: LiveCapabilities = {
      prompt: {
        support: "native",
        evidence: `agy${versionNote} stream-json contract: prompts are written to stdin as single {event:"user",message} lines; launch argv is the exact --input-format/--output-format stream-json flag pair`,
      },
      follow_up: {
        support: "hub-queued",
        evidence: 'follow_up accepted mid-turn is held in the transport and emitted as the next {event:"user"} stdin line only after the result envelope closes the current turn (queue is in-memory only)',
      },
      steer: { support: "unsupported", evidence: null },
      cancel: SUPPORTS_GROUP_SIGNALS
        ? {
            support: "signal",
            evidence:
              "cancel is SIGINT to the spawned process group (detached group leader via the shared child primitive, shell:false); no control frame is ever written to stdin for cancel",
          }
        : {
            // Signal-only cancel is capability-gated: on platforms without
            // reliable process-group signals, SIGINT would hit only the
            // leader and helpers would survive — so nothing is claimed.
            support: "unsupported",
            evidence: null,
          },
      status: {
        support: "derived",
        evidence: "status is derived from init/step_update/result stream activity; the stream-json protocol exposes no status request, so status commands are never forwarded",
      },
      permission_response: { support: "unsupported", evidence: null },
      resume: probe.resume_argv_verified
        ? {
            support: "native",
            evidence: `installed agy${versionNote} advertises ${AGY_RESUME_FLAG} in its own --help output; resume launches ${AGY_RESUME_FLAG} <conversation_id> and the init envelope must echo the same id before the session is accepted`,
          }
        : { support: "unsupported", evidence: null },
      checkpoint: { support: "unsupported", evidence: null },
      usage_reporting: {
        support: "derived",
        evidence: "usage is derived from the optional usage object on the result envelope (input_tokens/output_tokens/cached_tokens/cost_usd); fields the envelope omits stay null",
      },
    };
    return { transport: this.id, provider: this.provider, capabilities };
  }

  async open(request: LiveLaunchRequest): Promise<LiveLaunchReport> {
    if (this.request) {
      throw new LiveTransportError(liveError("launch", "LIVE_AGY_ALREADY_OPEN", "transport is already open", false));
    }
    this.request = request;
    this.sink ??= new EventSink(request.live_session_id, this.id);
    this.exitDeferred = deferred<void>();
    this.initDeferred = deferred<InitEnvelope>();

    let resumeConversation: string | null = null;
    if (request.resume) {
      if (request.resume.provider !== "agy") {
        throw new LiveTransportError(
          liveError("launch", "LIVE_AGY_RESUME_MISMATCH", "resume state is not an agy resume state", false),
        );
      }
      if (!request.resume.resume_argv_verified || !request.resume.provider_session_id) {
        throw new LiveTransportError(
          liveError(
            "launch",
            "LIVE_AGY_RESUME_UNVERIFIED",
            "agy resume requires a persisted resume_argv_verified state and an observed conversation id; starting fresh instead is forbidden",
            false,
          ),
        );
      }
      // Independently re-verify against the installed binary: a persisted
      // flag must not be trusted after the command may have changed.
      const freshProbe = await probeAgy(this.probeOptions());
      if (!freshProbe.resume_argv_verified) {
        throw new LiveTransportError(
          liveError(
            "launch",
            "LIVE_AGY_RESUME_ARGV_UNVERIFIED",
            `installed agy${freshProbe.version ? ` ${freshProbe.version}` : ""} no longer advertises ${AGY_RESUME_FLAG} in --help; refusing to guess resume argv`,
            false,
          ),
        );
      }
      this.resumeArgvVerified = true;
      resumeConversation = request.resume.provider_session_id;
    } else {
      const probe = await this.probeOnce();
      this.resumeArgvVerified = probe.resume_argv_verified;
    }

    const args: string[] = [...AGY_STREAM_JSON_ARGS];
    if (resumeConversation !== null) {
      args.push(AGY_RESUME_FLAG, resumeConversation);
    }

    let child: LiveChildHandle;
    try {
      child = await launchLiveChild({
        command: resolveAgyCommand(this.options),
        args,
        cwd: request.workspace,
        env: this.options.environment,
        maxStderrBytes: 64 * 1024,
      });
    } catch (error) {
      throw new LiveTransportError(
        liveError(
          "launch",
          "LIVE_AGY_SPAWN_FAILED",
          `agy process could not run: ${error instanceof Error ? error.message : String(error)}`,
          true,
        ),
      );
    }
    this.child = child;

    this.attachLineReader(child.stdout!, (line, bytes) => this.handleLine(line, bytes));
    child.onStderr((chunk) => {
      for (const line of chunk.toString("utf8").split("\n")) {
        if (line.trim()) {
          this.emit({ kind: "log", level: "info", text: this.bound(line) });
        }
      }
    });
    // Durable ownership boundary: record the group leader BEFORE the init
    // envelope race can fail, so a rejected open() can never orphan a
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
        void child.exited().then((info) => this.handleExit(info.exit_code, info.exit_signal));
        throw new LiveTransportError(structured);
      }
    }
    void child.exited().then((info) => this.handleExit(info.exit_code, info.exit_signal));

    const launchedAt = new Date().toISOString();
    let init: InitEnvelope;
    try {
      init = await this.raceInit(child);
    } catch (error) {
      this.terminateAfterFailedOpen();
      if (error instanceof LiveTransportError) {
        throw error;
      }
      throw new LiveTransportError(
        liveError(
          "launch",
          "LIVE_AGY_INIT_NOT_OBSERVED",
          `agy never produced its init envelope: ${error instanceof Error ? error.message : "unknown cause"}`,
          true,
        ),
      );
    }

    this.providerSessionId = init.conversation_id;

    if (resumeConversation !== null) {
      if (init.conversation_id !== resumeConversation) {
        const error = liveError(
          "protocol",
          "LIVE_AGY_RESUME_IDENTITY_MISMATCH",
          "agy resumed a different conversation than the --conversation argument; refusing the session",
          false,
        );
        this.emit({ kind: "error", error });
        this.fatal = true;
        this.terminateAfterFailedOpen();
        throw new LiveTransportError(error);
      }
      this.resumeIdentityEcho = init.conversation_id;
    }

    this.setStatus("idle");
    return {
      pid: child.pid,
      provider_session_id: this.providerSessionId,
      launched_at: launchedAt,
      resume_state: this.resumeState(),
    };
  }

  async send(command: LiveCommand): Promise<void> {
    if (!this.request || !this.child) {
      throw new LiveTransportError(
        liveError("transport", "LIVE_TRANSPORT_NOT_OPEN", "agy transport is not open", false),
      );
    }
    switch (command.kind) {
      case "prompt": {
        if (this.promptDelivered || this.activeTurn) {
          throw new LiveTransportError(
            liveError("transport", "LIVE_AGY_PROMPT_ALREADY_DELIVERED", "agy accepts its prompt exactly once per live session", false),
          );
        }
        this.promptDelivered = true;
        this.deliverTurn(command);
        return;
      }
      case "follow_up": {
        if (this.activeTurn || this.followUpQueue.length > 0) {
          // hub-queued: acceptance is honest because the queue is transient
          // and delivery happens at the next result boundary.
          this.followUpQueue.push(command);
          return;
        }
        this.deliverTurn(command);
        return;
      }
      case "steer": {
        throw new LiveTransportError(
          liveError("capability", "LIVE_AGY_STEER_UNSUPPORTED", "agy stream-json has no steer channel; queued guidance uses follow_up", false),
        );
      }
      case "cancel": {
        await this.cancelTurn(command);
        return;
      }
      case "status": {
        throw new LiveTransportError(
          liveError("capability", "LIVE_AGY_STATUS_DERIVED", "agy status is derived from the event stream; a status command must never be forwarded", false),
        );
      }
      case "permission_response": {
        throw new LiveTransportError(
          liveError("capability", "LIVE_AGY_PERMISSIONS_UNSUPPORTED", "agy stream-json emits no permission requests; there is nothing to answer", false),
        );
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
  resumeState(): AgyResumeState | null {
    if (!this.request) {
      return null;
    }
    const base = {
      provider: "agy" as const,
      provider_session_id: this.providerSessionId,
      resume_argv_verified: this.resumeArgvVerified,
    };
    if (this.resumeIdentityEcho !== null) {
      return {
        ...base,
        verified: true,
        verified_via: "agy init envelope echoed the same conversation_id passed via --conversation",
      };
    }
    return { ...base, verified: false, verified_via: null };
  }

  // -- internals ----------------------------------------------------------

  private probeOptions(): { command?: string; environment?: NodeJS.ProcessEnv } {
    return { command: this.options.command, environment: this.options.environment };
  }

  private probeOnce(): Promise<AgyProbeResult> {
    if (!this.describeProbe) {
      this.describeProbe = probeAgy(this.probeOptions());
    }
    return this.describeProbe;
  }

  private bound(text: string): LiveBoundedText {
    return boundText(text, this.request?.max_text_bytes ?? 4096);
  }

  private emit(body: LiveEventBody): void {
    this.sink?.push(body);
  }

  private setStatus(status: LiveStatus, note?: string | null): void {
    this.emit({ kind: "status", status, note: note ? this.bound(note).text : null });
  }

  private raceInit(child: LiveChildHandle): Promise<InitEnvelope> {
    const init = this.initDeferred!;
    let timer: NodeJS.Timeout | undefined;
    const timeout = new Promise<never>((_, reject) => {
      timer = setTimeout(() => reject(new Error(`no init envelope within ${INIT_TIMEOUT_MS}ms`)), INIT_TIMEOUT_MS);
      timer.unref?.();
    });
    const died = child.exited().then(
      (info) =>
        Promise.reject(
          new Error(`agy exited before init (code ${info.exit_code ?? "null"}, signal ${info.exit_signal ?? "null"})`),
        ),
    );
    return Promise.race([init.promise, timeout, died]).finally(() => {
      clearTimeout(timer);
    });
  }

  private attachLineReader(stream: NodeJS.ReadableStream, onLine: (line: string, bytes: number) => void): void {
    let pending = Buffer.alloc(0);
    stream.on("data", (chunk: Buffer | string) => {
      const data = typeof chunk === "string" ? Buffer.from(chunk, "utf8") : chunk;
      pending = Buffer.concat([pending, data]);
      for (;;) {
        const newline = pending.indexOf(0x0a);
        if (newline < 0) {
          if (pending.byteLength > MAX_ENVELOPE_BYTES) {
            this.fatalProtocolError("LIVE_AGY_ENVELOPE_TOO_LARGE", "agy emitted a stdout line over the envelope byte cap without a newline");
          }
          return;
        }
        const line = pending.subarray(0, newline);
        pending = pending.subarray(newline + 1);
        if (line.byteLength > MAX_ENVELOPE_BYTES) {
          this.fatalProtocolError("LIVE_AGY_ENVELOPE_TOO_LARGE", "agy emitted an oversized envelope");
          return;
        }
        const text = line.toString("utf8");
        if (text.trim()) {
          onLine(text, line.byteLength);
        }
      }
    });
  }

  private parseEnvelope(line: string): AgyEnvelope | null {
    let parsed: unknown;
    try {
      parsed = JSON.parse(line);
    } catch {
      this.fatalProtocolError("LIVE_AGY_MALFORMED_ENVELOPE", "agy emitted a stdout line that is not valid JSON");
      return null;
    }
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
      this.fatalProtocolError("LIVE_AGY_MALFORMED_ENVELOPE", "agy emitted an envelope that is not a JSON object");
      return null;
    }
    const candidate = parsed as { event?: unknown };
    if (typeof candidate.event !== "string") {
      this.fatalProtocolError("LIVE_AGY_MALFORMED_ENVELOPE", "agy emitted an envelope without a string event discriminator");
      return null;
    }
    return parsed as AgyEnvelope;
  }

  private handleLine(line: string, bytes: number): void {
    if (this.fatal) {
      return;
    }
    const envelope = this.parseEnvelope(line);
    if (!envelope) {
      return;
    }

    switch (envelope.event) {
      case "init": {
        if (this.initSeen) {
          this.fatalProtocolError("LIVE_AGY_UNEXPECTED_ENVELOPE", "agy emitted a second init envelope");
          return;
        }
        let conversationId: string | null = null;
        if (envelope.conversation_id !== undefined) {
          conversationId = optionalString(envelope.conversation_id) ?? "";
          if (conversationId === "") {
            this.fatalProtocolError("LIVE_AGY_MALFORMED_ENVELOPE", "agy init envelope carries a non-string conversation_id");
            return;
          }
        }
        this.initSeen = true;
        this.initDeferred?.resolve({ conversation_id: conversationId });
        return;
      }
      case "step_update": {
        this.handleStepUpdate(envelope, bytes);
        return;
      }
      case "result": {
        this.handleResult(envelope);
        return;
      }
      default: {
        this.fatalProtocolError(
          "LIVE_AGY_MALFORMED_ENVELOPE",
          "agy emitted an envelope with an unknown event; unknown frames are stream-fatal by contract",
        );
      }
    }
  }

  private handleStepUpdate(envelope: AgyEnvelope, bytes: number): void {
    if (!this.initSeen) {
      this.fatalProtocolError("LIVE_AGY_UNEXPECTED_ENVELOPE", "agy emitted a step_update before its init envelope");
      return;
    }
    const stepType = optionalString(envelope.step_type);
    if (!stepType) {
      this.fatalProtocolError("LIVE_AGY_MALFORMED_ENVELOPE", "agy step_update envelope has no string step_type");
      return;
    }
    const stepId = optionalString(envelope.step_id);

    if (stepType === "text" || stepType === "reasoning") {
      const text = optionalString(envelope.text);
      if (text === undefined) {
        this.fatalProtocolError("LIVE_AGY_MALFORMED_ENVELOPE", `agy ${stepType} step_update has no string text`);
        return;
      }
      const streamId = stepId ?? `agy-${stepType}`;
      if (this.activeTurn && stepType === "text") {
        this.activeTurn.assistantText.push(text);
        this.activeTurn.sawAssistantText = true;
        this.activeTurn.streamId = streamId;
      }
      this.emit({
        kind: "text",
        role: stepType === "text" ? "assistant" : "reasoning",
        stream_id: this.activeTurn ? this.activeTurn.streamId : streamId,
        text: this.bound(text),
        final: false,
      });
      return;
    }

    if (stepType === "tool_start") {
      const tool = optionalString(envelope.tool);
      if (!tool) {
        this.fatalProtocolError("LIVE_AGY_MALFORMED_ENVELOPE", "agy tool_start step_update has no string tool");
        return;
      }
      const preview = optionalString(envelope.input_preview);
      if (envelope.input_preview !== undefined && preview === undefined) {
        this.fatalProtocolError("LIVE_AGY_MALFORMED_ENVELOPE", "agy tool_start input_preview is not a string");
        return;
      }
      this.emit({
        kind: "tool_start",
        call_id: optionalString(envelope.call_id) ?? stepId ?? `agy-tool-${bytes}`,
        tool: this.bound(tool).text,
        input_preview: preview === undefined ? null : this.bound(preview),
      });
      return;
    }

    if (stepType === "tool_end") {
      const tool = optionalString(envelope.tool);
      if (!tool) {
        this.fatalProtocolError("LIVE_AGY_MALFORMED_ENVELOPE", "agy tool_end step_update has no string tool");
        return;
      }
      if (envelope.ok !== undefined && typeof envelope.ok !== "boolean") {
        this.fatalProtocolError("LIVE_AGY_MALFORMED_ENVELOPE", "agy tool_end ok is not a boolean");
        return;
      }
      const preview = optionalString(envelope.output_preview);
      if (envelope.output_preview !== undefined && preview === undefined) {
        this.fatalProtocolError("LIVE_AGY_MALFORMED_ENVELOPE", "agy tool_end output_preview is not a string");
        return;
      }
      this.emit({
        kind: "tool_end",
        call_id: optionalString(envelope.call_id) ?? stepId ?? `agy-tool-${bytes}`,
        tool: this.bound(tool).text,
        ok: envelope.ok ?? true,
        output_preview: preview === undefined ? null : this.bound(preview),
      });
      return;
    }

    this.emit({ kind: "unrecognized", transport_kind: stepType, bytes });
  }

  private handleResult(envelope: AgyEnvelope): void {
    if (!this.initSeen) {
      this.fatalProtocolError("LIVE_AGY_UNEXPECTED_ENVELOPE", "agy emitted a result before its init envelope");
      return;
    }
    if (!this.activeTurn) {
      this.fatalProtocolError("LIVE_AGY_UNEXPECTED_ENVELOPE", "agy emitted a result with no turn in flight");
      return;
    }
    if (envelope.usage !== undefined && (typeof envelope.usage !== "object" || envelope.usage === null || Array.isArray(envelope.usage))) {
      this.fatalProtocolError("LIVE_AGY_MALFORMED_ENVELOPE", "agy result usage is not an object");
      return;
    }

    const turn = this.activeTurn;
    this.activeTurn = null;

    if (envelope.usage !== undefined) {
      const usageFields = envelope.usage as Record<string, unknown>;
      const usage: LiveUsage = {
        input_tokens: optionalFiniteNumber(usageFields.input_tokens),
        output_tokens: optionalFiniteNumber(usageFields.output_tokens),
        cached_tokens: optionalFiniteNumber(usageFields.cached_tokens),
        cost_usd: optionalFiniteNumber(usageFields.cost_usd),
      };
      this.emit({ kind: "usage", usage });
    }
    if (turn.sawAssistantText) {
      this.emit({
        kind: "text",
        role: "assistant",
        stream_id: turn.streamId,
        text: { text: "", truncated: false },
        final: true,
      });
    }
    const subtype = optionalString(envelope.subtype) ?? "success";
    if (subtype !== "success" && subtype !== "cancelled") {
      this.emit({
        kind: "error",
        error: liveError("provider", "LIVE_AGY_RESULT_FAILED", `agy reported a failed result for the turn started at ${turn.startedAt}`, false),
      });
    }
    this.setStatus("idle");

    const next = this.followUpQueue.shift();
    if (next) {
      this.deliverTurn(next);
    }
  }

  private deliverTurn(command: LivePromptOrFollowUp): void {
    const stdin = this.child?.stdin;
    if (this.fatal || !stdin || !stdin.writable) {
      return;
    }
    this.activeTurn = {
      command,
      startedAt: new Date().toISOString(),
      streamId: `agy-${command.command_id}`,
      assistantText: [],
      sawAssistantText: false,
    };
    // The only bytes ever written to agy's stdin: user envelopes.
    void this.child!.writeStdin(`${JSON.stringify({ event: "user", message: command.text })}\n`);
    this.setStatus("running");
  }

  private async cancelTurn(command: LiveCancelCommand): Promise<void> {
    if (!this.activeTurn) {
      return;
    }
    this.setStatus("cancelling", command.reason ? this.bound(command.reason).text : null);
    this.signalGroup("SIGINT");
  }

  private fatalProtocolError(code: string, message: string): void {
    if (this.fatal) {
      return;
    }
    this.fatal = true;
    this.activeTurn = null;
    this.emit({ kind: "error", error: liveError("protocol", code, message, false) });
    this.setStatus("error");
    this.initDeferred?.reject(new LiveTransportError(liveError("protocol", code, message, false)));
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
          "LIVE_AGY_EXITED_BEFORE_RESULT",
          `agy exited (code ${code ?? "null"}, signal ${signal ?? "null"}) before the result envelope for the in-flight turn`,
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
    this.fatal = true;
    this.child?.closeStdin();
    if (!this.stopMode) {
      this.stopMode = "terminate";
      this.stopCall ??= this.shutdown("terminate");
    }
  }

  private async shutdown(mode: LiveStopMode): Promise<LiveStopReport> {
    const started = Date.now();
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
      this.signalGroup("SIGINT");
      if (!(await waitForExit(GRACEFUL_EXIT_MS))) {
        this.signalGroup("SIGTERM");
        await waitForExit(GRACEFUL_EXIT_MS);
      }
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

export function createAgyStreamJsonFactory(options: AgyTransportOptions = {}): LiveTransportFactory {
  return {
    transport: "agy-stream-json",
    provider: "agy",
    probe: (): Promise<LiveProbeResult> => probeAgy({ command: options.command, environment: options.environment }),
    create: () => new AgyStreamJsonTransport(options),
  };
}
