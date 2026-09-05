/**
 * Shared plumbing for the JSONL-over-stdio live RPC transports (`omp-rpc`,
 * `pi-rpc`). Both providers speak the same frame dialect for the surfaces the
 * hub uses — commands on stdin, `{type:"response"}` replies and event frames
 * on stdout, LF-delimited JSON — so framing, request correlation, the event
 * pump, byte bounding, turn-status tracking, shutdown, and the
 * `switch_session` resume round-trip live here once. Provider files keep only
 * their verified dialect differences (startup handshake, frame mapping,
 * capability claims).
 *
 * Wire rules (binding, both providers):
 *
 *   - Launch argv is always an argv array spawned with `shell:false`, and it
 *     never includes `--no-session`: live resume depends on provider session
 *     persistence.
 *   - stdout is parsed at the byte level: records are split on LF (U+000A)
 *     only, an optional trailing CR is stripped. Node `readline` is
 *     forbidden — it also splits on U+2028/U+2029, which are legal inside
 *     JSON strings.
 *   - Only the verified current dialect is used. The transports never send
 *     `negotiate_protocol`, so OMP protocol-v2 `rpc_chunk` reassembly never
 *     applies and the old bounded-chunk dialect is never guessed. A frame
 *     that is structurally outside the dialect (not JSON, not a JSON object,
 *     missing string `type`) is fatal: exactly one
 *     `PROVIDER_PROTOCOL_UNSUPPORTED` error event, bounded shutdown, pump
 *     end. A known-shape frame carrying a semantically unknown `type` is
 *     tolerated as a `provider_notice` log event (kind + label only, never
 *     raw content).
 */
import { spawn } from "node:child_process";
import { StringDecoder } from "node:string_decoder";
import { AgentHubError } from "../../errors.js";
import type {
  LiveBoundedText,
  LiveCommand,
  LiveError,
  LiveErrorStage,
  LiveEvent,
  LiveEventBody,
  LiveLaunchReport,
  LiveLaunchRequest,
  LiveProviderId,
  LiveProviderFactory,
  LiveStatus,
  LiveStopMode,
  LiveStopReport,
  LiveTransport,
  LiveTransportDescriptor,
  LiveTransportFactory,
  LiveTransportId,
  LiveUsage,
  ProviderResumeState,
  ResumeVerification,
} from "../types.js";

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

export class LiveTransportError extends AgentHubError {
  readonly liveError: LiveError;

  constructor(liveErrorValue: LiveError) {
    super(liveErrorValue.code, liveErrorValue.message);
    this.name = "LiveTransportError";
    this.liveError = liveErrorValue;
  }
}

/** Hub-generated structured error; provider stderr/transcripts never become messages. */
export function liveError(
  provider: LiveProviderId | null,
  code: string,
  message: string,
  stage: LiveErrorStage,
  retryable: boolean,
): LiveError {
  return { code, message, stage, retryable, provider };
}

// ---------------------------------------------------------------------------
// Wire abstraction (test seam; the default spawner is the only place that spawns)
// ---------------------------------------------------------------------------

export interface RpcWireExit {
  exitCode: number | null;
  exitSignal: string | null;
}

/** The narrow child-process surface the base needs — nothing else reaches through. */
export interface RpcWireHandle {
  readonly pid: number;
  onData(next: (chunk: Buffer) => void): void;
  /** Writes one outbound record; the caller supplies the trailing LF. */
  write(line: string): void;
  endStdin(): void;
  signal(signal: NodeJS.Signals): void;
  readonly exited: Promise<RpcWireExit>;
}

export type RpcWireSpawner = (argv: readonly string[], cwd: string) => Promise<RpcWireHandle>;

/** Launch argv arrays, `shell:false`, piped stdio — the production spawner. */
export const spawnRpcWire: RpcWireSpawner = async (argv, cwd) => {
  const [command, ...args] = argv;
  if (!command) {
    throw new Error("rpc wire spawner requires a non-empty argv");
  }
  const child = spawn(command, args, {
    cwd,
    env: process.env,
    shell: false,
    stdio: ["pipe", "pipe", "pipe"],
  });

  let settled = false;
  let resolveExit: (exit: RpcWireExit) => void = () => {};
  const exited = new Promise<RpcWireExit>((resolve) => {
    resolveExit = resolve;
  });

  child.once("error", () => {
    if (!settled) {
      settled = true;
      resolveExit({ exitCode: null, exitSignal: null });
    }
  });
  child.once("close", (exitCode, exitSignal) => {
    if (!settled) {
      settled = true;
      resolveExit({ exitCode, exitSignal });
    }
  });

  if (child.pid === undefined) {
    throw new Error(`failed to spawn ${command}`);
  }
  const pid = child.pid;
  const stdin = child.stdin;
  const stdout = child.stdout;
  if (!stdin || !stdout) {
    throw new Error(`failed to pipe stdio for ${command}`);
  }

  return {
    pid,
    onData(next) {
      stdout.on("data", (chunk: Buffer) => next(chunk));
    },
    write(line) {
      stdin.write(line);
    },
    endStdin() {
      stdin.end();
    },
    signal(signal) {
      child.kill(signal);
    },
    exited,
  };
};

// ---------------------------------------------------------------------------
// Provider factories
// ---------------------------------------------------------------------------

/** Preference-ordered selection among provider-matched candidates; no fallback guesses. */
export function selectPreferredTransport(
  factory: Pick<LiveProviderFactory, "provider" | "transports">,
  factories: readonly LiveTransportFactory[],
): LiveTransportFactory | null {
  for (const transport of factory.transports) {
    const match = factories.find(
      (candidate) => candidate.provider === factory.provider && candidate.transport === transport,
    );
    if (match) {
      return match;
    }
  }
  return null;
}

// ---------------------------------------------------------------------------
// Frames
// ---------------------------------------------------------------------------

/** One stdout frame of the verified dialect. Shape beyond a string `type` is unproven: every property is `unknown` and must be checked before use. */
export type RpcFrame = Record<string, unknown>;

/** Hard buffer guard for one physical stdout record; well above any verified frame. */
const MAX_FRAME_BYTES = 8 * 1024 * 1024;

interface Pending {
  kind: string;
  resolve: (frame: RpcFrame) => void;
  reject: (error: LiveTransportError) => void;
}

/** JSON objects from the wire carry unproven nesting; read nested fields through typed accessors, never re-guess the shape. */
export function objectField(value: unknown): Record<string, unknown> | undefined {
  return typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined;
}

export function readString(record: Record<string, unknown>, key: string): string | null {
  const value = record[key];
  return typeof value === "string" ? value : null;
}

export function readNumber(record: Record<string, unknown>, key: string): number | null {
  const value = record[key];
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

function isBusyState(state: Record<string, unknown>): boolean {
  return state.isStreaming === true || state.isCompacting === true;
}

// ---------------------------------------------------------------------------
// Base transport
// ---------------------------------------------------------------------------

export interface RpcSessionOptions {
  spawner?: RpcWireSpawner;
  /** Ready-frame wait budget (OMP); unused by providers without a ready frame. */
  readyTimeoutMs?: number;
  /** Optimistic `get_state` handshake budget (PI). */
  handshakeTimeoutMs?: number;
  commandTimeoutMs?: number;
  /** Per-phase bounded shutdown budget (stdin close, SIGTERM, SIGKILL). */
  shutdownGraceMs?: number;
}

export abstract class RpcSessionBase implements LiveTransport {
  readonly id: LiveTransportId;
  readonly provider: LiveProviderId;

  /**
   * Resume outcome of the most recent `open()` with a resume hint. A resume
   * that did not round-trip fails the launch, so after a successful `open()`
   * this is always `{verified:true}` — the field exists so callers (and
   * tests) can see the recorded basis, never a guess.
   */
  resumeVerification: ResumeVerification | null = null;

  protected readonly spawner: RpcWireSpawner;
  protected readonly readyTimeoutMs: number;
  protected readonly handshakeTimeoutMs: number;
  protected readonly commandTimeoutMs: number;
  protected readonly shutdownGraceMs: number;

  private wire: RpcWireHandle | null = null;
  private readonly byteDecoder = new StringDecoder("utf8");
  private byteBuffer = "";
  private readonly pending = new Map<string, Pending>();
  private readonly eventQueue: LiveEvent[] = [];
  private wakePump: (() => void) | null = null;
  private pumpEnded = false;
  private pumpSettled: Promise<void> = Promise.resolve();
  private resolvePumpSettled: () => void = () => {};
  private pumpConsumed = false;
  private opened = false;
  private stopPromise: Promise<LiveStopReport> | null = null;

  private liveSessionId = "";
  private maxTextBytes = 0;
  private requestId = 0;
  private eventSeq = 0;
  private currentStatus: LiveStatus = "starting";
  private fatal = false;
  private fatalReject: ((error: LiveTransportError) => void) | null = null;
  private shutdownInitiated = false;
  private providerSessionId: string | null = null;

  private messageSeq = 0;
  private activeMessage: { tag: string; openBlocks: Set<string> } | null = null;
  private lastUsageJson: string | null = null;

  constructor(
    transportId: LiveTransportId,
    providerId: LiveProviderId,
    options: RpcSessionOptions = {},
  ) {
    this.id = transportId;
    this.provider = providerId;
    this.spawner = options.spawner ?? spawnRpcWire;
    this.readyTimeoutMs = options.readyTimeoutMs ?? 15_000;
    this.handshakeTimeoutMs = options.handshakeTimeoutMs ?? 15_000;
    this.commandTimeoutMs = options.commandTimeoutMs ?? 30_000;
    this.shutdownGraceMs = options.shutdownGraceMs ?? 3_000;
  }

  // -- Provider hooks -------------------------------------------------------

  /** Launch argv (always an array, spawned `shell:false`, never `--no-session`). */
  protected abstract buildArgv(): string[];

  abstract describe(): Promise<LiveTransportDescriptor>;

  /**
   * Bring the session to a state-observed baseline and return the provider
   * session locator observed at startup (`sessionFile`, else `sessionId`).
   */
  protected abstract handshake(request: LiveLaunchRequest): Promise<string | null>;

  /** Per-provider mapping of non-response frames; must not throw. */
  protected abstract handleProviderFrame(frame: RpcFrame): void;

  /** Side effect on every successful response (e.g. OMP `agentInvoked:false` hints). */
  protected onResponseSuccess(_response: RpcFrame): void {}

  /**
   * The provider locator for a resume hint, or null when the hint carries no
   * usable handle (a resume must then fail, never silently start fresh).
   */
  protected abstract resumeHandle(resume: ProviderResumeState): string | null;

  // -- LiveTransport --------------------------------------------------------

  async open(request: LiveLaunchRequest): Promise<LiveLaunchReport> {
    if (this.opened) {
      throw new LiveTransportError(
        liveError(this.provider, "LIVE_SESSION_ALREADY_OPEN", "this transport session has already been opened", "transport", false),
      );
    }
    this.opened = true;
    this.liveSessionId = request.live_session_id;
    this.maxTextBytes = request.max_text_bytes;
    if (!Number.isInteger(this.maxTextBytes) || this.maxTextBytes < 0) {
      throw new LiveTransportError(
        liveError(this.provider, "LIVE_INVALID_MAX_TEXT_BYTES", "max_text_bytes must be a non-negative integer", "transport", false),
      );
    }

    const fatalSignal = new Promise<never>((_resolve, reject) => {
      this.fatalReject = reject;
    });
    this.pumpSettled = new Promise<void>((resolve) => {
      this.resolvePumpSettled = resolve;
    });

    try {
      const wire = await this.spawner(this.buildArgv(), request.workspace);
      this.wire = wire;
      wire.onData((chunk) => this.ingest(chunk));
      void wire.exited.then((exit) => this.onExit(exit));
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      throw await this.failLaunch(
        liveError(this.provider, "LIVE_LAUNCH_SPAWN_FAILED", `failed to spawn the provider process (${message})`, "launch", true),
        true,
      );
    }

    try {
      return await Promise.race([this.runOpen(request), fatalSignal]);
    } catch (error) {
      const detail =
        error instanceof LiveTransportError
          ? error.liveError
          : liveError(
              this.provider,
              "LIVE_LAUNCH_FAILED",
              error instanceof Error ? error.message : String(error),
              "launch",
              true,
            );
      // A protocol violation already surfaced its own error event; do not double-report.
      throw await this.failLaunch(detail, detail.code !== "PROVIDER_PROTOCOL_UNSUPPORTED");
    }
  }

  private async runOpen(request: LiveLaunchRequest): Promise<LiveLaunchReport> {
    this.providerSessionId = await this.handshake(request);
    if (request.resume) {
      await this.runResume(request.resume);
    }
    if (this.currentStatus === "starting") {
      this.setStatus("idle", "handshake complete");
    }
    return {
      pid: this.wire?.pid ?? null,
      provider_session_id: this.providerSessionId,
      launched_at: new Date().toISOString(),
    };
  }

  /**
   * Verified resume over the shared `switch_session` dialect. One success
   * response can be false (`success:true` with `data.cancelled:true`), so the
   * switch counts only when the fresh `get_state` echoes the requested
   * locator; anything else fails the launch.
   */
  private async runResume(resume: ProviderResumeState): Promise<void> {
    const failure = (message: string): LiveTransportError =>
      new LiveTransportError(
        liveError(this.provider, "LIVE_RESUME_VERIFICATION_FAILED", message, "state", true),
      );

    if (resume.provider !== this.provider) {
      throw failure(`resume hint carries provider "${resume.provider}", not this transport's provider`);
    }
    const handle = this.resumeHandle(resume);
    if (handle === null) {
      throw failure("resume hint carries no provider session locator");
    }

    let switched: RpcFrame | null = null;
    try {
      switched = await this.requestCommand("resume-switch", { type: "switch_session", sessionPath: handle });
    } catch {
      switched = null;
    }
    if (!switched || switched.success !== true) {
      throw failure("provider rejected the switch_session resume before it took effect");
    }
    const data = objectField(switched.data);
    if (!data || data.cancelled !== false) {
      throw failure("switch_session completed without confirming an uncancelled switch");
    }

    let state: Record<string, unknown> | null = null;
    try {
      const verified = await this.requestCommand("resume-verify", { type: "get_state" });
      state = objectField(verified.data) ?? null;
    } catch {
      state = null;
    }
    const file = state ? readString(state, "sessionFile") : null;
    const id = state ? readString(state, "sessionId") : null;
    if (file !== handle && !(file === null && id === handle)) {
      throw failure("get_state after switch_session did not echo the resumed session locator");
    }

    this.providerSessionId = handle;
    this.resumeVerification = {
      verified: true,
      verified_via:
        "rpc switch_session (success, cancelled=false) followed by a get_state locator echo of the session handle",
    };
  }

  /** Surface the launch failure on the event stream and prove the child is gone. */
  private async failLaunch(detail: LiveError, emitEvent: boolean): Promise<LiveTransportError> {
    if (emitEvent) {
      this.pushEvent({ kind: "error", error: detail });
    }
    this.shutdownInitiated = true;
    const wire = this.wire;
    if (wire) {
      wire.endStdin();
      wire.signal("SIGTERM");
      const exit = await raceTimeout(wire.exited, this.shutdownGraceMs);
      if (!exit) {
        wire.signal("SIGKILL");
        await raceTimeout(wire.exited, this.shutdownGraceMs);
      }
    }
    this.currentStatus = "error";
    this.rejectAllPending("LIVE_SESSION_CLOSED", "session closed during launch");
    this.endPump();
    return new LiveTransportError(detail);
  }

  async send(command: LiveCommand): Promise<void> {
    this.requireOpen();
    if (this.currentStatus === "closed" || this.currentStatus === "error" || this.fatal) {
      throw new LiveTransportError(
        liveError(this.provider, "LIVE_SESSION_CLOSED", "live session is no longer running", "transport", false),
      );
    }

    switch (command.kind) {
      case "prompt": {
        await this.requestCommand("prompt", { type: "prompt", message: command.text });
        return;
      }
      case "follow_up": {
        await this.requestCommand("follow_up", { type: "follow_up", message: command.text });
        return;
      }
      case "steer": {
        // Steering a session that is not running must never start a turn:
        // reject before writing any frame.
        if (this.currentStatus !== "running") {
          throw new LiveTransportError(
            liveError(
              this.provider,
              "LIVE_NOT_RUNNING",
              "steer was rejected while no provider turn was in flight; no turn was started",
              "state",
              true,
            ),
          );
        }
        await this.requestCommand("steer", { type: "steer", message: command.text });
        return;
      }
      case "cancel": {
        if (this.currentStatus === "running") {
          this.setStatus("cancelling", command.reason);
        }
        await this.requestCommand("cancel", { type: "abort" });
        if (this.currentStatus === "cancelling") {
          // Provider `abort` responses arrive only after the session idled.
          this.noteTurnEnd("cancel acknowledged by the provider");
        }
        return;
      }
      case "status": {
        const response = await this.requestCommand("status", { type: "get_state" });
        const state = objectField(response.data) ?? {};
        this.setStatus(isBusyState(state) ? "running" : "idle", "provider get_state");
        return;
      }
      case "permission_response": {
        throw new LiveTransportError(
          liveError(
            this.provider,
            "LIVE_CAPABILITY_UNSUPPORTED",
            "this transport declares permission_response unsupported; the answer never reached the provider",
            "capability",
            false,
          ),
        );
      }
    }
  }

  async *events(): AsyncIterable<LiveEvent> {
    this.requireOpen();
    if (this.pumpConsumed) {
      throw new LiveTransportError(
        liveError(this.provider, "LIVE_EVENTS_ALREADY_CONSUMED", "the event pump has a single consumer", "transport", false),
      );
    }
    this.pumpConsumed = true;
    for (;;) {
      const next = this.eventQueue.shift();
      if (next) {
        yield next;
        continue;
      }
      if (this.pumpEnded) {
        return;
      }
      await new Promise<void>((resolve) => {
        this.wakePump = resolve;
      });
    }
  }

  stop(mode: LiveStopMode): Promise<LiveStopReport> {
    this.requireOpen();
    if (!this.stopPromise) {
      this.stopPromise = this.shutdown(mode);
    }
    return this.stopPromise;
  }

  private async shutdown(mode: LiveStopMode): Promise<LiveStopReport> {
    const wire = this.wire;
    if (!wire) {
      this.currentStatus = "closed";
      this.endPump();
      return { status: "closed", exit_code: null, exit_signal: null, waited_ms: 0 };
    }

    const startedAt = Date.now();
    this.shutdownInitiated = true;
    if (!this.fatal) {
      this.setStatus("closing", "hub requested shutdown");
    }
    wire.endStdin();

    let exit = await raceTimeout(wire.exited, this.shutdownGraceMs);
    if (!exit) {
      wire.signal("SIGTERM");
      exit = await raceTimeout(wire.exited, this.shutdownGraceMs);
      if (!exit && mode === "terminate") {
        wire.signal("SIGKILL");
        exit = await raceTimeout(wire.exited, this.shutdownGraceMs);
      }
    }

    const waitedMs = Date.now() - startedAt;
    if (!exit) {
      this.currentStatus = "orphaned";
      return { status: "orphaned", exit_code: null, exit_signal: null, waited_ms: waitedMs };
    }
    // `onExit` (attached first) records the exit event and terminal status;
    // `closed` is reported only with that proof.
    await this.pumpSettled;
    return { status: "closed", exit_code: exit.exitCode, exit_signal: exit.exitSignal, waited_ms: waitedMs };
  }

  // -- Frame ingest -----------------------------------------------------------

  /** Byte-level LF framing: split on \n only, strip an optional trailing \r. */
  private ingest(chunk: Buffer): void {
    this.byteBuffer += this.byteDecoder.write(chunk);
    for (;;) {
      const newline = this.byteBuffer.indexOf("\n");
      if (newline === -1) {
        if (Buffer.byteLength(this.byteBuffer, "utf8") > MAX_FRAME_BYTES) {
          this.failProtocol("a stdout record exceeded the frame size guard");
          return;
        }
        return;
      }
      let line = this.byteBuffer.slice(0, newline);
      this.byteBuffer = this.byteBuffer.slice(newline + 1);
      if (line.endsWith("\r")) {
        line = line.slice(0, -1);
      }
      if (line.length === 0) {
        continue;
      }
      this.dispatch(line);
      if (this.fatal) {
        return;
      }
    }
  }

  private dispatch(line: string): void {
    let value: unknown;
    try {
      value = JSON.parse(line);
    } catch {
      this.failProtocol("a stdout record was not valid JSON");
      return;
    }
    const frame = objectField(value);
    if (!frame) {
      this.failProtocol("a stdout record was not a JSON object");
      return;
    }
    if (typeof frame.type !== "string" || frame.type.length === 0) {
      this.failProtocol("a stdout record carried no string type discriminator");
      return;
    }
    if (frame.type === "response") {
      this.handleResponse(frame);
      return;
    }
    this.handleProviderFrame(frame);
  }

  /** Structural violations are terminal; only a bounded reason crosses, never frame content. */
  protected failProtocol(reason: string): void {
    if (this.fatal) {
      return;
    }
    this.fatal = true;
    this.pushEvent({
      kind: "error",
      error: liveError(
        this.provider,
        "PROVIDER_PROTOCOL_UNSUPPORTED",
        `provider stdout violates the verified ${this.id} frame dialect: ${reason}`,
        "protocol",
        false,
      ),
    });
    const wire = this.wire;
    if (wire) {
      this.shutdownInitiated = true;
      wire.endStdin();
      wire.signal("SIGTERM");
      void raceTimeout(wire.exited, this.shutdownGraceMs).then((exit) => {
        if (!exit) {
          wire.signal("SIGKILL");
        }
      });
    } else {
      this.rejectAllPending("LIVE_SESSION_CLOSED", "protocol violation before spawn");
      this.endPump();
    }
    this.fatalReject?.(
      new LiveTransportError(
        liveError(
          this.provider,
          "PROVIDER_PROTOCOL_UNSUPPORTED",
          `provider stdout violates the verified ${this.id} frame dialect: ${reason}`,
          "protocol",
          false,
        ),
      ),
    );
  }

  private handleResponse(frame: RpcFrame): void {
    const id = typeof frame.id === "string" ? frame.id : null;
    const pending = id ? this.pending.get(id) : undefined;
    if (!id || !pending) {
      // Correlation edges from the dialect: unknown-command responses arrive
      // with no id, and `prompt` may get a late same-id error. Surface them
      // as bounded notices; never invent a resolution.
      const command =
        typeof frame.command === "string" && /^[a-z_]{1,32}$/.test(frame.command) ? frame.command : "unknown";
      if (frame.success === false) {
        this.emitLog("warn", `unmatched provider response: failed ${command}`);
      } else {
        this.emitLog("info", `provider_notice:response ${command}`);
      }
      return;
    }
    this.pending.delete(id);

    if (frame.success === true) {
      pending.resolve(frame);
      this.onResponseSuccess(frame);
      return;
    }
    pending.reject(
      new LiveTransportError(
        liveError(
          this.provider,
          "LIVE_COMMAND_REJECTED",
          `the provider rejected the ${pending.kind} command before completing it`,
          "provider",
          false,
        ),
      ),
    );
  }

  // -- Commands ---------------------------------------------------------------

  protected requestCommand(kind: string, fields: Record<string, unknown>, timeoutMs?: number): Promise<RpcFrame> {
    if (this.pumpEnded) {
      return Promise.reject(
        new LiveTransportError(
          liveError(this.provider, "LIVE_SESSION_CLOSED", "live session is closed", "transport", false),
        ),
      );
    }
    const id = `hub-${++this.requestId}`;
    const frame = { id, ...fields };
    return new Promise<RpcFrame>((resolve, reject) => {
      const timer = setTimeout(() => {
        if (this.pending.delete(id)) {
          reject(
            new LiveTransportError(
              liveError(
                this.provider,
                "LIVE_COMMAND_TIMEOUT",
                `the provider did not answer the ${kind} command in time`,
                "transport",
                true,
              ),
            ),
          );
        }
      }, clampMs(timeoutMs ?? this.commandTimeoutMs));
      this.pending.set(id, {
        kind,
        resolve: (response) => {
          clearTimeout(timer);
          resolve(response);
        },
        reject: (error) => {
          clearTimeout(timer);
          reject(error);
        },
      });
      this.wire?.write(`${JSON.stringify(frame)}\n`);
    });
  }

  /** Optimistic or post-ready `get_state`; command timeouts map to handshake timeouts. */
  protected async requestState(timeoutMs: number): Promise<RpcFrame> {
    try {
      return await this.requestCommand("get_state", { type: "get_state" }, timeoutMs);
    } catch (error) {
      if (error instanceof LiveTransportError && error.liveError.code === "LIVE_COMMAND_TIMEOUT") {
        throw new LiveTransportError(this.handshakeTimeoutDetail());
      }
      throw error;
    }
  }

  /** Structured error for a handshake (ready frame or optimistic `get_state`) that never completed. */
  protected handshakeTimeoutDetail(): LiveError {
    return liveError(
      this.provider,
      "LIVE_TRANSPORT_HANDSHAKE_TIMEOUT",
      `the provider did not answer the ${this.id} handshake in time`,
      "launch",
      true,
    );
  }

  private rejectAllPending(code: string, message: string): void {
    for (const entry of this.pending.values()) {
      entry.reject(
        new LiveTransportError(
          liveError(this.provider, code, `the ${entry.kind} command did not complete: ${message}`, "transport", true),
        ),
      );
    }
    this.pending.clear();
  }

  // -- Child exit ---------------------------------------------------------------

  private onExit(exit: RpcWireExit): void {
    if (this.pumpEnded) {
      return;
    }
    const intentional = this.shutdownInitiated || this.fatal;
    if (!intentional) {
      this.pushEvent({
        kind: "error",
        error: liveError(
          this.provider,
          "LIVE_PROVIDER_EXITED_UNEXPECTEDLY",
          "the provider process exited without a hub-initiated shutdown",
          "provider",
          true,
        ),
      });
      this.currentStatus = "error";
    } else {
      this.currentStatus = "closed";
    }
    this.pushEvent({ kind: "exit", intentional, exit_code: exit.exitCode, exit_signal: exit.exitSignal });
    this.rejectAllPending("LIVE_SESSION_CLOSED", "the provider process exited");
    this.endPump();
  }

  // -- Event pump ---------------------------------------------------------------

  protected setStatus(next: LiveStatus, note: string | null = null, force = false): void {
    if (this.currentStatus === "closed" || this.currentStatus === "error" || this.pumpEnded) {
      return;
    }
    if (!force && next === this.currentStatus) {
      return;
    }
    this.currentStatus = next;
    this.pushEvent({ kind: "status", status: next, note });
  }

  protected get status(): LiveStatus {
    return this.currentStatus;
  }

  protected noteTurnStart(): void {
    this.setStatus("running");
  }

  protected noteTurnEnd(note: string | null = "turn settled"): void {
    this.closeActiveMessage();
    this.setStatus("idle", note);
  }

  protected emitProviderNotice(type: string): void {
    this.emitLog("info", `provider_notice:${type}`);
  }

  protected emitLog(level: "info" | "warn" | "error", text: string): void {
    this.pushEvent({ kind: "log", level, text: this.bound(text) });
  }

  protected pushEvent(body: LiveEventBody): void {
    if (this.pumpEnded) {
      return;
    }
    this.eventSeq += 1;
    this.eventQueue.push({
      live_session_id: this.liveSessionId,
      seq: this.eventSeq,
      transport: this.id,
      occurred_at: new Date().toISOString(),
      body,
    });
    this.wakePump?.();
    this.wakePump = null;
  }

  private endPump(): void {
    if (this.pumpEnded) {
      return;
    }
    this.pumpEnded = true;
    this.wakePump?.();
    this.wakePump = null;
    this.resolvePumpSettled();
  }

  // -- Message/tool/usage mapping (shared across both providers) ----------------

  protected onMessageStart(frame: RpcFrame): void {
    const message = objectField(frame.message);
    if (message && readString(message, "role") === "assistant") {
      this.messageSeq += 1;
      this.activeMessage = { tag: `m${this.messageSeq}`, openBlocks: new Set() };
    }
  }

  protected onMessageUpdate(frame: RpcFrame): void {
    const delta = objectField(frame.assistantMessageEvent);
    if (!delta || typeof delta.type !== "string") {
      return;
    }
    const contentIndex = typeof delta.contentIndex === "number" ? delta.contentIndex : 0;

    switch (delta.type) {
      case "text_delta":
      case "thinking_delta": {
        const text = readString(delta, "delta");
        if (text === null || text.length === 0) {
          return;
        }
        const block = delta.type === "thinking_delta" ? `r${contentIndex}` : `t${contentIndex}`;
        const active = this.ensureActiveMessage();
        active.openBlocks.add(block);
        this.pushEvent({
          kind: "text",
          role: delta.type === "thinking_delta" ? "reasoning" : "assistant",
          stream_id: `${active.tag}:${block}`,
          text: this.bound(text),
          final: false,
        });
        return;
      }
      case "text_end":
      case "thinking_end": {
        const block = delta.type === "thinking_end" ? `r${contentIndex}` : `t${contentIndex}`;
        const active = this.activeMessage;
        if (active && active.openBlocks.delete(block)) {
          this.pushEvent({
            kind: "text",
            role: delta.type === "thinking_end" ? "reasoning" : "assistant",
            stream_id: `${active.tag}:${block}`,
            text: this.bound(""),
            final: true,
          });
        }
        return;
      }
      default:
        // toolcall_* deltas and anything newer: tool correlation comes from
        // the tool_execution_* frames, so these are intentionally not mapped.
        return;
    }
  }

  protected onMessageEnd(_frame: RpcFrame): void {
    this.closeActiveMessage();
  }

  private ensureActiveMessage(): { tag: string; openBlocks: Set<string> } {
    if (!this.activeMessage) {
      this.messageSeq += 1;
      this.activeMessage = { tag: `m${this.messageSeq}`, openBlocks: new Set() };
    }
    return this.activeMessage;
  }

  private closeActiveMessage(): void {
    const active = this.activeMessage;
    if (!active) {
      return;
    }
    for (const block of active.openBlocks) {
      this.pushEvent({
        kind: "text",
        role: block.startsWith("r") ? "reasoning" : "assistant",
        stream_id: `${active.tag}:${block}`,
        text: this.bound(""),
        final: true,
      });
    }
    this.activeMessage = null;
  }

  protected onToolStart(frame: RpcFrame): void {
    const callId = readString(frame, "toolCallId");
    const tool = readString(frame, "toolName");
    if (callId === null || tool === null) {
      this.emitLog("warn", "provider_notice:tool_execution_start (unusable correlation fields)");
      return;
    }
    const args = frame.args;
    this.pushEvent({
      kind: "tool_start",
      call_id: callId,
      tool,
      input_preview: args === undefined ? null : this.bound(safeJson(args)),
    });
  }

  protected onToolEnd(frame: RpcFrame): void {
    const callId = readString(frame, "toolCallId");
    const tool = readString(frame, "toolName");
    if (callId === null || tool === null) {
      this.emitLog("warn", "provider_notice:tool_execution_end (unusable correlation fields)");
      return;
    }
    const result = objectField(frame.result);
    const content = result && Array.isArray(result.content) ? result.content : null;
    const texts: string[] = [];
    if (content) {
      for (const part of content) {
        if (typeof part === "object" && part !== null && "text" in part && typeof part.text === "string") {
          texts.push(part.text);
        }
      }
    }
    const text = content ? texts.join("\n") : null;
    this.pushEvent({
      kind: "tool_end",
      call_id: callId,
      tool,
      ok: frame.isError !== true,
      output_preview: text === null ? null : this.bound(text),
    });
  }

  /** pi-dialect `usage` object → LiveUsage; null when the frame reports none. */
  protected mapUsage(usage: unknown): LiveUsage | null {
    const usageRecord = objectField(usage);
    if (!usageRecord) {
      return null;
    }
    const cost = objectField(usageRecord.cost);
    return {
      input_tokens: readNumber(usageRecord, "input"),
      output_tokens: readNumber(usageRecord, "output"),
      cached_tokens: readNumber(usageRecord, "cacheRead"),
      cost_usd: cost ? readNumber(cost, "total") : null,
    };
  }

  /** Emit a usage event only when the cumulative counters actually changed. */
  protected emitUsageIfChanged(usage: unknown): void {
    const mapped = this.mapUsage(usage);
    if (!mapped) {
      return;
    }
    const json = JSON.stringify(mapped);
    if (json === this.lastUsageJson) {
      return;
    }
    this.lastUsageJson = json;
    this.pushEvent({ kind: "usage", usage: mapped });
  }

  // -- Shared helpers ------------------------------------------------------------

  protected bound(text: string): LiveBoundedText {
    const bytes = Buffer.byteLength(text, "utf8");
    if (bytes <= this.maxTextBytes) {
      return { text, truncated: false };
    }
    const slice = Buffer.from(text, "utf8").subarray(0, this.maxTextBytes);
    let cut = slice.toString("utf8");
    while (cut.endsWith("\uFFFD")) {
      cut = cut.slice(0, -1);
    }
    return { text: cut, truncated: true };
  }

  private requireOpen(): void {
    if (!this.opened) {
      throw new LiveTransportError(
        liveError(this.provider, "LIVE_SESSION_NOT_OPEN", "open() must be called before using the transport", "transport", false),
      );
    }
  }
}

// ---------------------------------------------------------------------------
// Small guards
// ---------------------------------------------------------------------------

function safeJson(value: unknown): string {
  try {
    return JSON.stringify(value) ?? String(value);
  } catch {
    return "[unserializable]";
  }
}

/** Timer budgets are clamped so injected test budgets can never hang a timer. */
function clampMs(value: number): number {
  return Math.max(1, Math.min(600_000, value));
}

export function raceTimeout<T>(promise: Promise<T>, ms: number): Promise<T | undefined> {
  const bounded = clampMs(ms);
  return new Promise<T | undefined>((resolve) => {
    const timer = setTimeout(() => resolve(undefined), bounded);
    promise.then(
      (value) => {
        clearTimeout(timer);
        resolve(value);
      },
      () => {
        clearTimeout(timer);
        resolve(undefined);
      },
    );
  });
}
