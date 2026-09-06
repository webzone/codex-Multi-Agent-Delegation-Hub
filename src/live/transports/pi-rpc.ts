/**
 * Live transport for provider `pi` over `pi --mode rpc` (dialect verified
 * against the installed pi 0.85.0).
 *
 * Dialect facts this implementation is allowed to rely on — nothing else is
 * guessed:
 *
 *   - PI has **no ready frame** (verified against 0.85.0: the first stdout
 *     record is the correlated response to whatever was sent). The handshake
 *     is therefore an optimistic, correlated `get_state` with a timeout; any
 *     event frames that arrive before the response are processed normally.
 *   - Framing is strict JSONL on LF only, per the shipped `docs/rpc.md`:
 *     split on `\n`, strip a trailing `\r`, and never use Node `readline`
 *     (it also splits on U+2028/U+2029, which are legal inside JSON strings).
 *     The shared byte-level framer in `rpc-base.ts` implements exactly that.
 *   - Commands used: `prompt`, `follow_up`, `steer` (mid-turn only), `abort`,
 *     `get_state`, `switch_session`. `switch_session` answers
 *     `success:true` even when an extension *cancelled* the switch, so one
 *     success response can be false: a resume counts only when
 *     `data.cancelled === false` **and** a fresh `get_state` echoes the
 *     session locator.
 *   - `agent_end` may be followed by retry/compaction/queued continuations;
 *     `agent_settled` is the true turn-end signal in this dialect.
 *   - `message_update` carries a cumulative top-level `usage` object; it is
 *     mapped field-by-field with unreported fields staying null.
 *   - `--no-session` is never passed: sessions must persist for resume.
 */
import type {
  LiveCapabilityClaim,
  LiveCapabilities,
  LiveLaunchRequest,
  LiveTransportDescriptor,
  LiveTransportFactory,
  PiResumeState,
  ProviderResumeState,
  ResumeVerification,
} from "../types.js";
import { probePi } from "../probes/pi.js";
import {
  type RpcFrame,
  RpcSessionBase,
  type RpcSessionOptions,
  objectField,
  readString,
} from "./rpc-base.js";

const PI_DIALECT_EVIDENCE =
  "pi 0.85.0: shipped docs/rpc.md plus a live `pi --mode rpc` probe (no ready frame; first frame was the correlated get_state response)";

function claim(support: Exclude<LiveCapabilityClaim["support"], "unsupported">, evidence: string): LiveCapabilityClaim {
  return { support, evidence };
}

const PI_CAPABILITIES: LiveCapabilities = {
  prompt: claim("native", `${PI_DIALECT_EVIDENCE}; prompt answers with a same-id success ack on acceptance, events continue after`),
  follow_up: claim("native", `${PI_DIALECT_EVIDENCE}; follow_up queues next-turn input with a same-id success ack`),
  steer: claim(
    "native",
    `${PI_DIALECT_EVIDENCE}; steer delivers mid-turn only — a steer while not running is rejected LIVE_NOT_RUNNING before any frame is written`,
  ),
  cancel: claim("native", `${PI_DIALECT_EVIDENCE}; abort waits for idle before responding, so the response itself proves delivery`),
  status: claim("native", `${PI_DIALECT_EVIDENCE}; get_state reports isStreaming/isCompacting authoritatively`),
  // The extension UI sub-protocol exists, but its decision semantics do not
  // map onto hub permission verdicts with verified evidence; claim nothing.
  permission_response: { support: "unsupported", evidence: null },
  resume: claim(
    "native",
    `${PI_DIALECT_EVIDENCE}; a success response can be false (cancelled switch), so resume counts only after data.cancelled=false plus a get_state locator echo`,
  ),
  checkpoint: { support: "unsupported", evidence: null },
  usage_reporting: claim(
    "native",
    `${PI_DIALECT_EVIDENCE}; message_update carries {input,output,cacheRead,cacheWrite,cost.total}; mapped with unreported fields null and events only on change`,
  ),
};

export class PiRpcTransport extends RpcSessionBase {
  constructor(options: RpcSessionOptions = {}) {
    super("pi-rpc", "pi", options);
  }

  describe(): Promise<LiveTransportDescriptor> {
    return Promise.resolve({
      transport: "pi-rpc",
      provider: "pi",
      capabilities: PI_CAPABILITIES,
    });
  }

  protected buildArgv(): string[] {
    // Never `--no-session`: live resume rides provider session persistence.
    return ["pi", "--mode", "rpc"];
  }

  protected resumeHandle(resume: ProviderResumeState): string | null {
    return resume.provider === "pi" ? resume.resume_token : null;
  }

  protected buildResumeState(
    locator: string | null,
    prior: ProviderResumeState | null,
    verification: ResumeVerification,
  ): PiResumeState {
    // PI's opaque resume locator IS the observed session handle
    // (`sessionFile` else `sessionId`) fed back to `switch_session`. It was
    // never persisted before this launch observed it — the previous
    // transport-side hole that made every PI restart start fresh.
    const token =
      locator ??
      (prior?.provider === "pi" ? prior.resume_token : null);
    return {
      provider: "pi",
      provider_session_id: locator,
      ...verification,
      resume_token: token,
    };
  }

  protected async handshake(_request: LiveLaunchRequest): Promise<string | null> {
    // No ready frame exists in this dialect: send the correlated probe
    // immediately and accept events that beat the response.
    const state = await this.requestState(this.handshakeTimeoutMs);
    const data = objectField(state.data);
    if (!data) {
      return null;
    }
    return readString(data, "sessionFile") ?? readString(data, "sessionId");
  }

  protected handleProviderFrame(frame: RpcFrame): void {
    const type = readString(frame, "type") ?? "unknown";
    switch (type) {
      case "agent_start": {
        this.noteTurnStart();
        return;
      }
      case "agent_end": {
        // In this dialect the run may continue through retry, compaction, or
        // queued follow-ups; `agent_settled` is the terminal signal.
        this.emitProviderNotice("agent_end");
        return;
      }
      case "agent_settled": {
        this.noteTurnEnd();
        return;
      }
      case "message_start": {
        this.onMessageStart(frame);
        return;
      }
      case "message_update": {
        this.emitUsageIfChanged(frame.usage);
        this.onMessageUpdate(frame);
        return;
      }
      case "message_end": {
        this.onMessageEnd(frame);
        return;
      }
      case "tool_execution_start": {
        this.onToolStart(frame);
        return;
      }
      case "tool_execution_end": {
        this.onToolEnd(frame);
        return;
      }
      case "compaction_end": {
        const error = readString(frame, "errorMessage");
        if (error !== null) {
          this.emitLog("error", error);
        } else {
          this.emitProviderNotice("compaction_end");
        }
        return;
      }
      case "auto_retry_end": {
        if (frame.success === false) {
          this.emitLog("error", readString(frame, "finalError") ?? "provider_notice:auto_retry_end failed");
        } else {
          this.emitProviderNotice("auto_retry_end");
        }
        return;
      }
      case "extension_error": {
        this.emitLog("error", readString(frame, "error") ?? "provider_notice:extension_error");
        return;
      }
      case "bash_execution_update": {
        // The hub never sends the direct `bash` command; seeing one means
        // traffic it cannot have asked for.
        this.emitLog("warn", "provider_notice:bash_execution_update (no bash command was sent)");
        return;
      }
      case "extension_ui_request": {
        const method = readString(frame, "method");
        this.emitLog("info", method ? `provider_notice:extension_ui_request ${method}` : "provider_notice:extension_ui_request");
        return;
      }
      default: {
        // turn_*, queue_update, compaction_start, auto_retry_start,
        // summarization_retry_*, and every semantically unknown frame are
        // tolerated as notices: kind or short label only, never raw content.
        this.emitProviderNotice(type);
        return;
      }
    }
  }
}

export const piRpcTransportFactory: LiveTransportFactory = {
  transport: "pi-rpc",
  provider: "pi",
  probe: () => probePi(),
  create: () => new PiRpcTransport(),
};
