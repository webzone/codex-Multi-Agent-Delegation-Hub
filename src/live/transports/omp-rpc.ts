/**
 * Live transport for provider `omp` over `omp --mode rpc` (dialect verified
 * against the installed OMP 18.1.10).
 *
 * Dialect facts this implementation is allowed to rely on — nothing else is
 * guessed:
 *
 *   - Startup writes exactly one ready frame before processing commands:
 *     `{type:"ready", protocolVersion:1, ...}`. The handshake waits for it,
 *     tolerating any unsolicited frames that arrive first (observed from the
 *     installed binary: `extension_ui_request` setWidget and
 *     `available_commands_update` precede command replies). Such frames, and
 *     every semantically unknown frame, are tolerated as `provider_notice`
 *     log events carrying only the frame type or a short provider label —
 *     never raw content.
 *   - `protocolVersion === 1` is the verified framing dialect; anything else
 *     is `PROVIDER_PROTOCOL_UNSUPPORTED`. The transports never send
 *     `negotiate_protocol`, so protocol-v2 `rpc_chunk` reassembly never
 *     applies and the old bounded-chunk dialect is never guessed.
 *   - Commands used: `prompt`, `follow_up`, `steer` (mid-turn only), `abort`,
 *     `get_state`, `switch_session`. `switch_session` resolves a session
 *     *path*, so the durable locator is the `get_state` `sessionFile`.
 *   - `agent_end` closes a turn only when `isTerminal !== false` (an omitted
 *     field is terminal for dialect compatibility).
 *   - `--no-session` is never passed: sessions must persist for resume.
 */
import type {
  LiveCapabilityClaim,
  LiveCapabilities,
  LiveLaunchRequest,
  LiveTransportDescriptor,
  LiveTransportFactory,
  ProviderResumeState,
} from "../types.js";
import { probeOmp } from "../probes/omp.js";
import {
  type RpcFrame,
  LiveTransportError,
  RpcSessionBase,
  type RpcSessionOptions,
  objectField,
  raceTimeout,
  readString,
} from "./rpc-base.js";

const OMP_DIALECT_EVIDENCE =
  "omp 18.1.10: `omp --mode rpc` ready frame, unsolicited startup frames, and a correlated get_state response observed from the installed binary; command schema from the shipped OMP RPC reference";

function claim(support: Exclude<LiveCapabilityClaim["support"], "unsupported">, evidence: string): LiveCapabilityClaim {
  return { support, evidence };
}

const OMP_CAPABILITIES: LiveCapabilities = {
  prompt: claim("native", `${OMP_DIALECT_EVIDENCE}; prompt answers with a same-id success ack while agent lifecycle events carry turn progress`),
  follow_up: claim("native", `${OMP_DIALECT_EVIDENCE}; follow_up queues next-turn input with a same-id success ack`),
  steer: claim(
    "native",
    `${OMP_DIALECT_EVIDENCE}; steer delivers mid-turn only — a steer while not running is rejected LIVE_NOT_RUNNING before any frame is written`,
  ),
  cancel: claim("native", `${OMP_DIALECT_EVIDENCE}; abort answers once the session is idle, so the response itself proves delivery`),
  status: claim("native", `${OMP_DIALECT_EVIDENCE}; get_state reports isStreaming/isCompacting authoritatively`),
  permission_response: { support: "unsupported", evidence: null },
  resume: claim(
    "native",
    `${OMP_DIALECT_EVIDENCE}; resume counts only after a switch_session (success, cancelled=false) plus a get_state locator echo round-trip`,
  ),
  checkpoint: { support: "unsupported", evidence: null },
  // The 18.1.10 reference documents message_update deltas without committing
  // a usage payload; an unverified mapping must not become a claim.
  usage_reporting: { support: "unsupported", evidence: null },
};

export class OmpRpcTransport extends RpcSessionBase {
  private readySeen = false;
  private readonly readyGate: { promise: Promise<void>; resolve: () => void };

  constructor(options: RpcSessionOptions = {}) {
    super("omp-rpc", "omp", options);
    // Executor form is required: the target lib (ES2022) has no
    // `Promise.withResolvers` typings.
    let resolve = () => {};
    const promise = new Promise<void>((r) => {
      resolve = r;
    });
    this.readyGate = { promise, resolve: () => resolve() };
  }

  describe(): Promise<LiveTransportDescriptor> {
    return Promise.resolve({
      transport: "omp-rpc",
      provider: "omp",
      capabilities: OMP_CAPABILITIES,
    });
  }

  protected buildArgv(): string[] {
    // Never `--no-session`: live resume rides provider session persistence.
    return ["omp", "--mode", "rpc"];
  }

  protected resumeHandle(resume: ProviderResumeState): string | null {
    return resume.provider === "omp" ? resume.provider_session_id : null;
  }

  protected async handshake(_request: LiveLaunchRequest): Promise<string | null> {
    await raceTimeout(this.readyGate.promise, this.readyTimeoutMs);
    if (!this.readySeen) {
      // A protocol violation would already have failed the launch through the
      // fatal race in `open()`; reaching here means the ready frame never came.
      throw new LiveTransportError(this.handshakeTimeoutDetail());
    }

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
      case "ready": {
        if (this.readySeen) {
          this.emitLog("warn", "provider_notice:ready (duplicate ready frame)");
          return;
        }
        // Only protocol v1 framing is verified; v2-default framing or a
        // malformed ready frame is outside the supported dialect. The reason
        // is hub-generated and fixed — frame content never crosses.
        if (frame.protocolVersion !== 1) {
          this.readySeen = true;
          this.failProtocol("the ready frame did not advertise protocol v1");
          return;
        }
        this.readySeen = true;
        this.readyGate.resolve();
        return;
      }
      case "agent_start": {
        this.noteTurnStart();
        return;
      }
      case "agent_end": {
        // `isTerminal: false` means more work is scheduled; an omitted field
        // is terminal in this dialect.
        if (frame.isTerminal !== false) {
          this.noteTurnEnd();
        }
        return;
      }
      case "message_start": {
        this.onMessageStart(frame);
        return;
      }
      case "message_update": {
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
      case "notice": {
        this.emitLog("info", readString(frame, "text") ?? "provider_notice:notice");
        return;
      }
      case "extension_error": {
        this.emitLog("error", readString(frame, "error") ?? "provider_notice:extension_error");
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
      case "extension_ui_request": {
        const method = readString(frame, "method");
        this.emitLog("info", method ? `provider_notice:extension_ui_request ${method}` : "provider_notice:extension_ui_request");
        return;
      }
      case "rpc_chunk": {
        // Protocol v2 is never negotiated, so a chunk frame means framing we
        // cannot verify: reported, never reassembled by guess.
        this.emitLog("warn", "provider_notice:rpc_chunk (protocol v2 was never negotiated)");
        return;
      }
      case "host_tool_call":
      case "host_tool_cancel":
      case "host_uri_request":
      case "host_uri_cancel": {
        // This hub registers no host tools or URI schemes; these must not appear.
        this.emitLog("warn", `provider_notice:${type} (no host surface was registered)`);
        return;
      }
      default: {
        // Every other known-noise frame (turn_*, queue_update,
        // available_commands_update, prompt_result, retry/compaction/subagent
        // side channels, …) and every semantically unknown frame is tolerated
        // as a notice: kind or short label only, never raw content.
        this.emitProviderNotice(type);
        return;
      }
    }
  }

  protected onResponseSuccess(response: RpcFrame): void {
    // `prompt` may complete locally without an agent turn; surface that
    // completion so the hub does not wait for lifecycle events that never
    // come. An omitted `agentInvoked` means "rely on session events".
    if (readString(response, "command") !== "prompt") {
      return;
    }
    const data = objectField(response.data);
    if (data && data.agentInvoked === false) {
      this.setStatus("idle", "prompt completed locally without an agent turn (agentInvoked:false)", true);
    }
  }
}

export const ompRpcTransportFactory: LiveTransportFactory = {
  transport: "omp-rpc",
  provider: "omp",
  probe: () => probeOmp(),
  create: () => new OmpRpcTransport(),
};
