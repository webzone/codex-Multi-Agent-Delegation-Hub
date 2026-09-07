import { setImmediate } from "node:timers";

import type {
  Capabilities,
  Command,
  EventBody,
  LaunchReport,
  LaunchRequest,
  ProcessFacts,
  ProbeResult,
  ProviderFactory,
  ProviderId,
  ResumeState,
  SessionEvent,
  SessionRecord,
  StopMode,
  StopReport,
  Transport,
  TransportDescriptor,
  TransportFactory,
  TransportId,
} from "../src/kernel/contracts.js";
import { SESSION_SCHEMA_VERSION } from "../src/kernel/contracts.js";

/** A total, honest snapshot; override per test what the test is about. */
export function fullCapabilities(overrides: Partial<Capabilities> = {}): Capabilities {
  return {
    prompt: { support: "native", evidence: "fake ready handshake observed" },
    follow_up: { support: "hub-queued", evidence: "fake queues between turns" },
    steer: { support: "unsupported", evidence: null },
    cancel: { support: "native", evidence: "fake interrupt accepted mid-turn" },
    status: { support: "derived", evidence: "fake answers from stream evidence" },
    permission_response: { support: "native", evidence: "fake permission_response accepted" },
    resume: { support: "native", evidence: "fake session id round-tripped in probe" },
    usage_reporting: { support: "unsupported", evidence: null },
    ...overrides,
  };
}

export function textEvent(
  streamId: string,
  text: string,
  final: boolean,
): EventBody {
  return {
    kind: "text",
    role: "assistant",
    stream_id: streamId,
    text: { text, truncated: false },
    final,
  };
}

export const AT = "2026-09-07T00:00:00.000Z";
export const fixedClock = { now: (): Date => new Date(AT) };

/** Let pump/mirror microtask chains settle (used for not-resolved assertions). */
export async function flush(times = 4): Promise<void> {
  for (let i = 0; i < times; i += 1) {
    await new Promise((resolve) => setImmediate(resolve));
  }
}

/**
 * In-control fake transport: the test drives `emit`/`end`, observes `sent`,
 * and programs open/send/stop outcomes. Only `body` on emitted events is
 * meaningful — the kernel must re-stamp every envelope fact.
 */
export class FakeTransport implements Transport {
  readonly id: TransportId;
  readonly provider: ProviderId;
  readonly sent: Command[] = [];
  readonly stopCalls: StopMode[] = [];
  openRequest: LaunchRequest | null = null;
  openReport: LaunchReport = {
    pid: null,
    provider_session_id: "provider-session-1",
    launched_at: AT,
  };
  openError: Error | null = null;
  spawnFacts: ProcessFacts | null = { pid: 4242, pgid: 4242 };
  sendError: Error | null = null;
  stopReport: StopReport = { status: "closed", exit_code: 0, exit_signal: null, waited_ms: 1 };
  stopError: Error | null = null;

  private readonly pending: SessionEvent[] = [];
  private wake: (() => void) | null = null;
  private ended = false;

  constructor(private readonly descriptorValue: TransportDescriptor) {
    this.id = descriptorValue.transport;
    this.provider = descriptorValue.provider;
  }

  async describe(): Promise<TransportDescriptor> {
    return this.descriptorValue;
  }

  async open(request: LaunchRequest): Promise<LaunchReport> {
    this.openRequest = request;
    // Ownership before handshake can fail, exactly as the contract demands.
    if (request.report_process && this.spawnFacts !== null) {
      await request.report_process({ ...this.spawnFacts });
    }
    if (this.openError !== null) {
      throw this.openError;
    }
    return structuredClone(this.openReport);
  }

  async send(command: Command): Promise<void> {
    if (this.sendError !== null) {
      throw this.sendError;
    }
    this.sent.push(structuredClone(command));
  }

  events(): AsyncIterable<SessionEvent> {
    const state = this;
    return {
      [Symbol.asyncIterator](): AsyncIterator<SessionEvent> {
        return {
          next: async (): Promise<IteratorResult<SessionEvent>> => {
            for (;;) {
              if (state.pending.length > 0) {
                return { value: state.pending.shift() as SessionEvent, done: false };
              }
              if (state.ended) {
                return { value: undefined as never, done: true };
              }
              await new Promise<void>((resolve) => {
                state.wake = resolve;
              });
            }
          },
        };
      },
    };
  }

  emit(body: EventBody): void {
    this.pending.push({
      // Junk envelope facts: the kernel must overwrite all of them.
      session_id: "junk",
      seq: -7,
      transport: "junk-transport",
      occurred_at: "1970-01-01T00:00:00.000Z",
      body,
    });
    const wake = this.wake;
    this.wake = null;
    wake?.();
  }

  end(): void {
    this.ended = true;
    const wake = this.wake;
    this.wake = null;
    wake?.();
  }

  async stop(mode: StopMode): Promise<StopReport> {
    this.stopCalls.push(mode);
    if (this.stopError !== null) {
      throw this.stopError;
    }
    return { ...this.stopReport };
  }
}

export class FakeFactory implements TransportFactory {
  readonly created: FakeTransport[] = [];
  probeResult: ProbeResult = { found: true, version: "9.9.9", detail: "fake installed" };
  probeCalls = 0;
  configureTransport: ((transport: FakeTransport) => void) | null = null;

  constructor(
    readonly transport: TransportId,
    readonly provider: ProviderId,
    private capabilities: Capabilities = fullCapabilities(),
  ) {}

  async probe(): Promise<ProbeResult> {
    this.probeCalls += 1;
    return { ...this.probeResult };
  }

  create(): FakeTransport {
    const transport = new FakeTransport({
      transport: this.transport,
      provider: this.provider,
      capabilities: this.capabilities,
    });
    this.configureTransport?.(transport);
    this.created.push(transport);
    return transport;
  }
}

export class FakeProviderFactory implements ProviderFactory {
  pick: (factories: readonly TransportFactory[]) => TransportFactory | null = (factories) =>
    factories[0] ?? null;

  constructor(
    readonly provider: ProviderId,
    readonly transports: readonly TransportId[],
  ) {}

  selectTransport(factories: readonly TransportFactory[]): TransportFactory | null {
    return this.pick(factories);
  }
}

/** A durable mirror that records every committed record in order. */
export class RecordingMirror {
  readonly records: SessionRecord[] = [];
  failWhen: ((record: SessionRecord) => boolean) | null = null;
  failures = 0;

  async commit(record: SessionRecord): Promise<void> {
    if (this.failWhen !== null && this.failWhen(record)) {
      this.failures += 1;
      throw new Error("mirror is unavailable (fake)");
    }
    this.records.push(record);
  }

  get latest(): SessionRecord {
    const last = this.records[this.records.length - 1];
    if (last === undefined) {
      throw new Error("no records committed");
    }
    return last;
  }
}

export function resumeState(overrides: Partial<ResumeState> = {}): ResumeState {
  const base = {
    provider: overrides.provider ?? "fake",
    provider_session_id:
      overrides.provider_session_id === undefined ? "provider-session-1" : overrides.provider_session_id,
    data: overrides.data ?? { token: "opaque-token" },
    last_event_seq: overrides.last_event_seq ?? 0,
  };
  if (overrides.verified === true) {
    return { ...base, verified: true, verified_via: overrides.verified_via ?? "test-fixture" };
  }
  return { ...base, verified: false, verified_via: null };
}

export function sessionRecord(overrides: Partial<SessionRecord> = {}): SessionRecord {
  return {
    schema: SESSION_SCHEMA_VERSION,
    session_id: "11111111-2222-4333-8444-555555555555",
    provider: "fake",
    transport: "fake-rpc",
    capabilities: fullCapabilities(),
    workspace: "/fake/workspace",
    max_text_bytes: 65536,
    resume: resumeState(),
    status: "closed",
    revision: 3,
    last_error: null,
    created_at: AT,
    updated_at: AT,
    ...overrides,
  };
}
