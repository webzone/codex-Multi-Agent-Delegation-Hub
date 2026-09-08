import { AgentHubError } from "../errors.js";
import type {
  Capabilities,
  Command,
  KernelError,
  LaunchReport,
  ProviderFactory,
  ProviderId,
  ProbeResult,
  ResumeState,
  ResumeVerification,
  SessionEvent,
  StopMode,
  StopReport,
  Transport,
  TransportDescriptor,
  TransportFactory,
  TransportId,
} from "../kernel/contracts.js";
import type {
  LiveCapabilities,
  LiveCommand,
  LiveError,
  LiveEventBody,
  LiveProviderId,
  LiveTransport,
  LiveTransportFactory,
  ProviderResumeState,
} from "../live/types.js";
import {
  productionProviderFactories,
  productionTransportFactories,
} from "../live/bootstrap.js";

/**
 * Transport adapter — the P4 seam between the InteractionKernel (P1) and the
 * production provider transports (P3).
 *
 * The two contract generations are structurally parallel; the adapter maps
 * the three real divergences and nothing else:
 *
 *   1. session field naming: kernel `session_id` ↔ live `live_session_id`
 *      (commands out, events in);
 *   2. capability snapshots: live descriptors carry the ninth `checkpoint`
 *      claim, which is a WorkspaceLifecycle fact, not a kernel claim. The
 *      kernel snapshot drops it; the durable lifecycle record keeps the full
 *      live snapshot from the SAME launch (fetched through this adapter);
 *   3. resume handles: the kernel's provider-neutral `ResumeState` (opaque
 *      `data` + kernel-owned cursor) ↔ the live discriminated
 *      `ProviderResumeState` unions. Fold/unfold per provider; the kernel
 *      cursor is authoritative for `omp`, and data fields are never invented.
 *
 * It also captures, per session, the facts the kernel does not carry upward:
 * the full launch-scoped live descriptor (9 claims, policy-scoped — hermes
 * honestly varies them per launch) and the transport's raw resume state.
 * `takeLaunchFacts` hands them to the hub exactly once.
 */

/** The kernel-facing view of one per-session launch, recorded by the adapter. */
export interface LaunchFacts {
  /** The full 9-claim capability snapshot from THIS launch's descriptor. */
  capabilities: LiveCapabilities;
  /** The transport's raw post-handshake resume state, when it reported one. */
  resume_state: ProviderResumeState | null;
}

/** A kernel TransportFactory that also remembers raw per-launch live facts. */
export interface BridgedTransportFactory extends TransportFactory {
  readonly inner: LiveTransportFactory;
  takeLaunchFacts(sessionId: string): LaunchFacts | null;
}

// ---------------------------------------------------------------------------
// Capability mapping (checkpoint is a lifecycle fact, never a kernel claim)
// ---------------------------------------------------------------------------

/** Drop the lifecycle-owned `checkpoint` claim for the kernel snapshot. */
export function kernelCapabilities(claims: LiveCapabilities): Capabilities {
  const { checkpoint: _checkpoint, ...rest } = claims;
  return rest as unknown as Capabilities;
}

// ---------------------------------------------------------------------------
// Resume mapping (kernel-neutral ↔ live discriminated union)
// ---------------------------------------------------------------------------

/** Rebuild the verification pair as a proper discriminated value. */
export function verificationOf(
  verified: unknown,
  verified_via: unknown,
): ResumeVerification {
  if (verified === true && typeof verified_via === "string") {
    return { verified: true, verified_via };
  }
  return { verified: false, verified_via: null };
}

/** Fold a live provider resume state into the kernel-neutral handle. */
export function kernelizeResume(state: ProviderResumeState): ResumeState {
  const verification = verificationOf(state.verified, state.verified_via);
  const base = {
    provider_session_id: state.provider_session_id,
    ...verification,
  };
  switch (state.provider) {
    case "omp":
      return {
        provider: "omp",
        ...base,
        data: {},
        last_event_seq: state.last_event_seq,
      };
    case "agy":
      return {
        provider: "agy",
        ...base,
        data: { resume_argv_verified: state.resume_argv_verified },
        last_event_seq: 0,
      };
    case "pi":
      return {
        provider: "pi",
        ...base,
        data: { resume_token: state.resume_token },
        last_event_seq: 0,
      };
    case "hermes":
      return {
        provider: "hermes",
        ...base,
        data: { session_load_advertised: state.session_load_advertised },
        last_event_seq: 0,
      };
  }
}

/**
 * Unfold a kernel-neutral handle back into the provider's resume shape.
 * Required provider fields must already be in `data` — the adapter never
 * invents a resume fact a transport did not previously report.
 */
function liveResumeOf(state: ResumeState): ProviderResumeState {
  const verification = verificationOf(state.verified, state.verified_via);
  const base = {
    provider_session_id: state.provider_session_id,
    ...verification,
  };
  switch (state.provider) {
    case "omp":
      return {
        provider: "omp",
        ...base,
        last_event_seq: state.last_event_seq,
      };
    case "agy":
      return {
        provider: "agy",
        ...base,
        resume_argv_verified: booleanDataOf(state, "resume_argv_verified"),
      };
    case "pi":
      return {
        provider: "pi",
        ...base,
        resume_token: nullableStringDataOf(state, "resume_token"),
      };
    case "hermes":
      return {
        provider: "hermes",
        ...base,
        session_load_advertised: booleanDataOf(state, "session_load_advertised"),
      };
    default:
      throw new AgentHubError(
        "RESUME_STATE_INVALID",
        `no provider transport is paired with resume provider "${state.provider}"`,
      );
  }
}

function missingResumeField(state: ResumeState, key: string): AgentHubError {
  return new AgentHubError(
    "RESUME_STATE_INVALID",
    `resume handle for "${state.provider}" is missing its transport-reported "${key}" payload`,
  );
}

function booleanDataOf(state: ResumeState, key: string): boolean {
  const value = state.data[key];
  if (typeof value !== "boolean") throw missingResumeField(state, key);
  return value;
}

function nullableStringDataOf(state: ResumeState, key: string): string | null {
  const value = state.data[key];
  if (value !== null && typeof value !== "string") throw missingResumeField(state, key);
  return value;
}

// ---------------------------------------------------------------------------
// Error-stage projection (kernel stage vocabulary is a subset of live's;
// `checkpoint` is not a kernel stage, so an inbound claim of it is projected
// to `state`, the durable stage it actually names)
// ---------------------------------------------------------------------------

export function kernelErrorOf(error: LiveError): KernelError {
  const stage = error.stage === "checkpoint" ? "state" : error.stage;
  return {
    code: error.code,
    message: error.message,
    stage,
    retryable: error.retryable,
    provider: error.provider,
  };
}

function mapEventBody(body: LiveEventBody): SessionEvent["body"] {
  if (body.kind !== "error") {
    return body as SessionEvent["body"];
  }
  return { kind: "error", error: kernelErrorOf(body.error) };
}

// ---------------------------------------------------------------------------
// The transport wrapper
// ---------------------------------------------------------------------------

class BridgedTransport implements Transport {
  readonly id: TransportId;
  readonly provider: ProviderId;

  /** Set by `open()` before `describe()` runs; the kernel always opens first. */
  private openSessionId = "";

  constructor(
    private readonly inner: LiveTransport,
    private readonly factory: BridgedTransportFactoryImpl,
  ) {
    this.id = inner.id;
    this.provider = inner.provider;
  }

  async describe(): Promise<TransportDescriptor> {
    const full = await this.inner.describe();
    // Remember the full launch-scoped snapshot for the lifecycle record.
    this.factory.rememberDescriptor(this.openSessionId, full.capabilities);
    return {
      transport: full.transport,
      provider: full.provider,
      capabilities: kernelCapabilities(full.capabilities),
    };
  }

  async open(request: Parameters<Transport["open"]>[0]): Promise<LaunchReport> {
    this.openSessionId = request.session_id;
    const report = await this.inner.open({
      live_session_id: request.session_id,
      workspace: request.workspace,
      max_text_bytes: request.max_text_bytes,
      permission_policy: request.permission_policy,
      resume: request.resume === null ? null : liveResumeOf(request.resume),
      report_process: request.report_process,
    });
    this.factory.rememberResume(request.session_id, report.resume_state ?? null);
    return {
      pid: report.pid,
      provider_session_id: report.provider_session_id,
      launched_at: report.launched_at,
      resume_state:
        report.resume_state === null || report.resume_state === undefined
          ? null
          : kernelizeResume(report.resume_state),
    };
  }

  send(command: Command): Promise<void> {
    const { session_id: sessionId, ...rest } = command;
    return this.inner.send({ ...rest, live_session_id: sessionId } as unknown as LiveCommand);
  }

  async *events(): AsyncIterable<SessionEvent> {
    for await (const event of this.inner.events()) {
      yield {
        session_id: event.live_session_id,
        seq: event.seq,
        transport: event.transport,
        occurred_at: event.occurred_at,
        body: mapEventBody(event.body),
      };
    }
  }

  stop(mode: StopMode): Promise<StopReport> {
    return this.inner.stop(mode);
  }
}

class BridgedTransportFactoryImpl implements BridgedTransportFactory {
  readonly transport: TransportId;
  readonly provider: ProviderId;

  /** Per-session raw launch facts, consumed exactly once by the hub. */
  private readonly facts = new Map<string, Partial<LaunchFacts>>();

  constructor(readonly inner: LiveTransportFactory) {
    this.transport = inner.transport;
    this.provider = inner.provider;
  }

  probe(): Promise<ProbeResult> {
    return this.inner.probe();
  }

  create(): Transport {
    return new BridgedTransport(this.inner.create(), this);
  }

  rememberDescriptor(sessionId: string, capabilities: LiveCapabilities): void {
    if (sessionId === "") return;
    this.facts.set(sessionId, {
      ...this.facts.get(sessionId),
      capabilities,
    });
  }

  rememberResume(sessionId: string, resume: ProviderResumeState | null): void {
    if (sessionId === "") return;
    this.facts.set(sessionId, {
      ...this.facts.get(sessionId),
      resume_state: resume,
    });
  }

  takeLaunchFacts(sessionId: string): LaunchFacts | null {
    const facts = this.facts.get(sessionId);
    this.facts.delete(sessionId);
    if (facts === undefined || facts.capabilities === undefined) return null;
    return facts as LaunchFacts;
  }
}

/** Wrap one production (P3) factory into the kernel's TransportFactory seam. */
export function bridgeTransportFactory(factory: LiveTransportFactory): BridgedTransportFactory {
  return new BridgedTransportFactoryImpl(factory);
}

/** The four shipped bridges: omp-rpc (v2-only), pi-rpc, agy-stream-json, hermes-acp. */
export function productionBridgedFactories(): BridgedTransportFactory[] {
  return productionTransportFactories().map(bridgeTransportFactory);
}

/**
 * Provider-side selection for the kernel: preference orders and honest
 * declines come verbatim from the production provider factories; the answer
 * is mapped back through the same bridge instances (identity, not
 * re-wrapping — the kernel requires the offered instance back).
 */
export function productionBridgedProviderFactories(): ProviderFactory[] {
  return productionProviderFactories.map((providerFactory) => ({
    provider: providerFactory.provider,
    transports: providerFactory.transports,
    selectTransport(factories: readonly TransportFactory[]): TransportFactory | null {
      const bridges = factories.filter(
        (factory): factory is BridgedTransportFactory =>
          factory instanceof BridgedTransportFactoryImpl,
      );
      const selected = providerFactory.selectTransport(bridges.map((bridge) => bridge.inner));
      if (selected === null) return null;
      return bridges.find((bridge) => bridge.inner === selected) ?? null;
    },
  }));
}

export function isHubProvider(value: string): value is LiveProviderId {
  return ["omp", "pi", "agy", "hermes"].includes(value);
}
