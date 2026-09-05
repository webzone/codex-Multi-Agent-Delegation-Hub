import { AgentHubError } from "../errors.js";
import type {
  LiveProbeResult,
  LiveProviderId,
  LiveSessionState,
  LiveTransportFactory,
  LiveTransportId,
} from "./types.js";

/**
 * v3 live Agent Hub — Package 4 wiring seams.
 *
 * This module is the single plug-in point between the live surfaces (CLI
 * `agent-hub live …`, MCP `live_session_*`) and the implementations owned by
 * the other live packages:
 *
 *   - Package 1 (provider transports) registers `LiveTransportFactory`
 *     instances here via `registerLiveTransport()` / `LiveTransportRegistry`.
 *     A build without registered transports stays compile-safe and honest:
 *     every surface fails with `LIVE_TRANSPORT_UNAVAILABLE` and never guesses
 *     a command line or a transport.
 *   - Package 2 (durable live state) wires a `LiveResumeSource` here via
 *     `setLiveResumeSource()`. Until then `--resume` / `live_session_resume`
 *     fail with `LIVE_STATE_UNAVAILABLE`, never with a fake session.
 *
 * Nothing in this module launches processes, reads durable state, or knows a
 * wire format — those facts belong to Packages 1-3.
 */

/** Providers a live session can run — v3-only vocabulary (`pi`, `hermes`). */
export const supportedLiveAgents = ["omp", "agy", "pi", "hermes"] as const;

export function isLiveProvider(value: string): value is LiveProviderId {
  return (supportedLiveAgents as readonly string[]).includes(value);
}

/**
 * The 1:1 transport ↔ provider pairing fixed by the Gate 0 seed
 * (`src/live/types.ts`). A factory whose pair is not in this table is a bug
 * in the Package 1 implementation and must be refused at registration, never
 * resolved by a fallback guess.
 */
export const LIVE_TRANSPORT_PAIRINGS: Record<LiveTransportId, LiveProviderId> = {
  "omp-rpc": "omp",
  "agy-stream-json": "agy",
  "pi-rpc": "pi",
  "hermes-acp": "hermes",
};

/**
 * Process-local registry of live transport factories. Instances are cheap and
 * isolated (focused tests build their own); `liveTransportRegistry` is the
 * build-wide default the production surfaces use.
 */
export class LiveTransportRegistry {
  private readonly factories = new Map<LiveTransportId, LiveTransportFactory>();

  /**
   * Registers a factory for its provider. A second factory for the same
   * provider is refused — two live implementations for one provider is a
   * wiring bug, not something the surfaces should arbitrate.
   */
  register(factory: LiveTransportFactory): void {
    const expectedProvider = LIVE_TRANSPORT_PAIRINGS[factory.transport];
    if (expectedProvider === undefined || expectedProvider !== factory.provider) {
      throw new AgentHubError(
        "LIVE_TRANSPORT_PAIRING_INVALID",
        `live transport "${factory.transport}" must pair with provider "${expectedProvider ?? "unknown"}", got "${factory.provider}"`,
      );
    }
    for (const registered of this.factories.values()) {
      if (registered.provider === factory.provider && registered !== factory) {
        throw new AgentHubError(
          "LIVE_TRANSPORT_CONFLICT",
          `provider "${factory.provider}" already has a live transport registered ("${registered.transport}")`,
        );
      }
    }
    this.factories.set(factory.transport, factory);
  }

  factoryFor(provider: LiveProviderId): LiveTransportFactory | null {
    for (const factory of this.factories.values()) {
      if (factory.provider === provider) {
        return factory;
      }
    }
    return null;
  }

  /** The pairing-validated factory for a provider, or an honest failure. */
  require(provider: LiveProviderId): LiveTransportFactory {
    const factory = this.factoryFor(provider);
    if (factory === null) {
      throw new AgentHubError(
        "LIVE_TRANSPORT_UNAVAILABLE",
        `no live transport is registered for provider "${provider}"; its Package 1 implementation is not wired into this hub build`,
      );
    }
    return factory;
  }

  list(): LiveTransportFactory[] {
    return [...this.factories.values()];
  }
}

/** Probe document: the factory's answer plus the identity it came from. */
export interface LiveProbeDocument extends LiveProbeResult {
  provider: LiveProviderId;
  transport: LiveTransportId | null;
}

export async function probeLiveAgent(
  provider: LiveProviderId,
  registry: LiveTransportRegistry = liveTransportRegistry,
): Promise<LiveProbeDocument> {
  const factory = registry.require(provider);
  const result = await factory.probe();
  return { provider, transport: factory.transport, ...result };
}

export const liveTransportRegistry = new LiveTransportRegistry();

/** The seam Package 1 calls once per implemented transport. */
export function registerLiveTransport(factory: LiveTransportFactory): void {
  liveTransportRegistry.register(factory);
}

// ---------------------------------------------------------------------------
// Durable live-state seam (owned by Package 2)
// ---------------------------------------------------------------------------

/**
 * Reads the durable `LiveSessionState` for `agent-hub live --resume` and
 * `live_session_resume`. Implementations must return the exact metadata shape
 * the seed fixes — surfaces re-validate it before handing it to a transport.
 */
export interface LiveResumeSource {
  load(workspace: string, liveSessionId: string): Promise<LiveSessionState>;
}

/** The honest default on a seed-only build: no store, no resume, no pretense. */
export const unwiredLiveResumeSource: LiveResumeSource = {
  async load(_workspace: string, liveSessionId: string): Promise<LiveSessionState> {
    throw new AgentHubError(
      "LIVE_STATE_UNAVAILABLE",
      `cannot resume live session "${liveSessionId}": the durable live-session state store (Package 2) is not wired into this hub build`,
    );
  },
};

let resumeSource: LiveResumeSource = unwiredLiveResumeSource;

/** The seam Package 2 calls to wire the durable live-state store. */
export function setLiveResumeSource(source: LiveResumeSource): void {
  resumeSource = source;
}

export function getLiveResumeSource(): LiveResumeSource {
  return resumeSource;
}

/**
 * Runtime check on a state document crossing the `LiveResumeSource` seam.
 * The compile-time type fixes the shape; this gate refuses records that do
 * not actually honor it, so a Package 2 bug can never smuggle extra state
 * into a live launch.
 */
export function assertLiveSessionState(value: unknown): asserts value is LiveSessionState {
  if (typeof value !== "object" || value === null) {
    throw new AgentHubError("LIVE_STATE_INVALID", "live session state must be an object");
  }
  const record = value as Partial<LiveSessionState>;
  if (
    record.schema !== 1 ||
    typeof record.live_session_id !== "string" ||
    record.live_session_id.length === 0 ||
    (record.session_id !== null && typeof record.session_id !== "string") ||
    !isLiveProvider(String(record.provider)) ||
    !(String(record.transport) in LIVE_TRANSPORT_PAIRINGS) ||
    typeof record.capabilities !== "object" ||
    record.capabilities === null ||
    typeof record.status !== "string" ||
    typeof record.revision !== "number"
  ) {
    throw new AgentHubError(
      "LIVE_STATE_INVALID",
      "live session state does not match the Gate 0 durable record shape",
    );
  }
  // Provider-specific resume handles may never disagree with the record's own
  // provider: a mismatched handle is exactly the cross-wiring the seed forbids.
  if (record.resume !== null) {
    if (typeof record.resume !== "object") {
      throw new AgentHubError("LIVE_STATE_INVALID", "live session resume state must be an object");
    }
    if (record.resume.provider !== record.provider) {
      throw new AgentHubError(
        "LIVE_STATE_INVALID",
        `live session resume provider "${String(record.resume.provider)}" contradicts record provider "${record.provider}"`,
      );
    }
  }
}

/**
 * The canonical wire-data record guard for the live package: narrows to an
 * object map without pretending to know the fields — every consumer below
 * still checks the properties it actually uses.
 */
export function isLiveRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
