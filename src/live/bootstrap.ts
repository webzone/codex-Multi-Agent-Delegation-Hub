import { resolveRepositoryIdentity } from "../git.js";
import { AgentHubError } from "../errors.js";
import {
  getLiveResumeSource,
  liveTransportRegistry,
  setLiveResumeSource,
  LiveTransportRegistry,
  type LiveResumeSource,
} from "./provider-registry.js";
import { probeAgy } from "./probes/agy.js";
import { probeHermes } from "./probes/hermes.js";
import { ompProviderFactory, probeOmp } from "./probes/omp.js";
import { piProviderFactory, probePi } from "./probes/pi.js";
import { createAgyStreamJsonFactory } from "./transports/agy-stream-json.js";
import { createHermesAcpFactory } from "./transports/hermes-acp.js";
import { ompRpcTransportFactory } from "./transports/omp-rpc.js";
import { piRpcTransportFactory } from "./transports/pi-rpc.js";
import { loadLiveState } from "./state.js";
import { LiveSessionManager, type LiveManagerOptions } from "./manager.js";
import type { LiveSessionState, LiveTransportFactory } from "./types.js";

/**
 * Production bootstrap for the v3 live surfaces (CLI `agent-hub live`, MCP
 * `live_session_*`). The core safety machinery lives in `manager.ts`; this
 * module is the single place that WIRES it for production:
 *
 *   - every build of the CLI/MCP registers all four real transports
 *     (OMP RPC, PI RPC, AGY stream-json, Hermes ACP) — none is optional and
 *     none is a seam;
 *   - the durable live-state reader from `state.ts` is wired into the
 *     resume seam, so `--resume`/`live_session_resume` reads the real
 *     `agent-hub-live/v1` records instead of an unwired placeholder;
 *   - `repositoryCwd`/`commonDir` are resolved FROM the requested workspace
 *     (a plain `git rev-parse` on the caller's checkout), so hub-owned state
 *     always lands in the right repository while providers run in the
 *     hub-created OS-temp worktree the manager materializes.
 */

/**
 * The four real transport factories a production hub build must carry.
 * Module-level singletons: the registry's conflict rule compares factory
 * identity, so repeated production registration must be idempotent.
 */
const ompFactory = ompRpcTransportFactory;
const piFactory = piRpcTransportFactory;
const agyFactory = createAgyStreamJsonFactory();
const hermesFactory = createHermesAcpFactory();

export function productionTransportFactories(): readonly LiveTransportFactory[] {
  return [ompFactory, piFactory, agyFactory, hermesFactory];
}

/**
 * Register the four real transports into `registry` (default: the build-wide
 * one). Idempotent: the registry's conflict rule compares factory identity,
 * and these are the module singletons above.
 */
export function registerProductionLiveTransports(
  registry: LiveTransportRegistry = liveTransportRegistry,
): void {
  for (const factory of productionTransportFactories()) {
    registry.register(factory);
  }
}

/** The durable resume reader — the real implementation of the resume seam. */
export const durableLiveResumeSource: LiveResumeSource = {
  async load(workspace: string, liveSessionId: string): Promise<LiveSessionState> {
    const identity = await resolveRepositoryIdentity(workspace);
    return loadLiveState({
      commonDir: identity.common_dir,
      repositoryCwd: identity.worktree_root,
      liveSessionId,
    });
  },
};

/**
 * Wire the durable live-state reader into the resume seam. Idempotent, and
 * honest about the current state when asked via `getLiveResumeSource()`.
 */
export function wireDurableLiveResumeSource(): void {
  if (getLiveResumeSource() !== durableLiveResumeSource) {
    setLiveResumeSource(durableLiveResumeSource);
  }
}

/**
 * Providers the production hub can run, with their preference-ordered
 * transport lists (AGY/Hermes have exactly one transport each).
 */
export const productionProviderFactories = [
  ompProviderFactory,
  piProviderFactory,
  {
    provider: "agy" as const,
    transports: ["agy-stream-json"] as const,
    selectTransport(factories: readonly LiveTransportFactory[]) {
      return factories.find((factory) => factory.transport === "agy-stream-json") ?? null;
    },
  },
  {
    provider: "hermes" as const,
    transports: ["hermes-acp"] as const,
    selectTransport(factories: readonly LiveTransportFactory[]) {
      return factories.find((factory) => factory.transport === "hermes-acp") ?? null;
    },
  },
];

export interface CreateLiveManagerOptions extends Partial<Omit<LiveManagerOptions, "commonDir" | "repositoryCwd">> {
  /** Extra transport factories (tests inject fakes); production ones are always registered. */
  extraTransportFactories?: readonly LiveTransportFactory[];
  /** Skip the production transports (focused tests that script every launch). */
  withoutProductionTransports?: boolean;
}

/**
 * Build the production `LiveSessionManager` for one workspace: resolves the
 * repository identity from the requested workspace, registers all four real
 * transports, wires the durable live-state reader, and returns the CORE
 * manager (the only production manager).
 */
export async function createLiveManager(
  workspace: string,
  options: CreateLiveManagerOptions = {},
): Promise<LiveSessionManager> {
  const identity = await resolveRepositoryIdentity(workspace);
  // Production builds also populate the build-wide default registry, so
  // surfaces that consult it (for example `agent-hub live probe` through
  // `probeLiveAgent`) never see an unwired default on a real build.
  if (!options.withoutProductionTransports) {
    registerProductionLiveTransports();
  }
  const registry = new LiveTransportRegistry();
  if (!options.withoutProductionTransports) {
    for (const factory of productionTransportFactories()) {
      registry.register(factory);
    }
  }
  for (const factory of options.extraTransportFactories ?? []) {
    registry.register(factory);
  }
  wireDurableLiveResumeSource();
  const {
    extraTransportFactories: _extra,
    withoutProductionTransports: _without,
    ...managerOptions
  } = options;
  return new LiveSessionManager({
    ...managerOptions,
    commonDir: identity.common_dir,
    repositoryCwd: identity.worktree_root,
    transportFactories: [...registry.list()],
    providerFactories: options.providerFactories ?? productionProviderFactories,
  });
}

/**
 * Probe document for `agent-hub live probe`: runs the real provider probes
 * without launching a session. Kept here so the CLI never imports individual
 * probe modules directly.
 */
export async function probeLiveProviderReal(provider: "omp" | "agy" | "pi" | "hermes") {
  switch (provider) {
    case "omp":
      return probeOmp();
    case "pi":
      return probePi();
    case "agy":
      return probeAgy();
    case "hermes":
      return probeHermes();
    default: {
      const exhaustive: never = provider;
      throw new AgentHubError("LIVE_COMMAND_INVALID", `unknown live provider "${String(exhaustive)}"`);
    }
  }
}
