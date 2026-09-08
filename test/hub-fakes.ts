import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { AgentHub, type AgentHubOptions, type ProviderFactoryLike } from "../src/hub/agent-hub.js";
import { bridgeTransportFactory } from "../src/hub/transport-adapter.js";
import type {
  LiveCapabilities,
  LiveCommand,
  LiveEvent,
  LiveEventBody,
  LiveLaunchReport,
  LiveLaunchRequest,
  LiveProbeResult,
  LiveStopMode,
  LiveStopReport,
  LiveTransport,
  LiveTransportFactory,
} from "../src/live/types.js";
import type { LiveLeaseProbes } from "../src/live/lease.js";
import { resolveRepositoryIdentity } from "../src/git.js";
import { createGitRepository } from "./helpers.js";

/**
 * Fakes for the public integration layer. The durable store's vocabulary is
 * fixed to the four shipped pairings, so fakes pose as `omp`/`omp-rpc` — the
 * same rule every live-core fake follows.
 */

export function fullHubCapabilities(
  overrides: Partial<LiveCapabilities> = {},
): LiveCapabilities {
  return {
    prompt: { support: "native", evidence: "fake ready handshake" },
    follow_up: { support: "native", evidence: "fake ready handshake" },
    steer: { support: "native", evidence: "fake mid-turn channel" },
    cancel: { support: "native", evidence: "fake cancel message" },
    status: { support: "derived", evidence: "hub stream evidence" },
    permission_response: { support: "native", evidence: "fake rpc accepted" },
    resume: { support: "native", evidence: "fake round trip" },
    checkpoint: { support: "derived", evidence: "hub worktree capture" },
    usage_reporting: { support: "unsupported", evidence: null },
    ...overrides,
  };
}

export interface HubFakeTurnBehavior {
  /** Files written into the provider workspace when the turn is delivered. */
  writes?: Record<string, string>;
  /** Extra bodies pushed after the standard running…idle turn. */
  during?: LiveEventBody[];
  /** Suppress the standard turn settlement (leaves the turn in flight). */
  hang?: boolean;
}

export class HubFakeTransport implements LiveTransport {
  readonly id = "omp-rpc" as const;
  readonly provider = "omp" as const;
  readonly commands: LiveCommand[] = [];
  launch: LiveLaunchRequest | null = null;
  readonly stopCalls: LiveStopMode[] = [];
  stopResults: LiveStopReport[] = [];
  turnBehavior: HubFakeTurnBehavior | null = null;
  private queue: LiveEventBody[] = [];
  private wake: (() => void) | null = null;
  private ended = false;
  private readonly turnSettled = Promise.withResolvers<void>();

  /** Resolves once this transport's scripted turn emitted its terminal idle. */
  awaitTurnSettled(): Promise<void> {
    return this.turnSettled.promise;
  }

  constructor(
    private readonly caps: LiveCapabilities,
    private readonly opts: {
      pid?: number | null;
      resumeState?: "echo";
    } = {},
  ) {}

  async describe(): Promise<{
    transport: "omp-rpc";
    provider: "omp";
    capabilities: LiveCapabilities;
  }> {
    return { transport: this.id, provider: this.provider, capabilities: this.caps };
  }

  async open(request: LiveLaunchRequest): Promise<LiveLaunchReport> {
    this.launch = request;
    const pid = this.opts.pid === undefined ? 424_242 : this.opts.pid;
    if (pid !== null) {
      await request.report_process?.({ pid, pgid: pid });
    }
    const resumed =
      this.opts.resumeState === "echo" && request.resume
        ? {
            ...request.resume,
            verified: true as const,
            verified_via: "transport-verified:open-echo",
          }
        : null;
    return {
      pid,
      provider_session_id: "prov-1",
      launched_at: "2026-09-07T00:00:00.000Z",
      ...(resumed !== null ? { resume_state: resumed } : {}),
    };
  }

  async send(command: LiveCommand): Promise<void> {
    this.commands.push(command);
    if (command.kind === "prompt" || command.kind === "follow_up") {
      const behavior = this.turnBehavior;
      if (behavior !== null) {
        if (behavior.writes !== undefined && this.launch !== null) {
          for (const [file, contents] of Object.entries(behavior.writes)) {
            await writeFile(join(this.launch.workspace, file), contents, "utf8");
          }
        }
        this.push({ kind: "status", status: "running", note: null });
        for (const body of behavior.during ?? []) {
          this.push(body);
        }
        if (behavior.hang !== true) {
          this.push({
            kind: "text",
            role: "assistant",
            stream_id: "s-1",
            text: { text: `done: ${command.kind}`, truncated: false },
            final: true,
          });
          this.push({ kind: "status", status: "idle", note: null });
          this.turnSettled.resolve();
        }
      }
      return;
    }
    if (command.kind === "cancel") {
      this.push({ kind: "status", status: "idle", note: null });
      this.turnSettled.resolve();
    }
  }

  push(body: LiveEventBody): void {
    this.queue.push(body);
    this.wake?.();
  }

  endStream(): void {
    this.ended = true;
    this.wake?.();
  }

  async *events(): AsyncGenerator<LiveEvent> {
    for (;;) {
      while (this.queue.length > 0) {
        const body = this.queue.shift() as LiveEventBody;
        // Envelope lies on purpose: the kernel owns seq/time/id stamping.
        yield {
          live_session_id: "forged",
          seq: 999,
          transport: "hermes-acp",
          occurred_at: "forged",
          body,
        };
      }
      if (this.ended) return;
      await new Promise<void>((resolve) => {
        this.wake = resolve;
        if (this.queue.length > 0 || this.ended) resolve();
      });
    }
  }

  async stop(mode: LiveStopMode): Promise<LiveStopReport> {
    this.stopCalls.push(mode);
    const report = this.stopResults.shift() ?? {
      status: "closed" as const,
      exit_code: 0,
      exit_signal: null,
      waited_ms: 1,
    };
    this.endStream();
    return report;
  }
}

export class HubFakeFactory implements LiveTransportFactory {
  readonly transport = "omp-rpc" as const;
  readonly provider = "omp" as const;
  readonly created: HubFakeTransport[] = [];
  probeResult: LiveProbeResult = { found: true, version: "v2-evidence", detail: null };
  private readonly creationWaiters = new Set<() => void>();

  /** Await the transport that the `index`th `create()` will produce. */
  async transportAt(index: number): Promise<HubFakeTransport> {
    for (;;) {
      const transport = this.created[index];
      if (transport !== undefined) return transport;
      await new Promise<void>((resolve) => {
        this.creationWaiters.add(resolve);
        if (this.created[index] !== undefined) {
          this.creationWaiters.delete(resolve);
          resolve();
        }
      });
    }
  }

  /** Applied to every created transport (for surfaces that script no handle). */
  defaultTurn: HubFakeTurnBehavior | null = null;

  constructor(
    private readonly caps: () => LiveCapabilities = () => fullHubCapabilities(),
    private readonly transportOptions: { pid?: number | null; resumeState?: "echo" } = {},
  ) {}

  async probe(): Promise<LiveProbeResult> {
    return this.probeResult;
  }

  create(): LiveTransport {
    const transport = new HubFakeTransport(this.caps(), this.transportOptions);
    transport.turnBehavior = this.defaultTurn;
    this.created.push(transport);
    const waiters = [...this.creationWaiters];
    this.creationWaiters.clear();
    for (const notify of waiters) notify();
    return transport;
  }
}

/** Kernel-shaped selection (the hub hands kernel factories to this seam). */
export const hubFakeProviderFactory: ProviderFactoryLike = {
  provider: "omp",
  transports: ["omp-rpc"],
  selectTransport: (factories) => factories[0] ?? null,
};

export function hubFakeProbes(
  config: {
    alive?: (pid: number) => boolean;
    groupState?: (pgid: number) => "alive" | "gone" | "uncertain";
  } = {},
): LiveLeaseProbes {
  const alive = config.alive ?? ((pid: number) => pid === process.pid);
  return {
    probePid: (pid) => (alive(pid) ? "live" : "dead"),
    startToken: async () => "fake-token",
    killGroup: () => true,
    probeGroup: config.groupState ?? ((pgid) => (alive(pgid) ? "alive" : "gone")),
    now: () => new Date(),
  };
}

export interface HubHarness {
  repository: string;
  commonDir: string;
  tmpRoot: string;
  hub: AgentHub;
  factory: HubFakeFactory;
}

export async function createHubHarness(
  options: {
    capabilities?: () => LiveCapabilities;
    transportOptions?: { pid?: number | null; resumeState?: "echo" };
    hubOptions?: AgentHubOptions;
    probe?: LiveProbeResult;
  } = {},
): Promise<HubHarness> {
  const repository = await createGitRepository();
  const identity = await resolveRepositoryIdentity(repository);
  const tmpRoot = await mkdtemp(join(tmpdir(), "agent-hub-hub-"));
  const factory = new HubFakeFactory(options.capabilities, options.transportOptions);
  if (options.probe !== undefined) factory.probeResult = options.probe;
  const hub = await AgentHub.open(repository, {
    transportFactories: [bridgeTransportFactory(factory)],
    providerFactories: [hubFakeProviderFactory],
    tmpRoot,
    probes: hubFakeProbes(),
    ...options.hubOptions,
  });
  return { repository, commonDir: identity.common_dir, tmpRoot, hub, factory };
}

/** Attach a scripted turn to a transport the fake factory already created. */
export function scriptTurn(
  factory: HubFakeFactory,
  index: number,
  behavior: HubFakeTurnBehavior,
): HubFakeTransport {
  const transport = factory.created[index] as HubFakeTransport;
  transport.turnBehavior = behavior;
  return transport;
}
