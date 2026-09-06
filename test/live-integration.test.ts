import { existsSync, readFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { mkdtemp, readFile, readdir, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join, relative, sep } from "node:path";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import type { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { describe, expect, it } from "vitest";

import { deferred } from "../src/deferred.js";
import { AgentHubError } from "../src/errors.js";
import { resolveRepositoryIdentity } from "../src/git.js";
import { acquireRepositoryLock } from "../src/locks.js";
import { createHubServer } from "../src/mcp.js";
import {
  createLiveManager,
  durableLiveResumeSource,
  getLiveResumeSource,
  liveLeasePath,
  liveRefFor,
  LIVE_TRANSPORT_PAIRINGS,
  liveStatePath,
  LiveSessionManager,
  LiveTransportRegistry,
  probeLiveAgent,
  productionTransportFactories,
  registerProductionLiveTransports,
  supportedLiveAgents,
  type LiveManagerPhase,
} from "../src/live/index.js";
import { LIVE_WORKTREE_PREFIX } from "../src/live/worktree.js";
import type { LiveLeaseProbes } from "../src/live/lease.js";
import type {
  LiveCapabilities,
  LiveCommand,
  LiveEvent,
  LiveEventBody,
  LiveLaunchReport,
  LiveLaunchRequest,
  LivePermissionDecision,
  LivePermissionResponseCommand,
  LiveProbeResult,
  LiveProviderId,
  LiveStopMode,
  LiveStopReport,
  LiveTransport,
  LiveTransportDescriptor,
  LiveTransportFactory,
  LiveTransportId,
} from "../src/live/types.js";
import { createGitRepository, removeDirectory, resolveRef, runGit } from "./helpers.js";

// ---------------------------------------------------------------------------
// Shared fixtures
// ---------------------------------------------------------------------------

/**
 * Cross-package integration over the PRODUCTION wiring: every manager here is
 * built by `createLiveManager` (repository identity resolution, transport
 * registration, durable resume wiring, quotas, locks), and every provider is a
 * scripted in-process transport that follows the real launch contract —
 * including the mandatory `report_process` ownership callback. No real provider
 * binary is ever launched.
 */

function fullCapabilities(overrides: Partial<LiveCapabilities> = {}): LiveCapabilities {
  return {
    prompt: { support: "native", evidence: "scripted ready handshake" },
    follow_up: { support: "native", evidence: "scripted ready handshake" },
    steer: { support: "native", evidence: "scripted mid-turn channel" },
    cancel: { support: "native", evidence: "scripted cancel message" },
    status: { support: "derived", evidence: "hub stream evidence" },
    permission_response: { support: "native", evidence: "scripted accepted verdict" },
    resume: { support: "native", evidence: "scripted round trip" },
    checkpoint: { support: "derived", evidence: "hub worktree capture" },
    usage_reporting: { support: "unsupported", evidence: null },
    ...overrides,
  };
}

const HUB_START_TOKEN = "hub-process-start";
const FIXED_LAUNCH_AT = "2026-09-05T00:00:00.000Z";

interface KillRecord {
  pgid: number;
  signal: string;
}

/**
 * OS truth for the fakes: a pid a fake transport "spawned" is alive until a
 * `killGroup` says otherwise; nothing else exists, including this hub process,
 * which is exactly how a test says "the hub that made this lease is gone".
 * `atKill` runs at the moment a reap is attempted, while nothing has been
 * touched yet — the only honest way to prove reap-before-checkpoint-before-
 * cleanup ordering.
 */
interface Ownership {
  alive: Map<number, boolean>;
  kills: KillRecord[];
  atKill: (target: number, signal: string) => void;
}

function ownership(alive: Iterable<[number, boolean]> = []): Ownership {
  return { alive: new Map(alive), kills: [], atKill: () => undefined };
}

function fakeProbes(os: Ownership): LiveLeaseProbes {
  return {
    probePid: (pid) => (os.alive.get(pid) ? "live" : "dead"),
    startToken: async (pid) => (pid === process.pid ? HUB_START_TOKEN : `provider-start-${pid}`),
    killGroup: (pgid, signal) => {
      os.atKill(pgid, signal);
      os.kills.push({ pgid, signal });
      os.alive.set(pgid, false);
      return true;
    },
    probeGroup: (pgid) => (os.alive.get(pgid) ? "alive" : "gone"),
    // Recovery's bounded reap windows need a real wall clock; the *hub* clock
    // under test is injected separately and stays deterministic.
    now: () => new Date(),
  };
}

/** Nothing exists but this hub process — honest default for scripted runs. */
function hubOnlyProbes(): LiveLeaseProbes {
  return fakeProbes(ownership([[process.pid, true]]));
}

interface ScriptOptions {
  /** A number means the transport "spawns" and reports those facts to the hub. */
  pid?: number;
  alive?: Map<number, boolean>;
  /** Shared ordering tape: every hub→transport delivery lands here. */
  timeline?: string[];
  onSend?: (command: LiveCommand, transport: ScriptedTransport) => Promise<void> | void;
  /** Runs strictly after `report_process` landed: the handshake-failure seam. */
  onHandshake?: (request: LiveLaunchRequest, transport: ScriptedTransport) => Promise<void>;
  /** Echoes the launch resume hint back as the transport's own verified state. */
  verifyResumeState?: boolean;
  providerSessionId?: string | null;
  stopResults?: LiveStopReport[];
}

const CLOSED: LiveStopReport = { status: "closed", exit_code: 0, exit_signal: null, waited_ms: 1 };

class ScriptedTransport implements LiveTransport {
  readonly commands: LiveCommand[] = [];
  readonly stopCalls: LiveStopMode[] = [];
  launch: LiveLaunchRequest | null = null;
  stopResults: LiveStopReport[] = [];
  private queue: LiveEventBody[] = [];
  private wake: (() => void) | null = null;
  private ended = false;

  constructor(
    readonly id: LiveTransportId,
    readonly provider: LiveProviderId,
    readonly capabilities: LiveCapabilities,
    private readonly opts: ScriptOptions = {},
  ) {
    this.stopResults = opts.stopResults ?? [];
  }

  async describe(): Promise<LiveTransportDescriptor> {
    return { transport: this.id, provider: this.provider, capabilities: this.capabilities };
  }

  /**
   * Mirrors the production launch contract: spawn, THEN await
   * `report_process`, THEN attempt the handshake (which may fail).
   */
  async open(request: LiveLaunchRequest): Promise<LiveLaunchReport> {
    this.launch = request;
    if (this.opts.pid !== undefined) {
      this.opts.alive?.set(this.opts.pid, true);
      await request.report_process?.({ pid: this.opts.pid, pgid: this.opts.pid });
    }
    await this.opts.onHandshake?.(request, this);
    const report: LiveLaunchReport = {
      pid: this.opts.pid ?? null,
      provider_session_id: this.opts.providerSessionId ?? "prov-1",
      launched_at: FIXED_LAUNCH_AT,
    };
    const resume = request.resume;
    if (this.opts.verifyResumeState && resume !== null && resume.provider === "omp") {
      report.resume_state = {
        ...resume,
        verified: true,
        verified_via: "scripted-transport-handshake",
      };
    }
    return report;
  }

  async send(command: LiveCommand): Promise<void> {
    this.commands.push(command);
    this.opts.timeline?.push(`send:${command.kind}`);
    await this.opts.onSend?.(command, this);
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
        // Hub re-stamps every envelope; this metadata is a lie on purpose.
        yield {
          live_session_id: "forged",
          seq: 999,
          transport: "pi-rpc",
          occurred_at: "forged",
          body,
        };
      }
      if (this.ended) {
        return;
      }
      const { promise, resolve } = deferred<void>();
      this.wake = resolve;
      if (this.queue.length > 0 || this.ended) {
        resolve();
      }
      await promise;
    }
  }

  async stop(mode: LiveStopMode): Promise<LiveStopReport> {
    this.stopCalls.push(mode);
    const report = this.stopResults.shift() ?? CLOSED;
    this.endStream();
    return report;
  }
}

class ScriptedFactory implements LiveTransportFactory {
  readonly created: ScriptedTransport[] = [];
  probeResult: LiveProbeResult = { found: true, version: "scripted-1", detail: null };

  constructor(
    readonly transport: LiveTransportId,
    readonly provider: LiveProviderId,
    private readonly make: (creationIndex: number) => ScriptedTransport,
  ) {}

  async probe(): Promise<LiveProbeResult> {
    return this.probeResult;
  }

  create(): LiveTransport {
    const transport = this.make(this.created.length);
    this.created.push(transport);
    return transport;
  }
}

/** Deterministic hub clock: a second per observation, never a frozen clock. */
function fixedClock(startIso = "2026-09-05T00:00:00.000Z"): () => Date {
  let ms = Date.parse(startIso);
  return () => {
    const at = new Date(ms);
    ms += 1_000;
    return at;
  };
}

interface World {
  repository: string;
  commonDir: string;
  tmpRoot: string;
}

async function makeWorld(): Promise<World> {
  const repository = await createGitRepository();
  const identity = await resolveRepositoryIdentity(repository);
  const tmpRoot = await mkdtemp(join(tmpdir(), "agent-hub-live-integration-"));
  return { repository, commonDir: identity.common_dir, tmpRoot };
}

async function destroyWorld(world: World): Promise<void> {
  await removeDirectory(world.repository);
  await removeDirectory(world.tmpRoot);
}

interface Hub {
  manager: LiveSessionManager;
  /** Live ids in the order the injected generator handed them out. */
  ids: string[];
  /** How many times the hub took a repository lock through the injected seam. */
  locks: { acquired: number };
}

/**
 * The production entry point, with fakes ADDED rather than the machinery
 * replaced: identity resolution, the production provider factories, the durable
 * resume wiring, quotas, and the lock seam all stay in the path.
 */
async function buildHub(
  world: World,
  config: {
    factories: readonly LiveTransportFactory[];
    probes?: LiveLeaseProbes;
    phases?: LiveManagerPhase[];
  },
): Promise<Hub> {
  const ids: string[] = [];
  const locks = { acquired: 0 };
  const phases = config.phases;
  const manager = await createLiveManager(world.repository, {
    extraTransportFactories: config.factories,
    withoutProductionTransports: true,
    newLiveSessionId: () => {
      const id = randomUUID();
      ids.push(id);
      return id;
    },
    now: fixedClock(),
    tmpRoot: world.tmpRoot,
    leaseProbes: config.probes ?? hubOnlyProbes(),
    acquireLock: async (options) => {
      locks.acquired += 1;
      return acquireRepositoryLock(options);
    },
    observePhase: phases
      ? async (phase) => {
          phases.push(phase);
        }
      : undefined,
  });
  return { manager, ids, locks };
}

const tick = (): Promise<void> => new Promise((resolve) => setImmediate(resolve));

const captureRejection = (promise: Promise<unknown>): Promise<AgentHubError | null> =>
  promise.then(
    () => null,
    (error) => error as AgentHubError,
  );

/**
 * Bounded wait for work the hub finishes asynchronously after a turn result
 * already resolved — terminal teardown is reap → checkpoint → state → cleanup,
 * and cleanup is not part of the caller's promise.
 */
async function until(probe: () => Promise<boolean> | boolean, budgetMs = 15_000): Promise<boolean> {
  const deadline = Date.now() + budgetMs;
  for (;;) {
    if (await probe()) {
      return true;
    }
    if (Date.now() >= deadline) {
      return false;
    }
    await new Promise((resolve) => setTimeout(resolve, 25));
  }
}

async function leasesOnDisk(commonDir: string): Promise<string[]> {
  try {
    return await readdir(join(commonDir, "agent-hub", "live", "leases"));
  } catch {
    return [];
  }
}

async function leaseRecord(
  commonDir: string,
  liveSessionId: string,
): Promise<Record<string, unknown>> {
  return JSON.parse(await readFile(liveLeasePath(commonDir, liveSessionId), "utf8")) as Record<
    string,
    unknown
  >;
}

async function durableRecord(
  commonDir: string,
  liveSessionId: string,
): Promise<Record<string, unknown>> {
  return JSON.parse(await readFile(liveStatePath(commonDir, liveSessionId), "utf8")) as Record<
    string,
    unknown
  >;
}

async function durableStatus(commonDir: string, liveSessionId: string): Promise<string | null> {
  try {
    return (await durableRecord(commonDir, liveSessionId)).status as string;
  } catch {
    return null;
  }
}

/** Synchronous sibling, for observations taken from inside a sync probe hook. */
function durableStatusSync(commonDir: string, liveSessionId: string): string | null {
  try {
    const raw = JSON.parse(readFileSync(liveStatePath(commonDir, liveSessionId), "utf8")) as {
      status?: string;
    };
    return raw.status ?? null;
  } catch {
    return null;
  }
}

/** Every still-present live ref name, one per line. */
function liveRefs(repository: string): Promise<string> {
  return runGit(repository, ["for-each-ref", "--format=%(refname)", "refs/agent-hub/live"]);
}

async function connectTools(server: McpServer): Promise<Client> {
  const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
  await server.connect(serverTransport);
  const client = new Client({ name: "live-integration-client", version: "0.0.0" }, { capabilities: {} });
  await client.connect(clientTransport);
  return client;
}

function documentFrom(result: unknown): Record<string, any> {
  const content = (result as { content?: unknown }).content;
  if (!Array.isArray(content) || content.length === 0) {
    throw new Error("expected a content-bearing tool result");
  }
  return JSON.parse((content[0] as { text: string }).text) as Record<string, any>;
}

// ---------------------------------------------------------------------------
// 1. Production transport registration
// ---------------------------------------------------------------------------

describe("production transport registration", () => {
  it("registers exactly the four real transports and probes them without launching a session", async () => {
    const expected = ["agy-stream-json", "hermes-acp", "omp-rpc", "pi-rpc"];
    expect(productionTransportFactories().map((factory) => factory.transport).sort()).toEqual(expected);

    const registry = new LiveTransportRegistry();
    registerProductionLiveTransports(registry);
    expect(registry.list().map((factory) => factory.transport).sort()).toEqual(expected);
    expect(
      registry
        .list()
        .map((factory) => `${factory.provider}:${factory.transport}`)
        .sort(),
    ).toEqual(["agy:agy-stream-json", "hermes:hermes-acp", "omp:omp-rpc", "pi:pi-rpc"]);
    // Idempotent: the same module singletons re-register without a conflict.
    expect(() => registerProductionLiveTransports(registry)).not.toThrow();
    expect(registry.list()).toHaveLength(4);

    // The build-wide registry the CLI/MCP surfaces probe by default carries the
    // same four. Probing runs each provider's own detection only; whether a
    // provider happens to be installed here is nobody's business, so only the
    // pairing and the answer's shape are asserted.
    registerProductionLiveTransports();
    const documents = await Promise.all(supportedLiveAgents.map((provider) => probeLiveAgent(provider)));
    expect(documents.map((document) => document.transport).sort()).toEqual(expected);
    for (const document of documents) {
      const paired = document.transport === null ? null : LIVE_TRANSPORT_PAIRINGS[document.transport];
      expect(paired).toBe(document.provider);
      expect(typeof document.found).toBe("boolean");
      expect(document.version === null || typeof document.version === "string").toBe(true);
    }

    // A production manager build cannot be shadowed by an injected fake for a
    // provider the real transports already cover — the conflict is a wiring bug.
    const world = await makeWorld();
    const shadow = new ScriptedFactory(
      "omp-rpc",
      "omp",
      () => new ScriptedTransport("omp-rpc", "omp", fullCapabilities()),
    );
    const conflict = await captureRejection(
      createLiveManager(world.repository, { extraTransportFactories: [shadow] }),
    );
    expect(conflict?.code).toBe("LIVE_TRANSPORT_CONFLICT");
    expect(shadow.created).toHaveLength(0);
    await destroyWorld(world);
  }, 90_000);
});

// ---------------------------------------------------------------------------
// 2. Durable resume reader
// ---------------------------------------------------------------------------

describe("durable resume seam", () => {
  it("is the durable live-state reader the production bootstrap wired", async () => {
    const world = await makeWorld();
    const factory = new ScriptedFactory(
      "omp-rpc",
      "omp",
      () => new ScriptedTransport("omp-rpc", "omp", fullCapabilities()),
    );
    const hub = await buildHub(world, { factories: [factory] });

    expect(getLiveResumeSource()).toBe(durableLiveResumeSource);
    const unknown = randomUUID();
    await expect(getLiveResumeSource().load(world.repository, unknown)).rejects.toMatchObject({
      code: "LIVE_SESSION_NOT_FOUND",
    });
    await expect(hub.manager.resumeFromState({ live_session_id: unknown })).rejects.toMatchObject({
      code: "LIVE_SESSION_NOT_FOUND",
    });

    // The seam reads the same authoritative record the manager maintains.
    const started = await hub.manager.start({ provider: "omp" });
    const loaded = await getLiveResumeSource().load(world.repository, started.live_session_id);
    expect(loaded).toEqual(await hub.manager.view(started.live_session_id));
    expect(loaded.schema).toBe("agent-hub-live/v1");
    expect(loaded.live_session_id).toBe(started.live_session_id);
    expect(liveStatePath(world.commonDir, started.live_session_id)).toBe(
      join(world.commonDir, "agent-hub", "live", "sessions", `${started.live_session_id}.json`),
    );

    await hub.manager.close(started.live_session_id);
    await destroyWorld(world);
  });
});

// ---------------------------------------------------------------------------
// 3. Provider cwd isolation
// ---------------------------------------------------------------------------

describe("provider workspace isolation", () => {
  it("runs the provider in a hub-owned OS-temp worktree and leaves the caller checkout alone", async () => {
    const world = await makeWorld();
    const timeline: string[] = [];
    const factory = new ScriptedFactory(
      "omp-rpc",
      "omp",
      () =>
        new ScriptedTransport("omp-rpc", "omp", fullCapabilities(), {
          timeline,
          onSend: async (command, transport) => {
            if (command.kind === "prompt") {
              await writeFile(
                join(transport.launch?.workspace ?? ".", "provider-work.txt"),
                `${command.text}\n`,
              );
            }
          },
        }),
    );
    const hub = await buildHub(world, { factories: [factory] });

    const headBefore = await runGit(world.repository, ["rev-parse", "HEAD"]);
    const statusBefore = await runGit(world.repository, ["status", "--porcelain"]);
    const stagedBefore = await runGit(world.repository, ["diff", "--cached", "--name-only"]);
    expect(statusBefore).toBe("");

    const started = await hub.manager.start({ provider: "omp" });
    const transport = factory.created[0];
    const workspace = transport.launch?.workspace ?? "";
    expect(workspace).toBe(started.workspace);
    expect(transport.launch?.live_session_id).toBe(started.live_session_id);
    // The manager generated the id through the injected UUID generator.
    expect(hub.ids).toEqual([started.live_session_id]);
    // Inside the injected temp root, under the hub's own worktree prefix.
    const inside = relative(world.tmpRoot, workspace).split(sep);
    expect(inside[0].startsWith(LIVE_WORKTREE_PREFIX)).toBe(true);
    expect(inside.length).toBeGreaterThan(1);
    expect(inside[inside.length - 1]).toBe("worktree");
    expect(workspace.startsWith(join(world.repository, ".git"))).toBe(false);
    expect(await runGit(world.repository, ["worktree", "list", "--porcelain"])).toContain(workspace);

    const turn = hub.manager.prompt(started.live_session_id, "write into the provider workspace");
    transport.push({ kind: "status", status: "running", note: null });
    await tick();
    transport.push({ kind: "status", status: "idle", note: null });
    expect((await turn).outcome).toBe("succeeded");

    // The provider's file exists only in the hub worktree.
    expect(await readFile(join(workspace, "provider-work.txt"), "utf8")).toBe(
      "write into the provider workspace\n",
    );
    expect(existsSync(join(world.repository, "provider-work.txt"))).toBe(false);

    // Caller checkout byte-identical: same HEAD, nothing staged, nothing dirty.
    expect(await runGit(world.repository, ["rev-parse", "HEAD"])).toBe(headBefore);
    expect(await runGit(world.repository, ["status", "--porcelain"])).toBe(statusBefore);
    expect(await runGit(world.repository, ["diff", "--cached", "--name-only"])).toBe(stagedBefore);
    // Every worktree/state mutation went through the injected lock seam.
    expect(hub.locks.acquired).toBeGreaterThan(0);

    await hub.manager.close(started.live_session_id);
    await destroyWorld(world);
  });
});

// ---------------------------------------------------------------------------
// 4/5. Ownership proven at spawn, and cleanup only on proven death
// ---------------------------------------------------------------------------

describe("launch ownership and conservative failure", () => {
  const HANDSHAKE_CODE = "LIVE_PROTOCOL_HANDSHAKE_FAILED";

  /** Post-spawn failure seam: the process exists, the handshake does not finish. */
  function loseHandshake(stopResults: LiveStopReport[]) {
    return async (_request: LiveLaunchRequest, transport: ScriptedTransport): Promise<void> => {
      transport.stopResults = stopResults;
      throw new AgentHubError(HANDSHAKE_CODE, "the scripted provider died mid-handshake");
    };
  }

  it("retains lease, ownership facts, and worktree when an unproven group survives, and recovery reaps before it cleans", async () => {
    const world = await makeWorld();
    // Nothing is alive, this hub included: the lease's hub is considered gone.
    const os = ownership();
    const probes = fakeProbes(os);
    const failedPid = 424_242;
    const strandedPid = 424_243;

    // Instance 0 spawns, records ownership, then loses the handshake with a
    // shutdown that cannot prove the group is gone. Instance 1 is a healthy
    // provider whose hub later disappears with it still running.
    const factory = new ScriptedFactory(
      "omp-rpc",
      "omp",
      (index) =>
        new ScriptedTransport("omp-rpc", "omp", fullCapabilities(), {
          pid: index === 0 ? failedPid : strandedPid,
          alive: os.alive,
          onHandshake:
            index === 0
              ? loseHandshake([{ status: "orphaned", exit_code: null, exit_signal: null, waited_ms: 7 }])
              : undefined,
        }),
    );
    const hubA = await buildHub(world, { factories: [factory], probes });

    const failed = await captureRejection(hubA.manager.start({ provider: "omp" }));
    expect(failed?.code).toBe(HANDSHAKE_CODE);
    expect(hubA.ids).toHaveLength(1);
    const failedId = hubA.ids[0];

    // (a) Ownership reached the lease on disk before the handshake died.
    const lease = await leaseRecord(world.commonDir, failedId);
    expect(lease.provider_pid).toBe(failedPid);
    expect(lease.provider_pgid).toBe(failedPid);
    expect(lease.provider_start_token).toBe(`provider-start-${failedPid}`);
    expect(os.alive.get(failedPid)).toBe(true);
    expect(factory.created[0].stopCalls).toEqual(["terminate"]);

    // (b) The rejection names the unproven provider and refuses to claim cleanup.
    expect(failed?.message).toContain("could not be proven gone");
    expect(failed?.message).toContain(String(failedPid));

    // (c) Lease and worktree retained for recovery; no durable record exists.
    const retainedWorktree = lease.worktree_path as string;
    expect(existsSync(retainedWorktree)).toBe(true);
    expect(existsSync(liveLeasePath(world.commonDir, failedId))).toBe(true);
    expect(await durableStatus(world.commonDir, failedId)).toBeNull();

    // A second, healthy session in the same repository — then its hub dies.
    const stranded = await hubA.manager.start({ provider: "omp" });
    await writeFile(join(stranded.workspace, "survivor.txt"), "outlived the hub\n");
    const resources = new Map<number, { liveSessionId: string; worktreePath: string }>([
      [failedPid, { liveSessionId: failedId, worktreePath: retainedWorktree }],
      [strandedPid, { liveSessionId: stranded.live_session_id, worktreePath: stranded.workspace }],
    ]);

    const observed: {
      pgid: number;
      worktree_present: boolean;
      lease_present: boolean;
      durable_status: string | null;
    }[] = [];
    os.atKill = (target) => {
      const owned = resources.get(target);
      if (owned === undefined) {
        return;
      }
      observed.push({
        pgid: target,
        worktree_present: existsSync(owned.worktreePath),
        lease_present: existsSync(liveLeasePath(world.commonDir, owned.liveSessionId)),
        durable_status: durableStatusSync(world.commonDir, owned.liveSessionId),
      });
    };

    const phases: LiveManagerPhase[] = [];
    // A different hub process over the same leases, told the first hub is gone
    // while both provider pids are alive with matching start identities.
    const hubB = await buildHub(world, { factories: [factory], probes, phases });
    const report = await hubB.manager.recover();
    const outcomes = new Map(report.sessions.map((session) => [session.live_session_id, session]));

    expect(report.scanned).toBe(2);
    expect(observed).toHaveLength(2);
    for (const entry of observed) {
      // Reap ran while every resource was still in place …
      expect(entry.worktree_present).toBe(true);
      expect(entry.lease_present).toBe(true);
    }
    // … and before any status rewrite: the stranded record still read `idle`.
    const byPgid = new Map(observed.map((entry) => [entry.pgid, entry]));
    expect(byPgid.get(failedPid)?.durable_status).toBeNull();
    expect(byPgid.get(strandedPid)?.durable_status).toBe("idle");
    expect(observed.map((entry) => entry.pgid).sort()).toEqual([failedPid, strandedPid].sort());
    expect(os.kills.map((kill) => kill.signal)).toEqual(["SIGTERM", "SIGTERM"]);
    // Recovery's durable ordering is per session — reap, then pin, then rewrite.
    // Both leases got reaped; the three-phase run belongs to the stranded
    // session whichever order the lease scan happened to take.
    expect(phases).toHaveLength(4);
    expect(phases.filter((phase) => phase === "provider-reaped")).toHaveLength(2);
    expect(
      phases.findIndex(
        (phase, index) =>
          phase === "provider-reaped" &&
          phases[index + 1] === "checkpoint-captured" &&
          phases[index + 2] === "state-advanced",
      ),
    ).toBeGreaterThanOrEqual(0);

    // The failed launch left no durable record: recovery says so, and cleans.
    expect(outcomes.get(failedId)?.outcome).toBe("cleaned");
    expect(outcomes.get(failedId)?.detail).toContain("the launch never completed");

    // The stranded session was reaped, pinned, rewritten to orphaned …
    expect(outcomes.get(stranded.live_session_id)?.outcome).toBe("recovered");
    expect(outcomes.get(stranded.live_session_id)?.detail).toContain("provider reaped");
    const durable = await durableRecord(world.commonDir, stranded.live_session_id);
    expect(durable.status).toBe("orphaned");
    expect(durable.last_checkpoint_reason).toBe("crash_recovery");
    expect(durable.checkpoint_seq).toBe(1);
    expect(durable.current_commit).not.toBe(durable.base_commit);
    expect(await resolveRef(world.repository, liveRefFor(stranded.live_session_id))).toBe(
      durable.current_commit,
    );
    const subject = await runGit(world.repository, [
      "log",
      "-1",
      "--format=%s",
      durable.current_commit as string,
    ]);
    expect(subject.trim()).toContain("(crash_recovery)");

    // … and only then were its resources given back.
    expect(await leasesOnDisk(world.commonDir)).toEqual([]);
    expect(existsSync(retainedWorktree)).toBe(false);
    expect(existsSync(stranded.workspace)).toBe(false);
    expect(existsSync(dirname(stranded.workspace))).toBe(false);
    await destroyWorld(world);
  }, 60_000);

  it("releases lease and worktree when shutdown proves the provider died", async () => {
    const world = await makeWorld();
    const os = ownership();
    const deadPid = 424_252;
    const factory = new ScriptedFactory(
      "omp-rpc",
      "omp",
      () =>
        new ScriptedTransport("omp-rpc", "omp", fullCapabilities(), {
          pid: deadPid,
          alive: os.alive,
          onHandshake: loseHandshake([CLOSED]),
        }),
    );
    const hub = await buildHub(world, { factories: [factory], probes: fakeProbes(os) });

    const failed = await captureRejection(hub.manager.start({ provider: "omp" }));
    expect(failed?.code).toBe(HANDSHAKE_CODE);
    // Proven death keeps the original failure verbatim: no retention editorial.
    expect(failed?.message).toBe("the scripted provider died mid-handshake");
    const failedId = hub.ids[0];
    expect(await leasesOnDisk(world.commonDir)).toEqual([]);
    expect(await durableStatus(world.commonDir, failedId)).toBeNull();
    // The worktree the doomed launch materialized is gone with its parent.
    const leftovers = (await readdir(world.tmpRoot)).filter((name) =>
      name.startsWith(LIVE_WORKTREE_PREFIX),
    );
    expect(leftovers).toEqual([]);
    // Cleanup came from the transport's proven shutdown, never a signal.
    expect(os.kills).toEqual([]);
    expect(factory.created[0].stopCalls).toEqual(["terminate"]);
    await destroyWorld(world);
  });
});

// ---------------------------------------------------------------------------
// 6. Checkpoint reasons
// ---------------------------------------------------------------------------

describe("checkpoint reasons", () => {
  it("records cancel and error reasons on the durable record and advances the live ref each time", async () => {
    const world = await makeWorld();
    const factory = new ScriptedFactory(
      "omp-rpc",
      "omp",
      () =>
        new ScriptedTransport("omp-rpc", "omp", fullCapabilities(), {
          onSend: async (command, transport) => {
            if (command.kind === "prompt" || command.kind === "follow_up") {
              await writeFile(
                join(transport.launch?.workspace ?? ".", `${command.kind}.txt`),
                `${command.command_id}\n`,
              );
            }
          },
        }),
    );
    const hub = await buildHub(world, { factories: [factory] });
    const started = await hub.manager.start({ provider: "omp" });
    const id = started.live_session_id;
    const transport = factory.created[0];
    const baseCommit = started.state.base_commit;

    // Cancel-reason checkpoint from a cancelled turn.
    const cancelled = hub.manager.prompt(id, "big job");
    transport.push({ kind: "status", status: "running", note: null });
    await tick();
    expect((await hub.manager.cancel(id, "user aborted")).outcome).toBe("succeeded");
    transport.push({ kind: "status", status: "idle", note: null });
    const cancelResult = await cancelled;
    expect(cancelResult.outcome).toBe("cancelled");
    expect(cancelResult.checkpoint?.reason).toBe("cancel");
    const afterCancel = await durableRecord(world.commonDir, id);
    expect(afterCancel.last_checkpoint_reason).toBe("cancel");
    expect(afterCancel.checkpoint_seq).toBe(1);
    expect(afterCancel.current_commit).toBe(cancelResult.checkpoint?.commit);
    const refAfterCancel = await resolveRef(world.repository, liveRefFor(id));
    expect(refAfterCancel).toBe(cancelResult.checkpoint?.commit);
    expect(refAfterCancel).not.toBe(baseCommit);

    // A provider exit mid-turn ends in error status with an error checkpoint.
    const crashed = hub.manager.followUp(id, "keep going");
    transport.push({ kind: "status", status: "running", note: null });
    await tick();
    transport.push({ kind: "exit", intentional: false, exit_code: 137, exit_signal: null });
    const crashResult = await crashed;
    expect(crashResult.outcome).toBe("failed");
    expect(crashResult.exit_code).toBe(137);
    expect(crashResult.checkpoint?.reason).toBe("error");

    const afterCrash = await durableRecord(world.commonDir, id);
    expect(afterCrash.status).toBe("error");
    expect(afterCrash.last_checkpoint_reason).toBe("error");
    expect(afterCrash.checkpoint_seq).toBe(2);
    expect((afterCrash.last_error as Record<string, unknown>).code).toBe("LIVE_PROVIDER_EXITED");
    const refAfterCrash = await resolveRef(world.repository, liveRefFor(id));
    expect(refAfterCrash).toBe(afterCrash.current_commit);
    expect(refAfterCrash).not.toBe(refAfterCancel);

    // The crash tore the session down: the lease goes last, then the session.
    expect(await until(async () => (await leasesOnDisk(world.commonDir)).length === 0)).toBe(true);
    expect(await until(() => hub.manager.activeCount === 0)).toBe(true);
    await destroyWorld(world);
  });
});

// ---------------------------------------------------------------------------
// 7. Restart through resumeFromState
// ---------------------------------------------------------------------------

describe("restart from the durable record", () => {
  it("resumes the same live id on a fresh worktree at current_commit with the recorded resume state", async () => {
    const world = await makeWorld();
    const factory = new ScriptedFactory(
      "omp-rpc",
      "omp",
      () =>
        new ScriptedTransport("omp-rpc", "omp", fullCapabilities(), {
          verifyResumeState: true,
          onSend: async (command, transport) => {
            if (command.kind === "prompt") {
              await writeFile(join(transport.launch?.workspace ?? ".", "provider-note.txt"), "keep this\n");
            }
          },
        }),
    );
    const hub = await buildHub(world, { factories: [factory] });
    const started = await hub.manager.start({ provider: "omp" });
    const id = started.live_session_id;
    const transport = factory.created[0];
    const staleWorkspace = started.workspace;

    const turn = hub.manager.prompt(id, "produce the note");
    transport.push({ kind: "status", status: "running", note: null });
    await tick();
    transport.push({ kind: "exit", intentional: false, exit_code: 2, exit_signal: null });
    expect((await turn).outcome).toBe("failed");

    const terminal = await durableRecord(world.commonDir, id);
    expect(terminal.status).toBe("error");
    const pinnedCommit = terminal.current_commit;
    const priorRevision = terminal.revision;
    const priorResume = terminal.resume;

    // The crash path released its own resources (reap → pin → state → cleanup).
    expect(await until(async () => (await leasesOnDisk(world.commonDir)).length === 0)).toBe(true);
    expect(await until(() => existsSync(staleWorkspace) === false)).toBe(true);
    expect(existsSync(dirname(staleWorkspace))).toBe(false);

    // Nothing is left to reconcile before a restart.
    const recovery = await hub.manager.recover();
    expect(recovery.scanned).toBe(0);

    const resumed = await hub.manager.resumeFromState({ live_session_id: id });
    const resumedTransport = factory.created[1];

    expect(resumed.live_session_id).toBe(id);
    expect(resumed.state.revision).toBe(Number(priorRevision) + 1);
    expect(resumed.state.status).toBe("idle");
    // Fresh worktree, materialized at the checkpoint, holding the provider's file.
    expect(resumed.workspace).not.toBe(staleWorkspace);
    expect(resumed.state.worktree_path).toBe(resumed.workspace);
    expect(await readFile(join(resumed.workspace, "provider-note.txt"), "utf8")).toBe("keep this\n");
    expect(resumed.state.current_commit).toBe(pinnedCommit);
    // The transport was handed the durable resume hint and verified it back.
    expect(resumedTransport.launch?.resume).toEqual(priorResume);
    expect(resumedTransport.launch?.resume).not.toBeNull();
    expect(resumed.state.resume).toMatchObject({
      provider: "omp",
      provider_session_id: "prov-1",
      verified: true,
      verified_via: "scripted-transport-handshake",
    });
    expect((await durableRecord(world.commonDir, id)).worktree_path).toBe(resumed.workspace);
    expect(
      relative(world.tmpRoot, resumed.state.worktree_parent as string).startsWith(LIVE_WORKTREE_PREFIX),
    ).toBe(true);

    // The live ref namespace was advanced in place, never branched per restart.
    const refs = (await liveRefs(world.repository)).split("\n").filter(Boolean);
    expect(refs).toEqual([liveRefFor(id)]);
    expect(await resolveRef(world.repository, liveRefFor(id))).toBe(pinnedCommit);

    await hub.manager.close(id);
    await destroyWorld(world);
  });
});

// ---------------------------------------------------------------------------
// 8. follow_up delivery timing by capability claim
// ---------------------------------------------------------------------------

describe("follow_up delivery timing", () => {
  it("delivers a native claim immediately and a hub-queued claim only at the terminal boundary", async () => {
    const world = await makeWorld();

    const nativeTimeline: string[] = [];
    const nativeFactory = new ScriptedFactory(
      "omp-rpc",
      "omp",
      () =>
        new ScriptedTransport(
          "omp-rpc",
          "omp",
          fullCapabilities({ follow_up: { support: "native", evidence: "the provider queues it" } }),
          { timeline: nativeTimeline },
        ),
    );
    const nativeHub = await buildHub(world, { factories: [nativeFactory] });
    const native = await nativeHub.manager.start({ provider: "omp" });
    const nativeId = native.live_session_id;
    const nativeTransport = nativeFactory.created[0];

    const firstNative = nativeHub.manager.prompt(nativeId, "one");
    nativeTransport.push({ kind: "status", status: "running", note: null });
    await tick();
    const queuedNative = nativeHub.manager.followUp(nativeId, "two");
    await tick();
    nativeTimeline.push("hub:idle-event-published");
    // A native claim means the PROVIDER holds the text: it went out mid-turn.
    expect(nativeTimeline).toEqual(["send:prompt", "send:follow_up", "hub:idle-event-published"]);

    nativeTransport.push({ kind: "status", status: "idle", note: null });
    expect((await firstNative).outcome).toBe("succeeded");
    await tick();
    // Its result settles at the NEXT terminal boundary, not this one, and the
    // hub never re-sends what the provider already has.
    expect(
      await Promise.race([
        queuedNative.then(() => "settled" as const),
        tick().then(() => "pending" as const),
      ]),
    ).toBe("pending");
    expect(nativeTimeline).toEqual(["send:prompt", "send:follow_up", "hub:idle-event-published"]);
    nativeTransport.push({ kind: "status", status: "idle", note: null });
    const nativeFollowResult = await queuedNative;
    expect(nativeFollowResult.kind).toBe("follow_up");
    expect(nativeFollowResult.outcome).toBe("succeeded");
    expect(nativeTransport.commands.filter((command) => command.kind === "follow_up")).toHaveLength(1);
    await nativeHub.manager.close(nativeId);

    // Same hub machinery — the only difference is the provider's claim.
    const queuedTimeline: string[] = [];
    const queuedFactory = new ScriptedFactory(
      "omp-rpc",
      "omp",
      () =>
        new ScriptedTransport(
          "omp-rpc",
          "omp",
          fullCapabilities({
            follow_up: { support: "hub-queued", evidence: "the hub owns the next-turn queue" },
          }),
          { timeline: queuedTimeline },
        ),
    );
    const queuedHub = await buildHub(world, { factories: [queuedFactory] });
    const queued = await queuedHub.manager.start({ provider: "omp" });
    const queuedId = queued.live_session_id;
    const queuedTransport = queuedFactory.created[0];

    const firstQueued = queuedHub.manager.prompt(queuedId, "one");
    queuedTransport.push({ kind: "status", status: "running", note: null });
    await tick();
    const heldQueued = queuedHub.manager.followUp(queuedId, "two");
    await tick();
    // Nothing crossed the transport boundary while the turn was running.
    expect(queuedTimeline).toEqual(["send:prompt"]);

    queuedTimeline.push("hub:idle-event-published");
    queuedTransport.push({ kind: "status", status: "idle", note: null });
    expect((await firstQueued).outcome).toBe("succeeded");
    await tick();
    // The terminal boundary is what released the hub-queued text.
    expect(queuedTimeline).toEqual(["send:prompt", "hub:idle-event-published", "send:follow_up"]);
    queuedTransport.push({ kind: "status", status: "idle", note: null });
    expect((await heldQueued).outcome).toBe("succeeded");
    await queuedHub.manager.close(queuedId);

    await destroyWorld(world);
  });
});

// ---------------------------------------------------------------------------
// 9. Permission verdicts through the production MCP tool routing
// ---------------------------------------------------------------------------

describe("permission verdicts over the MCP tool surface", () => {
  it("routes allow_once and deny to the Hermes transport and refuses any widened verdict", async () => {
    const world = await makeWorld();
    const factory = new ScriptedFactory(
      "hermes-acp",
      "hermes",
      () =>
        new ScriptedTransport(
          "hermes-acp",
          "hermes",
          fullCapabilities({
            permission_response: {
              support: "native",
              evidence: "scripted ACP request_permission answer",
            },
          }),
        ),
    );
    const hub = await buildHub(world, { factories: [factory] });
    const client = await connectTools(createHubServer({ live: hub.manager }));

    const started = documentFrom(
      await client.callTool({
        name: "live_session_start",
        arguments: { agent: "hermes", workspace: world.repository },
      }),
    );
    const id = started.live_session_id as string;
    expect(started.state.transport).toBe("hermes-acp");
    const transport = factory.created[0];

    for (const requestId of ["req-allow", "req-deny", "req-widened"]) {
      transport.push({
        kind: "permission_request",
        request_id: requestId,
        tool: "write_file",
        summary: { text: `overwrite ${requestId}`, truncated: false },
      });
    }
    await tick();

    const allowed = documentFrom(
      await client.callTool({
        name: "live_session_command",
        arguments: {
          live_session_id: id,
          action: "permission_response",
          request_id: "req-allow",
          decision: "allow_once",
          note: "this one time",
        },
      }),
    );
    expect(allowed.result.kind).toBe("permission_response");
    expect(allowed.result.outcome).toBe("succeeded");
    expect(allowed.status).toBe("idle");

    const denied = documentFrom(
      await client.callTool({
        name: "live_session_command",
        arguments: {
          live_session_id: id,
          action: "permission_response",
          request_id: "req-deny",
          decision: "deny",
        },
      }),
    );
    expect(denied.result.outcome).toBe("succeeded");

    const delivered = (): LivePermissionResponseCommand[] =>
      transport.commands.filter(
        (command): command is LivePermissionResponseCommand => command.kind === "permission_response",
      );

    expect(delivered().map((command) => [command.request_id, command.decision])).toEqual([
      ["req-allow", "allow_once"],
      ["req-deny", "deny"],
    ]);
    // The verdict crosses to the Hermes transport verbatim: allow_once means
    // the agent's own allow_once option, deny its reject_once — never the
    // widened allow_always the ACP surface also offers, which the v3 hub has
    // no word for at all.
    const first = delivered()[0];
    expect([first.decision, first.note]).toEqual(["allow_once", "this one time"]);
    expect(JSON.stringify(delivered())).not.toContain("allow_always");
    expect(JSON.stringify(delivered())).not.toContain("allow_session");

    // A widened verdict never becomes a delivery the transport can see: the
    // tool boundary refuses it outright (protocol error or isError result),
    // and the request stays open for a real answer.
    let widenedRefused = false;
    try {
      const refused = await client.callTool({
        name: "live_session_command",
        arguments: {
          live_session_id: id,
          action: "permission_response",
          request_id: "req-widened",
          decision: "allow_session",
        },
      });
      widenedRefused = (refused as { isError?: boolean }).isError === true;
    } catch {
      widenedRefused = true;
    }
    expect(widenedRefused).toBe(true);
    expect(delivered()).toHaveLength(2);

    // At the manager boundary the same verdict is a caller error, never a deny.
    const widened = await captureRejection(
      hub.manager.respondPermission(id, "req-widened", "allow_session" as LivePermissionDecision, null),
    );
    expect(widened?.code).toBe("LIVE_COMMAND_INVALID");
    expect(delivered()).toHaveLength(2);

    const stillOpen = documentFrom(
      await client.callTool({
        name: "live_session_command",
        arguments: {
          live_session_id: id,
          action: "permission_response",
          request_id: "req-widened",
          decision: "deny",
        },
      }),
    );
    expect(stillOpen.result.outcome).toBe("succeeded");
    expect(delivered().map((command) => [command.request_id, command.decision])).toEqual([
      ["req-allow", "allow_once"],
      ["req-deny", "deny"],
      ["req-widened", "deny"],
    ]);

    await client.close();
    await hub.manager.close(id);
    await destroyWorld(world);
  });
});
