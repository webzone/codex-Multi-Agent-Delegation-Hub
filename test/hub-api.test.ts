import { readFile, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import { hostname } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { AgentHubError } from "../src/errors.js";
import { AgentHub } from "../src/hub/agent-hub.js";
import {
  createHubHarness,
  fullHubCapabilities,
  hubFakeProbes,
  hubFakeProviderFactory,
  HubFakeFactory,
  HubFakeTransport,
  scriptTurn,
} from "./hub-fakes.js";
import { bridgeTransportFactory } from "../src/hub/transport-adapter.js";
import { listLiveLeases, readLiveLease, liveLeasePath } from "../src/live/lease.js";
import { liveRefFor } from "../src/live/state.js";
import { removeDirectory, resolveRef, runGit } from "./helpers.js";

async function expectCode(
  promise: Promise<unknown>,
  code: string,
): Promise<AgentHubError> {
  try {
    await promise;
  } catch (error) {
    const failure = error as AgentHubError;
    expect(failure.code).toBe(code);
    return failure;
  }
  throw new Error(`expected rejection with code ${code}`);
}

describe("AgentHub launch", () => {
  it("reserves lease + worktree and records the honest launch pair", async () => {
    const world = await createHubHarness();
    const started = await world.hub.start({ provider: "omp" });

    expect(started.probe.found).toBe(true);
    expect(started.state.status).toBe("idle");
    expect(started.state.worktree_path).not.toBe(world.repository);
    // The durable record keeps the FULL 9-claim launch snapshot.
    expect(Object.keys(started.state.capabilities)).toHaveLength(9);
    // The kernel snapshot dropped only the lifecycle-owned checkpoint claim.
    expect(Object.keys(started.capabilities)).toHaveLength(8);
    expect(started.capabilities).not.toHaveProperty("checkpoint");

    const lease = await readLiveLease(world.commonDir, started.session_id);
    expect(lease?.worktree_path).toBe(started.state.worktree_path);
    // Spawn facts land on the lease the moment the provider process exists.
    expect(lease?.provider_pid).toBe(424_242);
    expect(await resolveRef(world.repository, liveRefFor(started.session_id))).toBe(
      started.state.base_commit,
    );

    // The kernel mirror record rides its own sidecar and parses clean.
    const recordPath = join(
      world.commonDir,
      "agent-hub",
      "live",
      "interaction-records",
      `${started.session_id}.json`,
    );
    const record = JSON.parse(await readFile(recordPath, "utf8")) as {
      schema: string;
      status: string;
    };
    expect(record.schema).toBe("agent-hub-interaction/v1");
    expect(record.status).toBe("idle");

    await world.hub.closeAll();
    await removeDirectory(world.repository);
  });

  it("refuses launch on an honest not-found probe and reserves nothing", async () => {
    const world = await createHubHarness({
      probe: { found: false, version: null, detail: "no RPC v2 evidence" },
    });
    const failure = await expectCode(world.hub.start({ provider: "omp" }), "TRANSPORT_UNAVAILABLE");
    expect(failure.message).toContain("no RPC v2 evidence");
    expect(await listLiveLeases(world.commonDir)).toHaveLength(0);
    expect(await world.hub.list()).toHaveLength(0);
    await removeDirectory(world.repository);
  });

  it("refuses a dirty caller checkout unless allow_dirty", async () => {
    const world = await createHubHarness();
    await writeFile(join(world.repository, "stray.txt"), "untracked\n", "utf8");
    await expectCode(world.hub.start({ provider: "omp" }), "DIRTY_WORKTREE");
    const started = await world.hub.start({ provider: "omp", allow_dirty: true });
    expect(started.state.status).toBe("idle");
    await world.hub.closeAll();
    await removeDirectory(world.repository);
  });

  it("enforces the durable lease quota per common dir", async () => {
    const world = await createHubHarness({ hubOptions: { commonDirQuota: 1 } });
    const first = await world.hub.start({ provider: "omp" });
    await expectCode(world.hub.start({ provider: "omp" }), "QUOTA_EXCEEDED");
    expect(await listLiveLeases(world.commonDir)).toHaveLength(1);
    await world.hub.close(first.session_id);
    const second = await world.hub.start({ provider: "omp" });
    expect(second.session_id).not.toBe(first.session_id);
    await world.hub.closeAll();
    await removeDirectory(world.repository);
  });
});

describe("AgentHub turns and checkpoints", () => {
  it("settles a prompt turn and pins the checkpoint chain", async () => {
    const world = await createHubHarness();
    const started = await world.hub.start({ provider: "omp" });
    const transport = scriptTurn(world.factory, 0, {
      writes: { "note.md": "worked on\n" },
    });

    const turn = await world.hub.prompt(started.session_id, "do the work");
    expect(turn.outcome).toBe("succeeded");
    expect(turn.checkpoint).not.toBeNull();
    expect(turn.checkpoint?.reason).toBe("turn_end");
    expect(turn.final_text?.text).toBe("done: prompt");

    const state = await world.hub.status(started.session_id);
    expect(state.state?.current_commit).toBe(turn.checkpoint?.commit);
    expect(state.state?.checkpoint_seq).toBe(1);
    expect(state.state?.last_checkpoint_reason).toBe("turn_end");
    expect(await resolveRef(world.repository, liveRefFor(started.session_id))).toBe(
      turn.checkpoint?.commit,
    );
    // The work lives in the hub worktree, never in the caller checkout.
    expect(existsSync(join(world.repository, "note.md"))).toBe(false);
    expect(existsSync(join(started.workspace, "note.md"))).toBe(true);
    // The kernel re-stamped envelopes: transport lies never survive.
    const events = world.hub.eventsAfter(started.session_id, 0);
    expect(events.status).toBe("ok");
    if (events.status === "ok") {
      expect(events.events.every((event) => event.session_id === started.session_id)).toBe(true);
    }
    void transport;
    await world.hub.closeAll();
    await removeDirectory(world.repository);
  });

  it("settles a cancelled turn with a cancel-reason checkpoint", async () => {
    const world = await createHubHarness();
    const started = await world.hub.start({ provider: "omp" });
    scriptTurn(world.factory, 0, { writes: { "half.md": "partial\n" }, hang: true });

    const running = world.hub.prompt(started.session_id, "slow work");
    const cancelled = await world.hub.cancel(started.session_id, "changed my mind");
    expect(cancelled.outcome).toBe("succeeded"); // the cancel command itself delivered
    const turn = await running;
    expect(turn.outcome).toBe("cancelled");
    expect(turn.checkpoint?.reason).toBe("cancel");

    await world.hub.closeAll();
    await removeDirectory(world.repository);
  });

  it("refuses steer pre-dispatch when the launch snapshot says unsupported", async () => {
    const world = await createHubHarness({
      capabilities: () =>
        fullHubCapabilities({ steer: { support: "unsupported", evidence: null } }),
    });
    const started = await world.hub.start({ provider: "omp" });
    const turn = await world.hub.steer(started.session_id, "left!").catch(() => null);
    // The kernel refuses capability-unsupported steer with a result, not a throw.
    expect(turn?.outcome).toBe("unsupported");
    expect(turn?.error?.stage).toBe("capability");
    const transport = world.factory.created[0] as HubFakeTransport;
    expect(transport.commands.some((command) => command.kind === "steer")).toBe(false);
    await world.hub.closeAll();
    await removeDirectory(world.repository);
  });
});

describe("AgentHub permissions", () => {
  it("delivers only contract verdicts, and only for observed requests", async () => {
    const world = await createHubHarness();
    const started = await world.hub.start({
      provider: "omp",
      permission_policy: "interactive",
    });
    scriptTurn(world.factory, 0, {
      during: [
        {
          kind: "permission_request",
          request_id: "req-1",
          tool: "shell",
          summary: { text: "rm -rf build", truncated: false },
        },
      ],
    });
    await world.hub.prompt(started.session_id, "clean the build dir");

    const answer = await world.hub.respondPermission(started.session_id, "req-1", "allow_once");
    expect(answer.outcome).toBe("succeeded");
    const transport = world.factory.created[0] as HubFakeTransport;
    const delivered = transport.commands.find((command) => command.kind === "permission_response");
    expect(delivered).toMatchObject({ request_id: "req-1", decision: "allow_once" });

    await expectCode(
      world.hub.respondPermission(
        started.session_id,
        "req-1",
        "allow_always" as "allow_once",
      ),
      "COMMAND_INVALID",
    );
    await expectCode(
      world.hub.respondPermission(started.session_id, "req-9", "deny"),
      "PERMISSION_REQUEST_UNKNOWN",
    );
    await world.hub.closeAll();
    await removeDirectory(world.repository);
  });
});

describe("AgentHub close, handoff, resume, gc", () => {
  it("tears down on proven shutdown and hands off the chain", async () => {
    const world = await createHubHarness();
    const started = await world.hub.start({ provider: "omp" });
    scriptTurn(world.factory, 0, { writes: { "a.md": "first\n" } });
    await world.hub.prompt(started.session_id, "task one");
    // Provider activity after the last pinned boundary: close must pin it.
    await writeFile(join(started.workspace, "late.md"), "late\n", "utf8");

    const closed = await world.hub.close(started.session_id);
    expect(closed.cleanup_errors).toHaveLength(0);
    expect(closed.checkpoint_taken).toBe(true);
    expect(await readLiveLease(world.commonDir, started.session_id)).toBeUndefined();
    expect(existsSync(started.workspace)).toBe(false);

    const handoff = await world.hub.handoff(started.session_id);
    expect(handoff.changed_files.sort()).toEqual(["a.md", "late.md"]);
    expect(handoff.checkpoints.map((c) => c.reason)).toEqual(["turn_end", "close"]);
    expect(handoff.status).toBe("closed");
    expect(handoff.final_commit).not.toBe(handoff.base_commit);
    expect(handoff.apply_hint).toContain("git cherry-pick");
    expect(handoff.ref).toBe(liveRefFor(started.session_id));
    await removeDirectory(world.repository);
  });

  it("retains everything when shutdown cannot be proven", async () => {
    const world = await createHubHarness();
    const started = await world.hub.start({ provider: "omp" });
    const transport = world.factory.created[0] as HubFakeTransport;
    transport.stopResults = [{ status: "orphaned", exit_code: null, exit_signal: null, waited_ms: 5 }];

    const closed = await world.hub.close(started.session_id);
    expect((closed.record as { status: string }).status).toBe("orphaned");
    expect(closed.checkpoint_taken).toBe(false);
    expect(await readLiveLease(world.commonDir, started.session_id)).toBeDefined();
    expect(existsSync(started.workspace)).toBe(true);
    await expectCode(world.hub.handoff(started.session_id), "SESSION_STILL_OWNED");
    await removeDirectory(world.repository);
  });

  it("resumes the same durable line with identity verification", async () => {
    const world = await createHubHarness({ transportOptions: { resumeState: "echo" } });
    const first = await world.hub.start({ provider: "omp" });
    scriptTurn(world.factory, 0, { writes: { "a.md": "first\n" } });
    await world.hub.prompt(first.session_id, "task one");
    await world.hub.close(first.session_id);
    const afterClose = await world.hub.status(first.session_id);
    const closeRevision = afterClose.state?.revision ?? 0;
    const refAfterClose = await resolveRef(world.repository, liveRefFor(first.session_id));

    const resumed = await world.hub.resume(first.session_id);
    expect(resumed.session_id).toBe(first.session_id);
    const resumedState = await world.hub.status(first.session_id);
    expect(resumedState.state?.revision).toBeGreaterThan(closeRevision);
    // The chain is continuous: same ref, checkpoint head kept.
    expect(resumedState.state?.current_commit).toBe(refAfterClose);
    expect(resumedState.state?.worktree_path).not.toBe(first.workspace);
    // The transport echoed the durable handle: verification is observed, not claimed.
    expect(resumedState.state?.resume?.verified_via).toBe("transport-verified:open-echo");

    scriptTurn(world.factory, 1, { writes: { "b.md": "second\n" } });
    const turn = await world.hub.followUp(first.session_id, "keep going");
    expect(turn.outcome).toBe("succeeded");
    await world.hub.close(first.session_id);

    const handoff = await world.hub.handoff(first.session_id);
    expect(handoff.changed_files.sort()).toEqual(["a.md", "b.md"]);
    await removeDirectory(world.repository);
  });

  it("refuses resume while leased", async () => {
    const world = await createHubHarness();
    const started = await world.hub.start({ provider: "omp" });
    // A second hub over the same repository must not adopt the live session.
    const other = await AgentHub.open(world.repository, {
      transportFactories: [bridgeTransportFactory(new HubFakeFactory())],
      providerFactories: [hubFakeProviderFactory],
      tmpRoot: world.tmpRoot,
      probes: hubFakeProbes(),
    });
    // While the session is live its state is non-terminal: that gate answers first.
    await expectCode(other.resume(started.session_id), "SESSION_NOT_RESUMABLE");
    // A terminal state with a still-held lease (crash between rewrite and
    // teardown) must route through gc, never be adopted underneath the lease.
    const statePath = join(
      world.commonDir,
      "agent-hub",
      "live",
      "sessions",
      `${started.session_id}.json`,
    );
    const state = JSON.parse(await readFile(statePath, "utf8")) as Record<string, unknown>;
    await writeFile(statePath, JSON.stringify({ ...state, status: "orphaned" }, null, 2), "utf8");
    await expectCode(other.resume(started.session_id), "LIVE_LEASE_EXISTS");
    await world.hub.closeAll();
    await removeDirectory(world.repository);
  });

  it("gc reconciles an orphaned lease: reaps, pins, rewrites, releases", async () => {
    const world = await createHubHarness();
    const started = await world.hub.start({ provider: "omp" });
    scriptTurn(world.factory, 0, { writes: { "work.md": "dangling\n" }, hang: true });
    void world.hub.prompt(started.session_id, "abandoned task").catch(() => undefined);

    // Forge hub loss on the durable artifact: a dead hub pid with provider
    // facts that probe as a dead leader and a gone owned group.
    const lease = await readLiveLease(world.commonDir, started.session_id);
    expect(lease).toBeDefined();
    const forged = {
      ...lease!,
      hub_pid: 999_999,
      hub_start_token: "dead-dead-dead",
    };
    await writeFile(
      liveLeasePath(world.commonDir, started.session_id),
      `${JSON.stringify(forged, null, 2)}\n`,
      "utf8",
    );

    // Reconciliation runs where the session is NOT attached: a fresh hub.
    const other = await AgentHub.open(world.repository, {
      transportFactories: [bridgeTransportFactory(new HubFakeFactory())],
      providerFactories: [hubFakeProviderFactory],
      tmpRoot: world.tmpRoot,
      probes: hubFakeProbes(),
    });
    const report = await other.gc();
    const entry = report.sessions.find((session) => session.session_id === started.session_id);
    expect(entry?.outcome).toBe("recovered");
    expect(entry?.detail).toContain("orphaned");
    expect(await readLiveLease(world.commonDir, started.session_id)).toBeUndefined();
    expect(existsSync(started.workspace)).toBe(false);

    const state = await world.hub.status(started.session_id);
    expect(state.state?.status).toBe("orphaned");
    expect(state.state?.last_error?.code).toBe("SESSION_ORPHANED");
    // The surviving work was pinned BEFORE the rewrite.
    const chain = await runGit(world.repository, [
      "log",
      "--format=%s",
      liveRefFor(started.session_id),
      "--not",
      state.state?.base_commit as string,
    ]);
    expect(chain).toContain("crash_recovery");

    // Handoff on an orphan carries the honest warning.
    const handoff = await world.hub.handoff(started.session_id);
    expect(handoff.warning).toContain("orphaned");
    await removeDirectory(world.repository);
  });

  it("gc dry-run touches nothing", async () => {
    const world = await createHubHarness();
    const started = await world.hub.start({ provider: "omp" });
    const lease = await readLiveLease(world.commonDir, started.session_id);
    await writeFile(
      liveLeasePath(world.commonDir, started.session_id),
      `${JSON.stringify({ ...lease!, hub_pid: 999_999 }, null, 2)}\n`,
      "utf8",
    );
    const other = await AgentHub.open(world.repository, {
      transportFactories: [bridgeTransportFactory(new HubFakeFactory())],
      providerFactories: [hubFakeProviderFactory],
      tmpRoot: world.tmpRoot,
      probes: hubFakeProbes(),
    });
    const report = await other.gc({ dry_run: true });
    const entry = report.sessions.find((session) => session.session_id === started.session_id);
    expect(entry?.outcome).toBe("dry-run");
    expect(entry?.detail).toContain("would");
    expect(report.worktrees_pruned).toBe(false);
    expect(await readLiveLease(world.commonDir, started.session_id)).toBeDefined();
    expect(existsSync(started.workspace)).toBe(true);
    await removeDirectory(world.repository);
  });

  it("hostname proof keeps foreign leases hands-off", async () => {
    const world = await createHubHarness();
    const started = await world.hub.start({ provider: "omp" });
    const lease = await readLiveLease(world.commonDir, started.session_id);
    await writeFile(
      liveLeasePath(world.commonDir, started.session_id),
      `${JSON.stringify({ ...lease!, hub_hostname: "not-this-host" }, null, 2)}\n`,
      "utf8",
    );
    const other = await AgentHub.open(world.repository, {
      transportFactories: [bridgeTransportFactory(new HubFakeFactory())],
      providerFactories: [hubFakeProviderFactory],
      tmpRoot: world.tmpRoot,
      probes: hubFakeProbes(),
    });
    const report = await other.gc();
    const entry = report.sessions.find((session) => session.session_id === started.session_id);
    expect(entry?.outcome).toBe("foreign");
    expect(entry?.detail).toContain("not-this-host");
    expect(await readLiveLease(world.commonDir, started.session_id)).toBeDefined();
    expect(hostname()).toBeTruthy();
    await removeDirectory(world.repository);
  });
});
