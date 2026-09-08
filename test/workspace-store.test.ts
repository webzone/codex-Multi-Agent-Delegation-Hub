import { randomUUID } from "node:crypto";
import { mkdtemp, readdir, stat, writeFile } from "node:fs/promises";
import { hostname, tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { deferred } from "../src/deferred.js";
import { lockPathFor } from "../src/locks.js";
import {
  leasePath,
  readJsonFile,
  resultPath,
  workspacePendingPath,
  workspaceRecordPath,
  writeJsonAtomic,
} from "../src/workspace/home.js";
import {
  classifyLease,
  readLease,
  recordWorkspaceLease,
  WORKSPACE_LEASE_SCHEMA,
} from "../src/workspace/leases.js";
import type { WorkspaceLeaseRecord } from "../src/workspace/leases.js";
import {
  parseWorkspaceTransaction,
  WORKSPACE_TRANSACTION_SCHEMA_VERSION,
  workspaceRefFor,
} from "../src/workspace/records.js";
import {
  loadWorkspace,
  readRecordRaw,
  updateRecordCas,
  withWorkspaceLock,
  workspaceLockName,
  type StoreContext,
} from "../src/workspace/store.js";
import { removeDirectory, resolveRef, runGit } from "./helpers.js";
import { expectCode, fakeProbes, makeFixture } from "./workspace-fakes.js";
import type { Fixture, OpenedSession } from "./workspace-fakes.js";

/**
 * Unit proof for the custody store (P2 WorkspaceLifecycle): locking, CAS
 * discipline, atomic writes, sidecar recovery outcomes with on-disk evidence,
 * out-of-band ref agreement, and the provider-lease ownership matrix.
 */

const COMMIT = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;
const running = { kind: "status", status: "running", note: null } as const;
const idle = { kind: "status", status: "idle", note: null } as const;
const doneText = {
  kind: "text",
  role: "assistant",
  stream_id: "m1",
  text: { text: "done", truncated: false },
  final: true,
} as const;

async function exists(path: string): Promise<boolean> {
  return (await stat(path).catch(() => null)) !== null;
}

function ctxFor(fx: Fixture, over: Partial<StoreContext> = {}): StoreContext {
  return { home: fx.home, repositoryCwd: fx.repo, now: fx.clock.now, ...over };
}

/** Deliver one prompt that changes the worktree, publishing result `seq`. */
async function publishTurn(fx: Fixture, s: OpenedSession, name: string) {
  const turn = fx.lc.turn(fx.kernel, s.id, "prompt", "work");
  await fx.fileInWorktree(s.workspace.worktree_path, name, `${name} content\n`);
  s.transport.emit(running);
  s.transport.emit(doneText);
  s.transport.emit(idle);
  return turn;
}

/** Provision + deliver a prompt that crashes the hub right after the
 *  captured sidecar was written: the sidecar survives, the ref does not move. */
async function crashAtCaptured(fx: Fixture): Promise<OpenedSession> {
  const s = await fx.openSession();
  const turn = publishTurn(fx, s, "feature.txt");
  await expectCode(turn, "SIMULATED_CRASH");
  return s;
}

async function scratchHome(): Promise<string> {
  return mkdtemp(join(tmpdir(), "agent-hub-store-test-"));
}

describe("custody locking", () => {
  it("refuses a second holder at once and honors the wait budget through a release", async () => {
    const fx = await makeFixture();
    try {
      const sessionId = randomUUID();
      const holder = deferred<void>();
      const release = deferred<void>();
      let holderDone = false;
      const held = withWorkspaceLock(ctxFor(fx), sessionId, async () => {
        holder.resolve();
        await release.promise;
        holderDone = true;
        return "held-first";
      });
      await holder.promise;
      expect(await exists(lockPathFor(fx.home, workspaceLockName(sessionId)))).toBe(true);

      // Zero wait budget: contention is reported immediately, never queued.
      await expectCode(
        withWorkspaceLock(ctxFor(fx), sessionId, async () => "should-not-run"),
        "LOCK_BUSY",
      );

      // A wait budget survives the holder's release and then runs exclusively.
      const waiting = withWorkspaceLock(
        ctxFor(fx, { lockWaitMs: 5_000, lockRetryDelayMs: 10 }),
        sessionId,
        async () => {
          expect(holderDone).toBe(true);
          return "acquired-after-release";
        },
      );
      await new Promise((resolveWait) => setTimeout(resolveWait, 50));
      release.resolve();
      expect(await held).toBe("held-first");
      expect(await waiting).toBe("acquired-after-release");
      expect(await exists(lockPathFor(fx.home, workspaceLockName(sessionId)))).toBe(false);
    } finally {
      await fx.cleanup();
    }
  });
});

describe("record CAS discipline", () => {
  it("lands exactly-one advances and refuses stale, stalled, and leaping writers", async () => {
    const fx = await makeFixture();
    try {
      const s = await fx.openSession();
      const ctx = ctxFor(fx);
      const updated = await updateRecordCas(ctx, s.id, 0, (record) => ({
        ...record,
        agent: "renamed",
        revision: 1,
      }));
      expect(updated.revision).toBe(1);
      expect(updated.agent).toBe("renamed");

      await expectCode(
        updateRecordCas(ctx, s.id, 0, (record) => ({ ...record, agent: "stale", revision: 1 })),
        "WORKSPACE_CAS_CONFLICT",
      );
      await expectCode(
        updateRecordCas(ctx, s.id, 1, (record) => ({ ...record, agent: "leap", revision: 3 })),
        "WORKSPACE_TRANSITION_INVALID",
      );
      await expectCode(
        updateRecordCas(ctx, s.id, 1, (record) => ({ ...record, agent: "stall", revision: 1 })),
        "WORKSPACE_TRANSITION_INVALID",
      );
      await expectCode(
        updateRecordCas(ctx, randomUUID(), 0, (record) => ({ ...record, revision: 1 })),
        "WORKSPACE_NOT_FOUND",
      );

      const disk = await readRecordRaw(fx.home, s.id);
      expect(disk.status).toBe("present");
      if (disk.status === "present") {
        expect(disk.record.revision).toBe(1);
        expect(disk.record.agent).toBe("renamed");
      }
    } finally {
      await fx.cleanup();
    }
  });

  it("reports a corrupt record as corrupt and refuses to build on it", async () => {
    const fx = await makeFixture();
    try {
      const s = await fx.openSession();
      await writeFile(workspaceRecordPath(fx.home, s.id), "{ not json", "utf8");
      expect(await readRecordRaw(fx.home, s.id)).toEqual({ status: "corrupt" });

      const loaded = await loadWorkspace(ctxFor(fx), s.id);
      expect(loaded.status).toBe("inconsistent");
      expect((loaded as { detail: string }).detail).toContain("corrupt");

      await expectCode(
        updateRecordCas(ctxFor(fx), s.id, 0, (record) => ({ ...record, revision: 1 })),
        "WORKSPACE_STATE_INCONSISTENT",
      );
      // The hub never guesses a record away: the garbage stays for audit.
      expect(await exists(workspaceRecordPath(fx.home, s.id))).toBe(true);
    } finally {
      await fx.cleanup();
    }
  });
});

describe("atomic JSON persistence", () => {
  it("writes through tmp+rename leaving no temp residue and creating parents", async () => {
    const home = await scratchHome();
    try {
      const target = join(home, "agent-hub", "workspaces", "nested", "record.json");
      await writeJsonAtomic(target, { v: 1 });
      expect(await readJsonFile(target)).toEqual({ v: 1 });
      await writeJsonAtomic(target, { v: 2 });
      expect(await readJsonFile(target)).toEqual({ v: 2 });
      const leftovers = (await readdir(join(home, "agent-hub", "workspaces", "nested"))).filter(
        (name) => name.endsWith(".tmp"),
      );
      expect(leftovers).toEqual([]);
      expect(await readJsonFile(join(home, "absent.json"))).toBeUndefined();
    } finally {
      await removeDirectory(home);
    }
  });
});

describe("loadWorkspace ref agreement", () => {
  it("reports none while honest, and inconsistency for any out-of-band ref move", async () => {
    const fx = await makeFixture();
    try {
      const s = await fx.openSession();
      const ctx = ctxFor(fx);
      const refName = workspaceRefFor(s.id);

      const fresh = await loadWorkspace(ctx, s.id);
      expect(fresh.status).toBe("present");
      if (fresh.status === "present") {
        expect(fresh.recovery).toEqual({ outcome: "none" });
      }

      await publishTurn(fx, s, "feature.txt");
      const honest = await loadWorkspace(ctx, s.id);
      expect(honest.status).toBe("present");
      if (honest.status === "present") {
        expect(honest.recovery).toEqual({ outcome: "none" });
        expect(honest.record.revision).toBe(1);
        expect(await resolveRef(fx.repo, refName)).toBe(honest.record.head_commit);
      }

      // Rewind the custody ref out-of-band: the hub must see disagreement.
      await runGit(fx.repo, ["update-ref", refName, s.workspace.base_commit]);
      const rewound = await loadWorkspace(ctx, s.id);
      expect(rewound.status).toBe("inconsistent");
      expect((rewound as { detail: string }).detail).toContain("custody ref points at");
      const disk = await readRecordRaw(fx.home, s.id);
      expect(disk.status).toBe("present");
      if (disk.status === "present") {
        expect(disk.record.revision).toBe(1);
      }
      expect(await exists(workspacePendingPath(fx.home, s.id))).toBe(false);
      expect(await resolveRef(fx.repo, refName)).toBe(s.workspace.base_commit);

      // Ref-absent is honest only while the head never left the base.
      await runGit(fx.repo, ["update-ref", "-d", refName]);
      const deleted = await loadWorkspace(ctx, s.id);
      expect(deleted.status).toBe("inconsistent");
      expect((deleted as { detail: string }).detail).toContain("(absent)");
    } finally {
      await fx.cleanup();
    }
  });
});

describe("pending transaction recovery", () => {
  it("abandons a captured sidecar the ref proves never landed, then reports none", async () => {
    const fx = await makeFixture({ failOncePhase: "captured-written" });
    try {
      const s = await crashAtCaptured(fx);
      const pending = workspacePendingPath(fx.home, s.id);
      expect(await exists(pending)).toBe(true);

      const loaded = await loadWorkspace(ctxFor(fx), s.id);
      expect(loaded.status).toBe("present");
      if (loaded.status === "present") {
        expect(loaded.recovery.outcome).toBe("abandoned");
        expect(loaded.recovery.outcome === "abandoned" && loaded.recovery.detail).toContain("never moved it");
        expect(loaded.record.revision).toBe(0);
        expect(loaded.record.head_commit).toBe(s.workspace.base_commit);
      }
      expect(await exists(pending)).toBe(false);
      expect(await exists(resultPath(fx.home, s.id, 1))).toBe(false);
      expect(await resolveRef(fx.repo, workspaceRefFor(s.id))).toBeNull();

      // The abandonment is durable: later loads see nothing to recover.
      const again1 = await loadWorkspace(ctxFor(fx), s.id);
      const again2 = await loadWorkspace(ctxFor(fx), s.id);
      expect(again1.status === "present" && again1.recovery).toEqual({ outcome: "none" });
      expect(again2.status === "present" && again2.recovery).toEqual({ outcome: "none" });
    } finally {
      await fx.cleanup();
    }
  });

  it("abandons a handcrafted intent sidecar whose ref and revision still match", async () => {
    const fx = await makeFixture();
    try {
      const s = await fx.openSession();
      const turn = publishTurn(fx, s, "feature.txt");
      const ctx = ctxFor(fx);
      const refName = workspaceRefFor(s.id);
      const published = await turn;
      const head = published.result!.commit;

      // A hub that died mid-plan leaves exactly this shape on disk.
      const intent = {
        schema: WORKSPACE_TRANSACTION_SCHEMA_VERSION,
        session_id: s.id,
        kind: "result" as const,
        reason: "turn_end" as const,
        seq: 2,
        command_id: "cmd-2",
        ref: refName,
        expected_ref: head,
        expected_revision: 1,
        capture_phase: "intent" as const,
        new_commit: null,
        tree: null,
        next_record: null,
        result: null,
        prepared_at: fx.clock.now().toISOString(),
      };
      await writeJsonAtomic(workspacePendingPath(fx.home, s.id), intent);

      const loaded = await loadWorkspace(ctx, s.id);
      expect(loaded.status).toBe("present");
      expect(loaded.status === "present" && loaded.recovery.outcome).toBe("abandoned");
      expect(await exists(workspacePendingPath(fx.home, s.id))).toBe(false);
      const disk = await readRecordRaw(fx.home, s.id);
      expect(disk.status === "present" && disk.record.revision).toBe(1);
      expect(await exists(resultPath(fx.home, s.id, 2))).toBe(false);
      expect(await resolveRef(fx.repo, refName)).toBe(head);
    } finally {
      await fx.cleanup();
    }
  });

  it("replays a landed sidecar onto the record and the result file", async () => {
    const fx = await makeFixture({ failOncePhase: "captured-written" });
    try {
      const s = await crashAtCaptured(fx);
      const pending = workspacePendingPath(fx.home, s.id);
      const tx = parseWorkspaceTransaction(await readJsonFile(pending));
      expect(tx.capture_phase).toBe("captured");
      expect(tx.new_commit).toMatch(COMMIT);
      const before = await readRecordRaw(fx.home, s.id);
      expect(before.status === "present" && before.record.revision).toBe(0);

      // The crash's twin: the ref CAS had already won before the hub died.
      await runGit(fx.repo, ["update-ref", workspaceRefFor(s.id), tx.new_commit!]);
      const loaded = await loadWorkspace(ctxFor(fx), s.id);
      expect(loaded.status).toBe("present");
      if (loaded.status === "present") {
        expect(loaded.recovery.outcome).toBe("landed");
        expect(loaded.recovery.outcome === "landed" && loaded.recovery.detail).toContain("replayed");
      }

      const after = await readRecordRaw(fx.home, s.id);
      expect(after.status).toBe("present");
      if (after.status === "present") {
        expect(after.record.revision).toBe(1);
        expect(after.record.head_commit).toBe(tx.new_commit);
        expect(after.record.last_result_seq).toBe(1);
      }
      const resultOnDisk = await readJsonFile(resultPath(fx.home, s.id, 1));
      expect(resultOnDisk).toEqual(tx.result);
      expect(await exists(pending)).toBe(false);
      expect(await resolveRef(fx.repo, workspaceRefFor(s.id))).toBe(tx.new_commit);
    } finally {
      await fx.cleanup();
    }
  });

  it("calls a ref pointing anywhere else inconsistency and changes nothing", async () => {
    const fx = await makeFixture({ failOncePhase: "captured-written" });
    try {
      const s = await crashAtCaptured(fx);
      const pending = workspacePendingPath(fx.home, s.id);
      const ctx = ctxFor(fx);
      const refName = workspaceRefFor(s.id);

      // The ref moved to a value neither expected nor produced by the plan.
      await runGit(fx.repo, ["update-ref", refName, s.workspace.base_commit]);
      const first = await loadWorkspace(ctx, s.id);
      expect(first.status).toBe("inconsistent");
      const detail = (first as { detail: string }).detail;
      expect(detail).toContain("sidecar expects");
      expect(detail).toContain("but the ref points at");

      // Exhaustive retain-and-report: nothing is deleted or rewritten.
      expect(await exists(pending)).toBe(true);
      const disk = await readRecordRaw(fx.home, s.id);
      expect(disk.status === "present" && disk.record.revision).toBe(0);
      expect(await exists(resultPath(fx.home, s.id, 1))).toBe(false);
      expect(await resolveRef(fx.repo, refName)).toBe(s.workspace.base_commit);

      const held = await readJsonFile(pending);
      const second = await loadWorkspace(ctx, s.id);
      expect(second.status).toBe("inconsistent");
      expect((second as { detail: string }).detail).toBe(detail);
      expect(await readJsonFile(pending)).toEqual(held);
    } finally {
      await fx.cleanup();
    }
  });

  it("refuses to guess a corrupt sidecar away", async () => {
    const fx = await makeFixture({ failOncePhase: "captured-written" });
    try {
      const s = await crashAtCaptured(fx);
      const pending = workspacePendingPath(fx.home, s.id);
      await writeFile(pending, "{ the hub died mid-rename", "utf8");

      const loaded = await loadWorkspace(ctxFor(fx), s.id);
      expect(loaded.status).toBe("inconsistent");
      expect((loaded as { detail: string }).detail).toContain("corrupt");
      // The unverifiable evidence is preserved, the record untouched.
      expect(await exists(pending)).toBe(true);
      const disk = await readRecordRaw(fx.home, s.id);
      expect(disk.status === "present" && disk.record.revision).toBe(0);
    } finally {
      await fx.cleanup();
    }
  });
});

describe("provider process leases", () => {
  const leaseJson = (over: Record<string, unknown> = {}): Record<string, unknown> => ({
    schema: WORKSPACE_LEASE_SCHEMA,
    session_id: randomUUID(),
    provider: "fake",
    transport: "fake-rpc",
    provider_pid: 411111,
    provider_pgid: 411111,
    provider_start_token: "fake-start-token",
    hub_pid: process.pid,
    hub_hostname: hostname(),
    hub_start_token: "hub-token",
    created_at: "2026-09-07T00:00:00.000Z",
    ...over,
  });

  async function handcraftedLease(
    home: string,
    over: Record<string, unknown> = {},
  ): Promise<WorkspaceLeaseRecord> {
    const value = leaseJson(over);
    const sessionId = value.session_id as string;
    await writeJsonAtomic(leasePath(home, sessionId), value);
    const read = await readLease(home, sessionId);
    expect(read.status).toBe("present");
    if (read.status !== "present") {
      throw new Error("unreachable: handcrafted lease must parse");
    }
    return read.lease;
  }

  it("records spawn facts idempotently and refuses to overwrite another owner", async () => {
    const home = await scratchHome();
    try {
      const sessionId = randomUUID();
      const input = {
        home,
        session_id: sessionId,
        facts: { pid: 411111, pgid: 411111 },
        provider: "fake",
        transport: "fake-rpc",
        hubStartToken: "hub-token",
        now: () => new Date(Date.UTC(2026, 8, 7)),
        probes: fakeProbes({ pid: "dead", group: "gone" }),
      };
      const first = await recordWorkspaceLease(input);
      expect(first).toMatchObject({
        schema: WORKSPACE_LEASE_SCHEMA,
        session_id: sessionId,
        provider_pid: 411111,
        provider_pgid: 411111,
        provider_start_token: "fake-start-token",
        hub_pid: process.pid,
        hub_hostname: hostname(),
        hub_start_token: "hub-token",
        created_at: "2026-09-07T00:00:00.000Z",
      });

      // The same process re-recording is a replay, not a rewrite.
      expect(await recordWorkspaceLease(input)).toEqual(first);
      const replay = await readLease(home, sessionId);
      expect(replay.status === "present" && replay.lease).toEqual(first);

      // A different process — even with one shared identity field — collides.
      await expectCode(
        recordWorkspaceLease({ ...input, facts: { pid: 422222, pgid: 411111 } }),
        "WORKSPACE_LEASE_CONFLICT",
      );
      await expectCode(
        recordWorkspaceLease({ ...input, facts: { pid: 411111, pgid: 999999 } }),
        "WORKSPACE_LEASE_CONFLICT",
      );
      const after = await readLease(home, sessionId);
      expect(after.status === "present" && after.lease).toEqual(first);
    } finally {
      await removeDirectory(home);
    }
  });

  it("treats a corrupt lease file as an unverifiable owner, never overwritable", async () => {
    const home = await scratchHome();
    try {
      const sessionId = randomUUID();
      await writeJsonAtomic(leasePath(home, sessionId), { schema: "bogus", provider_pid: 1 });
      expect(await readLease(home, sessionId)).toEqual({ status: "corrupt" });
      await expectCode(
        recordWorkspaceLease({
          home,
          session_id: sessionId,
          facts: { pid: 411111, pgid: 411111 },
          provider: "fake",
          transport: "fake-rpc",
          hubStartToken: "hub-token",
          now: () => new Date(Date.UTC(2026, 8, 7)),
          probes: fakeProbes({ pid: "dead", group: "gone" }),
        }),
        "WORKSPACE_LEASE_CONFLICT",
      );
      // Ownership stays unverifiable — the file is never quietly replaced.
      expect(await readLease(home, sessionId)).toEqual({ status: "corrupt" });
    } finally {
      await removeDirectory(home);
    }
  });

  it("proves a dead leader with a dead group, and stays uncertain without that proof", async () => {
    const home = await scratchHome();
    try {
      const lease = await handcraftedLease(home);
      const gone = await classifyLease(lease, fakeProbes({ pid: "dead", group: "gone" }));
      expect(gone).toEqual({ state: "hub-gone", provider: { state: "dead" } });

      // Group identity missing: helper survival cannot be probed at all.
      const noGroup = await handcraftedLease(home, { provider_pgid: null });
      const unknown = await classifyLease(noGroup, fakeProbes({ pid: "dead", group: "gone" }));
      expect(unknown.state).toBe("hub-gone");
      expect(unknown.state === "hub-gone" && unknown.provider.state).toBe("uncertain");
      expect(
        unknown.state === "hub-gone" &&
          unknown.provider.state === "uncertain" &&
          unknown.provider.reason,
      ).toContain("no process-group identity");

      // A recorded group the leader does not provably lead is not its death.
      const otherGroup = await handcraftedLease(home, { provider_pgid: 555555 });
      const notLeader = await classifyLease(otherGroup, fakeProbes({ pid: "dead", group: "gone" }));
      expect(notLeader.state === "hub-gone" && notLeader.provider.state).toBe("uncertain");
    } finally {
      await removeDirectory(home);
    }
  });

  it("keeps a live provider uncertain while its process group may still write", async () => {
    const home = await scratchHome();
    try {
      const lease = await handcraftedLease(home);
      const alive = await classifyLease(lease, fakeProbes({ pid: "dead", group: "alive" }));
      expect(alive.state).toBe("hub-gone");
      expect(alive.state === "hub-gone" && alive.provider).toMatchObject({
        state: "uncertain",
        reapable: false,
      });
      expect(
        alive.state === "hub-gone" &&
          alive.provider.state === "uncertain" &&
          alive.provider.reason,
      ).toContain("still exists");

      const unprobed = await classifyLease(lease, fakeProbes({ pid: "dead", group: "uncertain" }));
      expect(
        unprobed.state === "hub-gone" &&
          unprobed.provider.state === "uncertain" &&
          unprobed.provider.reason,
      ).toContain("cannot be probed");
    } finally {
      await removeDirectory(home);
    }
  });

  it("reaps only an exact-identity provider leading its own group; pid reuse is never reapable", async () => {
    const home = await scratchHome();
    try {
      const lease = await handcraftedLease(home);
      const alive = await classifyLease(lease, fakeProbes({ pid: "live", group: "gone" }));
      expect(alive).toEqual({
        state: "hub-gone",
        provider: { state: "alive", reapable: true },
      });

      const notLeading = await handcraftedLease(home, { provider_pgid: 555555 });
      const groupDoubt = await classifyLease(notLeading, fakeProbes({ pid: "live", group: "gone" }));
      expect(
        groupDoubt.state === "hub-gone" &&
          groupDoubt.provider.state === "uncertain" &&
          groupDoubt.provider.reason,
      ).toContain("provably leads");

      // Same pid, different start identity: pid reuse — signalling it is unsafe.
      const reused = await classifyLease(lease, fakeProbes({ pid: "live", group: "gone", token: "someone-elses-start" }));
      expect(
        reused.state === "hub-gone" &&
          reused.provider.state === "uncertain" &&
          reused.provider.reason,
      ).toContain("reused");

      // An unreadable start identity is honest uncertainty, never a proof.
      const unreadable = await classifyLease(lease, fakeProbes({ pid: "live", group: "gone", token: null }));
      expect(unreadable.state === "hub-gone" && unreadable.provider.state).toBe("uncertain");
    } finally {
      await removeDirectory(home);
    }
  });

  it("honors a foreign host unconditionally and a live matching hub as untouchable", async () => {
    const home = await scratchHome();
    try {
      const foreign = await handcraftedLease(home, { hub_hostname: "not-this-host" });
      expect(
        await classifyLease(foreign, fakeProbes({ pid: "live", group: "alive" }), "this-host"),
      ).toEqual({ state: "foreign-host", owner_hostname: "not-this-host" });

      const liveHub = await handcraftedLease(home, { hub_pid: 1, hub_start_token: "fake-start-token" });
      expect(
        await classifyLease(liveHub, fakeProbes({ pid: "live", group: "gone" }), hostname(), process.pid),
      ).toEqual({ state: "hub-live" });

      // A live pid with a stale hub start token is pid reuse: bookkeeping
      // says the hub is gone, and the provider's own fate decides.
      const reusedHub = await handcraftedLease(home, { hub_pid: 1, hub_start_token: "stale-hub-token" });
      const gone = await classifyLease(reusedHub, fakeProbes({ pid: "live", group: "gone" }), hostname(), process.pid);
      expect(gone.state).toBe("hub-gone");
      expect(gone.state === "hub-gone" && gone.provider).toEqual({ state: "alive", reapable: true });
    } finally {
      await removeDirectory(home);
    }
  });
});
