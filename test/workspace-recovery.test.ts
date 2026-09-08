import { randomUUID } from "node:crypto";
import { mkdir, stat, writeFile } from "node:fs/promises";
import { hostname } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import {
  leasePath,
  resultPath,
  runtimeMirrorPath,
  worktreePath,
  workspaceRecordPath,
  writeJsonAtomic,
} from "../src/workspace/home.js";
import type { LeaseProbes, WorkspaceLeaseRecord } from "../src/workspace/index.js";
import { WORKSPACE_LEASE_SCHEMA, WORKSPACE_RUNTIME_SCHEMA_VERSION } from "../src/workspace/index.js";
import { workspaceRefFor } from "../src/workspace/records.js";
import { sessionRecord, textEvent } from "./kernel-fakes.js";
import { fakeProbes, makeFixture } from "./workspace-fakes.js";
import type { Fixture } from "./workspace-fakes.js";
import { resolveRef } from "./helpers.js";

/**
 * Recovery reconciles durable custody against what the process can prove.
 * It never deletes: every scenario re-asserts that worktrees, refs, and
 * results survived the pass.
 */

const running = { kind: "status", status: "running", note: null } as const;
const idle = { kind: "status", status: "idle", note: null } as const;

async function exists(path: string): Promise<boolean> {
  return (await stat(path).catch(() => null)) !== null;
}

function handcraftedLease(sessionId: string, overrides: Partial<WorkspaceLeaseRecord> = {}): WorkspaceLeaseRecord {
  return {
    schema: WORKSPACE_LEASE_SCHEMA,
    session_id: sessionId,
    provider: "fake",
    transport: "fake-rpc",
    provider_pid: 411111,
    provider_pgid: 411111,
    provider_start_token: "fake-start-token",
    hub_pid: process.pid,
    hub_hostname: hostname(),
    hub_start_token: "fake-start-token",
    created_at: "2026-09-07T00:00:00.000Z",
    ...overrides,
  };
}

/** Drive one delivered, published turn (file write so the chain advances). */
async function driveTurn(fx: Fixture, s: { id: string; workspace: { worktree_path: string }; transport: Fixture["factory"]["created"][number] }) {
  const turn = fx.lc.turn(fx.kernel, s.id, "prompt", "do work");
  await fx.fileInWorktree(s.workspace.worktree_path, "output.txt", `produced by ${s.id}\n`);
  s.transport.emit(running);
  s.transport.emit(textEvent("turn-1", "done", true));
  s.transport.emit(idle);
  return (await turn).result;
}

describe("workspace recovery — reconcile, never delete", () => {
  it("reconciles kernel-attached sessions as live with nothing to repair", async () => {
    const fx = await makeFixture();
    try {
      const s = await fx.openSession();
      const report = await fx.lc.recover(fx.kernel.attached());
      expect(report.live).toEqual([s.id]);
      expect(report.orphans).toEqual([]);
      expect(report.inconsistencies).toEqual([]);
      expect(report.reconciled).toEqual([]);
      expect(report.unclaimed).toEqual({ worktrees: [], leases: [], runtimes: [], unknown_segments: [] });
      expect(await exists(worktreePath(fx.home, s.id))).toBe(true);
    } finally {
      await fx.cleanup();
    }
  });

  it("closes a hub-gone orphan whose lease proves the provider dead, retaining everything", async () => {
    const fx = await makeFixture();
    try {
      const s = await fx.openSession();
      const published = await driveTurn(fx, s);
      // Late provider write the kernel never published: recovery's final
      // capture must land it on the lineage.
      await fx.fileInWorktree(s.workspace.worktree_path, "rescue.txt", "written after the last turn\n");

      const report = await fx.bareLifecycle().recover([]);
      const orphan = report.orphans.find((o) => o.session_id === s.id);
      expect(orphan).toBeDefined();
      expect(orphan!.classification).toBe("hub-gone");
      expect(orphan!.reapable).toBe(false);
      expect(orphan!.closed_by_recovery).toBe(true);
      expect(orphan!.detail).toContain("proven dead");
      expect(report.live).toEqual([]);
      expect(report.inconsistencies).toEqual([]);

      const inspected = await fx.lc.inspect(s.id);
      expect(inspected.workspace.custody).toBe("closed");
      expect(inspected.workspace.close_evidence).toContain("proven dead");
      expect(inspected.workspace.head_commit).not.toBe(published!.commit);
      expect(await resolveRef(fx.repo, workspaceRefFor(s.id))).toBe(inspected.workspace.head_commit);

      // Retention contract: worktree, ref, and every result stay on disk.
      expect(await exists(worktreePath(fx.home, s.id))).toBe(true);
      expect(await exists(resultPath(fx.home, s.id, 1))).toBe(true);

      // The runtime mirror is rewritten — orphaned, and honestly marked so.
      expect(inspected.runtime.state).toBe("present");
      if (inspected.runtime.state === "present") {
        expect(inspected.runtime.rewritten_by_recovery).toBe(true);
        expect(inspected.runtime.record.status).toBe("orphaned");
        expect(inspected.runtime.record.session_id).toBe(s.id);
      }
    } finally {
      await fx.cleanup();
    }
  });

  it("reports a lease whose hub process is still live as live, hands-off", async () => {
    const fx = await makeFixture();
    try {
      const s = await fx.openSession();
      await writeJsonAtomic(
        leasePath(fx.home, s.id),
        handcraftedLease(s.id, { hub_pid: 1 }),
      );

      const report = await fx
        .bareLifecycle({ probes: fakeProbes({ pid: "live", group: "alive", token: "fake-start-token" }) })
        .recover([]);
      expect(report.live).toEqual([s.id]);
      expect(report.orphans).toEqual([]);
      expect(report.inconsistencies).toEqual([]);
      expect(report.reconciled).toEqual([]);

      const inspected = await fx.lc.inspect(s.id);
      expect(inspected.workspace.custody).toBe("live");
      expect(await exists(leasePath(fx.home, s.id))).toBe(true);
      expect(await exists(worktreePath(fx.home, s.id))).toBe(true);
    } finally {
      await fx.cleanup();
    }
  });

  it("classifies a lease from another host as a foreign orphan, hands-off", async () => {
    const fx = await makeFixture();
    try {
      const s = await fx.openSession();
      await writeJsonAtomic(
        leasePath(fx.home, s.id),
        handcraftedLease(s.id, { hub_hostname: "elsewhere.invalid" }),
      );

      const report = await fx.bareLifecycle().recover([]);
      expect(report.live).toEqual([]);
      expect(report.orphans).toEqual([
        {
          session_id: s.id,
          classification: "foreign-host",
          reapable: false,
          closed_by_recovery: false,
          detail: expect.stringContaining("elsewhere.invalid"),
        },
      ]);
      expect((await fx.lc.inspect(s.id)).workspace.custody).toBe("live");
      expect(await exists(worktreePath(fx.home, s.id))).toBe(true);
    } finally {
      await fx.cleanup();
    }
  });

  it("keeps an orphan with a dead provider but a living process group unclosed and unreapable", async () => {
    const fx = await makeFixture();
    try {
      const s = await fx.openSession();
      // The fixture-recorded lease (spawn pid/pgid) + a probe script where
      // the provider pid is gone but its group still exists.
      const report = await fx
        .bareLifecycle({ probes: fakeProbes({ pid: "dead", group: "alive" }) })
        .recover([]);
      expect(report.orphans).toEqual([
        {
          session_id: s.id,
          classification: "hub-gone",
          reapable: false,
          closed_by_recovery: false,
          detail: expect.stringContaining("still exists"),
        },
      ]);
      expect(report.live).toEqual([]);

      const inspected = await fx.lc.inspect(s.id);
      expect(inspected.workspace.custody).toBe("live");
      expect(inspected.runtime.state).toBe("present");
      if (inspected.runtime.state === "present") {
        expect(inspected.runtime.rewritten_by_recovery).toBe(false);
      }
      expect(await exists(worktreePath(fx.home, s.id))).toBe(true);
    } finally {
      await fx.cleanup();
    }
  });

  it(
    "reaps only on provable ownership: reports reapable, admits survival, closes only when the group is gone",
    { timeout: 30_000 },
    async () => {
      const fx = await makeFixture({ probes: fakeProbes({ pid: "live", group: "alive", token: "fake-start-token" }) });
      try {
        const s = await fx.openSession();

        // Pass 1: exact-identity lease, provider alive → reapable=true,
        // but nothing is touched without an explicit reap request.
        const first = await fx.bareLifecycle().recover([]);
        expect(first.orphans).toEqual([
          {
            session_id: s.id,
            classification: "hub-gone",
            reapable: true,
            closed_by_recovery: false,
            detail: "provider alive with an exact-identity lease",
          },
        ]);
        expect((await fx.lc.inspect(s.id)).workspace.custody).toBe("live");

        // Pass 2: reap requested, but the group survives every signal.
        // Survival is reported honestly; closure is NOT assumed.
        const second = await fx
          .bareLifecycle({ probes: fakeProbes({ pid: "live", group: "alive" }) })
          .recover([], { reapOrphans: true });
        expect(second.orphans).toEqual([
          {
            session_id: s.id,
            classification: "hub-gone",
            reapable: true,
            closed_by_recovery: false,
            detail: "reap attempted: survived",
          },
        ]);
        expect((await fx.lc.inspect(s.id)).workspace.custody).toBe("live");

        // Pass 3: the group provably dies under the reaped signals (group
        // probe goes alive-then-gone), so closure becomes provable.
        let groupProbes = 0;
        const dyingGroupProbes: LeaseProbes = {
          probePid: () => "live",
          probeGroup: () => (groupProbes += 1) === 1 ? "alive" : "gone",
          startToken: async () => "fake-start-token",
          killGroup: () => true,
        };
        const third = await fx.bareLifecycle({ probes: dyingGroupProbes }).recover([], { reapOrphans: true });
        expect(third.orphans).toHaveLength(1);
        expect(third.orphans[0]!.closed_by_recovery).toBe(true);
        expect(third.orphans[0]!.detail).toContain("reap attempted: reaped");
        expect(third.orphans[0]!.detail).toContain("custody closed by recovery");
        expect((await fx.lc.inspect(s.id)).workspace.custody).toBe("closed");

        // Recovery closed custody; it deleted nothing.
        expect(await exists(worktreePath(fx.home, s.id))).toBe(true);
        expect(await exists(resultPath(fx.home, s.id, 1))).toBe(false); // no turn was ever run
      } finally {
        await fx.cleanup();
      }
    },
  );

  it("reports provisioned-never-started custody without touching it", async () => {
    const fx = await makeFixture();
    try {
      const id = randomUUID();
      await fx.lc.provision({ session_id: id, repository_cwd: fx.repo });

      const report = await fx.bareLifecycle().recover([]);
      expect(report.orphans).toEqual([
        {
          session_id: id,
          classification: "provisioned-never-started",
          reapable: false,
          closed_by_recovery: false,
          detail: expect.any(String),
        },
      ]);
      expect(report.live).toEqual([]);
      expect(report.inconsistencies).toEqual([]);
      expect((await fx.lc.inspect(id)).workspace.custody).toBe("live");
      expect(await exists(worktreePath(fx.home, id))).toBe(true);
      expect(await exists(leasePath(fx.home, id))).toBe(false);
      expect(await exists(runtimeMirrorPath(fx.home, id))).toBe(false);
    } finally {
      await fx.cleanup();
    }
  });

  it("reports unclaimed worktrees, leases, orphaned runtime mirrors, and junk segments — and keeps them all", async () => {
    const fx = await makeFixture();
    try {
      const strayWorktree = randomUUID();
      await mkdir(worktreePath(fx.home, strayWorktree), { recursive: true });
      await writeFile(join(worktreePath(fx.home, strayWorktree), "debris.txt"), "orphan debris\n");

      const strayLease = randomUUID();
      await writeJsonAtomic(leasePath(fx.home, strayLease), handcraftedLease(strayLease));

      // Custody directory whose record is gone but whose runtime mirror lives on.
      const ghostRuntime = randomUUID();
      await writeJsonAtomic(runtimeMirrorPath(fx.home, ghostRuntime), {
        schema: WORKSPACE_RUNTIME_SCHEMA_VERSION,
        mirrored_at: "2026-09-07T00:00:00.000Z",
        rewritten_by_recovery: false,
        record: sessionRecord({ session_id: ghostRuntime, status: "running" }),
      });

      // Namespace entry that names no session at all.
      const junkSegment = join(fx.home, "agent-hub", "worktrees", "scratch-note");
      await mkdir(junkSegment, { recursive: true });

      const report = await fx.bareLifecycle().recover([]);
      expect(report.unclaimed.worktrees).toEqual([worktreePath(fx.home, strayWorktree)]);
      expect(report.unclaimed.leases).toEqual([strayLease]);
      expect(report.unclaimed.runtimes).toEqual([ghostRuntime]);
      expect(report.unclaimed.unknown_segments).toEqual([junkSegment]);
      expect(report.inconsistencies).toEqual([
        { session_id: ghostRuntime, detail: "custody directory exists without a record" },
      ]);

      // Everything unclaimed is retained on disk.
      expect(await exists(join(worktreePath(fx.home, strayWorktree), "debris.txt"))).toBe(true);
      expect(await exists(leasePath(fx.home, strayLease))).toBe(true);
      expect(await exists(runtimeMirrorPath(fx.home, ghostRuntime))).toBe(true);
      expect(await exists(junkSegment)).toBe(true);
    } finally {
      await fx.cleanup();
    }
  });

  it("is non-destructive across a mixed home of live, orphaned, foreign, and unclaimed work", async () => {
    const fx = await makeFixture();
    try {
      const attached = await fx.openSession({ agent: "attached" });
      const deadOrphan = await fx.openSession({ agent: "dead-orphan" });
      await driveTurn(fx, deadOrphan);

      const foreignId = randomUUID();
      await fx.lc.provision({ session_id: foreignId, repository_cwd: fx.repo });
      await writeJsonAtomic(
        leasePath(fx.home, foreignId),
        handcraftedLease(foreignId, { hub_hostname: "elsewhere.invalid" }),
      );

      const provOnlyId = randomUUID();
      await fx.lc.provision({ session_id: provOnlyId, repository_cwd: fx.repo });

      const strayId = randomUUID();
      await mkdir(worktreePath(fx.home, strayId), { recursive: true });
      // A hub that restarted and brought up exactly one session: this is
      // the honest attached() input for that new process.
      const resumedAttached = fx.kernel.attached().filter((r) => r.session_id === attached.id);

      const report = await fx.bareLifecycle().recover(resumedAttached);
      const byId = new Map(report.orphans.map((o) => [o.session_id, o]));
      expect(report.live).toEqual([attached.id]);
      expect(byId.get(deadOrphan.id)!.closed_by_recovery).toBe(true);
      expect(byId.get(foreignId)!.classification).toBe("foreign-host");
      expect(byId.get(provOnlyId)!.classification).toBe("provisioned-never-started");
      expect(report.inconsistencies).toEqual([]);
      expect(report.unclaimed.worktrees).toEqual([worktreePath(fx.home, strayId)]);

      // Nothing deleted anywhere: worktrees, custody records, the published
      // ref and result of the closed orphan, and the unclaimed directory all live.
      for (const id of [attached.id, deadOrphan.id, foreignId, provOnlyId]) {
        expect(await exists(worktreePath(fx.home, id))).toBe(true);
        expect(await exists(workspaceRecordPath(fx.home, id))).toBe(true);
      }
      expect(await resolveRef(fx.repo, workspaceRefFor(deadOrphan.id))).not.toBeNull();
      expect(await exists(resultPath(fx.home, deadOrphan.id, 1))).toBe(true);
      expect(await exists(worktreePath(fx.home, strayId))).toBe(true);
    } finally {
      await fx.cleanup();
    }
  });
});
