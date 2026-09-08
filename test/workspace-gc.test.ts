import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, rm, stat, writeFile } from "node:fs/promises";
import { hostname, tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it, vi } from "vitest";

import { acquireRepositoryLock } from "../src/locks.js";
import {
  leasePath,
  readJsonFile,
  resultPath,
  runtimeMirrorPath,
  tombstonePath,
  worktreePath,
  workspaceRecordPath,
  writeJsonAtomic,
} from "../src/workspace/home.js";
import type { WorkspaceLeaseRecord } from "../src/workspace/index.js";
import { WORKSPACE_LEASE_SCHEMA, WORKSPACE_RUNTIME_SCHEMA_VERSION } from "../src/workspace/index.js";
import type { WorkspaceRecord } from "../src/workspace/records.js";
import { workspaceRefFor } from "../src/workspace/records.js";
import * as gitops from "../src/workspace/gitops.js";
import { sessionRecord, textEvent } from "./kernel-fakes.js";
import { fakeProbes, makeFixture } from "./workspace-fakes.js";
import type { Fixture } from "./workspace-fakes.js";
import { removeDirectory, resolveRef, runGit } from "./helpers.js";

/**
 * GC is the only deletion path. Each test flips exactly ONE precondition
 * off the eligible state and asserts the honest retain code; deletion is
 * asserted with every precondition on.
 */

const RETENTION_MS = 60_000;
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

/**
 * Closed + exactly handed-off fast path: a provisioned workspace with late
 * working state that the close capture advanced (head !== base, ref at
 * head, zero results). The caller decides when retention expires.
 */
async function decidedWorkspace(fx: Fixture): Promise<{ id: string; head: string }> {
  const id = randomUUID();
  await fx.lc.provision({ session_id: id, repository_cwd: fx.repo });
  await fx.fileInWorktree(worktreePath(fx.home, id), "leftover-work.txt", `late work for ${id}\n`);
  await fx.lc.finalizeClosure({ session_id: id, evidence: "test: start attempt abandoned" });
  const rec = (await fx.lc.inspect(id)).workspace;
  await fx.lc.handoff({
    session_id: id,
    decision: "accepted",
    result_seq: rec.last_result_seq,
    commit: rec.head_commit,
  });
  return { id, head: rec.head_commit };
}

/** Kernel-flow eligible state: one shipped turn, closed custody, exact handoff. */
async function shippedWorkspace(fx: Fixture): Promise<{ id: string; head: string; resultCommit: string }> {
  const s = await fx.openSession();
  const turn = fx.lc.turn(fx.kernel, s.id, "prompt", "ship the feature");
  await fx.fileInWorktree(s.workspace.worktree_path, "feature.txt", "shipped\n");
  s.transport.emit(running);
  s.transport.emit(textEvent("turn-1", "done", true));
  s.transport.emit(idle);
  const published = await turn;
  await fx.lc.closeSession(fx.kernel, s.id);
  const rec = (await fx.lc.inspect(s.id)).workspace;
  await fx.lc.handoff({
    session_id: s.id,
    decision: "accepted",
    result_seq: rec.last_result_seq,
    commit: rec.head_commit,
  });
  return { id: s.id, head: rec.head_commit, resultCommit: published.result!.commit };
}

describe("workspace GC — the only deletion path", () => {
  it("deletes a decided, expired, consistent workspace down to its last durable trace", async () => {
    const fx = await makeFixture({ retentionMs: RETENTION_MS });
    try {
      const s1 = await fx.openSession();
      const turn = fx.lc.turn(fx.kernel, s1.id, "prompt", "write a file");
      await fx.fileInWorktree(s1.workspace.worktree_path, "out.txt", "hello\n");
      s1.transport.emit(running);
      s1.transport.emit(textEvent("turn-1", "done", true));
      s1.transport.emit(idle);
      await turn;
      await fx.lc.closeSession(fx.kernel, s1.id);
      const rec1 = (await fx.lc.inspect(s1.id)).workspace;
      await fx.lc.handoff({
        session_id: s1.id,
        decision: "accepted",
        result_seq: rec1.last_result_seq,
        commit: rec1.head_commit,
      });

      // A second, closed-but-never-handed-off workspace must survive the pass.
      const s2 = await fx.openSession();
      await fx.lc.closeSession(fx.kernel, s2.id);

      fx.clock.advance(RETENTION_MS + 1);
      const report = await fx.lc.gc(fx.kernel.attached());

      expect(report.deleted).toEqual([
        {
          session_id: s1.id,
          last_result_seq: rec1.last_result_seq,
          head_commit: rec1.head_commit,
          decision: "accepted",
          continued: false,
        },
      ]);
      expect(await exists(worktreePath(fx.home, s1.id))).toBe(false);
      expect(await exists(join(fx.home, "agent-hub", "workspaces", s1.id))).toBe(false);
      expect(await exists(leasePath(fx.home, s1.id))).toBe(false);
      expect(await resolveRef(fx.repo, workspaceRefFor(s1.id))).toBeNull();

      const tomb = await readJsonFile(tombstonePath(fx.home, s1.id));
      expect(tomb).toMatchObject({
        schema: "agent-hub-workspace-deleted/v1",
        session_id: s1.id,
        hub_home: fx.home,
        head_commit: rec1.head_commit,
        last_result_seq: rec1.last_result_seq,
        decision: "accepted",
      });

      const worktreeList = await runGit(fx.repo, ["worktree", "list", "--porcelain"]);
      expect(worktreeList).not.toContain(worktreePath(fx.home, s1.id));
      expect(worktreeList).toContain(worktreePath(fx.home, s2.id));

      // The untouched neighbour is retained by its own failed precondition.
      expect(report.retained).toEqual([
        { session_id: s2.id, code: "handoff-undecided", detail: expect.any(String) },
      ]);
      expect(await exists(worktreePath(fx.home, s2.id))).toBe(true);
    } finally {
      await fx.cleanup();
    }
  });

  it("retains a closed workspace that was never handed off (results survive takeover)", async () => {
    const fx = await makeFixture({ retentionMs: RETENTION_MS });
    try {
      const id = randomUUID();
      await fx.lc.provision({ session_id: id, repository_cwd: fx.repo });
      await fx.lc.finalizeClosure({ session_id: id, evidence: "test: closed, takeover pending" });
      fx.clock.advance(RETENTION_MS + 1);

      const report = await fx.lc.gc();
      expect(report.deleted).toEqual([]);
      expect(report.retained).toEqual([
        { session_id: id, code: "handoff-undecided", detail: expect.any(String) },
      ]);
      expect(await exists(worktreePath(fx.home, id))).toBe(true);
    } finally {
      await fx.cleanup();
    }
  });

  it("arms retention at the decision: one ms before expiry retained, one ms after deleted", async () => {
    const fx = await makeFixture({ retentionMs: RETENTION_MS });
    try {
      const { id, head } = await decidedWorkspace(fx);

      fx.clock.advance(RETENTION_MS - 1);
      const early = await fx.lc.gc();
      expect(early.deleted).toEqual([]);
      expect(early.retained).toEqual([
        { session_id: id, code: "retention-active", detail: expect.any(String) },
      ]);
      expect(await exists(worktreePath(fx.home, id))).toBe(true);

      fx.clock.advance(2);
      const late = await fx.lc.gc();
      expect(late.deleted).toEqual([
        { session_id: id, last_result_seq: 0, head_commit: head, decision: "accepted", continued: false },
      ]);
      expect(await exists(worktreePath(fx.home, id))).toBe(false);
    } finally {
      await fx.cleanup();
    }
  });

  it("retains a workspace the kernel still attaches", async () => {
    const fx = await makeFixture({ retentionMs: RETENTION_MS });
    try {
      const s = await fx.openSession();
      const report = await fx.lc.gc(fx.kernel.attached());
      expect(report.deleted).toEqual([]);
      expect(report.retained).toEqual([
        { session_id: s.id, code: "runtime-attached", detail: expect.any(String) },
      ]);
      expect(await exists(worktreePath(fx.home, s.id))).toBe(true);
    } finally {
      await fx.cleanup();
    }
  });

  it("retains a workspace whose runtime mirror still reports a live status", async () => {
    const fx = await makeFixture({ retentionMs: RETENTION_MS });
    try {
      const { id } = await decidedWorkspace(fx);
      // Simulate a kernel mirror that still shows the session running.
      await writeJsonAtomic(runtimeMirrorPath(fx.home, id), {
        schema: WORKSPACE_RUNTIME_SCHEMA_VERSION,
        mirrored_at: "2026-09-07T00:00:00.000Z",
        rewritten_by_recovery: false,
        record: sessionRecord({ session_id: id, status: "running" }),
      });

      const report = await fx.lc.gc();
      expect(report.deleted).toEqual([]);
      expect(report.retained).toEqual([
        { session_id: id, code: "runtime-live", detail: expect.any(String) },
      ]);
      expect(await exists(worktreePath(fx.home, id))).toBe(true);
    } finally {
      await fx.cleanup();
    }
  });

  it("retains when the leased provider process is still alive (lease-live)", async () => {
    const fx = await makeFixture({ retentionMs: RETENTION_MS });
    try {
      const { id } = await decidedWorkspace(fx);
      fx.clock.advance(RETENTION_MS + 1);
      await writeJsonAtomic(leasePath(fx.home, id), handcraftedLease(id));
      const report = await fx
        .bareLifecycle({ probes: fakeProbes({ pid: "live", group: "alive" }) })
        .gc();
      expect(report.deleted).toEqual([]);
      expect(report.retained).toEqual([
        { session_id: id, code: "lease-live", detail: expect.any(String) },
      ]);
      expect(await exists(leasePath(fx.home, id))).toBe(true);
      expect(await exists(worktreePath(fx.home, id))).toBe(true);
    } finally {
      await fx.cleanup();
    }
  });

  it("retains when provider fate cannot be proven (lease-uncertain)", async () => {
    const fx = await makeFixture({ retentionMs: RETENTION_MS });
    try {
      const { id } = await decidedWorkspace(fx);
      // Provider pid dead but its process group still exists: helpers may
      // still be mutating the worktree, so nothing may be assumed dead.
      await writeJsonAtomic(leasePath(fx.home, id), handcraftedLease(id));
      fx.clock.advance(RETENTION_MS + 1);
      const report = await fx
        .bareLifecycle({ probes: fakeProbes({ pid: "dead", group: "alive" }) })
        .gc();
      expect(report.deleted).toEqual([]);
      expect(report.retained).toEqual([
        { session_id: id, code: "lease-uncertain", detail: expect.stringContaining("still exists") },
      ]);
      expect(await exists(worktreePath(fx.home, id))).toBe(true);
    } finally {
      await fx.cleanup();
    }
  });

  it("retains a lease owned by another host, hands-off (lease-foreign)", async () => {
    const fx = await makeFixture({ retentionMs: RETENTION_MS });
    try {
      const { id } = await decidedWorkspace(fx);
      await writeJsonAtomic(
        leasePath(fx.home, id),
        handcraftedLease(id, { hub_hostname: "elsewhere.invalid" }),
      );
      fx.clock.advance(RETENTION_MS + 1);
      const report = await fx.lc.gc();
      expect(report.deleted).toEqual([]);
      expect(report.retained).toEqual([
        { session_id: id, code: "lease-foreign", detail: expect.stringContaining("elsewhere.invalid") },
      ]);
      expect(await exists(worktreePath(fx.home, id))).toBe(true);
    } finally {
      await fx.cleanup();
    }
  });

  it("retains when another live hub process still owns the workspace (hub-live)", async () => {
    const fx = await makeFixture({ retentionMs: RETENTION_MS });
    try {
      const { id } = await decidedWorkspace(fx);
      await writeJsonAtomic(
        leasePath(fx.home, id),
        handcraftedLease(id, { hub_pid: 1 }),
      );
      fx.clock.advance(RETENTION_MS + 1);
      const report = await fx
        .bareLifecycle({ probes: fakeProbes({ pid: "live", group: "alive", token: "fake-start-token" }) })
        .gc();
      expect(report.deleted).toEqual([]);
      expect(report.retained).toEqual([
        { session_id: id, code: "hub-live", detail: expect.any(String) },
      ]);
      expect(await exists(worktreePath(fx.home, id))).toBe(true);
    } finally {
      await fx.cleanup();
    }
  });

  it("retains a workspace whose custody lock is held elsewhere (locked-by-peer)", async () => {
    const fx = await makeFixture({ retentionMs: RETENTION_MS });
    try {
      const { id } = await decidedWorkspace(fx);
      const lock = await acquireRepositoryLock({ commonDir: fx.home, name: `ws-${id}` });
      try {
        const report = await fx.lc.gc();
        expect(report.deleted).toEqual([]);
        expect(report.retained).toEqual([
          { session_id: id, code: "locked-by-peer", detail: expect.any(String) },
        ]);
        expect(await exists(worktreePath(fx.home, id))).toBe(true);
        expect((await fx.lc.list()).map((r) => r.session_id)).toContain(id);
      } finally {
        await lock.release();
      }
    } finally {
      await fx.cleanup();
    }
  });

  it("skips the whole pass when the admin lock is held elsewhere (admin-locked)", async () => {
    const fx = await makeFixture({ retentionMs: RETENTION_MS });
    try {
      const { id } = await decidedWorkspace(fx);
      const lock = await acquireRepositoryLock({ commonDir: fx.home, name: "workspace-admin" });
      try {
        const report = await fx.lc.gc();
        expect(report.deleted).toEqual([]);
        expect(report.retained).toEqual([{ session_id: "*", code: "admin-locked", detail: expect.any(String) }]);
        expect(await exists(worktreePath(fx.home, id))).toBe(true);
      } finally {
        await lock.release();
      }
    } finally {
      await fx.cleanup();
    }
  });

  it("retains when an out-of-band ref sits where the record expects none (ref-diverged)", async () => {
    const fx = await makeFixture({ retentionMs: RETENTION_MS });
    try {
      const id = randomUUID();
      await fx.lc.provision({ session_id: id, repository_cwd: fx.repo });
      const rec0 = (await fx.lc.inspect(id)).workspace;
      const nowMs = fx.clock.currentMs();
      const decided: WorkspaceRecord = {
        ...structuredClone(rec0),
        custody: "closed",
        runtime_status: "closed",
        closed_at: new Date(nowMs).toISOString(),
        close_evidence: "test: handcrafted closure",
        handoff: {
          decision: "discarded",
          result_seq: 0,
          commit: rec0.head_commit,
          decided_at: new Date(nowMs).toISOString(),
          consumer: null,
        },
        retention_until: new Date(nowMs + RETENTION_MS).toISOString(),
        revision: rec0.revision + 1,
        updated_at: new Date(nowMs).toISOString(),
      };
      // The record never advanced the chain, so GC expects the ref to be
      // absent; someone else created it pointing at the head anyway.
      await writeJsonAtomic(workspaceRecordPath(fx.home, id), decided);
      await runGit(fx.repo, ["update-ref", workspaceRefFor(id), rec0.head_commit]);
      fx.clock.advance(RETENTION_MS + 1);

      const report = await fx.lc.gc();
      expect(report.deleted).toEqual([]);
      expect(report.retained).toEqual([
        { session_id: id, code: "ref-diverged", detail: expect.any(String) },
      ]);
      // GC touched neither the ref nor the worktree.
      expect(await resolveRef(fx.repo, workspaceRefFor(id))).toBe(rec0.head_commit);
      expect(await exists(worktreePath(fx.home, id))).toBe(true);
    } finally {
      await fx.cleanup();
    }
  });

  it("retains when a published result file is missing (results-missing)", async () => {
    const fx = await makeFixture({ retentionMs: RETENTION_MS });
    try {
      const { id } = await shippedWorkspace(fx);
      await rm(resultPath(fx.home, id, 1), { force: true });
      fx.clock.advance(RETENTION_MS + 1);

      const report = await fx.lc.gc();
      expect(report.deleted).toEqual([]);
      expect(report.retained).toEqual([
        { session_id: id, code: "results-missing", detail: expect.stringContaining("result 1 of 1") },
      ]);
      expect(await exists(worktreePath(fx.home, id))).toBe(true);
    } finally {
      await fx.cleanup();
    }
  });

  it("retains when a result names a commit the repository no longer proves (commit-unverifiable)", async () => {
    const fx = await makeFixture({ retentionMs: RETENTION_MS });
    try {
      const { id } = await shippedWorkspace(fx);
      const stored = (await readJsonFile(resultPath(fx.home, id, 1))) as Record<string, unknown>;
      // Shape-valid (40-hex) commit that exists nowhere in the object store.
      await writeJsonAtomic(resultPath(fx.home, id, 1), {
        ...stored,
        commit: "ab".repeat(20),
      });
      fx.clock.advance(RETENTION_MS + 1);

      const report = await fx.lc.gc();
      expect(report.deleted).toEqual([]);
      expect(report.retained).toEqual([
        { session_id: id, code: "commit-unverifiable", detail: expect.stringContaining("ab".repeat(20)) },
      ]);
      expect(await exists(worktreePath(fx.home, id))).toBe(true);
    } finally {
      await fx.cleanup();
    }
  });

  it("retains a record whose worktree path is outside hub custody (worktree-not-custodial)", async () => {
    const fx = await makeFixture({ retentionMs: RETENTION_MS });
    try {
      const { id } = await decidedWorkspace(fx);
      const foreignDir = await mkdtemp(join(tmpdir(), "agent-hub-foreign-wt-"));
      try {
        const rec = (await fx.lc.inspect(id)).workspace;
        await writeJsonAtomic(workspaceRecordPath(fx.home, id), {
          ...structuredClone(rec),
          worktree_path: foreignDir,
        });
        fx.clock.advance(RETENTION_MS + 1);

        const report = await fx.lc.gc();
        expect(report.deleted).toEqual([]);
        expect(report.retained).toEqual([
          { session_id: id, code: "worktree-not-custodial", detail: expect.any(String) },
        ]);
        // Neither the real custody worktree nor the foreign directory was touched.
        expect(await exists(worktreePath(fx.home, id))).toBe(true);
        expect(await exists(foreignDir)).toBe(true);
      } finally {
        await removeDirectory(foreignDir);
      }
    } finally {
      await fx.cleanup();
    }
  });

  it("retains when the custody path is no longer this repository's worktree (worktree-foreign)", async () => {
    const fx = await makeFixture({ retentionMs: RETENTION_MS });
    try {
      const { id, head } = await decidedWorkspace(fx);
      const custodyPath = worktreePath(fx.home, id);
      await removeDirectory(custodyPath);
      await mkdir(custodyPath, { recursive: true });
      await writeFile(join(custodyPath, "squatter.txt"), "not a worktree anymore\n");

      fx.clock.advance(RETENTION_MS + 1);
      const report = await fx.lc.gc();
      expect(report.deleted).toEqual([]);
      expect(report.retained).toEqual([
        { session_id: id, code: "worktree-foreign", detail: expect.any(String) },
      ]);
      // The squatter files and the custody ref survive untouched.
      expect(await exists(join(custodyPath, "squatter.txt"))).toBe(true);
      expect(await resolveRef(fx.repo, workspaceRefFor(id))).toBe(head);
    } finally {
      await fx.cleanup();
    }
  });

  it("completes a deletion interrupted mid-way and finishes tombstoned custody leftovers", async () => {
    const fx = await makeFixture({ retentionMs: RETENTION_MS });
    try {
      const { id, head } = await shippedWorkspace(fx);

      // Simulate the crash AFTER the tombstone landed and the ref CAS-delete
      // ran, but before the worktree/custody-tree removal.
      await writeJsonAtomic(tombstonePath(fx.home, id), {
        schema: "agent-hub-workspace-deleted/v1",
        session_id: id,
        hub_home: fx.home,
        repository_cwd: fx.repo,
        worktree_path: worktreePath(fx.home, id),
        head_commit: head,
        last_result_seq: 1,
        decision: "accepted",
        deleted_at: "2026-09-07T00:00:00.000Z",
      });
      await runGit(fx.repo, ["update-ref", "-d", workspaceRefFor(id)]);

      // A tombstoned unclaimed worktree dir gets finished; an unclaimed dir
      // with no tombstone is retained and reported; junk segments are kept
      // and named.
      const tombstonedStray = randomUUID();
      await mkdir(worktreePath(fx.home, tombstonedStray), { recursive: true });
      await writeFile(join(worktreePath(fx.home, tombstonedStray), "debris.txt"), "x\n");
      await writeJsonAtomic(tombstonePath(fx.home, tombstonedStray), {
        schema: "agent-hub-workspace-deleted/v1",
        session_id: tombstonedStray,
        hub_home: fx.home,
        repository_cwd: fx.repo,
        worktree_path: worktreePath(fx.home, tombstonedStray),
        head_commit: head,
        last_result_seq: 0,
        decision: "discarded",
        deleted_at: "2026-09-07T00:00:00.000Z",
      });
      const ownerlessStray = randomUUID();
      await mkdir(worktreePath(fx.home, ownerlessStray), { recursive: true });
      const junkSegment = join(fx.home, "agent-hub", "worktrees", "scratch-note");
      await mkdir(junkSegment, { recursive: true });

      fx.clock.advance(RETENTION_MS + 1);
      const report = await fx.lc.gc();

      expect(report.deleted).toEqual([
        { session_id: id, last_result_seq: 1, head_commit: head, decision: "accepted", continued: true },
      ]);
      expect(await exists(worktreePath(fx.home, id))).toBe(false);
      expect(await exists(join(fx.home, "agent-hub", "workspaces", id))).toBe(false);
      expect(await exists(leasePath(fx.home, id))).toBe(false);

      expect(await exists(worktreePath(fx.home, tombstonedStray))).toBe(false);
      expect(report.unclaimed.worktrees).toEqual([worktreePath(fx.home, ownerlessStray)]);
      expect(await exists(worktreePath(fx.home, ownerlessStray))).toBe(true);
      expect(report.unclaimed.unknown_segments).toEqual([junkSegment]);
      expect(await exists(junkSegment)).toBe(true);
      expect(report.unclaimed.cleanup_errors).toEqual([]);
      expect(report.pruned_repositories).toHaveLength(1);
    } finally {
      await fx.cleanup();
    }
  });

  it("retains custody when tombstone continuation cannot remove the worktree", async () => {
    const fx = await makeFixture({ retentionMs: RETENTION_MS });
    const removal = vi.spyOn(gitops, "removeWorktree").mockResolvedValue({
      removed: false,
      reason: "simulated removal failure",
    });
    try {
      const { id, head } = await shippedWorkspace(fx);
      await writeJsonAtomic(tombstonePath(fx.home, id), {
        schema: "agent-hub-workspace-deleted/v1",
        session_id: id,
        hub_home: fx.home,
        repository_cwd: fx.repo,
        worktree_path: worktreePath(fx.home, id),
        head_commit: head,
        last_result_seq: 1,
        decision: "accepted",
        deleted_at: "2026-09-07T00:00:00.000Z",
      });
      await runGit(fx.repo, ["update-ref", "-d", workspaceRefFor(id)]);

      fx.clock.advance(RETENTION_MS + 1);
      const report = await fx.lc.gc();

      expect(removal).toHaveBeenCalled();
      expect(report.deleted).toEqual([]);
      expect(report.retained).toEqual([
        { session_id: id, code: "worktree-remove-failed", detail: "simulated removal failure" },
      ]);
      expect(await exists(worktreePath(fx.home, id))).toBe(true);
      expect(await exists(workspaceRecordPath(fx.home, id))).toBe(true);
      expect(await exists(leasePath(fx.home, id))).toBe(true);
    } finally {
      removal.mockRestore();
      await fx.cleanup();
    }
  });

  it("a second gc over a deleted workspace is a clean no-op", async () => {
    const fx = await makeFixture({ retentionMs: RETENTION_MS });
    try {
      const { id, head } = await decidedWorkspace(fx);
      fx.clock.advance(RETENTION_MS + 1);
      const first = await fx.lc.gc();
      expect(first.deleted).toEqual([
        { session_id: id, last_result_seq: 0, head_commit: head, decision: "accepted", continued: false },
      ]);

      const second = await fx.lc.gc();
      expect(second).toEqual({
        deleted: [],
        retained: [],
        unclaimed: { worktrees: [], leases: [], unknown_segments: [], cleanup_errors: [] },
        pruned_repositories: [],
      });
    } finally {
      await fx.cleanup();
    }
  });

  // KNOWN SRC BUG (reported, left failing on purpose):
  // finalizeClosure on a workspace whose close capture did NOT advance runs
  // the ref CAS anyway and CREATES the custody ref at the unchanged head
  // (base). gcOne's expectedRef is `null` for head===base && seq===0, so the
  // workspace is retained as "ref-diverged" forever — with a detail message
  // that prints the SAME sha on both sides of the complaint. A clean-closed,
  // decided, expired, fully consistent workspace is designed to be reclaimable.
  it("reclaims a cleanly closed, zero-result workspace once decided and expired", async () => {
    const fx = await makeFixture({ retentionMs: RETENTION_MS });
    try {
      const id = randomUUID();
      await fx.lc.provision({ session_id: id, repository_cwd: fx.repo });
      await fx.lc.finalizeClosure({ session_id: id, evidence: "test: abandoned start, nothing written" });
      const rec = (await fx.lc.inspect(id)).workspace;
      await fx.lc.handoff({
        session_id: id,
        decision: "accepted",
        result_seq: rec.last_result_seq,
        commit: rec.head_commit,
      });
      fx.clock.advance(RETENTION_MS + 1);

      const report = await fx.lc.gc();
      expect(report.retained).toEqual([]);
      expect(report.deleted).toEqual([
        { session_id: id, last_result_seq: 0, head_commit: rec.head_commit, decision: "accepted", continued: false },
      ]);
      expect(await exists(worktreePath(fx.home, id))).toBe(false);
    } finally {
      await fx.cleanup();
    }
  });
});
