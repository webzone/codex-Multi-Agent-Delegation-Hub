import { readdir, stat } from "node:fs/promises";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { kernelError } from "../src/kernel/contracts.js";
import type { SessionRecord } from "../src/kernel/contracts.js";
import { workspaceRefFor } from "../src/workspace/records.js";
import { randomUUID } from "node:crypto";
import { writeJsonAtomic, workspacePendingPath } from "../src/workspace/home.js";
import {
  CRASH,
  expectCode,
  fakeTurn,
  makeFixture,
} from "./workspace-fakes.js";
import { runGit, resolveRef } from "./helpers.js";

const running = { kind: "status", status: "running", note: null } as const;
const idle = { kind: "status", status: "idle", note: null } as const;
const COMMIT = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;

async function exists(path: string): Promise<boolean> {
  return (await stat(path).catch(() => null)) !== null;
}

describe("workspace provisioning", () => {
  it("gives every agent its own isolated worktree under the hub home", async () => {
    const fx = await makeFixture();
    try {
      const a = await fx.openSession({ agent: "a" });
      const b = await fx.openSession({ agent: "b" });
      expect(a.workspace.worktree_path).not.toBe(b.workspace.worktree_path);
      expect(a.workspace.worktree_path.startsWith(join(fx.home, "agent-hub", "worktrees", a.id))).toBe(true);
      const listed = await runGit(fx.repo, ["worktree", "list", "--porcelain"]);
      expect(listed).toContain(a.workspace.worktree_path);
      expect(listed).toContain(b.workspace.worktree_path);
      expect(a.workspace.agent).toBe("a");
      const base = (await runGit(fx.repo, ["rev-parse", "HEAD"])).trim();
      expect(a.workspace.base_commit).toBe(base);
      expect(a.workspace.head_commit).toBe(base);
      expect(a.workspace.last_result_seq).toBe(0);
      expect(a.workspace.revision).toBe(0);
    } finally {
      await fx.cleanup();
    }
  });

  it("refuses duplicate custody and unverifiable bases", async () => {
    const fx = await makeFixture();
    try {
      const a = await fx.openSession();
      await expectCode(
        fx.lc.provision({ session_id: a.id, repository_cwd: fx.repo }),
        "WORKSPACE_EXISTS",
      );
      await expectCode(
        fx.lc.provision({ session_id: randomUUID(), repository_cwd: fx.repo, base: "0".repeat(40) }),
        "WORKSPACE_BASE_UNVERIFIABLE",
      );
      await expectCode(
        fx.lc.provision({ session_id: "not-a-uuid", repository_cwd: fx.repo }),
        "WORKSPACE_ID_INVALID",
      );
    } finally {
      await fx.cleanup();
    }
  });
});

describe("result publication identity", () => {
  it("publishes exact identity for every terminal turn, changed tree or not", async () => {
    const fx = await makeFixture();
    try {
      const s = await fx.openSession();
      const turn1 = fx.lc.turn(fx.kernel, s.id, "prompt", "work");
      await fx.fileInWorktree(s.workspace.worktree_path, "feature.txt", "content\n");
      s.transport.emit(running);
      s.transport.emit({ kind: "text", role: "assistant", stream_id: "m1", text: { text: "done", truncated: false }, final: true });
      s.transport.emit(idle);
      const pub1 = await turn1;
      expect(pub1.result).not.toBeNull();
      expect(pub1.result!.seq).toBe(1);
      expect(pub1.result!.tree_changed).toBe(true);
      expect(pub1.result!.commit).toMatch(COMMIT);
      expect(pub1.result!.commit).not.toBe(s.workspace.base_commit);
      expect(pub1.result!.ref).toBe(workspaceRefFor(s.id));
      const refAfter1 = await resolveRef(fx.repo, pub1.result!.ref);
      expect(refAfter1).toBe(pub1.result!.commit);
      const recAfter1 = (await fx.lc.inspect(s.id)).workspace;
      expect(recAfter1.head_commit).toBe(pub1.result!.commit);
      expect(recAfter1.last_result_seq).toBe(1);

      // Second turn touches nothing: the result record still exists and
      // names the SAME commit identity with tree_changed false.
      const turn2 = fx.lc.turn(fx.kernel, s.id, "follow_up", "nothing to do");
      s.transport.emit(running);
      s.transport.emit({ kind: "text", role: "assistant", stream_id: "m2", text: { text: "nope", truncated: false }, final: true });
      s.transport.emit(idle);
      const pub2 = await turn2;
      expect(pub2.result!.seq).toBe(2);
      expect(pub2.result!.tree_changed).toBe(false);
      expect(pub2.result!.commit).toBe(pub1.result!.commit);
      expect(pub2.result!.tree).toBe(pub1.result!.tree);
      expect(await resolveRef(fx.repo, pub2.result!.ref)).toBe(pub1.result!.commit);

      // Durable artifacts under AGENT_HUB_HOME.
      const resultsDir = join(fx.home, "agent-hub", "workspaces", s.id, "results");
      expect((await readdir(resultsDir)).sort()).toEqual(["1.json", "2.json"]);
      const stored = await fx.lc.result(s.id, 2);
      expect(stored.command_id).toBe(pub2.turn.command_id);
      expect(stored.outcome).toBe("succeeded");
      expect(pub2.turn.final_text).toEqual({ text: "nope", truncated: false });
      expect(stored.final_text).toBeNull();
    } finally {
      await fx.cleanup();
    }
  });

  it("publishes failed and cancelled turns with their honest outcome", async () => {
    const fx = await makeFixture();
    try {
      const s = await fx.openSession();
      await fx.fileInWorktree(s.workspace.worktree_path, "partial.txt", "half a job\n");
      const failed = await fx.lc.publishTurnResult(
        s.id,
        fakeTurn(s.id, {
          outcome: "failed",
          error: kernelError("PROVIDER_DIED", "the provider died mid-turn", "provider", false, "fake"),
        }),
      );
      expect(failed.result!.outcome).toBe("failed");
      expect(failed.result!.error?.code).toBe("PROVIDER_DIED");
      expect(failed.result!.tree_changed).toBe(true);

      const cancelled = await fx.lc.publishTurnResult(
        s.id,
        fakeTurn(s.id, { kind: "follow_up", outcome: "cancelled" }),
      );
      expect(cancelled.result!.seq).toBe(2);
      expect(cancelled.result!.outcome).toBe("cancelled");
      // No file churn between the two publishes: seq 2 names the same commit.
      expect(cancelled.result!.commit).toBe(failed.result!.commit);
      expect(cancelled.result!.tree_changed).toBe(false);
    } finally {
      await fx.cleanup();
    }
  });

  it("never publishes commands that were not delivered turns", async () => {
    const fx = await makeFixture();
    try {
      const s = await fx.openSession();
      const refused = await fx.lc.publishTurnResult(
        s.id,
        fakeTurn(s.id, { outcome: "unsupported" }),
      );
      expect(refused.result).toBeNull();
      expect(refused.publish_skipped_reason).toContain("never delivered");
      const notATurn = await fx.lc.publishTurnResult(s.id, fakeTurn(s.id, { kind: "steer" }));
      expect(notATurn.result).toBeNull();
      const rec = (await fx.lc.inspect(s.id)).workspace;
      expect(rec.last_result_seq).toBe(0);
      expect(await exists(join(fx.home, "agent-hub", "workspaces", s.id, "results"))).toBe(false);
    } finally {
      await fx.cleanup();
    }
  });

  it("refuses to publish onto closed custody", async () => {
    const fx = await makeFixture();
    try {
      const s = await fx.openSession();
      await fx.lc.finalizeClosure({ session_id: s.id, evidence: "test closure" });
      await expectCode(
        fx.lc.publishTurnResult(s.id, fakeTurn(s.id)),
        "WORKSPACE_CLOSED",
      );
    } finally {
      await fx.cleanup();
    }
  });
});

describe("publication crash safety (sidecar → CAS → state)", () => {
  it("abandons a crash that happened before the ref moved; the turn can republish", async () => {
    const fx = await makeFixture({ failOncePhase: "captured-written" });
    try {
      const s = await fx.openSession();
      const turn = fx.lc.turn(fx.kernel, s.id, "prompt", "work");
      await fx.fileInWorktree(s.workspace.worktree_path, "x.txt", "x\n");
      s.transport.emit(running);
      s.transport.emit({ kind: "text", role: "assistant", stream_id: "m", text: { text: "t", truncated: false }, final: true });
      s.transport.emit(idle);
      await expectCode(turn, CRASH);
      expect(await exists(join(fx.home, "agent-hub", "workspaces", s.id, "record.pending.json"))).toBe(true);

      const recovered = await fx.lc.recover(fx.kernel.attached());
      expect(recovered.reconciled).toHaveLength(1);
      expect(recovered.reconciled[0]!.outcome).toBe("abandoned");
      const rec = (await fx.lc.inspect(s.id)).workspace;
      expect(rec.last_result_seq).toBe(0);
      expect(rec.head_commit).toBe(rec.base_commit);
      expect(await resolveRef(fx.repo, workspaceRefFor(s.id))).toBeNull();

      // Republishing the same terminal turn now lands cleanly.
      const again = await fx.lc.publishTurnResult(s.id, fakeTurn(s.id));
      expect(again.result!.seq).toBe(1);
      expect(again.result!.tree_changed).toBe(true);
      expect(await resolveRef(fx.repo, workspaceRefFor(s.id))).toBe(again.result!.commit);
    } finally {
      await fx.cleanup();
    }
  });

  it("lands a crash that happened after the ref moved; nothing is lost", async () => {
    const fx = await makeFixture({ failOncePhase: "ref-updated" });
    try {
      const s = await fx.openSession();
      const turn = fx.lc.turn(fx.kernel, s.id, "prompt", "work");
      await fx.fileInWorktree(s.workspace.worktree_path, "y.txt", "y\n");
      s.transport.emit(running);
      s.transport.emit({ kind: "text", role: "assistant", stream_id: "m", text: { text: "t", truncated: false }, final: true });
      s.transport.emit(idle);
      await expectCode(turn, CRASH);
      // Ref moved, record and result file not yet written: the sidecar holds
      // everything needed to complete the transition.
      const movedRef = await resolveRef(fx.repo, workspaceRefFor(s.id));
      expect(movedRef).toMatch(COMMIT);

      // A FRESH hub process (same home) lands it.
      const revived = fx.bareLifecycle();
      const recovered = await revived.recover([]);
      expect(recovered.reconciled.some((r) => r.outcome === "landed")).toBe(true);
      const rec = (await revived.inspect(s.id)).workspace;
      expect(rec.last_result_seq).toBe(1);
      expect(rec.head_commit).toBe(movedRef);
      expect(await exists(join(fx.home, "agent-hub", "workspaces", s.id, "results", "1.json"))).toBe(true);
      expect(await exists(join(fx.home, "agent-hub", "workspaces", s.id, "record.pending.json"))).toBe(false);
    } finally {
      await fx.cleanup();
    }
  });

  it("refuses an unverifiable sidecar instead of guessing", async () => {
    const fx = await makeFixture();
    try {
      const s = await fx.openSession();
      await writeJsonAtomic(workspacePendingPath(fx.home, s.id), { schema: "bogus" });
      const recovered = await fx.lc.recover([]);
      expect(recovered.inconsistencies.some((i) => i.detail.includes("corrupt or invalid"))).toBe(true);
      // Publishing cannot proceed past an unverifiable pending transaction.
      await expectCode(fx.lc.publishTurnResult(s.id, fakeTurn(s.id)), "WORKSPACE_STATE_INCONSISTENT");
      // The bogus sidecar is retained for audit, never silently removed.
      expect(await exists(workspacePendingPath(fx.home, s.id))).toBe(true);
    } finally {
      await fx.cleanup();
    }
  });
});

describe("closure, handoff, and retention", () => {
  it("close captures late writes and retains every artifact", async () => {
    const fx = await makeFixture();
    try {
      const s = await fx.openSession();
      const turn = fx.lc.turn(fx.kernel, s.id, "prompt", "work");
      await fx.fileInWorktree(s.workspace.worktree_path, "a.txt", "a\n");
      s.transport.emit(running);
      s.transport.emit(idle);
      const pub = await turn;

      await fx.fileInWorktree(s.workspace.worktree_path, "late.txt", "provider wrote after the turn\n");
      const closed = await fx.lc.closeSession(fx.kernel, s.id);
      expect(closed.finalize.advanced).toBe(true);
      expect(closed.finalize.capture_commit).not.toBe(pub.result!.commit);
      expect(closed.finalize.workspace.custody).toBe("closed");
      expect(closed.finalize.workspace.closed_at).not.toBeNull();

      // Nothing deleted.
      expect(await exists(s.workspace.worktree_path)).toBe(true);
      expect(await resolveRef(fx.repo, workspaceRefFor(s.id))).toBe(closed.finalize.workspace.head_commit);
      const rec = (await fx.lc.inspect(s.id)).workspace;
      expect(rec.last_result_seq).toBe(1);
      expect(await exists(join(fx.home, "agent-hub", "workspaces", s.id, "results", "1.json"))).toBe(true);

      // Idempotent: closing again changes nothing.
      const again = await fx.lc.finalizeClosure({ session_id: s.id, evidence: "again" });
      expect(again.workspace.revision).toBe(closed.finalize.workspace.revision);
      expect(again.skipped_capture_reason).toContain("idempotent");
    } finally {
      await fx.cleanup();
    }
  });

  it("handoff demands closed custody and the exact published identity", async () => {
    const fx = await makeFixture();
    try {
      const s = await fx.openSession();
      await expectCode(
        fx.lc.handoff({ session_id: s.id, decision: "accepted", result_seq: 0, commit: s.workspace.base_commit }),
        "WORKSPACE_NOT_CLOSED",
      );
      await fx.lc.finalizeClosure({ session_id: s.id, evidence: "test closure" });

      await expectCode(
        fx.lc.handoff({ session_id: s.id, decision: "accepted", result_seq: 9, commit: "ab".repeat(20) }),
        "WORKSPACE_HANDOFF_MISMATCH",
      );
      const decided = await fx.lc.handoff({
        session_id: s.id,
        decision: "accepted",
        result_seq: s.workspace.last_result_seq,
        commit: s.workspace.head_commit,
        consumer: "consumer-x",
      });
      const base = Date.parse(decided.handoff!.decided_at);
      expect(Date.parse(decided.retention_until!)).toBe(base + 24 * 3600_000);
      expect(decided.handoff!.consumer).toBe("consumer-x");

      const replay = await fx.lc.handoff({
        session_id: s.id,
        decision: "accepted",
        result_seq: s.workspace.last_result_seq,
        commit: s.workspace.head_commit,
      });
      expect(replay.revision).toBe(decided.revision);

      await expectCode(
        fx.lc.handoff({
          session_id: s.id,
          decision: "discarded",
          result_seq: s.workspace.last_result_seq,
          commit: s.workspace.head_commit,
        }),
        "WORKSPACE_HANDOFF_CONFLICT",
      );
    } finally {
      await fx.cleanup();
    }
  });

  it("a discarded decision arms the same retention", async () => {
    const fx = await makeFixture({ retentionMs: 60_000 });
    try {
      const s = await fx.openSession();
      await fx.lc.finalizeClosure({ session_id: s.id, evidence: "test closure" });
      const rec = (await fx.lc.inspect(s.id)).workspace;
      const decided = await fx.lc.handoff({
        session_id: s.id,
        decision: "discarded",
        result_seq: rec.last_result_seq,
        commit: rec.head_commit,
      });
      expect(decided.handoff!.decision).toBe("discarded");
      expect(Date.parse(decided.retention_until!)).toBe(Date.parse(decided.handoff!.decided_at) + 60_000);
    } finally {
      await fx.cleanup();
    }
  });
});

describe("kernel seam wiring", () => {
  it("mirrors kernel records and records spawn leases durably", async () => {
    const fx = await makeFixture();
    try {
      const s = await fx.openSession();
      const inspected = await fx.lc.inspect(s.id);
      expect(inspected.runtime.state).toBe("present");
      if (inspected.runtime.state === "present") {
        expect(inspected.runtime.record.session_id).toBe(s.id);
      }
      expect(inspected.lease.state).toBe("hub-gone");
      if (inspected.lease.state === "hub-gone") {
        // The scripted spawn pid is not alive on this machine.
        expect(inspected.lease.provider.state === "dead" || inspected.lease.provider.state === "uncertain").toBe(true);
      }
      const turn = fx.lc.turn(fx.kernel, s.id, "prompt", "go");
      s.transport.emit(running);
      s.transport.emit(idle);
      await turn;
      const after = await fx.lc.inspect(s.id);
      if (after.runtime.state === "present") {
        const rec: SessionRecord = after.runtime.record;
        expect(rec.status === "running" || rec.status === "idle").toBe(true);
      }
      expect(after.workspace.provider).toBe("fake");
      expect(after.workspace.transport).toBe("fake-rpc");
    } finally {
      await fx.cleanup();
    }
  });

  it("refuses a structural lie at the mirror boundary and at spawn", async () => {
    const fx = await makeFixture();
    try {
      await expectCode(
        fx.lc.mirror.commit({ schema: "bogus" } as never),
        "SESSION_RECORD_INVALID",
      );
      await expectCode(fx.lc.onProviderSpawn("not-a-uuid", { pid: 1, pgid: 1 }), "WORKSPACE_ID_INVALID");
    } finally {
      await fx.cleanup();
    }
  });

  it("closeSession proves closure through the kernel stop report", async () => {
    const fx = await makeFixture();
    try {
      const s = await fx.openSession();
      const closed = await fx.lc.closeSession(fx.kernel, s.id);
      expect(closed.close.stop?.status).toBe("closed");
      expect(closed.finalize.workspace.close_evidence).toContain("stop=closed");
      expect((await fx.lc.inspect(s.id)).workspace.custody).toBe("closed");
    } finally {
      await fx.cleanup();
    }
  });
});
