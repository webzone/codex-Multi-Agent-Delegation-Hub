import { stat } from "node:fs/promises";

import { afterEach, describe, expect, it } from "vitest";

import { AgentHub } from "../src/hub/agent-hub.js";
import * as publicApi from "../src/index.js";
import { AgentHubError } from "../src/errors.js";
import { readJsonFile, tombstonePath, worktreePath } from "../src/workspace/home.js";
import { workspaceRefFor } from "../src/workspace/records.js";
import { resolveRef, runGit } from "./helpers.js";
import {
  HubFakeFactory,
  createHubHarness,
  hubOptionsFor,
  scriptTurn,
  type HubHarness,
} from "./hub-fakes.js";

/**
 * Public AgentHub API — durable custody contract.
 *
 * Everything below runs the shipped stack: real Git repositories, a real
 * temp AGENT_HUB_HOME, the P2 custody store, leases, GC — only the
 * transport is fake. The contract under test is P2's: close never deletes;
 * deletion requires an exact accepted/discarded handoff plus expired
 * retention; automatic cleanup collects only that and nothing else.
 */

const harnesses: HubHarness[] = [];

async function harness(options: Parameters<typeof createHubHarness>[0] = {}): Promise<HubHarness> {
  const created = await createHubHarness(options);
  harnesses.push(created);
  return created;
}

afterEach(async () => {
  for (const h of harnesses.splice(0)) {
    await h.hub.closeAll().catch(() => undefined);
  }
});

async function headCommit(repository: string): Promise<string> {
  return (await runGit(repository, ["rev-parse", "HEAD"])).trim();
}

describe("AgentHub start: P2 custody provisioning", () => {
  it("starts in a hub-owned isolated worktree keyed by the session id", async () => {
    const h = await harness();
    const started = await h.hub.start({ provider: "omp" });
    expect(started.transport).toBe("omp-rpc");
    expect(started.provider).toBe("omp");
    expect(started.probe.found).toBe(true);
    const workspace = started.workspace;
    expect(workspace.custody).toBe("live");
    expect(workspace.session_id).toBe(started.session_id);
    expect(workspace.base_commit).toBe(await headCommit(h.repository));
    expect(workspace.worktree_path).toBe(worktreePath(h.home, started.session_id));
    expect(started.worktree_path).toBe(workspace.worktree_path);
    await expect(stat(workspace.worktree_path)).resolves.toBeTruthy();
    // Isolation: the provider runs in the custody worktree, never the caller checkout.
    expect((await h.factory.transportAt(0)).launch?.workspace).toBe(workspace.worktree_path);
    expect(workspace.handoff).toBeNull();
    expect(workspace.retention_until).toBeNull();
    expect(workspace.last_result_seq).toBe(0);
  });

  it("refuses non-UUID session ids before touching custody", async () => {
    const h = await harness();
    await expect(h.hub.start({ provider: "omp", session_id: "task-1" })).rejects.toMatchObject({
      code: "WORKSPACE_ID_INVALID",
    });
    expect(await h.hub.list()).toEqual([]);
  });

  it("a probe decline never provisions custody", async () => {
    const h = await harness({ probe: { found: false, version: null, detail: "not installed" } });
    await expect(h.hub.start({ provider: "omp" })).rejects.toMatchObject({
      code: "TRANSPORT_UNAVAILABLE",
    });
    expect(await h.hub.list()).toEqual([]);
  });

  it("a launch that fails after provisioning closes custody honestly and retains everything", async () => {
    const h = await harness({ capabilities: () => ({} as never) });
    await expect(h.hub.start({ provider: "omp" })).rejects.toBeInstanceOf(AgentHubError);
    const [record] = await h.hub.list();
    expect(record).toBeDefined();
    expect(record.workspace.custody).toBe("closed");
    expect(record.workspace.close_evidence).toContain("start failed");
    // Nothing was deleted: the worktree stays for review until a decision.
    await expect(stat(record.workspace.worktree_path)).resolves.toBeTruthy();
    const report = await h.hub.cleanup();
    expect(report.cleanup.retained.map((r) => r.code)).toContain("handoff-undecided");
    expect(report.cleanup.deleted).toEqual([]);
  });
});

describe("AgentHub turns: exact result identity (P2 publication)", () => {
  it("a changed-tree turn publishes seq/parent/commit/tree/ref", async () => {
    const h = await harness();
    const started = await h.hub.start({ provider: "omp" });
    scriptTurn(h.factory, 0, { writes: { "feature.txt": "work" } });
    const turn = await h.hub.prompt(started.session_id, "do it");
    expect(turn.outcome).toBe("succeeded");
    expect(turn.publish_error).toBeUndefined();
    const result = turn.result!;
    expect(result.seq).toBe(1);
    expect(result.kind).toBe("prompt");
    expect(result.tree_changed).toBe(true);
    expect(result.parent).toBe(started.workspace.base_commit);
    expect(result.ref).toBe(workspaceRefFor(started.session_id));
    expect(await resolveRef(h.repository, result.ref)).toBe(result.commit);
    expect(result.commit).not.toBe(result.parent);
    const tree = (await runGit(h.repository, ["rev-parse", `${result.commit}^{tree}`])).trim();
    expect(result.tree).toBe(tree);
    const after = await h.hub.status(started.session_id);
    expect(after.workspace.last_result_seq).toBe(1);
    expect(after.workspace.head_commit).toBe(result.commit);
  });

  it("an unchanged-tree turn still publishes a result naming the current head", async () => {
    const h = await harness();
    const started = await h.hub.start({ provider: "omp" });
    scriptTurn(h.factory, 0, {});
    const one = await h.hub.prompt(started.session_id, "think");
    const two = await h.hub.followUp(started.session_id, "think more");
    expect(one.result!.seq).toBe(1);
    expect(two.result!.seq).toBe(2);
    expect(one.result!.tree_changed).toBe(false);
    expect(two.result!.tree_changed).toBe(false);
    expect(two.result!.commit).toBe(one.result!.commit);
    expect(one.result!.commit).toBe(started.workspace.base_commit);
    expect(two.result!.parent).toBe(one.result!.commit);
  });

  it("non-turn commands carry no result and say why", async () => {
    const h = await harness();
    const started = await h.hub.start({ provider: "omp" });
    const cancel = await h.hub.cancel(started.session_id, null);
    expect(cancel.result).toBeNull();
    expect(cancel.publish_skipped_reason).toContain("not a turn");
  });

  it("the result sequence is gapless across mixed changed turns", async () => {
    const h = await harness();
    const started = await h.hub.start({ provider: "omp" });
    const seqs: number[] = [];
    const commits: string[] = [];
    for (let i = 0; i < 3; i += 1) {
      scriptTurn(h.factory, 0, { writes: { [`f${i}.txt`]: `x${i}` } });
      const turn =
        i === 0
          ? await h.hub.prompt(started.session_id, `t${i}`)
          : await h.hub.followUp(started.session_id, `t${i}`);
      seqs.push(turn.result!.seq);
      commits.push(turn.result!.commit);
    }
    expect(seqs).toEqual([1, 2, 3]);
    for (let i = 1; i < commits.length; i += 1) {
      expect(commits[i]).not.toBe(commits[i - 1]);
    }
  });
});

describe("AgentHub close: retention until an explicit handoff decision", () => {
  it("close retains the worktree, ref, results, and lease; automatic cleanup still never deletes", async () => {
    const h = await harness();
    const started = await h.hub.start({ provider: "omp" });
    scriptTurn(h.factory, 0, { writes: { "deliverable.txt": "gold" } });
    const turn = await h.hub.prompt(started.session_id, "mine it");
    const closed = await h.hub.close(started.session_id);

    expect(closed.record.status).toBe("closed");
    expect(closed.finalize.workspace.custody).toBe("closed");
    expect(closed.finalize.workspace.handoff).toBeNull();
    expect(closed.finalize.workspace.retention_until).toBeNull();
    expect(closed.lease_released).toBe(true);

    // The blocker contract: nothing was deleted by close.
    await expect(stat(worktreePath(h.home, started.session_id))).resolves.toBeTruthy();
    expect(await resolveRef(h.repository, workspaceRefFor(started.session_id))).toBe(
      turn.result!.commit,
    );

    // Startup-catch-up cleanup (the manual `gc` path) also refuses to delete.
    const report = await h.hub.cleanup();
    expect(report.cleanup.deleted).toEqual([]);
    expect(report.cleanup.retained).toContainEqual(
      expect.objectContaining({ session_id: started.session_id, code: "handoff-undecided" }),
    );
    await expect(stat(worktreePath(h.home, started.session_id))).resolves.toBeTruthy();
  });

  it("age alone never deletes an undecided workspace", async () => {
    let nowMs = Date.UTC(2026, 8, 7, 12, 0, 0);
    const h = await harness({
      hubOptions: { now: () => new Date(nowMs), retentionMs: 1_000 },
    });
    const started = await h.hub.start({ provider: "omp" });
    scriptTurn(h.factory, 0, { writes: { "a.txt": "1" } });
    await h.hub.prompt(started.session_id, "work");
    await h.hub.close(started.session_id);
    nowMs += 60 * 60 * 1000; // an hour later
    const report = await h.hub.cleanup();
    expect(report.cleanup.deleted).toEqual([]);
    expect(report.cleanup.retained.map((r) => r.code)).toContain("handoff-undecided");
    await expect(stat(worktreePath(h.home, started.session_id))).resolves.toBeTruthy();
  });

  it("close with an unproven stop keeps the lease and says so honestly", async () => {
    const h = await harness();
    const started = await h.hub.start({ provider: "omp" });
    const transport = await h.factory.transportAt(0);
    transport.stopResults.push({
      status: "orphaned",
      exit_code: null,
      exit_signal: null,
      waited_ms: 1,
    });
    const closed = await h.hub.close(started.session_id);
    expect(closed.record.status).toBe("orphaned");
    expect(closed.lease_released).toBe(false);
    // Custody was still finalized (capture + close), nothing deleted.
    expect(closed.finalize.workspace.custody).toBe("closed");
    expect(closed.finalize.workspace.runtime_status).toBe("orphaned");
    await expect(stat(worktreePath(h.home, started.session_id))).resolves.toBeTruthy();
  });
});

describe("AgentHub handoff: the exact-identity decision", () => {
  async function closedSession(h: HubHarness) {
    const started = await h.hub.start({ provider: "omp" });
    scriptTurn(h.factory, 0, { writes: { "out.txt": "result" } });
    const turn = await h.hub.prompt(started.session_id, "produce");
    await h.hub.close(started.session_id);
    return { started, turn };
  }

  it("refuses decisions that do not name the exact published head", async () => {
    const h = await harness();
    const { started, turn } = await closedSession(h);
    const wrongSeq = h.hub.handoff(started.session_id, {
      decision: "accepted",
      result_seq: 99,
      commit: turn.result!.commit,
    });
    await expect(wrongSeq).rejects.toMatchObject({ code: "WORKSPACE_HANDOFF_MISMATCH" });
    const wrongCommit = h.hub.handoff(started.session_id, {
      decision: "accepted",
      result_seq: turn.result!.seq,
      commit: "0".repeat(40),
    });
    await expect(wrongCommit).rejects.toMatchObject({ code: "WORKSPACE_HANDOFF_MISMATCH" });
    const record = await h.hub.status(started.session_id);
    expect(record.workspace.handoff).toBeNull();
  });

  it("lands the exact decision, is idempotent, and never revises", async () => {
    const h = await harness({ hubOptions: { retentionMs: 60_000 } });
    const { started, turn } = await closedSession(h);
    const decision = {
      decision: "accepted" as const,
      result_seq: turn.result!.seq,
      commit: turn.result!.commit,
      consumer: "release-bot",
    };
    const landed = await h.hub.handoff(started.session_id, decision);
    expect(landed.handoff?.decision).toBe("accepted");
    expect(landed.handoff?.result_seq).toBe(turn.result!.seq);
    expect(landed.handoff?.commit).toBe(turn.result!.commit);
    expect(landed.handoff?.consumer).toBe("release-bot");
    expect(landed.retention_until).not.toBeNull();
    const replay = await h.hub.handoff(started.session_id, decision);
    expect(replay.revision).toBe(landed.revision);
    await expect(
      h.hub.handoff(started.session_id, { ...decision, decision: "discarded" }),
    ).rejects.toMatchObject({ code: "WORKSPACE_HANDOFF_CONFLICT" });
  });

  it("handoff on a still-live session is refused", async () => {
    const h = await harness();
    const started = await h.hub.start({ provider: "omp" });
    await expect(
      h.hub.handoff(started.session_id, {
        decision: "discarded",
        result_seq: 0,
        commit: started.workspace.base_commit,
      }),
    ).rejects.toMatchObject({ code: "WORKSPACE_NOT_CLOSED" });
  });
});

describe("AgentHub GC: only decided + expired workspaces are ever collected", () => {
  it("collects an accepted, retention-expired workspace and leaves a tombstone", async () => {
    let nowMs = Date.UTC(2026, 8, 7, 12, 0, 0);
    const h = await harness({ hubOptions: { now: () => new Date(nowMs), retentionMs: 60_000 } });
    const started = await h.hub.start({ provider: "omp" });
    scriptTurn(h.factory, 0, { writes: { "ship.txt": "yes" } });
    const turn = await h.hub.prompt(started.session_id, "ship");
    await h.hub.close(started.session_id);
    await h.hub.handoff(started.session_id, {
      decision: "accepted",
      result_seq: turn.result!.seq,
      commit: turn.result!.commit,
    });

    // Inside the window: retained with the exact reason.
    let report = await h.hub.cleanup();
    expect(report.cleanup.deleted).toEqual([]);
    expect(report.cleanup.retained).toContainEqual(
      expect.objectContaining({ session_id: started.session_id, code: "retention-active" }),
    );

    nowMs += 61_000;
    report = await h.hub.cleanup();
    expect(report.cleanup.deleted).toEqual([
      {
        session_id: started.session_id,
        last_result_seq: turn.result!.seq,
        head_commit: turn.result!.commit,
        decision: "accepted",
        continued: false,
      },
    ]);
    await expect(stat(worktreePath(h.home, started.session_id))).rejects.toThrow();
    expect(await resolveRef(h.repository, workspaceRefFor(started.session_id))).toBeNull();
    const tomb = await readJsonFile(tombstonePath(h.home, started.session_id));
    expect((tomb as { decision: string }).decision).toBe("accepted");
    expect(await h.hub.list()).toEqual([]);
    // The repository worktree bookkeeping is pruned too.
    const worktreeList = await runGit(h.repository, ["worktree", "list", "--porcelain"]);
    expect(worktreeList).not.toContain(worktreePath(h.home, started.session_id));
  });

  it("startup catch-up cleanup collects a decided, expired workspace automatically", async () => {
    let nowMs = Date.UTC(2026, 8, 7, 12, 0, 0);
    const clock = { now: () => new Date(nowMs) };
    const h = await harness({ hubOptions: { ...clock, retentionMs: 1_000 } });
    const started = await h.hub.start({ provider: "omp" });
    scriptTurn(h.factory, 0, { writes: { "tmp.txt": "junk" } });
    const turn = await h.hub.prompt(started.session_id, "scratch");
    await h.hub.close(started.session_id);
    await h.hub.handoff(started.session_id, {
      decision: "discarded",
      result_seq: turn.result!.seq,
      commit: turn.result!.commit,
    });
    nowMs += 5_000;
    await h.hub.closeAll();

    // A fresh hub over the same home performs startup catch-up by default.
    const factory = new HubFakeFactory();
    const hub2 = await AgentHub.open(
      h.repository,
      hubOptionsFor(h, factory, { ...clock, autoCleanup: true }),
    );
    const collected = hub2.lastCleanup?.cleanup.deleted ?? [];
    expect(collected).toEqual([
      {
        session_id: started.session_id,
        last_result_seq: turn.result!.seq,
        head_commit: turn.result!.commit,
        decision: "discarded",
        continued: false,
      },
    ]);
    await expect(stat(worktreePath(h.home, started.session_id))).rejects.toThrow();
    await hub2.closeAll();
  });

  it("an actively referenced session is untouchable even when its peer is collectible", async () => {
    let nowMs = Date.UTC(2026, 8, 7, 12, 0, 0);
    const h = await harness({
      hubOptions: { now: () => new Date(nowMs), retentionMs: 1_000 },
    });
    const started = await h.hub.start({ provider: "omp" });
    scriptTurn(h.factory, 0, { writes: { "x.txt": "1" } });
    const turn = await h.hub.prompt(started.session_id, "work");
    await h.hub.close(started.session_id);
    await h.hub.handoff(started.session_id, {
      decision: "accepted",
      result_seq: turn.result!.seq,
      commit: turn.result!.commit,
    });
    // A second, ATTACHED session shares the pass.
    const second = await h.hub.start({ provider: "omp" });
    nowMs += 60_000;
    const report = await h.hub.cleanup();
    const decided = report.cleanup.deleted.find((d) => d.session_id === started.session_id);
    expect(decided).toBeDefined(); // the decided one is collectible
    expect(report.cleanup.retained).toContainEqual(
      expect.objectContaining({ session_id: second.session_id, code: "runtime-attached" }),
    );
    await expect(stat(worktreePath(h.home, second.session_id))).resolves.toBeTruthy();
  });
});

describe("AgentHub resume: same durable line, guarded by custody", () => {
  it("resume after close continues the same session id, worktree, and result sequence", async () => {
    const h = await harness({ transportOptions: { resumeState: "echo" } });
    const first = await h.hub.start({ provider: "omp" });
    scriptTurn(h.factory, 0, { writes: { "a.txt": "1" } });
    const turnOne = await h.hub.prompt(first.session_id, "phase 1");
    await h.hub.close(first.session_id);

    const resumed = await h.hub.resume(first.session_id);
    expect(resumed.session_id).toBe(first.session_id);
    expect(resumed.workspace.worktree_path).toBe(first.workspace.worktree_path);
    expect(resumed.transport).toBe("omp-rpc");
    scriptTurn(h.factory, 1, { writes: { "b.txt": "2" } });
    const turnTwo = await h.hub.followUp(first.session_id, "phase 2");
    expect(turnTwo.result!.seq).toBe(2);
    expect(turnTwo.result!.parent).toBe(turnOne.result!.commit);
    expect(turnTwo.result!.commit).not.toBe(turnOne.result!.commit);
  });

  it("resume after a handoff decision is refused", async () => {
    const h = await harness();
    const started = await h.hub.start({ provider: "omp" });
    scriptTurn(h.factory, 0, { writes: { "a.txt": "1" } });
    const turn = await h.hub.prompt(started.session_id, "work");
    await h.hub.close(started.session_id);
    await h.hub.handoff(started.session_id, {
      decision: "accepted",
      result_seq: turn.result!.seq,
      commit: turn.result!.commit,
    });
    await expect(h.hub.resume(started.session_id)).rejects.toMatchObject({
      code: "WORKSPACE_HANDOFF_DECIDED",
    });
  });

  it("resume over a session attached in this process is refused before custody moves", async () => {
    const h = await harness();
    const started = await h.hub.start({ provider: "omp" });
    await expect(h.hub.resume(started.session_id)).rejects.toMatchObject({
      code: "SESSION_ALREADY_LIVE",
    });
  });

  it("a second hub cannot take over a lease that is still live here", async () => {
    const h = await harness();
    const started = await h.hub.start({ provider: "omp" });
    const otherFactory = new HubFakeFactory();
    const hub2 = await AgentHub.open(h.repository, hubOptionsFor(h, otherFactory));
    // The lease names this very process (hub_pid), so the takeover is refused
    // outright — even though close would have released it honestly.
    await expect(hub2.resume(started.session_id)).rejects.toMatchObject({
      code: "WORKSPACE_LIVE",
    });
    await hub2.closeAll();
  });
});

describe("public package surface", () => {
  it("exports no legacy delegate/live vocabulary and no transport-pinning surface", () => {
    const api = publicApi as Record<string, unknown>;
    for (const legacy of [
      "asDelegateError",
      "liveResumeOf",
      "kernelizeResume",
      "kernelCapabilities",
      "mergeKernelResume",
      "kernelResumeFromState",
      "projectRecordFromState",
      "HUB_TRANSPORT_BY_PROVIDER",
      "HUB_SESSION_QUOTA",
      "HUB_REF_NAMESPACE",
      "ATTACH_CLOSE_DRAIN_DEFAULT_MS",
      "delegate",
      "fanout",
    ]) {
      expect(api[legacy], legacy).toBeUndefined();
    }
    // The canonical pieces are present.
    expect(api.AgentHub).toBeDefined();
    expect(api.WorkspaceLifecycle).toBeDefined();
    expect(api.AttachInputPump).toBeDefined();
  });

  it("hub start carries no public transport input and reports the selected one as fact", async () => {
    const h = await harness();
    const started = await h.hub.start({
      provider: "omp",
      // @ts-expect-error the public request type has no `transport` input.
      transport: "omp-rpc",
    });
    // A stray runtime `transport` key is ignored, never honored: selection is
    // internal. The fact still reports the auto-selected transport.
    expect(started.transport).toBe("omp-rpc");
    expect(h.factory.created.length).toBe(1);
  });
});
