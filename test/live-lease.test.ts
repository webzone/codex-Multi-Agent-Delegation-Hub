import { mkdtemp, mkdir, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";

import { describe, expect, it } from "vitest";

import {
  classifyLiveLease,
  createLiveLease,
  listLiveLeases,
  liveLeasePath,
  readLiveLease,
  removeLiveLease,
  reapOrphanedProvider,
  updateLiveLeaseProvider,
  type LiveLeaseProbes,
  type LiveLeaseRecord,
} from "../src/live/lease.js";

const LIVE_ID = "aaaaaaaa-1111-4222-8333-bbbbbbbbbbbb";
const PROVIDER_PID = 424_242;

async function tempCommonDir(): Promise<string> {
  const dir = await mkdtemp(join(tmpdir(), "agent-hub-lease-test-"));
  return dir;
}

async function seedLease(
  commonDir: string,
  overrides: Partial<Parameters<typeof createLiveLease>[0]> = {},
): Promise<LiveLeaseRecord> {
  return createLiveLease({
    commonDir,
    live_session_id: LIVE_ID,
    provider: "omp",
    worktree_path: join(commonDir, "worktree"),
    provider_pid: PROVIDER_PID,
    provider_pgid: PROVIDER_PID,
    provider_start_token: "prov-start",
    hub_start_token: "hub-start",
    ...overrides,
  });
}

interface FakeProbeConfig {
  alive?: number[];
  starts?: Record<number, string | null>;
  kills?: Array<{ target: number; signal: string }>;
  diesOnSignal?: boolean;
  /** Explicit group-probe answers; default derives from the pid liveness set. */
  groups?: Record<number, "alive" | "gone" | "uncertain">;
}

function fakeProbes(config: FakeProbeConfig = {}): LiveLeaseProbes {
  const alive = new Set(config.alive ?? []);
  const starts = config.starts ?? {};
  const kills = config.kills ?? [];
  return {
    probePid: (pid) => (alive.has(pid) ? "live" : "dead"),
    startToken: async (pid) => (pid in starts ? starts[pid] : null),
    killGroup: (pgid, signal) => {
      kills.push({ target: pgid, signal });
      if (config.diesOnSignal ?? true) {
        alive.delete(pgid);
      }
      return true;
    },
    probeGroup: (pgid) => config.groups?.[pgid] ?? (alive.has(pgid) ? "alive" : "gone"),
    now: () => new Date(),
  };
}

async function expectErrorCode(promise: Promise<unknown>, code: string): Promise<void> {
  await expect(promise).rejects.toMatchObject({ code });
}

describe("lease record lifecycle", () => {
  it("claims exclusively and reads back normalized", async () => {
    const commonDir = await tempCommonDir();
    const lease = await seedLease(commonDir);

    expect(lease.token).toMatch(/^[0-9a-f-]{36}$/);
    expect(lease.hub_hostname).toBe(lease.hub_hostname.trim());
    await expectErrorCode(seedLease(commonDir), "LIVE_LEASE_EXISTS");

    const read = await readLiveLease(commonDir, LIVE_ID);
    expect(JSON.stringify(read)).toBe(JSON.stringify(lease));
  });

  it("refuses ids that are not generated UUIDs before touching a path", async () => {
    const commonDir = await tempCommonDir();
    await expectErrorCode(
      seedLease(commonDir, { live_session_id: "../../evil" as string }),
      "LIVE_LEASE_INVALID_ID",
    );
    expect(() => liveLeasePath(commonDir, "not-a-uuid")).toThrow(
      expect.objectContaining({ code: "LIVE_LEASE_INVALID_ID" }),
    );
  });

  it("absent is absent; corrupt is corrupt, never guessed", async () => {
    const commonDir = await tempCommonDir();
    expect(await readLiveLease(commonDir, LIVE_ID)).toBeUndefined();

    const path = liveLeasePath(commonDir, LIVE_ID);
    await mkdir(dirname(path), { recursive: true });
    await writeFile(path, "{ not json", "utf8");
    await expectErrorCode(readLiveLease(commonDir, LIVE_ID), "LIVE_LEASE_CORRUPT");

    const listed = await listLiveLeases(commonDir);
    expect(listed).toHaveLength(1);
    expect(listed[0].record).toBeNull();
    expect(listed[0].live_session_id).toBe(LIVE_ID);
  });

  it("updates provider facts token-checked", async () => {
    const commonDir = await tempCommonDir();
    const lease = await seedLease(commonDir, { provider_pid: null, provider_pgid: null });

    const updated = await updateLiveLeaseProvider(commonDir, lease, {
      provider_pid: PROVIDER_PID,
      provider_pgid: PROVIDER_PID,
      provider_start_token: "prov-start",
    });
    expect(updated.token).toBe(lease.token);
    expect(updated.provider_pid).toBe(PROVIDER_PID);

    await expectErrorCode(
      updateLiveLeaseProvider(
        commonDir,
        { ...lease, token: "stolen-token" },
        { provider_pid: 1, provider_pgid: 1, provider_start_token: null },
      ),
      "LIVE_LEASE_NOT_OWNER",
    );
  });

  it("releases only for the token that owns it", async () => {
    const commonDir = await tempCommonDir();
    const lease = await seedLease(commonDir);

    await expectErrorCode(removeLiveLease(commonDir, LIVE_ID, "wrong"), "LIVE_LEASE_NOT_OWNER");
    await removeLiveLease(commonDir, LIVE_ID, lease.token);
    await expectErrorCode(removeLiveLease(commonDir, LIVE_ID, lease.token), "LIVE_LEASE_NOT_FOUND");
  });
});

describe("classifyLiveLease", () => {
  it("hands off foreign hosts without probing", async () => {
    const commonDir = await tempCommonDir();
    const lease = { ...(await seedLease(commonDir)), hub_hostname: "somewhere-else" };
    expect(await classifyLiveLease(lease, fakeProbes())).toEqual({
      state: "foreign-host",
      owner_hostname: "somewhere-else",
    });
  });

  it("calls a session live only when the hub identity matches", async () => {
    const commonDir = await tempCommonDir();
    const lease = await seedLease(commonDir);
    const sameHub = {
      hub_pid: lease.hub_pid,
      hub_start_token: "hub-start",
    };
    const probes = fakeProbes({
      alive: [lease.hub_pid],
      starts: { [lease.hub_pid]: "hub-start", [PROVIDER_PID]: "prov-start" },
    });
    void sameHub;
    expect(await classifyLiveLease(lease, probes)).toEqual({ state: "hub-live" });
  });

  it("treats a live but identity-mismatched hub pid as gone (PID reuse)", async () => {
    const commonDir = await tempCommonDir();
    const lease = await seedLease(commonDir);
    const probes = fakeProbes({
      alive: [lease.hub_pid],
      starts: { [lease.hub_pid]: "someone-elses-boot", [PROVIDER_PID]: "prov-start" },
    });
    const result = await classifyLiveLease(lease, probes);
    expect(result.state).toBe("hub-gone");
  });

  it("proves a reapable provider only by exact start identity", async () => {
    const commonDir = await tempCommonDir();
    const lease = await seedLease(commonDir);
    const deadHub = fakeProbes({
      alive: [PROVIDER_PID],
      starts: { [PROVIDER_PID]: "prov-start" },
    });
    expect(await classifyLiveLease(lease, deadHub)).toEqual({
      state: "hub-gone",
      provider: { state: "alive", reapable: true },
    });

    const reused = fakeProbes({
      alive: [PROVIDER_PID],
      starts: { [PROVIDER_PID]: "innocent-newcomer" },
    });
    const verdict = await classifyLiveLease(lease, reused);
    if (verdict.state !== "hub-gone" || verdict.provider.state !== "uncertain") {
      expect.unreachable("reused pid must classify as hub-gone with an uncertain provider");
    }
    expect(verdict.provider.reapable).toBe(false);
    expect(verdict.provider.reason).toContain("reused");
    const unreadable = fakeProbes({ alive: [PROVIDER_PID], starts: {} });
    expect(
      await classifyLiveLease(lease, unreadable).then((v) => (v.state === "hub-gone" ? v.provider.state : "?")),
    ).toBe("uncertain");
  });

  it("probes a dead provider pid as dead; a null pid proves nothing about the provider", async () => {
    const commonDir = await tempCommonDir();
    const lease = await seedLease(commonDir);
    const gone = fakeProbes({ alive: [] });
    expect(await classifyLiveLease(lease, gone)).toEqual({
      state: "hub-gone",
      provider: { state: "dead" },
    });

    // A null provider pid only proves recorded ownership never landed —
    // provider death may never be inferred from it. The classification must
    // stay conservative: uncertain, non-reapable, with a stated reason.
    const otherDir = await tempCommonDir();
    const noProcess = await seedLease(otherDir, { provider_pid: null, provider_pgid: null });
    const verdict = await classifyLiveLease(noProcess, gone);
    if (verdict.state !== "hub-gone" || verdict.provider.state !== "uncertain") {
      expect.unreachable("a null provider pid must classify as hub-gone with an uncertain provider");
    }
    expect(verdict.provider.reapable).toBe(false);
    expect(verdict.provider.reason).toContain("no provider pid");
  });

  it("refuses to call a gone leader dead while its group survives or is unprobeable", async () => {
    const commonDir = await tempCommonDir();
    const lease = await seedLease(commonDir);

    // Leader PID gone but kill(-pgid, 0) still answers: helpers live.
    const helperAlive = fakeProbes({ alive: [], groups: { [PROVIDER_PID]: "alive" } });
    const survivor = await classifyLiveLease(lease, helperAlive);
    if (survivor.state !== "hub-gone" || survivor.provider.state !== "uncertain") {
      expect.unreachable("a surviving group must never classify the provider as dead");
    }
    expect(survivor.provider.reapable).toBe(false);
    expect(survivor.provider.reason).toContain("still exists");

    // EPERM/unknown from the group probe is never death proof either.
    const unprobeable = fakeProbes({ alive: [], groups: { [PROVIDER_PID]: "uncertain" } });
    const unknown = await classifyLiveLease(lease, unprobeable);
    if (unknown.state !== "hub-gone" || unknown.provider.state !== "uncertain") {
      expect.unreachable("an unprobeable group must classify as uncertain, not dead");
    }
    expect(unknown.provider.reason).toContain("cannot be probed");

    // No recorded group identity: the group probe could not even name the
    // right group, so leader death proves nothing about helpers.
    const otherDir = await tempCommonDir();
    const noPgid = await seedLease(otherDir, { provider_pgid: null });
    const noIdentity = await classifyLiveLease(noPgid, fakeProbes({ alive: [] }));
    if (noIdentity.state !== "hub-gone" || noIdentity.provider.state !== "uncertain") {
      expect.unreachable("leader death without a recorded pgid must stay uncertain");
    }
    expect(noIdentity.provider.reason).toContain("no process-group identity");
  });

  it("classifies a live pid with an unrecorded or foreign group as uncertain, never reapable", async () => {
    const commonDir = await tempCommonDir();
    // Start identity matches perfectly — yet without a recorded PGID the
    // hub owns NO provable group; `kill(-pid)` would be a guess.
    const noPgid = await seedLease(commonDir, { provider_pgid: null });
    const aliveProbes = fakeProbes({ alive: [PROVIDER_PID], starts: { [PROVIDER_PID]: "prov-start" } });
    const verdict = await classifyLiveLease(noPgid, aliveProbes);
    if (verdict.state !== "hub-gone" || verdict.provider.state !== "uncertain") {
      expect.unreachable("a live provider with no recorded pgid must stay uncertain/manual");
    }
    expect(verdict.provider.reapable).toBe(false);
    expect(verdict.provider.reason).toContain("no process-group identity");

    // A recorded pgid that is not the leader pid breaks the detached-launch
    // invariant: group ownership is unprovable, so nothing is reapable.
    const otherDir = await tempCommonDir();
    const foreignPgid = await seedLease(otherDir, { provider_pgid: PROVIDER_PID + 1 });
    const foreign = await classifyLiveLease(foreignPgid, aliveProbes);
    if (foreign.state !== "hub-gone" || foreign.provider.state !== "uncertain") {
      expect.unreachable("a pgid that differs from the leader pid must stay uncertain");
    }
    expect(foreign.provider.reapable).toBe(false);
    expect(foreign.provider.reason).toContain("not the group");
  });
});

describe("reapOrphanedProvider", () => {
  it("signals the owned group with TERM then KILL, proving death by probe", async () => {
    const commonDir = await tempCommonDir();
    const lease = await seedLease(commonDir);
    const kills: Array<{ target: number; signal: string }> = [];
    // Survives SIGTERM, dies to SIGKILL.
    const alive = new Set<number>([PROVIDER_PID]);
    const probes: LiveLeaseProbes = {
      probePid: (pid) => (alive.has(pid) ? "live" : "dead"),
      startToken: async () => "prov-start",
      killGroup: (pgid, signal) => {
        kills.push({ target: pgid, signal });
        if (signal === "SIGKILL") {
          alive.delete(pgid);
        }
        return true;
      },
      probeGroup: (pgid) => (alive.has(pgid) ? "alive" : "gone"),
      now: () => new Date(),
    };
    const outcome = await reapOrphanedProvider(
      lease,
      { state: "alive", reapable: true },
      probes,
      { graceMs: 60, killWaitMs: 60, pollMs: 5 },
    );
    expect(outcome.status).toBe("reaped");
    expect(kills.map((k) => k.signal)).toEqual(["SIGTERM", "SIGKILL"]);
    expect(kills.every((k) => k.target === PROVIDER_PID)).toBe(true);
  });

  it("reports survival instead of pretending", async () => {
    const commonDir = await tempCommonDir();
    const lease = await seedLease(commonDir);
    const probes = fakeProbes({ alive: [PROVIDER_PID], starts: { [PROVIDER_PID]: "prov-start" }, diesOnSignal: false });
    const outcome = await reapOrphanedProvider(
      lease,
      { state: "alive", reapable: true },
      probes,
      { graceMs: 30, killWaitMs: 30, pollMs: 5 },
    );
    expect(outcome.status).toBe("survived");
  });

  it("only reports reaped when the whole group answers gone, not on leader death", async () => {
    const commonDir = await tempCommonDir();
    const lease = await seedLease(commonDir);
    const kills: Array<{ target: number; signal: string }> = [];
    // The leader dies to SIGTERM; a helper keeps the group alive until KILL.
    const alive = new Set<number>([PROVIDER_PID]);
    let groupGone = false;
    const probes: LiveLeaseProbes = {
      probePid: (pid) => (alive.has(pid) ? "live" : "dead"),
      startToken: async () => "prov-start",
      killGroup: (pgid, signal) => {
        kills.push({ target: pgid, signal });
        if (signal === "SIGTERM") {
          alive.delete(pgid);
        }
        if (signal === "SIGKILL") {
          groupGone = true;
        }
        return true;
      },
      probeGroup: () => (groupGone ? "gone" : "alive"),
      now: () => new Date(),
    };
    const outcome = await reapOrphanedProvider(
      lease,
      { state: "alive", reapable: true },
      probes,
      { graceMs: 60, killWaitMs: 60, pollMs: 5 },
    );
    // Leader-only death would have "reaped" inside the TERM window; the
    // group probe must force the KILL escalation and only then certify.
    expect(outcome.status).toBe("reaped");
    expect(kills.map((k) => k.signal)).toEqual(["SIGTERM", "SIGKILL"]);
  });

  it("reports uncertain, never a fake reap, when the group cannot be probed", async () => {
    const commonDir = await tempCommonDir();
    const lease = await seedLease(commonDir);
    const probes = fakeProbes({
      alive: [],
      groups: { [PROVIDER_PID]: "uncertain" },
      starts: { [PROVIDER_PID]: "prov-start" },
    });
    const outcome = await reapOrphanedProvider(
      lease,
      { state: "alive", reapable: true },
      probes,
      { graceMs: 20, killWaitMs: 20, pollMs: 5 },
    );
    expect(outcome.status).toBe("uncertain");
    if (outcome.status === "uncertain") {
      expect(outcome.reason).toContain("could not be probed");
    }
  });

  it("never signals an uncertain classification", async () => {
    const commonDir = await tempCommonDir();
    const lease = await seedLease(commonDir);
    const kills: Array<{ target: number; signal: string }> = [];
    const outcome = await reapOrphanedProvider(
      lease,
      { state: "uncertain", reapable: false, reason: "pid reused" },
      fakeProbes({ kills }),
      { graceMs: 10, killWaitMs: 10 },
    );
    expect(outcome.status).toBe("not-attempted");
    expect(kills).toEqual([]);
  });

  it("refuses to signal a pid as a group when the lease records no pgid", async () => {
    const commonDir = await tempCommonDir();
    const noPgid = await seedLease(commonDir, { provider_pgid: null });
    const kills: Array<{ target: number; signal: string }> = [];
    const probes = fakeProbes({
      kills,
      alive: [PROVIDER_PID],
      starts: { [PROVIDER_PID]: "prov-start" },
    });
    // Even a caller ASSERTING `alive, reapable` cannot make this path guess
    // PGID == PID: no recorded group identity, no group signal, ever.
    const outcome = await reapOrphanedProvider(
      noPgid,
      { state: "alive", reapable: true },
      probes,
      { graceMs: 10, killWaitMs: 10 },
    );
    expect(outcome.status).toBe("not-attempted");
    if (outcome.status === "not-attempted") {
      expect(outcome.reason).toContain("refusing to signal a pid as a group");
    }
    expect(kills).toEqual([]);
  });
});

describe("listLiveLeases quota view", () => {
  it("counts every lease file, corrupt included, conservatively", async () => {
    const commonDir = await tempCommonDir();
    await seedLease(commonDir, { live_session_id: "11111111-2222-4333-8444-555555555555" });
    await seedLease(commonDir, { live_session_id: "66666666-3333-4444-9555-777777777777" });
    const corruptPath = liveLeasePath(commonDir, "88888888-4444-4555-a666-999999999999");
    await mkdir(dirname(corruptPath), { recursive: true });
    await writeFile(corruptPath, JSON.stringify({ schema: 1, junk: true }), "utf8");

    const listed = await listLiveLeases(commonDir);
    expect(listed).toHaveLength(3);
    expect(listed.filter((l) => l.record !== null)).toHaveLength(2);

    const survivor = listed.find((l) => l.record !== null);
    if (survivor?.record) {
      const raw = JSON.parse(await readFile(liveLeasePath(commonDir, survivor.live_session_id), "utf8"));
      expect(raw.schema).toBe(1);
    }
  });
});
