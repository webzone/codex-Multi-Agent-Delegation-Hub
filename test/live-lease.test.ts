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

  it("probes a dead provider pid as dead, none as none", async () => {
    const commonDir = await tempCommonDir();
    const lease = await seedLease(commonDir);
    const gone = fakeProbes({ alive: [] });
    expect(await classifyLiveLease(lease, gone)).toEqual({
      state: "hub-gone",
      provider: { state: "dead" },
    });


    const otherDir = await tempCommonDir();
    const noProcess = await seedLease(otherDir, { provider_pid: null, provider_pgid: null });
    expect(await classifyLiveLease(noProcess, gone)).toEqual({
      state: "hub-gone",
      provider: { state: "none" },
    });
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
