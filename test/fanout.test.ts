import { access, mkdtemp, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { createWorktreeAtBase, pruneWorktrees, removeWorktree, resolveRepositoryIdentity, type BaseWorktree } from "../src/git.js";
import { acquireRepositoryLock, lockPathFor, type RepositoryLock } from "../src/locks.js";
import { fanOut, FANOUT_MAX_CANDIDATES, FANOUT_MAX_CONCURRENCY_LIMIT, WORKTREE_ADMIN_LOCK_NAME } from "../src/fanout.js";
import { deferred } from "../src/deferred.js";
import type { AgentAdapter, FanOutCandidateResult, FanOutCandidateSpec } from "../src/types.js";
import { createGitRepository, removeDirectory, runGit } from "./helpers.js";

/** Event-driven concurrency bookkeeping: no timers, no sleeps. */
class Tracker {
  active = 0;
  maxActive = 0;
  starts: string[] = [];
  finishes: string[] = [];
  private notified = deferred<void>();
  begin(id: string): void {
    this.active += 1;
    this.maxActive = Math.max(this.maxActive, this.active);
    this.starts.push(id);
    this.notify();
  }

  end(id: string): void {
    this.active -= 1;
    this.finishes.push(id);
    this.notify();
  }

  async until(predicate: () => boolean): Promise<void> {
    while (!predicate()) {
      await this.notified.promise;
    }
  }

  private notify(): void {
    this.notified.resolve();
    this.notified = deferred<void>();
  }
}

function writeAdapter(
  label: string,
  tracker: Tracker,
  gate: Promise<void>,
  behavior: "ok" | "throw" | "exit" = "ok",
): AgentAdapter {
  return {
    id: `fake-${label}`,
    async execute({ cwd }) {
      tracker.begin(label);
      await gate;
      try {
        if (behavior === "throw") {
          throw new Error(`${label} exploded`);
        }
        await writeFile(join(cwd, `${label}.txt`), `${label} work\n`);
        return {
          exit_code: behavior === "exit" ? 3 : 0,
          stdout: "",
          stderr: "",
          session_id: null,
          stdout_truncated: false,
          stderr_truncated: false,
          error: null,
        };
      } finally {
        tracker.end(label);
      }
    },
  };
}

function specs(count: number): FanOutCandidateSpec[] {
  return Array.from({ length: count }, (_, index) => ({
    label: `c${index}`,
    task: `task-${index}`,
    agent: `agent-${index}`,
  }));
}

function adapterMap(entries: Array<[string, AgentAdapter]>): (agent: string) => AgentAdapter {
  const lookup: Record<string, AgentAdapter> = Object.fromEntries(entries);
  return (agent) => lookup[agent] as AgentAdapter;
}

async function worktreeCount(repository: string): Promise<number> {
  const output = await runGit(repository, ["worktree", "list", "--porcelain"]);
  return (output.match(/^worktree /gm) ?? []).length;
}

async function refCount(repository: string): Promise<number> {
  const output = await runGit(repository, ["for-each-ref", "refs/agent-hub/candidates"]);
  return output.split("\n").filter(Boolean).length;
}

describe("fan-out", { timeout: 30_000 }, () => {
  it("runs overlapping waves up to the default cap and preserves input order", async () => {
    const repo = await createGitRepository();
    const tracker = new Tracker();
    const gates = Array.from({ length: 6 }, () => deferred<void>());
    const candidates = specs(6);
    const adapters = candidates.map((spec, index) =>
      [spec.agent, writeAdapter(spec.label as string, tracker, gates[index].promise)] as [string, AgentAdapter],
    );

    try {
      const run = fanOut(
        { workspace: repo, candidates },
        { resolveAdapter: adapterMap(adapters) },
      );

      await tracker.until(() => tracker.starts.length === 4);
      // Four in flight at once proves overlap; the gate blocks prove the cap:
      // nothing beyond index 3 could start while all four are parked.
      expect(tracker.active).toBe(4);
      // Arbitration order of the admin lock is not specified: only the set
      // of started candidates and the overlap are observable guarantees.
      expect([...tracker.starts].sort()).toEqual(["c0", "c1", "c2", "c3"]);

      for (const index of [3, 2, 1, 0]) {
        gates[index].resolve();
        await tracker.until(() => tracker.finishes.includes(`c${index}`));
      }
      for (const gate of gates) {
        gate.resolve();
      }

      const result = await run;
      expect(result.max_concurrency).toBe(4);
      expect(result.status).toBe("success");
      expect(tracker.maxActive).toBe(4);
      // Completion order (c3 finished first) differs from result order.
      expect(tracker.finishes[0]).toBe("c3");
      expect(result.candidates.map((candidate) => candidate.task)).toEqual(candidates.map((c) => c.task));
      expect(result.candidates.map((candidate) => candidate.index)).toEqual([0, 1, 2, 3, 4, 5]);
      for (const candidate of result.candidates) {
        expect(candidate.status).toBe("success");
        expect(candidate.artifact?.commit).toBeTruthy();
        expect(candidate.artifact?.changed_files).toEqual([`c${candidate.index}.txt`]);
      }
    } finally {
      for (const gate of gates) {
        gate.resolve();
      }
      await removeDirectory(repo);
    }
  });

  it("honors configured caps and validates the 1..8 bound", async () => {
    const repo = await createGitRepository();
    try {
      for (const bad of [0, 2.5, FANOUT_MAX_CONCURRENCY_LIMIT + 1]) {
        await expect(
          fanOut({ workspace: repo, candidates: specs(2), maxConcurrency: bad }),
        ).rejects.toMatchObject({ code: "INVALID_CONCURRENCY" });
      }
      await expect(fanOut({ workspace: repo, candidates: [] })).rejects.toMatchObject({
        code: "INVALID_CANDIDATES",
      });
      // The total candidate count is capped independently of the in-flight
      // cap: exceeding it fails validation even with no concurrency claim.
      await expect(
        fanOut({ workspace: repo, candidates: specs(FANOUT_MAX_CANDIDATES + 1) }),
      ).rejects.toMatchObject({ code: "INVALID_CANDIDATES" });
      await expect(
        fanOut({
          workspace: repo,
          candidates: specs(FANOUT_MAX_CANDIDATES + 1),
          maxConcurrency: FANOUT_MAX_CANDIDATES + 1,
        }),
      ).rejects.toMatchObject({ code: "INVALID_CANDIDATES" });
      // The boundary itself is valid: reaching the workspace probe proves the
      // candidate check passed exactly at the cap.
      await expect(
        fanOut({
          workspace: "/definitely/not/a/repository",
          candidates: specs(FANOUT_MAX_CANDIDATES),
        }),
      ).rejects.toMatchObject({ code: "NOT_GIT_REPOSITORY" });

      const busyTracker = new Tracker();
      const busy = await fanOut(
        { workspace: repo, candidates: specs(10), maxConcurrency: FANOUT_MAX_CONCURRENCY_LIMIT },
        {
          resolveAdapter: adapterMap(
            specs(10).map((spec, index) => [
              spec.agent,
              writeAdapter(spec.label as string, busyTracker, Promise.resolve(), "ok"),
            ] as [string, AgentAdapter]),
          ),
        },
      );
      expect(busy.max_concurrency).toBe(FANOUT_MAX_CONCURRENCY_LIMIT);
      expect(busyTracker.maxActive).toBeLessThanOrEqual(FANOUT_MAX_CONCURRENCY_LIMIT);
      expect(busy.candidates.every((candidate) => candidate.status === "success")).toBe(true);

      const serialTracker = new Tracker();
      const serial = await fanOut(
        { workspace: repo, candidates: specs(3), maxConcurrency: 1 },
        {
          resolveAdapter: adapterMap(
            specs(3).map((spec, index) => [
              spec.agent,
              writeAdapter(spec.label as string, serialTracker, Promise.resolve(), "ok"),
            ] as [string, AgentAdapter]),
          ),
        },
      );
      expect(serialTracker.maxActive).toBe(1);
      expect(serial.candidates.map((candidate) => candidate.task)).toEqual(["task-0", "task-1", "task-2"]);
    } finally {
      await removeDirectory(repo);
    }
  });

  it("pins every candidate to the base captured once before dispatch", async () => {
    const repo = await createGitRepository();
    const head = (await runGit(repo, ["rev-parse", "HEAD"])).trim();
    const branch = (await runGit(repo, ["symbolic-ref", "--short", "HEAD"])).trim();
    const tracker = new Tracker();
    const candidates = specs(3);
    const adapters = candidates.map((spec) =>
      [spec.agent, writeAdapter(spec.label as string, tracker, Promise.resolve())] as [string, AgentAdapter],
    );

    try {
      const result = await fanOut({ workspace: repo, candidates }, { resolveAdapter: adapterMap(adapters) });

      expect(result.base.head).toBe(head);
      expect(result.base.branch).toBe(branch);
      expect(await realpath(result.base.worktree_root)).toBe(await realpath(repo));
      expect(await realpath(result.base.common_dir)).toBe(await realpath(join(repo, ".git")));

      for (const candidate of result.candidates) {
        const artifact = candidate.artifact as NonNullable<FanOutCandidateResult["artifact"]>;
        expect(artifact.parent).toBe(head);
        expect((await runGit(repo, ["rev-parse", `${artifact.commit as string}^`])).trim()).toBe(head);
        // Sibling output must not leak: each diff covers only its own file.
        expect(artifact.diff).toContain(`+c${candidate.index} work`);
        for (const other of candidates) {
          if (other.label !== `c${candidate.index}`) {
            expect(artifact.diff).not.toContain(`+${other.label} work`);
          }
        }
        expect(artifact.changed_files).toEqual([`c${candidate.index}.txt`]);
      }
    } finally {
      await removeDirectory(repo);
    }
  });

  it("isolates sibling failures, keeps partial work as artifacts, and cleans up", async () => {
    const repo = await createGitRepository();
    const tracker = new Tracker();
    let pruneCalls = 0;
    const entries: Array<[string, AgentAdapter]> = [
      ["agent-0", writeAdapter("f0", tracker, Promise.resolve(), "throw")],
      ["agent-1", writeAdapter("f1", tracker, Promise.resolve(), "exit")],
      ["agent-2", writeAdapter("f2", tracker, Promise.resolve(), "ok")],
    ];

    try {
      const result = await fanOut(
        { workspace: repo, candidates: specs(3) },
        {
          resolveAdapter: adapterMap(entries),
          pruneWorktrees: async (workspace) => {
            pruneCalls += 1;
            await pruneWorktrees(workspace);
          },
        },
      );

      expect(result.error).toBeNull();
      expect(result.status).toBe("partial");
      expect(result.candidates.map((candidate) => candidate.index)).toEqual([0, 1, 2]);

      const [failed, partial, ok] = result.candidates;
      expect(failed.status).toBe("failure");
      expect(failed.error?.code).toBe("INTERNAL_ERROR");
      expect(failed.artifact?.empty).toBe(true);
      expect(failed.artifact?.commit).toBeNull();

      expect(partial.status).toBe("failure");
      expect(partial.error?.code).toBe("AGENT_FAILED");
      expect(partial.artifact?.commit).toBeTruthy();
      expect(partial.artifact?.changed_files).toEqual(["f1.txt"]);

      expect(ok.status).toBe("success");
      expect(ok.artifact?.changed_files).toEqual(["f2.txt"]);

      // Exactly one prune for the whole fan-out; every worktree gone.
      expect(pruneCalls).toBe(1);
      expect(await worktreeCount(repo)).toBe(1);
      for (const candidate of result.candidates) {
        await expect(access(candidate.execution_workspace)).rejects.toThrow();
      }
      expect(await runGit(repo, ["status", "--porcelain=v1", "--untracked-files=all"])).toBe("");
      // The core keeps artifact refs alive for its caller (a later competition,
      // a merge, or the terminal path's cleanup); it never GCs on its own.
      expect(await refCount(repo)).toBe(2);
    } finally {
      await removeDirectory(repo);
    }
  });

  it("applies v1 pre-flight semantics before touching any worktree", async () => {
    const repo = await createGitRepository();
    const tracker = new Tracker();
    const notARepo = await mkdtemp(join(tmpdir(), "agent-hub-nonrepo-"));
    const entries: Array<[string, AgentAdapter]> = specs(2).map((spec, index) => [
      spec.agent,
      writeAdapter(spec.label as string, tracker, Promise.resolve()),
    ]);

    try {
      await expect(fanOut({ workspace: notARepo, candidates: specs(2) })).rejects.toMatchObject({
        code: "NOT_GIT_REPOSITORY",
      });

      await writeFile(join(repo, "dirty.txt"), "dirty\n");
      await expect(
        fanOut({ workspace: repo, candidates: specs(2) }, { resolveAdapter: adapterMap(entries) }),
      ).rejects.toMatchObject({ code: "DIRTY_WORKTREE" });
      expect(tracker.starts).toEqual([]);
      expect(await worktreeCount(repo)).toBe(1);

      const allowed = await fanOut(
        { workspace: repo, candidates: specs(2), allowDirty: true },
        { resolveAdapter: adapterMap(entries) },
      );
      expect(allowed.candidates.every((candidate) => candidate.status === "success")).toBe(true);
      // Candidates see the committed base, never the caller's dirty file.
      for (const candidate of allowed.candidates) {
        expect(candidate.artifact?.changed_files).toEqual([`c${candidate.index}.txt`]);
      }
    } finally {
      await removeDirectory(notARepo);
      await removeDirectory(repo);
    }
  });

  it("serializes worktree administration under the repo lock but never execution", async () => {
    const repo = await createGitRepository();
    const identity = await resolveRepositoryIdentity(repo);
    const adminTracker = new Tracker();
    const adapterTracker = new Tracker();
    const sharedGate = deferred<void>();

    let holdRelease: () => void = () => {};
    const hold = new Promise<void>((resolve) => {
      holdRelease = resolve;
    });
    let firstCreate = true;

    async function slowCreate(workspace: string, base: string): Promise<BaseWorktree> {
      adminTracker.begin(`create-${adminTracker.starts.length}`);
      try {
        const worktree = await createWorktreeAtBase(workspace, base);
        if (firstCreate) {
          firstCreate = false;
          await hold; // Park inside the admin lock while siblings queue up.
        }
        return worktree;
      } finally {
        adminTracker.end(`create-${adminTracker.finishes.length}`);
      }
    }

    async function timedRemove(workspace: string, worktree: BaseWorktree): Promise<void> {
      adminTracker.begin("remove");
      try {
        await removeWorktree(workspace, worktree);
      } finally {
        adminTracker.end("remove");
      }
    }

    const candidates = specs(4);
    const entries: Array<[string, AgentAdapter]> = candidates.map((spec) => [
      spec.agent,
      {
        id: `fake-${spec.label}`,
        async execute({ cwd }) {
          adapterTracker.begin(spec.label as string);
          if (adapterTracker.starts.length === 4) {
            sharedGate.resolve(); // Only opens when all four agents are inside.
          }
          await sharedGate.promise;
          await writeFile(join(cwd, `${spec.label}.txt`), "work\n");
          adapterTracker.end(spec.label as string);
          return {
            exit_code: 0,
            stdout: "",
            stderr: "",
            session_id: null,
            stdout_truncated: false,
            stderr_truncated: false,
            error: null,
          };
        },
      },
    ]);

    try {
      const run = fanOut(
        { workspace: repo, candidates, maxConcurrency: 4 },
        {
          resolveAdapter: adapterMap(entries),
          createWorktree: slowCreate,
          removeWorktree: timedRemove,
        },
      );

      await adminTracker.until(() => adminTracker.starts.length === 1);
      expect(adminTracker.active).toBe(1);
      // The first create is parked holding the admin lock; no sibling has
      // entered worktree administration, so they are all blocked on the lock.
      expect(adminTracker.starts.length).toBe(1);
      await expect(access(lockPathFor(identity.common_dir, WORKTREE_ADMIN_LOCK_NAME))).resolves.toBeUndefined();

      holdRelease();
      await adminTracker.until(() => adminTracker.starts.length >= 2);

      await adapterTracker.until(() => adapterTracker.starts.length === 4);
      expect(adapterTracker.active).toBe(4); // Four agents run concurrently.
      sharedGate.resolve();

      const result = await run;
      expect(adminTracker.maxActive).toBe(1); // add/remove/prune never overlap.
      expect(adapterTracker.maxActive).toBe(4);
      expect(result.base.common_dir).toBe(identity.common_dir);
      expect(result.candidates.every((candidate) => candidate.status === "success")).toBe(true);
      expect(await worktreeCount(repo)).toBe(1);
    } finally {
      holdRelease();
      sharedGate.resolve();
      await removeDirectory(repo);
    }
  });

  it("aggregates candidate outcomes into the status field", async () => {
    const repo = await createGitRepository();
    const tracker = new Tracker();
    try {
      const allFail = await fanOut(
        { workspace: repo, candidates: specs(2) },
        {
          resolveAdapter: adapterMap(
            specs(2).map((spec, index) => [
              spec.agent,
              writeAdapter(spec.label as string, tracker, Promise.resolve(), "exit"),
            ] as [string, AgentAdapter]),
          ),
        },
      );
      // Zero successes is a failure even with no fan-out-level error.
      expect(allFail.status).toBe("failure");
      expect(allFail.error).toBeNull();
      expect(allFail.candidates.every((candidate) => candidate.status === "failure")).toBe(true);

      let pruneCalls = 0;
      const greenButBrokenPrune = await fanOut(
        { workspace: repo, candidates: specs(2) },
        {
          resolveAdapter: adapterMap(
            specs(2).map((spec, index) => [
              spec.agent,
              writeAdapter(spec.label as string, tracker, Promise.resolve(), "ok"),
            ] as [string, AgentAdapter]),
          ),
          pruneWorktrees: async (workspace) => {
            pruneCalls += 1;
            if (pruneCalls === 1) throw new Error("prune exploded");
            await pruneWorktrees(workspace);
          },
        },
      );
      // A fan-out-level error outranks even an all-green candidate set.
      expect(greenButBrokenPrune.status).toBe("failure");
      expect(greenButBrokenPrune.error?.message).toContain("prune exploded");
      expect(greenButBrokenPrune.candidates.every((candidate) => candidate.status === "success")).toBe(true);
    } finally {
      await removeDirectory(repo);
    }
  });

  it("retains the candidate worktree, attempts teardown, and reports the leak when the admin lock release fails", async () => {
    const repo = await createGitRepository();
    const tracker = new Tracker();
    let attempts = 0;
    let leaked: string | null = null;
    let wedgedRecord: string | null = null;

    // Only the setup claim's release misbehaves. The follow-up attempts are
    // sane — they simply cannot get in while the stuck record is on disk.
    const firstReleaseFails = async (commonDir: string): Promise<RepositoryLock> => {
      attempts += 1;
      const lock = await acquireRepositoryLock({ commonDir, name: WORKTREE_ADMIN_LOCK_NAME });
      if (attempts > 1) {
        return lock;
      }
      return {
        ...lock,
        release: async () => {
          throw new Error("simulated admin lock release failure");
        },
      };
    };

    try {
      const result = await fanOut(
        { workspace: repo, candidates: specs(1) },
        {
          resolveAdapter: adapterMap(
            specs(1).map((spec) => [
              spec.agent,
              writeAdapter(spec.label as string, tracker, Promise.resolve(), "ok"),
            ] as [string, AgentAdapter]),
          ),
          acquireAdminLock: firstReleaseFails,
        },
      );
      const candidate = result.candidates[0];
      leaked = candidate.execution_workspace;
      wedgedRecord = lockPathFor(result.base.common_dir, WORKTREE_ADMIN_LOCK_NAME);

      // The handle survived the failed release: the artifact was captured from
      // the worktree rather than thrown away with the release error.
      expect(candidate.artifact?.commit).toBeTruthy();
      expect(candidate.artifact?.changed_files).toEqual(["c0.txt"]);

      // Teardown and the trailing prune were both attempted...
      expect(attempts).toBe(3);
      // ...and the hub never force-removes a worktree behind a lock it cannot
      // hold, so the worktree is still there and says so in the error.
      await expect(access(candidate.execution_workspace)).resolves.toBeUndefined();
      expect(await worktreeCount(repo)).toBe(2);
      expect(candidate.error?.code).toBe("LOCK_RELEASE_FAILED");
      expect(candidate.error?.message).toContain("simulated admin lock release failure");
      expect(candidate.error?.message).toContain(candidate.execution_workspace);
      expect(candidate.status).toBe("failure");
      // The blocked attempts are visible at fan-out level too.
      expect(result.error?.code).toBe("LOCK_BUSY");
      expect(result.status).toBe("failure");
    } finally {
      // Clear what the simulated failure wedged: the stuck lock record, then
      // the leaked worktree directory, then the repository itself.
      await rm(wedgedRecord ?? lockPathFor(join(repo, ".git"), WORKTREE_ADMIN_LOCK_NAME), {
        recursive: true,
        force: true,
      });
      if (leaked) {
        await removeDirectory(join(leaked, ".."));
      }
      await removeDirectory(repo);
    }
  });
});
