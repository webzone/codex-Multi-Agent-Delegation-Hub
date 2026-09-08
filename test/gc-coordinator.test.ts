import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import {
  GcCoordinator,
  gcCoordinatorStatePath,
  runGcWorker,
} from "../src/workspace/gc-coordinator.js";
import { readJsonFile, removeTree, writeJsonAtomic } from "../src/workspace/home.js";

describe("durable GC coordinator", () => {
  it("keeps the earliest handoff deadline and starts an independent worker", async () => {
    const home = await mkdtemp(join(tmpdir(), "agent-hub-gc-coordinator-"));
    const started: string[] = [];
    const coordinator = new GcCoordinator(home, {
      spawnWorker: (workerHome) => started.push(workerHome),
    });
    const early = new Date(Date.now() + 60_000).toISOString();
    const late = new Date(Date.now() + 120_000).toISOString();

    await coordinator.arm({ repository_cwd: process.cwd(), retention_until: late });
    await coordinator.arm({ repository_cwd: "/tmp/another-repository", retention_until: early });

    expect(await readJsonFile(gcCoordinatorStatePath(home))).toMatchObject({
      schema: "agent-hub-gc-coordinator/v1",
      home,
      repository_cwd: "/tmp/another-repository",
      due_at: early,
      revision: 2,
    });
    expect(started).toEqual([home, home]);
    await removeTree(home);
  });

  it("catches up an expired empty schedule and removes its durable state", async () => {
    const home = await mkdtemp(join(tmpdir(), "agent-hub-gc-worker-"));
    await writeJsonAtomic(gcCoordinatorStatePath(home), {
      schema: "agent-hub-gc-coordinator/v1",
      home,
      repository_cwd: process.cwd(),
      due_at: new Date(Date.now() - 1).toISOString(),
      revision: 0,
    });

    await runGcWorker(home);

    expect(await readJsonFile(gcCoordinatorStatePath(home))).toBeUndefined();
    await removeTree(home);
  });
});

