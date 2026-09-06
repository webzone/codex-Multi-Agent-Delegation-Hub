import { spawnSync } from "node:child_process";

import { expect } from "vitest";

import type { LiveStopMode, LiveStopReport } from "../src/live/types.js";

/**
 * The shared authorization-boundary regression scenario for EVERY live
 * transport shutdown path (`launchLiveChild`, the `omp-rpc`/`pi-rpc` JSONL
 * wires, `agy-stream-json`, `hermes-acp`):
 *
 *   The leader exits FIRST while an owned process-group helper survives, with
 *   no shutdown signal ever in flight:
 *
 *     1. `graceful` must send no SIGKILL and no escalation of any kind. It
 *        only proves the owned group gone and returns `orphaned`, retaining
 *        resources: the survivor is still alive afterwards.
 *     2. A later `terminate` is fresh authorization: it must signal the owned
 *        PGID with the bounded TERM→KILL ladder even though the leader's exit
 *        was already observed, prove the group gone, and return `closed`.
 *
 * Each transport wires the fixture to its own seam (a real OS process group,
 * or an injected fake wire with an observable group). The claims are shared.
 */
export interface LeaderFirstSurvivorFixture {
  /** Begin a shutdown; the caller controls pacing for fake-timer seams. */
  stop(mode: LiveStopMode): Promise<LiveStopReport>;
  /** The observable survivor member of the group the leader left behind. */
  survivorAlive(): boolean | Promise<boolean>;
  /** Group-directed signals the seam recorded, when the seam can observe them. */
  signals?(): readonly string[];
}

export async function runLeaderFirstSurvivorScenario(
  fixture: LeaderFirstSurvivorFixture,
): Promise<void> {
  // Snapshot before each phase: fake-wire seams return their live arrays.
  const signals = (): readonly string[] | undefined =>
    fixture.signals === undefined ? undefined : [...fixture.signals()];

  // Phase 1 — graceful: prove-only. Orphaned honestly, survivor untouched,
  // nothing signalled wherever the seam can observe signals at all.
  const orphaned = await fixture.stop("graceful");
  expect(orphaned.status).toBe("orphaned");
  expect(await fixture.survivorAlive()).toBe(true);
  expect(signals() ?? []).toEqual([]);

  // Phase 2 — terminate: fresh authorization reaches the owned group, and
  // the already-resolved leader exit may NOT shortcut the escalation.
  const closed = await fixture.stop("terminate");
  expect(closed.status).toBe("closed");
  expect(await fixture.survivorAlive()).toBe(false);
  if (signals() !== undefined) {
    expect(signals()).toContain("SIGTERM");
  }
}

/**
 * True only while `pid` exists as a live (non-zombie) process. Only ESRCH
 * proves absence; EPERM proves presence with foreign ownership; a zombie is
 * already reaped-bound and cannot mutate anything, matching the hub's rule
 * that a group whose members are all dead answers ESRCH.
 */
export function realProcessAlive(pid: number): boolean {
  try {
    process.kill(pid, 0);
  } catch (error) {
    return (error as NodeJS.ErrnoException).code === "EPERM";
  }
  const state = spawnSync("ps", ["-o", "state=", "-p", String(pid)], {
    encoding: "utf8",
  }).stdout.trim();
  return state.length > 0 && !state.startsWith("Z");
}
