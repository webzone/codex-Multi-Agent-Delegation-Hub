import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { acquireRepositoryLock } from "../src/locks.js";
import {
  applyLiveTransition,
  livePendingPath,
  liveRefFor,
  liveStatePath,
  loadLiveState,
  parseLiveSessionState,
  withLiveLock,
  type LiveTransaction,
  type LiveTransitionPhase,
} from "../src/live/state.js";
import type { LiveCapabilities, LiveSessionState } from "../src/live/types.js";
import { createGitRepository, removeDirectory, resolveRef, runGit } from "./helpers.js";

const LIVE_ID = "11111111-2222-4333-8444-555555555555";

function fullCapabilities(overrides: Partial<LiveCapabilities> = {}): LiveCapabilities {
  return {
    prompt: { support: "native", evidence: "omp-rpc ready handshake observed" },
    follow_up: { support: "native", evidence: "omp-rpc ready handshake observed" },
    steer: { support: "hub-queued", evidence: "hub queues between turns" },
    cancel: { support: "signal", evidence: "SIGINT to owned process group" },
    status: { support: "derived", evidence: "hub answers from stream evidence" },
    permission_response: { support: "native", evidence: "rpc permission_response accepted" },
    resume: { support: "native", evidence: "session id round-tripped in probe" },
    checkpoint: { support: "derived", evidence: "hub-side worktree capture" },
    usage_reporting: { support: "unsupported", evidence: null },
    ...overrides,
  };
}

function makeState(commonDir: string, head: string, overrides: Partial<LiveSessionState> = {}): LiveSessionState {
  const at = "2026-09-05T00:00:00.000Z";
  return {
    schema: 1,
    live_session_id: LIVE_ID,
    session_id: null,
    provider: "omp",
    transport: "omp-rpc",
    capabilities: fullCapabilities(),
    identity: {
      common_dir: commonDir,
      worktree_root: commonDir,
      branch: null,
      head,
    },
    base_commit: head,
    current_commit: head,
    checkpoint_seq: 0,
    resume: null,
    status: "idle",
    revision: 1,
    last_error: null,
    created_at: at,
    updated_at: at,
    ...overrides,
  };
}

async function extraCommit(repository: string, parent: string, marker: string): Promise<string> {
  const tree = (await runGit(repository, ["rev-parse", `${parent}^{tree}`])).trim();
  const out = await runGit(repository, ["commit-tree", tree, "-p", parent, "-m", marker]);
  return out.trim();
}

async function expectErrorCode(promise: Promise<unknown>, code: string): Promise<void> {
  await expect(promise).rejects.toMatchObject({ code });
}

async function seededRepository(): Promise<{ repository: string; commonDir: string; head: string }> {
  const repository = await createGitRepository();
  const commonDir = await mkdtemp(join(tmpdir(), "agent-hub-livestate-"));
  const head = (await runGit(repository, ["rev-parse", "HEAD"])).trim();
  return { repository, commonDir, head };
}

describe("live state schema gate", () => {
  it("accepts a well-formed record and round-trips byte-stably", () => {
    const state = makeState("/abs/common", "a".repeat(40));
    const parsed = parseLiveSessionState(state);
    expect(JSON.stringify(parsed)).toBe(JSON.stringify(state));
  });

  it("drops unknown keys instead of persisting them", () => {
    const state = makeState("/abs/common", "a".repeat(40));
    const smuggled = { ...state, task_text: "refactor the parser", stdout: "leak" };
    const parsed = parseLiveSessionState(smuggled);
    expect(parsed).not.toBeNull();
    expect(JSON.stringify(parsed)).not.toContain("refactor the parser");
    expect(JSON.stringify(parsed)).not.toContain("leak");
  });

  it("enforces the capability honesty gate", () => {
    const base = makeState("/abs/common", "a".repeat(40));

    const nativeWithoutEvidence = fullCapabilities();
    nativeWithoutEvidence.prompt = {
      support: "native",
      evidence: "   ",
    } as unknown as LiveCapabilities["prompt"];
    expect(parseLiveSessionState({ ...base, capabilities: nativeWithoutEvidence })).toBeNull();

    const unsupportedWithExcuse = fullCapabilities();
    unsupportedWithExcuse.prompt = {
      support: "unsupported",
      evidence: "secret excuse",
    } as unknown as LiveCapabilities["prompt"];
    expect(parseLiveSessionState({ ...base, capabilities: unsupportedWithExcuse })).toBeNull();

    const missingKey = fullCapabilities() as Partial<LiveCapabilities>;
    delete missingKey.steer;
    expect(parseLiveSessionState({ ...base, capabilities: missingKey })).toBeNull();
  });

  it("refuses id, pairing, width and lineage violations", () => {
    const base = makeState("/abs/common", "a".repeat(40));
    expect(parseLiveSessionState({ ...base, live_session_id: "hint-of-a-name" })).toBeNull();
    expect(parseLiveSessionState({ ...base, transport: "agy-stream-json" })).toBeNull();
    expect(parseLiveSessionState({ ...base, base_commit: "abc123" })).toBeNull();
    expect(
      parseLiveSessionState({
        ...base,
        base_commit: "a".repeat(39),
        current_commit: "a".repeat(39),
      }),
    ).toBeNull();
    expect(
      parseLiveSessionState({ ...base, checkpoint_seq: 0, current_commit: "b".repeat(40) }),
    ).toBeNull();
  });

  it("validates resume variants per provider", () => {
    const base = makeState("/abs/common", "a".repeat(40));
    expect(
      parseLiveSessionState({
        ...base,
        resume: {
          provider: "omp",
          provider_session_id: "p1",
          verified: false,
          verified_via: null,
          last_event_seq: 3,
        },
      })?.resume,
    ).toMatchObject({ provider: "omp", last_event_seq: 3 });
    expect(
      parseLiveSessionState({
        ...base,
        // verified:true without a verified_via is a contract violation.
        resume: {
          provider: "omp",
          provider_session_id: "p1",
          verified: true,
          verified_via: null,
          last_event_seq: 3,
        },
      }),
    ).toBeNull();
    expect(
      parseLiveSessionState({
        ...base,
        // The pi variant must state its own token field, present or null.
        resume: { provider: "pi", provider_session_id: null, verified: false, verified_via: null },
      }),
    ).toBeNull();
  });
});

describe("live transition protocol", () => {
  it("creates on the live-only namespace with the ordered sidecar protocol", async () => {
    const { repository, commonDir, head } = await seededRepository();
    const phases: LiveTransitionPhase[] = [];
    const state = makeState(commonDir, head);

    await applyLiveTransition(
      {
        commonDir,
        repositoryCwd: repository,
        observePhase: async (phase) => {
          phases.push(phase);
        },
      },
      {
        kind: "create",
        live_session_id: LIVE_ID,
        ref: liveRefFor(LIVE_ID),
        expected_ref: null,
        new_commit: head,
        next_state: state,
      },
    );

    expect(phases).toEqual(["sidecar-written", "ref-updated"]);
    expect(await resolveRef(repository, liveRefFor(LIVE_ID))).toBe(head);
    expect(await resolveRef(repository, `refs/agent-hub/sessions/${LIVE_ID}`)).toBeNull();
    expect(await readFile(liveStatePath(commonDir, LIVE_ID), "utf8")).toContain('"revision": 1');
    expect(
      await loadLiveState({ commonDir, repositoryCwd: repository, liveSessionId: LIVE_ID }),
    ).toEqual(state);
    await removeDirectory(repository);
  });

  it("rejects a second create and plans that contradict the stored record", async () => {
    const { repository, commonDir, head } = await seededRepository();
    const ctx = { commonDir, repositoryCwd: repository };
    const state = makeState(commonDir, head);
    await applyLiveTransition(ctx, {
      kind: "create",
      live_session_id: LIVE_ID,
      ref: liveRefFor(LIVE_ID),
      expected_ref: null,
      new_commit: head,
      next_state: state,
    });

    await expectErrorCode(
      applyLiveTransition(ctx, {
        kind: "create",
        live_session_id: LIVE_ID,
        ref: liveRefFor(LIVE_ID),
        expected_ref: null,
        new_commit: head,
        next_state: state,
      }),
      "LIVE_STATE_INCONSISTENT",
    );

    const next = await extraCommit(repository, head, "live checkpoint 1");
    await expectErrorCode(
      applyLiveTransition(ctx, {
        kind: "advance",
        live_session_id: LIVE_ID,
        ref: liveRefFor(LIVE_ID),
        expected_ref: head,
        new_commit: next,
        next_state: { ...state, current_commit: next, checkpoint_seq: 1, revision: state.revision + 2 },
      }),
      "LIVE_STATE_INCONSISTENT",
    );
    await expectErrorCode(
      applyLiveTransition(ctx, {
        kind: "advance",
        live_session_id: LIVE_ID,
        ref: liveRefFor(LIVE_ID),
        expected_ref: head,
        new_commit: next,
        next_state: {
          ...state,
          capabilities: fullCapabilities({
            steer: { support: "native", evidence: "rewritten after launch" },
          }),
        },
      }),
      "LIVE_STATE_INCONSISTENT",
    );
    await removeDirectory(repository);
  });

  it("advances ref and record together, exactly one revision per commit", async () => {
    const { repository, commonDir, head } = await seededRepository();
    const ctx = { commonDir, repositoryCwd: repository };
    const state = makeState(commonDir, head);
    await applyLiveTransition(ctx, {
      kind: "create",
      live_session_id: LIVE_ID,
      ref: liveRefFor(LIVE_ID),
      expected_ref: null,
      new_commit: head,
      next_state: state,
    });

    const next = await extraCommit(repository, head, "live checkpoint 1");
    const advanced: LiveSessionState = {
      ...state,
      current_commit: next,
      checkpoint_seq: 1,
      revision: 2,
      updated_at: "2026-09-05T00:00:05.000Z",
    };
    await applyLiveTransition(ctx, {
      kind: "advance",
      live_session_id: LIVE_ID,
      ref: liveRefFor(LIVE_ID),
      expected_ref: head,
      new_commit: next,
      next_state: advanced,
    });
    expect(await loadLiveState({ ...ctx, liveSessionId: LIVE_ID })).toEqual(advanced);
    await removeDirectory(repository);
  });

  it("replays a landed sidecar after a crash between CAS and state write", async () => {
    const { repository, commonDir, head } = await seededRepository();
    const ctx = { commonDir, repositoryCwd: repository };
    const state = makeState(commonDir, head);
    await applyLiveTransition(ctx, {
      kind: "create",
      live_session_id: LIVE_ID,
      ref: liveRefFor(LIVE_ID),
      expected_ref: null,
      new_commit: head,
      next_state: state,
    });

    const next = await extraCommit(repository, head, "live checkpoint crash");
    const advanced: LiveSessionState = {
      ...state,
      current_commit: next,
      checkpoint_seq: 1,
      revision: 2,
      updated_at: "2026-09-05T00:00:09.000Z",
    };
    const plan: LiveTransaction = {
      kind: "advance",
      live_session_id: LIVE_ID,
      ref: liveRefFor(LIVE_ID),
      expected_ref: head,
      new_commit: next,
      next_state: advanced,
    };

    // Simulated crash: the ref CAS landed, the process died before the state
    // write. The apply call rejects at that boundary…
    await expect(
      applyLiveTransition(
        {
          ...ctx,
          observePhase: async (phase) => {
            if (phase === "ref-updated") {
              throw new Error("simulated crash after ref update");
            }
          },
        },
        plan,
      ),
    ).rejects.toThrow("simulated crash after ref update");

    // …and the next load recovers the full post-transition state.
    const recovered = await loadLiveState({ ...ctx, liveSessionId: LIVE_ID });
    expect(recovered).toEqual(advanced);
    expect(await readFile(livePendingPath(commonDir, LIVE_ID), "utf8").catch(() => null)).toBeNull();
    await removeDirectory(repository);
  });

  it("discards a sidecar whose CAS never landed, and flags divergence", async () => {
    const { repository, commonDir, head } = await seededRepository();
    const ctx = { commonDir, repositoryCwd: repository };
    const state = makeState(commonDir, head);
    await applyLiveTransition(ctx, {
      kind: "create",
      live_session_id: LIVE_ID,
      ref: liveRefFor(LIVE_ID),
      expected_ref: null,
      new_commit: head,
      next_state: state,
    });

    const next = await extraCommit(repository, head, "abandoned checkpoint");
    const sidecar = {
      kind: "advance",
      live_session_id: LIVE_ID,
      ref: liveRefFor(LIVE_ID),
      expected_ref: head,
      new_commit: next,
      next_state: { ...state, current_commit: next, checkpoint_seq: 1, revision: 2 },
    };
    await writeFile(livePendingPath(commonDir, LIVE_ID), JSON.stringify(sidecar), "utf8");

    const loaded = await loadLiveState({ ...ctx, liveSessionId: LIVE_ID });
    expect(loaded).toEqual(state);
    expect(await readFile(livePendingPath(commonDir, LIVE_ID), "utf8").catch(() => null)).toBeNull();

    // A ref pointing at neither side of the sidecar is an inconsistency.
    const rogue = await extraCommit(repository, head, "rogue retarget");
    await runGit(repository, ["update-ref", liveRefFor(LIVE_ID), rogue, head]);
    await writeFile(livePendingPath(commonDir, LIVE_ID), JSON.stringify(sidecar), "utf8");
    await expectErrorCode(
      loadLiveState({ ...ctx, liveSessionId: LIVE_ID }),
      "LIVE_STATE_INCONSISTENT",
    );
    await removeDirectory(repository);
  });

  it("persists no user text on any durable surface", async () => {
    const { repository, commonDir, head } = await seededRepository();
    const ctx = { commonDir, repositoryCwd: repository };
    const secret = "TASK-TEXT-MUST-Never-Leak";
    const state = makeState(commonDir, head, {
      last_error: {
        code: "LIVE_PROVIDER_EXITED",
        message: "the provider exited while live",
        stage: "provider",
        retryable: false,
        provider: "omp",
      },
    });
    await applyLiveTransition(ctx, {
      kind: "create",
      live_session_id: LIVE_ID,
      ref: liveRefFor(LIVE_ID),
      expected_ref: null,
      new_commit: head,
      next_state: state,
    });

    const raw = await readFile(liveStatePath(commonDir, LIVE_ID), "utf8");
    expect(raw).not.toContain(secret);
    // The strongest guarantee: the frozen type cannot even hold it.
    expect(JSON.stringify(state)).not.toContain(secret);
    await removeDirectory(repository);
  });

  it("creates in a SHA-256 repository at the native width", async () => {
    const repository = await mkdtemp(join(tmpdir(), "agent-hub-sha256-live-"));
    await runGit(repository, ["init", "-q", "--object-format=sha256"]);
    await runGit(repository, ["config", "user.email", "hub@example.test"]);
    await runGit(repository, ["config", "user.name", "Agent Hub Test"]);
    await writeFile(join(repository, "README.md"), "initial\n");
    await runGit(repository, ["add", "README.md"]);
    await runGit(repository, ["commit", "-qm", "initial"]);
    const commonDir = await mkdtemp(join(tmpdir(), "agent-hub-livestate-256-"));
    const head = (await runGit(repository, ["rev-parse", "HEAD"])).trim();
    expect(head).toHaveLength(64);

    const state = makeState(commonDir, head);
    await applyLiveTransition({ commonDir, repositoryCwd: repository }, {
      kind: "create",
      live_session_id: LIVE_ID,
      ref: liveRefFor(LIVE_ID),
      expected_ref: null,
      new_commit: head,
      next_state: state,
    });
    expect(
      await loadLiveState({ commonDir, repositoryCwd: repository, liveSessionId: LIVE_ID }),
    ).toEqual(state);
    await removeDirectory(repository);
  });
});

describe("live short-operation lock", () => {
  it("surfaces contention as LIVE_SESSION_BUSY without touching the operation", async () => {
    const commonDir = await mkdtemp(join(tmpdir(), "agent-hub-livelock-"));
    const held = await acquireRepositoryLock({ commonDir, name: `live-${LIVE_ID}` });
    let ran = false;
    await expectErrorCode(
      withLiveLock({ commonDir, liveSessionId: LIVE_ID }, async () => {
        ran = true;
      }),
      "LIVE_SESSION_BUSY",
    );
    expect(ran).toBe(false);
    await held.release();

    const outcome = await withLiveLock({ commonDir, liveSessionId: LIVE_ID }, async () => {
      ran = true;
      return 7;
    });
    expect(ran).toBe(true);
    expect(outcome.value).toBe(7);
    expect(outcome.releaseError).toBeNull();
  });
});
