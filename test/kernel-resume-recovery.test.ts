import { describe, expect, it } from "vitest";

import { AgentHubError } from "../src/errors.js";
import { InteractionKernel } from "../src/kernel/index.js";
import type { KernelPhase, SessionRecord } from "../src/kernel/index.js";
import type { FakeTransport } from "./kernel-fakes.js";
import {
  FakeFactory,
  FakeProviderFactory,
  RecordingMirror,
  fixedClock,
  flush,
  fullCapabilities,
  resumeState,
  sessionRecord,
  textEvent,
} from "./kernel-fakes.js";

async function expectCode(run: () => Promise<unknown>, code: string): Promise<AgentHubError> {
  try {
    await run();
  } catch (error) {
    expect(error).toBeInstanceOf(AgentHubError);
    expect((error as AgentHubError).code).toBe(code);
    return error as AgentHubError;
  }
  throw new Error(`expected ${code} to be thrown, nothing was`);
}

const running = { kind: "status", status: "running", note: null } as const;
const idle = { kind: "status", status: "idle", note: null } as const;

/** Drive one session through a full turn and an orderly close; return the durable record. */
async function closedRecord() {
  const factory = new FakeFactory("fake-rpc", "fake");
  const mirror = new RecordingMirror();
  const kernel = new InteractionKernel({
    transportFactories: [factory],
    durable: mirror,
    ...fixedClock,
  });
  const started = await kernel.start({ provider: "fake", workspace: "/ws" });
  const transport = factory.created[0]!;
  const turn = kernel.prompt(started.session_id, "go");
  transport.emit(running);
  transport.emit(textEvent("m1", "done", true));
  transport.emit(idle);
  await turn;
  const closed = await kernel.close(started.session_id);
  transport.end();
  return { kernel, factory, mirror, closed };
}

function honestResumeTransport(provider: string, sessionId: string) {
  return (transport: FakeTransport) => {
    transport.openReport.resume_state = {
      provider,
      provider_session_id: sessionId,
      data: { token: "rotated" },
      last_event_seq: 999, // a transport claim the kernel must overwrite
      verified: false,
      verified_via: null,
    };
  };
}

describe("resume boundary", () => {
  it("refreshes capabilities, upgrades verification on the round trip, and owns the cursor", async () => {
    const { closed } = await closedRecord();
    const record = closed.record;
    expect(record.status).toBe("closed");
    expect(record.resume?.last_event_seq).toBe(3);

    const factory = new FakeFactory(
      "fake-rpc",
      "fake",
      fullCapabilities({ steer: { support: "native", evidence: "this launch observed steer" } }),
    );
    factory.configureTransport = honestResumeTransport("fake", "provider-session-1");
    const mirror = new RecordingMirror();
    const kernel = new InteractionKernel({
      transportFactories: [factory],
      durable: mirror,
      ...fixedClock,
    });
    const resumed = await kernel.resume(record);

    // Claims are launch-scoped: the NEW descriptor wins.
    expect(resumed.capabilities.steer.support).toBe("native");
    expect(resumed.record.revision).toBe(record.revision + 1);
    expect(resumed.record.status).toBe("idle");

    const resume = resumed.record.resume!;
    expect(resume.data).toEqual({ token: "rotated" });
    // Kernel-owned cursor: the transport's 999 claim is overwritten.
    expect(resume.last_event_seq).toBe(record.resume!.last_event_seq);
    // The hub-side upgrade rides an observed session-id round trip only.
    expect(resume.verified).toBe(true);
    expect(resume.verified_via).toBe("hub-resume:fake-rpc");

    // The durable hint reached open() verbatim (kernel cursor included).
    expect(factory.created[0]!.openRequest!.resume).toEqual(record.resume);

    // Gapless continuation: first new event is seed + 1, and pre-seed
    // cursors expire honestly instead of replaying consumed history.
    const transport = factory.created[0]!;
    transport.emit(running);
    await flush();
    const seed = record.resume!.last_event_seq;
    const replay = kernel.eventsAfter(resumed.session_id, seed);
    expect(replay.status).toBe("ok");
    if (replay.status === "ok") {
      expect(replay.events.map((e) => e.seq)).toEqual([seed + 1]);
    }
    expect(kernel.eventsAfter(resumed.session_id, 0).status).toBe("expired");
  });

  it("refuses a transport that shows no resume state under a durable hint", async () => {
    const { closed } = await closedRecord();
    const factory = new FakeFactory("fake-rpc", "fake"); // openReport has no resume_state
    const kernel = new InteractionKernel({
      transportFactories: [factory],
      ...fixedClock,
    });
    await expectCode(() => kernel.resume(closed.record), "RESUME_VERIFICATION_FAILED");
    // A launch that spawned then failed must attempt proven shutdown.
    expect(factory.created[0]!.stopCalls).toEqual(["terminate"]);
  });

  it("refuses a resume that lands on a different provider identity", async () => {
    const { closed } = await closedRecord();
    const factory = new FakeFactory("fake-rpc", "fake");
    factory.configureTransport = (transport) => {
      transport.openReport.resume_state = {
        provider: "fake",
        provider_session_id: "provider-session-OTHER",
        data: {},
        last_event_seq: 0,
        verified: false,
        verified_via: null,
      };
    };
    const kernel = new InteractionKernel({
      transportFactories: [factory],
      ...fixedClock,
    });
    await expectCode(() => kernel.resume(closed.record), "RESUME_VERIFICATION_FAILED");
  });

  it("refuses a resumed transport that describes a foreign identity", async () => {
    const { closed } = await closedRecord();
    const factory = new FakeFactory("fake-rpc", "fake");
    factory.configureTransport = (transport) => {
      honestResumeTransport("fake", "provider-session-1")(transport);
      // The descriptor drifts to a different transport after open.
      const original = transport.describe.bind(transport);
      transport.describe = async () => {
        const descriptor = await original();
        return { ...descriptor, transport: "other-rpc" };
      };
    };
    const kernel = new InteractionKernel({
      transportFactories: [factory],
      ...fixedClock,
    });
    await expectCode(() => kernel.resume(closed.record), "RESUME_VERIFICATION_FAILED");
  });

  it("refuses non-terminal records, foreign schemas, and live ids", async () => {
    const factory = new FakeFactory("fake-rpc", "fake");
    const kernel = new InteractionKernel({
      transportFactories: [factory],
      ...fixedClock,
    });
    await expectCode(
      () => kernel.resume(sessionRecord({ status: "running" })),
      "SESSION_NOT_RESUMABLE",
    );
    await expectCode(
      () => kernel.resume({ ...sessionRecord(), schema: "agent-hub-live/v1" }),
      "SESSION_RECORD_INVALID",
    );
    const started = await kernel.start({ provider: "fake", workspace: "/ws" });
    await expectCode(
      () => kernel.resume(sessionRecord({ session_id: started.session_id, status: "closed" })),
      "SESSION_ALREADY_LIVE",
    );
  });

  it("refuses a provider factory returning a transport outside the candidate set", async () => {
    const offered = new FakeFactory("fake-rpc", "fake");
    const stranger = new FakeFactory("other-rpc", "intruder");
    const providerFactory = new FakeProviderFactory("fake", ["fake-rpc"]);
    providerFactory.pick = () => stranger;
    const kernel = new InteractionKernel({
      transportFactories: [offered, stranger],
      providerFactories: [providerFactory],
      ...fixedClock,
    });
    await expectCode(() => kernel.selectTransport("fake"), "TRANSPORT_UNAVAILABLE");
  });
});

describe("crash and shutdown honesty", () => {
  it("a provider exit settles the turn failed and proves shutdown before the record", async () => {
    const factory = new FakeFactory("fake-rpc", "fake");
    const mirror = new RecordingMirror();
    const phases: KernelPhase[] = [];
    const kernel = new InteractionKernel({
      transportFactories: [factory],
      durable: mirror,
      observePhase: async (phase) => {
        phases.push(phase);
      },
      ...fixedClock,
    });
    const started = await kernel.start({ provider: "fake", workspace: "/ws" });
    const transport = factory.created[0]!;
    const turn = kernel.prompt(started.session_id, "go");
    transport.emit(running);
    const queued = kernel.followUp(started.session_id, "never runs");
    transport.emit({ kind: "exit", intentional: false, exit_code: 1, exit_signal: null });

    const result = await turn;
    expect(result.outcome).toBe("failed");
    expect(result.error).toMatchObject({ code: "PROVIDER_EXITED", stage: "provider" });
    // Queued work fails honestly and was never dispatched.
    const queuedResult = await queued;
    expect(queuedResult.outcome).toBe("failed");
    expect(transport.sent.map((c) => c.kind)).toEqual(["prompt"]);

    expect(transport.stopCalls).toEqual(["terminate"]);
    expect(mirror.latest).toMatchObject({
      status: "error",
      last_error: { code: "PROVIDER_EXITED", provider: "fake" },
    });
    // stop proved BEFORE the terminal record committed.
    expect(phases.at(-1)).toBe("record-committed");
    expect(phases[phases.length - 2]).toBe("transport-stopped");
    await flush(); // let the crash path finish (subscriber close, detach)
    expect(kernel.attached()).toHaveLength(0);
  });

  it("an unprovable shutdown records orphaned, never an assumed closed", async () => {
    const factory = new FakeFactory("fake-rpc", "fake");
    factory.configureTransport = (transport) => {
      transport.stopReport = { status: "orphaned", exit_code: null, exit_signal: null, waited_ms: 50 };
    };
    const mirror = new RecordingMirror();
    const kernel = new InteractionKernel({
      transportFactories: [factory],
      durable: mirror,
      ...fixedClock,
    });
    const started = await kernel.start({ provider: "fake", workspace: "/ws" });
    const transport = factory.created[0]!;
    const turn = kernel.prompt(started.session_id, "go");
    transport.emit(running);
    transport.emit({ kind: "exit", intentional: false, exit_code: null, exit_signal: "SIGKILL" });
    const result = await turn;
    expect(result.outcome).toBe("failed");
    expect(mirror.latest.status).toBe("orphaned");
    expect(mirror.latest.last_error?.message).toContain("could not be proven");
  });

  it("an exhausted event stream is a crash, never a success", async () => {
    const factory = new FakeFactory("fake-rpc", "fake");
    const kernel = new InteractionKernel({ transportFactories: [factory], ...fixedClock });
    const started = await kernel.start({ provider: "fake", workspace: "/ws" });
    const transport = factory.created[0]!;
    const turn = kernel.prompt(started.session_id, "go");
    transport.end();
    const result = await turn;
    expect(result.outcome).toBe("failed");
    expect(result.error?.code).toBe("TRANSPORT_EXHAUSTED");
  });

  it("close cancels the in-flight turn after a proven stop and records closed", async () => {
    const factory = new FakeFactory("fake-rpc", "fake");
    const mirror = new RecordingMirror();
    const phases: KernelPhase[] = [];
    const kernel = new InteractionKernel({
      transportFactories: [factory],
      durable: mirror,
      observePhase: async (phase) => {
        phases.push(phase);
      },
      ...fixedClock,
    });
    const started = await kernel.start({ provider: "fake", workspace: "/ws" });
    const transport = factory.created[0]!;
    const turn = kernel.prompt(started.session_id, "go");
    transport.emit(running);
    const queued = kernel.followUp(started.session_id, "never runs");

    const close = await kernel.close(started.session_id);
    transport.end();
    expect((await turn).outcome).toBe("cancelled");
    expect((await queued).outcome).toBe("failed");
    expect(close.record).toMatchObject({ status: "closed" });
    expect(close.stop).toMatchObject({ status: "closed" });
    expect(mirror.latest.status).toBe("closed");
    expect(phases.slice(-2)).toEqual(["transport-stopped", "record-committed"]);

    // Idempotent: a second close reports the settled record, re-runs nothing.
    const again = await kernel.close(started.session_id);
    expect(again.stop).toBeNull();
    expect(again.record.revision).toBe(close.record.revision);
    expect(transport.stopCalls).toEqual(["graceful"]);

    // Commands after close are refused; the workspace stays untouched.
    await expectCode(() => kernel.prompt(started.session_id, "x"), "SESSION_NOT_LIVE");
  });

  it("an unproven close orphans, and only an authorized terminate may retry", async () => {
    const factory = new FakeFactory("fake-rpc", "fake");
    factory.configureTransport = (transport) => {
      transport.stopReport = { status: "orphaned", exit_code: null, exit_signal: null, waited_ms: 10 };
    };
    const mirror = new RecordingMirror();
    const kernel = new InteractionKernel({
      transportFactories: [factory],
      durable: mirror,
      ...fixedClock,
    });
    const started = await kernel.start({ provider: "fake", workspace: "/ws" });
    const transport = factory.created[0]!;

    const orphaned = await kernel.close(started.session_id);
    transport.end();
    expect(orphaned.record).toMatchObject({ status: "orphaned" });
    expect(orphaned.record.last_error).toMatchObject({ code: "STOP_UNPROVEN", stage: "shutdown" });

    const passive = await kernel.close(started.session_id);
    expect(passive.stop).toBeNull(); // only terminate may re-attempt
    expect(transport.stopCalls).toEqual(["graceful"]);

    transport.stopReport = { status: "closed", exit_code: 0, exit_signal: null, waited_ms: 1 };
    const retried = await kernel.close(started.session_id, "terminate");
    expect(retried.record.status).toBe("closed");
    expect(transport.stopCalls).toEqual(["graceful", "terminate"]);
  });

  it("refuses to start over a spawn whose shutdown could not be proven", async () => {
    const spawns: { session_id: string; pid: number; pgid: number }[] = [];
    const factory = new FakeFactory("fake-rpc", "fake");
    factory.configureTransport = (transport) => {
      transport.openError = new Error("handshake died");
      transport.stopReport = { status: "orphaned", exit_code: null, exit_signal: null, waited_ms: 5 };
    };
    const kernel = new InteractionKernel({
      transportFactories: [factory],
      onProviderSpawn: async (session_id, facts) => {
        spawns.push({ session_id, ...facts });
      },
      ...fixedClock,
    });
    const error = await expectCode(
      () => kernel.start({ provider: "fake", workspace: "/ws" }),
      "INTERNAL_ERROR",
    );
    // Ownership reached the durable boundary BEFORE the handshake died.
    expect(spawns).toEqual([{ session_id: expect.any(String), pid: 4242, pgid: 4242 }]);
    expect(error.message).toContain("pid 4242");
    expect(error.message).toContain("retained");
    expect(factory.created[0]!.stopCalls).toEqual(["terminate"]);
  });
});

describe("durable mirror failure degrades — never a false success", () => {
  it("a failed status mirror fails the turn, drops the queue, keeps ownership", async () => {
    const factory = new FakeFactory("fake-rpc", "fake");
    const mirror = new RecordingMirror();
    mirror.failWhen = (record) => record.status === "running";
    const kernel = new InteractionKernel({
      transportFactories: [factory],
      durable: mirror,
      ...fixedClock,
    });
    const started = await kernel.start({ provider: "fake", workspace: "/ws" });
    const transport = factory.created[0]!;
    const turn = kernel.prompt(started.session_id, "go");
    transport.emit(running);
    const queued = kernel.followUp(started.session_id, "never dispatched");

    const result = await turn;
    expect(result.outcome).toBe("failed");
    expect(result.error).toMatchObject({ code: "MIRROR_WRITE_FAILED", stage: "state" });
    const queuedResult = await queued;
    expect(queuedResult.outcome).toBe("failed");
    expect(transport.sent.map((c) => c.kind)).toEqual(["prompt"]);

    // The best-effort terminal rewrite lands (only "running" commits were
    // poisoned), so the mirror ends on the degraded truth…
    expect(mirror.records.at(-1)).toMatchObject({
      status: "error",
      last_error: { code: "MIRROR_WRITE_FAILED", stage: "state" },
    });
    // …and the kernel refuses further commands for the degraded session.
    await expectCode(() => kernel.prompt(started.session_id, "x"), "SESSION_NOT_LIVE");
    expect(kernel.attached()).toHaveLength(0);
  });

  it("commit failure at launch never registers a session", async () => {
    const factory = new FakeFactory("fake-rpc", "fake");
    factory.configureTransport = (transport) => {
      transport.stopReport = { status: "closed", exit_code: 0, exit_signal: null, waited_ms: 1 };
    };
    const mirror = new RecordingMirror();
    mirror.failWhen = () => true;
    const kernel = new InteractionKernel({
      transportFactories: [factory],
      durable: mirror,
      ...fixedClock,
    });
    await expectCode(
      () => kernel.start({ provider: "fake", workspace: "/ws" }),
      "INTERNAL_ERROR",
    );
    expect(kernel.attached()).toHaveLength(0);
    // Spawn happened, so proven shutdown was attempted before reporting.
    expect(factory.created[0]!.stopCalls).toEqual(["terminate"]);
  });
});

describe("recovery boundary", () => {
  it("attached() names exactly the live sessions with their mirrors", async () => {
    const factory = new FakeFactory("fake-rpc", "fake");
    const kernel = new InteractionKernel({ transportFactories: [factory], ...fixedClock });
    expect(kernel.attached()).toEqual([]);
    const a = await kernel.start({ provider: "fake", workspace: "/ws-a" });
    const b = await kernel.start({ provider: "fake", workspace: "/ws-b" });
    expect(kernel.attached().map((r) => r.session_id).sort()).toEqual(
      [a.session_id, b.session_id].sort(),
    );
    const closed = await kernel.close(b.session_id);
    factory.created[1]!.end();
    expect(kernel.attached().map((r) => r.session_id)).toEqual([a.session_id]);
    expect(closed.record.workspace).toBe("/ws-b");
  });

  it("closeAll settles every live session", async () => {
    const factory = new FakeFactory("fake-rpc", "fake");
    const kernel = new InteractionKernel({ transportFactories: [factory], ...fixedClock });
    await kernel.start({ provider: "fake", workspace: "/ws-a" });
    await kernel.start({ provider: "fake", workspace: "/ws-b" });
    const results = await kernel.closeAll();
    expect(results.map((r) => r.record.status)).toEqual(["closed", "closed"]);
    expect(kernel.attached()).toHaveLength(0);
    for (const transport of factory.created) {
      transport.end();
    }
    await kernel.settle();
  });

  it("a durable record rebuilt from parsed JSON crosses the resume boundary", async () => {
    const { closed } = await closedRecord();
    const fromDisk = JSON.parse(JSON.stringify(closed.record)) as SessionRecord;
    const factory = new FakeFactory("fake-rpc", "fake");
    factory.configureTransport = honestResumeTransport("fake", "provider-session-1");
    const kernel = new InteractionKernel({
      transportFactories: [factory],
      ...fixedClock,
    });
    const resumed = await kernel.resume(fromDisk);
    expect(resumed.session_id).toBe(fromDisk.session_id);
    expect(resumed.record.capabilities).toEqual(fromDisk.capabilities);
    await kernel.close(resumed.session_id);
    factory.created[0]!.end();
  });

  it("junk durable records never reach a transport", async () => {
    const factory = new FakeFactory("fake-rpc", "fake");
    const kernel = new InteractionKernel({
      transportFactories: [factory],
      ...fixedClock,
    });
    const junk = { ...sessionRecord(), capabilities: "all-working-i-trust-it" };
    await expectCode(() => kernel.resume(junk), "CAPABILITY_SNAPSHOT_INVALID");
    expect(factory.created.length).toBe(0);
  });

  it("a resume hint on a fresh start obeys the same honesty gate", async () => {
    const plain = new FakeFactory("fake-rpc", "fake");
    const kernel = new InteractionKernel({ transportFactories: [plain], ...fixedClock });
    await expectCode(
      () =>
        kernel.start({
          provider: "fake",
          workspace: "/ws",
          resume: resumeState({ provider_session_id: "provider-session-1", last_event_seq: 7 }),
        }),
      "RESUME_VERIFICATION_FAILED",
    );

    // With a transport-observed handle the hint is honored — and the event
    // cursor continues gaplessly from the durable position.
    const honest = new FakeFactory("fake-rpc", "fake");
    honest.configureTransport = honestResumeTransport("fake", "provider-session-1");
    const kernel2 = new InteractionKernel({ transportFactories: [honest], ...fixedClock });
    const started = await kernel2.start({
      provider: "fake",
      workspace: "/ws",
      resume: resumeState({ provider_session_id: "provider-session-1", last_event_seq: 7 }),
    });
    expect(started.record.resume).toMatchObject({
      last_event_seq: 7, // hint cursor preserved; the 999 claim dropped
      verified: true,
      verified_via: "hub-resume:fake-rpc",
      data: { token: "rotated" },
    });
    expect(kernel2.eventCursor(started.session_id)).toBe(7);
    expect(kernel2.eventsAfter(started.session_id, 0).status).toBe("expired");
  });
});
