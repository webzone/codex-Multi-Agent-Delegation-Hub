import { describe, expect, it } from "vitest";

import { AgentHubError } from "../src/errors.js";
import { InteractionKernel } from "../src/kernel/index.js";
import type { Capabilities, SessionEvent } from "../src/kernel/index.js";
import {
  FakeFactory,
  RecordingMirror,
  fixedClock,
  flush,
  fullCapabilities,
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

async function booted(capabilities: Capabilities = fullCapabilities()) {
  const factory = new FakeFactory("fake-rpc", "fake", capabilities);
  const mirror = new RecordingMirror();
  const kernel = new InteractionKernel({
    transportFactories: [factory],
    durable: mirror,
    ...fixedClock,
  });
  const started = await kernel.start({ provider: "fake", workspace: "/ws" });
  const transport = factory.created[0]!;
  return { kernel, factory, mirror, transport, id: started.session_id };
}

const running = { kind: "status", status: "running", note: null } as const;
const idle = { kind: "status", status: "idle", note: null } as const;

describe("request correlation and turn settlement", () => {
  it("settles a prompt exactly once, correlated by command_id", async () => {
    const { kernel, id, transport } = await booted();
    const turn = kernel.prompt(id, "write the thing");
    expect(transport.sent).toHaveLength(1);
    expect(transport.sent[0]).toMatchObject({ kind: "prompt", text: "write the thing", session_id: id });

    transport.emit(running);
    transport.emit(textEvent("m1", "Hel", false));
    transport.emit(textEvent("m1", "lo!", true));
    transport.emit(idle);

    const result = await turn;
    expect(result).toMatchObject({
      session_id: id,
      command_id: transport.sent[0]!.command_id,
      kind: "prompt",
      outcome: "succeeded",
      final_text: { text: "Hello!", truncated: false },
    });

    // Second idle for an empty turn must not re-settle anything.
    transport.emit(idle);
    await flush();
    expect(kernel.view(id).revision).toBe(3); // launch=1, running=2, turn_end=3
  });

  it("captures usage and provider error evidence on the turn", async () => {
    const { kernel, id, transport } = await booted();
    const turn = kernel.prompt(id, "go");
    transport.emit(running);
    transport.emit({
      kind: "usage",
      usage: { input_tokens: 10, output_tokens: 4, cached_tokens: null, cost_usd: null },
    });
    transport.emit({
      kind: "error",
      error: { code: "RATE_LIMITED", message: "provider says slow down", stage: "provider", retryable: true, provider: "fake" },
    });
    transport.emit(idle);
    const result = await turn;
    expect(result.outcome).toBe("failed");
    expect(result.usage).toEqual({ input_tokens: 10, output_tokens: 4, cached_tokens: null, cost_usd: null });
    expect(result.error).toMatchObject({ code: "RATE_LIMITED", stage: "provider", retryable: true });
  });

  it("accepts the prompt exactly once", async () => {
    const { kernel, id, transport } = await booted();
    const turn = kernel.prompt(id, "first");
    await expectCode(() => kernel.prompt(id, "second"), "PROMPT_ALREADY_ACCEPTED");
    transport.emit(running);
    transport.emit(idle);
    await turn;
  });

  it("drains hub-queued follow-ups in order between turns", async () => {
    const { kernel, id, transport } = await booted();
    const turn1 = kernel.prompt(id, "t1");
    transport.emit(running);

    const f1 = kernel.followUp(id, "q1");
    const f2 = kernel.followUp(id, "q2");
    expect(transport.sent.filter((c) => c.kind === "follow_up")).toHaveLength(0);

    transport.emit(idle); // ends t1, dispatches q1
    const r1 = await turn1;
    expect(r1.outcome).toBe("succeeded");
    transport.emit(idle); // ends q1, dispatches q2
    const q1s = await f1;
    transport.emit(idle); // ends q2
    const q2s = await f2;

    expect([q1s.outcome, q2s.outcome]).toEqual(["succeeded", "succeeded"]);
    const followUps = transport.sent.flatMap((c) => (c.kind === "follow_up" ? [c.text] : []));
    expect(followUps).toEqual(["q1", "q2"]);
  });

  it("delivers native follow-ups immediately and never re-sends on drain", async () => {
    const { kernel, id, transport } = await booted(
      fullCapabilities({ follow_up: { support: "native", evidence: "provider queues mid-run" } }),
    );
    const turn1 = kernel.prompt(id, "t1");
    transport.emit(running);
    const f1 = kernel.followUp(id, "q1");
    // `native` promised immediate delivery to the PROVIDER, not hub queueing.
    expect(transport.sent.map((c) => c.kind)).toEqual(["prompt", "follow_up"]);

    transport.emit(idle); // t1 ends; q1 becomes tracked without re-sending
    await turn1;
    expect(transport.sent.filter((c) => c.kind === "follow_up")).toHaveLength(1);
    transport.emit(idle); // q1's own turn ends
    expect((await f1).outcome).toBe("succeeded");
  });

  it("bounds the follow-up queue honestly", async () => {
    const factory = new FakeFactory("fake-rpc", "fake");
    const kernel = new InteractionKernel({
      transportFactories: [factory],
      followUpQueue: { maxMessages: 1 },
      ...fixedClock,
    });
    const started = await kernel.start({ provider: "fake", workspace: "/ws" });
    const transport = factory.created[0]!;
    const turn = kernel.prompt(started.session_id, "t1");
    transport.emit(running);
    const queued = kernel.followUp(started.session_id, "q1");
    await expectCode(() => kernel.followUp(started.session_id, "q2"), "QUEUE_FULL");
    transport.emit(idle); // t1 ends; q1 dispatches
    await turn;
    await flush();
    transport.emit(idle); // q1's own turn ends
    expect((await queued).outcome).toBe("succeeded");
  });
});

describe("capability gating: steer / cancel / status / permission", () => {
  it("refuses an unsupported steer pre-dispatch, delivering nothing", async () => {
    const { kernel, id, transport } = await booted(); // steer: unsupported
    const turn = kernel.prompt(id, "go");
    transport.emit(running);
    const refused = await kernel.steer(id, "left!");
    expect(refused).toMatchObject({ outcome: "unsupported", kind: "steer" });
    expect(refused.error).toMatchObject({ code: "CAPABILITY_UNSUPPORTED", stage: "capability" });
    expect(transport.sent.map((c) => c.kind)).toEqual(["prompt"]);
    transport.emit(idle);
    await turn;
  });

  it("delivers steer while a turn runs and refuses it otherwise", async () => {
    const { kernel, id, transport } = await booted(
      fullCapabilities({ steer: { support: "hub-queued", evidence: "provider accepts guidance mid-turn" } }),
    );
    await expectCode(() => kernel.steer(id, "too early"), "SESSION_NOT_RUNNING");
    const turn = kernel.prompt(id, "go");
    transport.emit(running);
    await flush(); // let the pump consume the running event
    const steered = await kernel.steer(id, "left!");
    expect(steered.outcome).toBe("succeeded");
    const steerCommand = transport.sent.find((c) => c.kind === "steer");
    expect(steerCommand).toMatchObject({ text: "left!", session_id: id });
    transport.emit(idle);
    await turn;
  });

  it("reports an honest cancel no-op and cancels the live turn", async () => {
    const { kernel, id, transport } = await booted();
    const noTurn = await kernel.cancel(id, null);
    expect(noTurn.outcome).toBe("succeeded");
    expect(transport.sent).toHaveLength(0); // nothing was in flight; nothing sent

    const turn = kernel.prompt(id, "go");
    transport.emit(running);
    const cancelled = await kernel.cancel(id, "user asked");
    expect(cancelled.outcome).toBe("succeeded");
    expect(transport.sent[1]).toMatchObject({ kind: "cancel", reason: "user asked" });
    transport.emit(idle);
    expect((await turn).outcome).toBe("cancelled");
  });

  it("answers a derived status locally and forwards only native status", async () => {
    const { kernel, id, transport } = await booted(); // status: derived
    const turn = kernel.prompt(id, "go");
    transport.emit(running);
    transport.emit(running);
    await flush(); // let the pump consume both running events
    const answer = await kernel.requestStatus(id);
    expect(JSON.parse(answer.final_text!.text)).toEqual({
      status: "running",
      turn_in_flight: true,
      last_event_seq: 2,
      queued_follow_ups: 0,
    });
    expect(transport.sent.map((c) => c.kind)).toEqual(["prompt"]); // NEVER forwarded
    transport.emit(idle);
    await turn;

    const native = await booted(
      fullCapabilities({ status: { support: "native", evidence: "get_state observed in probe" } }),
    );
    const statusResult = await native.kernel.requestStatus(native.id);
    expect(statusResult.outcome).toBe("succeeded");
    expect(native.transport.sent.map((c) => c.kind)).toEqual(["status"]);
  });

  it("gates permission responses to observed, open requests with the two-word vocabulary", async () => {
    const { kernel, id, transport } = await booted();
    const turn = kernel.prompt(id, "go");
    transport.emit(running);
    transport.emit({
      kind: "permission_request",
      request_id: "r-1",
      tool: "bash",
      summary: { text: "rm -rf build", truncated: false },
    });
    await flush(); // let the pump observe the permission request

    await expectCode(() => kernel.respondPermission(id, "r-1", "allow_always", null), "COMMAND_INVALID");
    await expectCode(() => kernel.respondPermission(id, "r-9", "deny", null), "PERMISSION_REQUEST_UNKNOWN");
    const answered = await kernel.respondPermission(id, "r-1", "allow_once", "fine");
    expect(answered.outcome).toBe("succeeded");
    expect(transport.sent.at(-1)).toMatchObject({
      kind: "permission_response",
      request_id: "r-1",
      decision: "allow_once",
      note: "fine",
    });
    // Answering twice is unknown-request territory, not a second delivery.
    await expectCode(() => kernel.respondPermission(id, "r-1", "deny", null), "PERMISSION_REQUEST_UNKNOWN");
    transport.emit(idle);
    await turn;
  });

  it("refuses permission responses when the claim is honestly unsupported", async () => {
    const { kernel, id, transport } = await booted(
      fullCapabilities({ permission_response: { support: "unsupported", evidence: null } }),
    );
    const turn = kernel.prompt(id, "go");
    transport.emit(running);
    transport.emit({
      kind: "permission_request",
      request_id: "r-1",
      tool: "bash",
      summary: { text: "echo", truncated: false },
    });
    const refused = await kernel.respondPermission(id, "r-1", "deny", null);
    expect(refused.outcome).toBe("unsupported");
    expect(transport.sent.some((c) => c.kind === "permission_response")).toBe(false);
    transport.emit(idle);
    await turn;
  });
});

describe("event streaming", () => {
  it("re-stamps every envelope fact and keeps seq gapless", async () => {
    const { kernel, id, transport } = await booted();
    const turn = kernel.prompt(id, "go");
    transport.emit(running);
    transport.emit(textEvent("m1", "hi", true));
    transport.emit(idle);
    await turn;

    const replay = kernel.eventsAfter(id, 0);
    expect(replay.status).toBe("ok");
    if (replay.status !== "ok") return;
    expect(replay.events.map((e) => e.seq)).toEqual([1, 2, 3]);
    for (const event of replay.events) {
      expect(event.session_id).toBe(id);
      expect(event.transport).toBe("fake-rpc");
      expect(event.occurred_at).toBe("2026-09-07T00:00:00.000Z");
    }
    expect(kernel.eventCursor(id)).toBe(3);
  });

  it("returns an honest expiry verdict for evicted cursors", async () => {
    const factory = new FakeFactory("fake-rpc", "fake");
    const kernel = new InteractionKernel({
      transportFactories: [factory],
      ring: { maxEvents: 2 },
      ...fixedClock,
    });
    const started = await kernel.start({ provider: "fake", workspace: "/ws" });
    const transport = factory.created[0]!;
    transport.emit(running);
    transport.emit(textEvent("m1", "a", false));
    transport.emit(textEvent("m1", "b", true));
    await flush();
    const replay = kernel.eventsAfter(started.session_id, 0);
    expect(replay).toMatchObject({ status: "expired", cursor: 0, earliest_replayable_cursor: 1 });
  });

  it("tails live events and ends cleanly at session close", async () => {
    const { kernel, id, transport } = await booted();
    const seen: SessionEvent[] = [];
    const tail = (async () => {
      for await (const event of kernel.streamEvents(id, { after: 0 })) {
        seen.push(event);
      }
      return seen.length;
    })();

    transport.emit(running);
    transport.emit(textEvent("m1", "hi", false));
    for (let i = 0; seen.length < 2 && i < 20; i += 1) {
      await flush(1);
    }
    expect(seen.map((e) => e.seq)).toEqual([1, 2]);

    await kernel.close(id);
    transport.end();
    expect(await tail).toBe(2);
    // Late subscribers only ever see the committed tail.
    const after: SessionEvent[] = [];
    for await (const event of kernel.streamEvents(id, { after: 0 })) {
      after.push(event);
    }
    expect(after.map((e) => e.seq)).toEqual([1, 2]);
  });

  it("rejects a live tail behind an evicted cursor", async () => {
    const factory = new FakeFactory("fake-rpc", "fake");
    const kernel = new InteractionKernel({
      transportFactories: [factory],
      ring: { maxEvents: 2 },
      ...fixedClock,
    });
    const started = await kernel.start({ provider: "fake", workspace: "/ws" });
    const transport = factory.created[0]!;
    transport.emit(running);
    transport.emit(textEvent("m1", "a", false));
    transport.emit(textEvent("m1", "b", false));
    await flush();
    const iterator = kernel.streamEvents(started.session_id, { after: 0 })[Symbol.asyncIterator]();
    await expectCode(() => iterator.next(), "EVENT_CURSOR_EXPIRED");
  });
});
