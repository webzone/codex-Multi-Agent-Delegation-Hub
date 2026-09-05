import { describe, expect, it } from "vitest";

import { AgentHubError } from "../src/errors.js";
import {
  boundLiveEvent,
  LiveEventRing,
  liveEventBytes,
  LIVE_EVENT_MAX_BYTES,
  LIVE_RING_MAX_BYTES,
  LIVE_RING_MAX_EVENTS,
} from "../src/live/events.js";
import type { LiveEvent, LiveEventBody } from "../src/live/types.js";

function makeEvent(seq: number, body: LiveEventBody, occurredAt = "2026-09-05T00:00:00.000Z"): LiveEvent {
  return {
    live_session_id: "11111111-2222-4333-8444-555555555555",
    seq,
    transport: "omp-rpc",
    occurred_at: occurredAt,
    body,
  };
}

function textBody(text: string): LiveEventBody {
  return {
    kind: "text",
    role: "assistant",
    stream_id: "s-1",
    text: { text, truncated: false },
    final: true,
  };
}

function expectThrowCode(run: () => unknown, code: string): void {
  try {
    run();
  } catch (error) {
    expect((error as AgentHubError).code).toBe(code);
    return;
  }
  expect.unreachable(`expected ${code} to be thrown`);
}

describe("frozen ring and event bounds", () => {
  it("matches the package contract", () => {
    expect(LIVE_RING_MAX_EVENTS).toBe(4096);
    expect(LIVE_RING_MAX_BYTES).toBe(8 * 1024 * 1024);
    expect(LIVE_EVENT_MAX_BYTES).toBe(256 * 1024);
  });
});

describe("boundLiveEvent", () => {
  it("passes small events through untouched", () => {
    const event = makeEvent(1, textBody("small"));
    const result = boundLiveEvent(event);
    expect(result.truncated).toBe(false);
    expect(result.event).toBe(event);
  });

  it("shrinks an oversized text event below the cap and flags truncation", () => {
    const huge = "字".repeat(120_000) + "é".repeat(200_000);
    const event = makeEvent(1, textBody(huge));
    expect(liveEventBytes(event)).toBeGreaterThan(LIVE_EVENT_MAX_BYTES);

    const result = boundLiveEvent(event);
    expect(result.truncated).toBe(true);
    expect(result.bytes).toBeLessThanOrEqual(LIVE_EVENT_MAX_BYTES);
    const body = result.event.body;
    if (body.kind !== "text") {
      expect.unreachable("kind must survive");
    }
    expect(body.text.truncated).toBe(true);
    expect(Buffer.byteLength(body.text.text, "utf8")).toBeLessThanOrEqual(LIVE_EVENT_MAX_BYTES);
    // No code point may be cut in half: a lone surrogate tail would serialize
    // as U+FFFD, and the tail must sit at a boundary we chose.
    expect(/[\uD800-\uDBFE]$/.test(body.text.text)).toBe(false);

    // The caller's event is never mutated in place.
    expect(event.body.kind === "text" && event.body.text.truncated).toBe(false);
  });

  it("flags truncation on every bounded slot it touches", () => {
    const body: LiveEventBody = {
      kind: "tool_start",
      call_id: "c1",
      tool: "edit",
      input_preview: { text: "x".repeat(300_000), truncated: false },
    };
    const result = boundLiveEvent(makeEvent(1, body));
    expect(result.truncated).toBe(true);
    expect(result.bytes).toBeLessThanOrEqual(LIVE_EVENT_MAX_BYTES);
    if (result.event.body.kind !== "tool_start") {
      expect.unreachable("kind must survive");
    }
    expect(result.event.body.input_preview?.truncated).toBe(true);
  });

  it("refuses structurally oversized events instead of padding them", () => {
    const event = makeEvent(1, { kind: "exit", intentional: false, exit_code: 1, exit_signal: null });
    const bloated: LiveEvent = { ...event, occurred_at: "z".repeat(300_000) };
    expectThrowCode(() => boundLiveEvent(bloated), "LIVE_EVENT_TOO_LARGE");
  });
});

describe("LiveEventRing seq discipline", () => {
  it("enforces no-gap seqs starting at 1", () => {
    const ring = new LiveEventRing();
    expect(ring.nextSeq).toBe(1);
    ring.push(makeEvent(1, textBody("a")));
    expectThrowCode(() => ring.push(makeEvent(3, textBody("b"))), "LIVE_EVENT_SEQ_GAP");
    ring.push(makeEvent(2, textBody("b")));
    expect(ring.nextSeq).toBe(3);
  });

  it("applies the per-event bound on push", () => {
    const ring = new LiveEventRing({ maxEventBytes: 1024 });
    const published = ring.push(makeEvent(1, textBody("y".repeat(5000))));
    expect(published.truncated).toBe(true);
    expect(published.bytes).toBeLessThanOrEqual(1024);
    const replay = ring.readAfter(0);
    if (replay.status !== "ok") {
      expect.unreachable("fresh ring replays");
    }
    const stored = replay.events[0];
    if (stored.body.kind !== "text") {
      expect.unreachable("kind must survive");
    }
    expect(stored.body.text.truncated).toBe(true);
  });
});

describe("LiveEventRing eviction", () => {
  it("evicts oldest events by count and leaves an observable gap", () => {
    const ring = new LiveEventRing({ maxEvents: 4 });
    for (let seq = 1; seq <= 6; seq += 1) {
      ring.push(makeEvent(seq, textBody(`e${seq}`)));
    }
    expect(ring.count).toBe(4);
    expect(ring.oldestSeq).toBe(3);

    const tail = ring.readAfter(2);
    expect(tail.status === "ok" && tail.events.map((e) => e.seq)).toEqual([3, 4, 5, 6]);

    const stale = ring.readAfter(1);
    expect(stale).toEqual({ status: "expired", cursor: 1, earliest_replayable_cursor: 2 });

    const gone = ring.readAfter(0);
    expect(gone.status).toBe("expired");
  });

  it("evicts by serialized bytes and keeps the ring within the bound", () => {
    const probe = makeEvent(1, textBody("q".repeat(400)));
    const perEvent = liveEventBytes(probe);
    const ring = new LiveEventRing({ maxBytes: perEvent * 3 });
    for (let seq = 1; seq <= 5; seq += 1) {
      ring.push(makeEvent(seq, textBody("q".repeat(400))));
      expect(ring.serializedBytes).toBeLessThanOrEqual(perEvent * 3);
    }
    expect(ring.count).toBe(3);
    expect(ring.oldestSeq).toBe(3);
  });

  it("keeps the newest event even when the ring budget is tight", () => {
    const ring = new LiveEventRing({ maxBytes: 320, maxEventBytes: 300 });
    ring.push(makeEvent(1, textBody("small")));
    const big = makeEvent(2, textBody("w".repeat(600)));
    const published = ring.push(big);
    expect(published.bytes).toBeLessThanOrEqual(300);
    expect(ring.oldestSeq).toBe(2);
  });
});

describe("LiveEventRing cursors", () => {
  it("treats the oldest-minus-one cursor as a full replay", () => {
    const ring = new LiveEventRing({ maxEvents: 3 });
    for (let seq = 1; seq <= 3; seq += 1) {
      ring.push(makeEvent(seq, textBody("x")));
    }
    const replay = ring.readAfter(0);
    expect(replay.status === "ok" && replay.events.map((e) => e.seq)).toEqual([1, 2, 3]);
    if (replay.status === "ok") {
      expect(replay.next_cursor).toBe(3);
    }
  });

  it("answers an ahead-of-world cursor honestly and empty", () => {
    const ring = new LiveEventRing();
    ring.push(makeEvent(1, textBody("x")));
    const replay = ring.readAfter(1);
    expect(replay.status === "ok" && replay.events).toEqual([]);
    expect(replay.status === "ok" && replay.next_cursor).toBe(1);
  });

  it("reports an empty ring as expired for any nonzero cursor", () => {
    const ring = new LiveEventRing();
    expect(ring.readAfter(0)).toEqual({ status: "ok", events: [], next_cursor: 0 });
    expect(ring.readAfter(5)).toEqual({ status: "expired", cursor: 5, earliest_replayable_cursor: 0 });
  });

  it("rejects impossible cursors", () => {
    const ring = new LiveEventRing();
    expectThrowCode(() => ring.readAfter(-1), "LIVE_EVENT_CURSOR_INVALID");
    expectThrowCode(() => ring.readAfter(1.5), "LIVE_EVENT_CURSOR_INVALID");
  });
});
