import { describe, expect, it } from "vitest";

import { AgentHubError } from "../src/errors.js";
import {
  parseResumeState,
  parseSessionRecord,
  validateCapabilities,
} from "../src/kernel/contracts.js";
import { boundEvent, EventRing, eventBytes } from "../src/kernel/events.js";
import type { SessionEvent } from "../src/kernel/contracts.js";
import { fullCapabilities, sessionRecord, textEvent } from "./kernel-fakes.js";

function expectCode(run: () => unknown, code: string): AgentHubError {
  try {
    run();
  } catch (error) {
    expect(error).toBeInstanceOf(AgentHubError);
    expect((error as AgentHubError).code).toBe(code);
    return error as AgentHubError;
  }
  throw new Error(`expected ${code} to be thrown, nothing was`);
}

function event(seq: number, body: SessionEvent["body"] = textEvent("s1", "hi", false)): SessionEvent {
  return {
    session_id: "s",
    seq,
    transport: "fake-rpc",
    occurred_at: "2026-09-07T00:00:00.000Z",
    body,
  };
}

describe("validateCapabilities", () => {
  it("refuses a snapshot that omits a capability (silence is not support)", () => {
    const partial = { ...fullCapabilities() } as Record<string, unknown>;
    delete partial.steer;
    const error = expectCode(() => validateCapabilities(partial), "CAPABILITY_SNAPSHOT_INVALID");
    expect(error.message).toContain("steer");
  });

  it("refuses unknown capability names", () => {
    const extra = { ...fullCapabilities(), teleport: { support: "native", evidence: "x" } };
    const error = expectCode(() => validateCapabilities(extra), "CAPABILITY_SNAPSHOT_INVALID");
    expect(error.message).toContain('unknown capability "teleport"');
  });

  it("enforces the evidence rule in both directions", () => {
    expectCode(
      () => validateCapabilities({ ...fullCapabilities(), prompt: { support: "native", evidence: "" } }),
      "CAPABILITY_SNAPSHOT_INVALID",
    );
    expectCode(
      () =>
        validateCapabilities({
          ...fullCapabilities(),
          steer: { support: "unsupported", evidence: "secretly works" },
        }),
      "CAPABILITY_SNAPSHOT_INVALID",
    );
  });

  it("refuses derived on an injectable command kind but allows it for status", () => {
    expectCode(
      () => validateCapabilities({ ...fullCapabilities(), prompt: { support: "derived", evidence: "x" } }),
      "CAPABILITY_SNAPSHOT_INVALID",
    );
    const ok = validateCapabilities({
      ...fullCapabilities(),
      status: { support: "derived", evidence: "hub answers from stream evidence" },
    });
    expect(ok.status.support).toBe("derived");
  });

  it("accepts a total honest snapshot", () => {
    expect(() => validateCapabilities(fullCapabilities())).not.toThrow();
  });
});

describe("parseSessionRecord", () => {
  it("round-trips an honest record", () => {
    const record = sessionRecord();
    expect(parseSessionRecord(record)).toEqual(record);
  });

  it("refuses foreign schema ids", () => {
    expectCode(
      () => parseSessionRecord({ ...sessionRecord(), schema: "agent-hub-live/v1" }),
      "SESSION_RECORD_INVALID",
    );
  });

  it("drops unknown keys instead of smuggling them through", () => {
    const polluted = { ...sessionRecord(), task_text: "never stored", extra: 7 };
    const keys = Object.keys(parseSessionRecord(polluted));
    expect(keys).not.toContain("task_text");
    expect(keys).not.toContain("extra");
  });

  it("refuses records whose capability snapshot lies", () => {
    const lying = sessionRecord();
    (lying.capabilities as Record<string, unknown>).cancel = { support: "native" };
    expectCode(() => parseSessionRecord(lying), "CAPABILITY_SNAPSHOT_INVALID");
  });

  it("refuses verified resume handles without a basis", () => {
    const record = sessionRecord();
    record.resume = {
      provider: "fake",
      provider_session_id: "p",
      data: {},
      last_event_seq: 0,
      verified: true,
      verified_via: null as unknown as string,
    };
    expectCode(() => parseSessionRecord(record), "SESSION_RECORD_INVALID");
  });
});

describe("parseResumeState", () => {
  it("requires a plain-object payload", () => {
    expectCode(
      () => parseResumeState({ provider: "fake", provider_session_id: null, data: "wire-bytes", last_event_seq: 0, verified: false, verified_via: null }),
      "SESSION_RECORD_INVALID",
    );
  });

  it("keeps verified true only with verified_via", () => {
    const good = parseResumeState({
      provider: "fake",
      provider_session_id: "p",
      data: { token: "t" },
      last_event_seq: 12,
      verified: true,
      verified_via: "hub-resume:fake-rpc",
    });
    expect(good.verified).toBe(true);
    expect(good.last_event_seq).toBe(12);
  });
});

describe("EventRing", () => {
  it("enforces gapless seqs", () => {
    const ring = new EventRing();
    ring.push(event(1));
    expectCode(() => ring.push(event(3)), "EVENT_SEQ_GAP");
  });

  it("reports expiry honestly when events were evicted", () => {
    const ring = new EventRing({ maxEvents: 2 });
    ring.push(event(1));
    ring.push(event(2));
    ring.push(event(3));
    const stale = ring.readAfter(0);
    expect(stale.status).toBe("expired");
    if (stale.status === "expired") {
      expect(stale.earliest_replayable_cursor).toBe(1);
    }
    const fresh = ring.readAfter(1);
    expect(fresh.status).toBe("ok");
    if (fresh.status === "ok") {
      expect(fresh.events.map((e) => e.seq)).toEqual([2, 3]);
      expect(fresh.next_cursor).toBe(3);
    }
  });

  it("treats a seeded resume cursor as caught-up, not expired", () => {
    const ring = new EventRing({ seedCursor: 41 });
    expect(ring.nextSeq).toBe(42);
    expect(ring.readAfter(41).status).toBe("ok");
    const behind = ring.readAfter(0);
    expect(behind.status).toBe("expired");
    if (behind.status === "expired") {
      expect(behind.earliest_replayable_cursor).toBe(41);
    }
    ring.push(event(42));
    expect(ring.readAfter(41)).toMatchObject({ status: "ok", next_cursor: 42 });
  });

  it("rejects negative or fractional cursors", () => {
    const ring = new EventRing();
    expectCode(() => ring.readAfter(-1), "EVENT_CURSOR_INVALID");
    expectCode(() => ring.readAfter(1.5), "EVENT_CURSOR_INVALID");
  });
});

describe("boundEvent", () => {
  it("shrinks the largest text leaf and flags it, without mutating the input", () => {
    const big = event(1, textEvent("s1", "字".repeat(2000), false));
    const before = JSON.stringify(big);
    const bounded = boundEvent(big, 512);
    expect(bounded.truncated).toBe(true);
    expect(bounded.bytes).toBeLessThanOrEqual(512);
    if (bounded.event.body.kind === "text") {
      expect(bounded.event.body.text.truncated).toBe(true);
      expect(bounded.event.body.text.text.length).toBeGreaterThan(0);
    }
    expect(JSON.stringify(big)).toBe(before);
  });

  it("refuses structurally oversized bodies instead of padding", () => {
    const hugeStatus: SessionEvent = event(1, {
      kind: "status",
      status: "running",
      note: "x".repeat(5000),
    });
    expect(eventBytes(boundEvent(hugeStatus, 512).event)).toBeLessThanOrEqual(512);
    // A usage event has no text at all: the transport must bound it itself.
    const usage: SessionEvent = event(1, {
      kind: "usage",
      usage: { input_tokens: 1, output_tokens: 1, cached_tokens: null, cost_usd: null },
    });
    const manyEvents = { ...usage, body: { ...usage.body, extra: "y".repeat(1000) } };
    expectCode(
      () => boundEvent(manyEvents as unknown as SessionEvent, 60),
      "EVENT_TOO_LARGE",
    );
  });
});
