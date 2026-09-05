import { AgentHubError } from "../errors.js";
import type { LiveEvent, LiveEventBody } from "./types.js";

/**
 * Bounded per-session event ring (v3 live, Package 1).
 *
 * Three independent bounds, all observable:
 *   - at most `LIVE_RING_MAX_EVENTS` events and `LIVE_RING_MAX_BYTES` of
 *     serialized bytes per session; pushing past either evicts the *oldest*
 *     events, and every eviction is a provable seq gap for later cursors;
 *   - no single event may serialize above `LIVE_EVENT_MAX_BYTES`; oversized
 *     events are shrunk by truncating the text-bearing fields in their own
 *     bodies (`truncated: true` lands on every `LiveBoundedText` touched),
 *     never by dropping the event silently;
 *   - `seq` continuity is enforced on push: the ring is the single place that
 *     can tell a real gap (eviction) from a broken producer, so a producer
 *     that skips a seq is rejected outright.
 *
 * Cursor rule: a consumer replays with the last `seq` it saw. If events it
 * never saw were evicted underneath it (`cursor + 1 < oldest buffered seq`),
 * the replay cannot be honest — it would silently skip work — so the read
 * reports `expired` (surfaced as `EVENT_CURSOR_EXPIRED` by callers) together
 * with the oldest cursor still replayable; the consumer resynchronizes from
 * durable state instead of from the ring.
 */

export const LIVE_RING_MAX_EVENTS = 4096;
export const LIVE_RING_MAX_BYTES = 8 * 1024 * 1024;
export const LIVE_EVENT_MAX_BYTES = 256 * 1024;

/** Serialized size of an event as the ring counts it. */
export function liveEventBytes(event: LiveEvent): number {
  return Buffer.byteLength(JSON.stringify(event), "utf8");
}

/** Cut `text` to at most `maxBytes` of UTF-8 without splitting a code point. */
export function truncateUtf8(text: string, maxBytes: number): string {
  if (Buffer.byteLength(text, "utf8") <= maxBytes) {
    return text;
  }
  if (maxBytes <= 0) {
    return "";
  }
  // The byte slice may end mid-sequence; toString replaces the remainder with
  // a U+FFFD that can overshoot the bound again, so trim code points until fit.
  let cut = Buffer.from(text, "utf8").subarray(0, maxBytes).toString("utf8");
  while (Buffer.byteLength(cut, "utf8") > maxBytes) {
    cut = cut.slice(0, -1);
  }
  return cut;
}

/**
 * A shrinkable text position inside a cloned event body. `slot` is the actual
 * `LiveBoundedText` object when one exists: writing through it sets the
 * truncation flag on the very object the stored event will serialize.
 */
interface TextLeaf {
  read(): string;
  write(value: string): void;
  markTruncated(): void;
  size(): number;
}

function boundedLeaf(slot: { text: string; truncated: boolean }): TextLeaf {
  return {
    read: () => slot.text,
    write: (value) => {
      slot.text = value;
      slot.truncated = true;
    },
    markTruncated: () => {
      slot.truncated = true;
    },
    size: () => Buffer.byteLength(slot.text, "utf8"),
  };
}

function plainLeaf(read: () => string, write: (value: string) => void): TextLeaf {
  return {
    read,
    write,
    markTruncated: () => {},
    size: () => Buffer.byteLength(read(), "utf8"),
  };
}

/** Text-bearing positions of a body; every branch enumerates honestly. */
function textLeaves(body: LiveEventBody): TextLeaf[] {
  switch (body.kind) {
    case "status": {
      if (body.note === null) {
        return [];
      }
      // `note` is a plain hub-carried string with no flag slot; shrinkage is
      // observable through the publish result instead.
      return [plainLeaf(() => body.note ?? "", (v) => { body.note = v; })];
    }
    case "text":
      return [boundedLeaf(body.text)];
    case "tool_start": {
      const preview = body.input_preview;
      return preview === null ? [] : [boundedLeaf(preview)];
    }
    case "tool_end": {
      const preview = body.output_preview;
      return preview === null ? [] : [boundedLeaf(preview)];
    }
    case "permission_request":
      return [boundedLeaf(body.summary)];
    case "log":
      return [boundedLeaf(body.text)];
    case "error":
      // Structured error messages are hub-generated; shrinking stays
      // observable through the publish result.
      return [plainLeaf(() => body.error.message, (v) => { body.error.message = v; })];
    case "unrecognized": {
      if (body.transport_kind === null) {
        return [];
      }
      return [plainLeaf(() => body.transport_kind ?? "", (v) => { body.transport_kind = v; })];
    }
    default:
      // `usage` and `exit` carry no text at all.
      return [];
  }
}

export interface LiveEventPublishResult {
  /** The event as it must be stored: guaranteed within the per-event bound. */
  event: LiveEvent;
  /** True when anything was cut to fit — never silent. */
  truncated: boolean;
  bytes: number;
}

/**
 * Enforce the per-event byte bound by shrinking text-bearing fields. The
 * largest leaf is cut first, repeatedly, so a single huge field absorbs most
 * of the damage instead of every field losing the same absolute amount. The
 * caller's event is never mutated; truncation happens on a copied body.
 *
 * Throws `LIVE_EVENT_TOO_LARGE` only when every text field is already empty
 * and the event still does not fit — a structurally oversized event is a
 * producer (transport) bug, not something to pad into compliance.
 */
export function boundLiveEvent(
  event: LiveEvent,
  maxBytes = LIVE_EVENT_MAX_BYTES,
): LiveEventPublishResult {
  const initial = liveEventBytes(event);
  if (initial <= maxBytes) {
    return { event, truncated: false, bytes: initial };
  }

  const shrunk: LiveEvent = {
    live_session_id: event.live_session_id,
    seq: event.seq,
    transport: event.transport,
    occurred_at: event.occurred_at,
    body: structuredClone(event.body),
  };
  const leaves = textLeaves(shrunk.body);
  if (leaves.length === 0) {
    throw new AgentHubError(
      "LIVE_EVENT_TOO_LARGE",
      `event (kind ${event.body.kind}) is ${initial} bytes with no truncatable text fields; the transport must bound this body itself`,
    );
  }

  let bytes = liveEventBytes(shrunk);
  while (bytes > maxBytes) {
    const excess = bytes - maxBytes;
    const candidates = leaves.filter((leaf) => leaf.size() > 0);
    if (candidates.length === 0) {
      throw new AgentHubError(
        "LIVE_EVENT_TOO_LARGE",
        `event (kind ${event.body.kind}) remains ${bytes} bytes above the bound after every text field was emptied`,
      );
    }
    const leaf = candidates.reduce((best, current) =>
      current.size() > best.size() ? current : best,
    );
    leaf.write(truncateUtf8(leaf.read(), Math.max(0, leaf.size() - excess)));
    leaf.markTruncated();
    bytes = liveEventBytes(shrunk);
  }

  return { event: shrunk, truncated: true, bytes };
}

export interface LiveEventRingOptions {
  maxEvents?: number;
  maxBytes?: number;
  maxEventBytes?: number;
}

export type LiveEventReplay =
  | { status: "ok"; events: LiveEvent[]; next_cursor: number }
  | { status: "expired"; cursor: number; earliest_replayable_cursor: number };

/**
 * Ring of recent events for one live session. Producers call `push` with
 * hub-stamped events (seq must continue without gaps); consumers call
 * `readAfter` with the last seq they saw.
 */
export class LiveEventRing {
  private readonly maxEvents: number;
  private readonly maxBytes: number;
  private readonly maxEventBytes: number;
  private events: LiveEvent[] = [];
  private bytes = 0;
  private totalSeen = 0;

  constructor(options: LiveEventRingOptions = {}) {
    this.maxEvents = options.maxEvents ?? LIVE_RING_MAX_EVENTS;
    this.maxBytes = options.maxBytes ?? LIVE_RING_MAX_BYTES;
    this.maxEventBytes = options.maxEventBytes ?? LIVE_EVENT_MAX_BYTES;
  }

  get count(): number {
    return this.events.length;
  }

  get serializedBytes(): number {
    return this.bytes;
  }

  /** Seq the next pushed event must carry (1 before the first event). */
  get nextSeq(): number {
    return this.totalSeen + 1;
  }

  /** Oldest seq still buffered, or null when nothing is buffered. */
  get oldestSeq(): number | null {
    return this.events.length > 0 ? this.events[0].seq : null;
  }

  push(event: LiveEvent): LiveEventPublishResult {
    if (event.seq !== this.totalSeen + 1) {
      throw new AgentHubError(
        "LIVE_EVENT_SEQ_GAP",
        `ring expected seq ${this.totalSeen + 1} but received ${event.seq}; event seqs must start at 1 with no gaps`,
      );
    }

    const bounded = boundLiveEvent(event, this.maxEventBytes);
    if (bounded.bytes > this.maxBytes) {
      throw new AgentHubError(
        "LIVE_EVENT_TOO_LARGE",
        `event of ${bounded.bytes} bytes cannot fit the ${this.maxBytes}-byte ring at all`,
      );
    }

    this.events.push(bounded.event);
    this.bytes += bounded.bytes;
    this.totalSeen = bounded.event.seq;

    while (this.events.length > this.maxEvents || this.bytes > this.maxBytes) {
      const evicted = this.events.shift();
      if (evicted === undefined) {
        break;
      }
      this.bytes -= liveEventBytes(evicted);
    }

    return bounded;
  }

  /**
   * Replay everything after `cursor` (a seq the consumer already saw; 0 for
   * from-the-start). A cursor with provably-evicted holes behind it returns
   * `expired` rather than a dishonestly short replay.
   */
  readAfter(cursor: number): LiveEventReplay {
    if (!Number.isInteger(cursor) || cursor < 0) {
      throw new AgentHubError(
        "LIVE_EVENT_CURSOR_INVALID",
        `cursor must be a non-negative integer seq, got ${String(cursor)}`,
      );
    }

    if (this.events.length === 0) {
      // Nothing is buffered. Only a consumer that has seen nothing (cursor 0)
      // can be honestly satisfied; any other cursor refers to events this
      // ring no longer has.
      if (cursor > 0) {
        return { status: "expired", cursor, earliest_replayable_cursor: 0 };
      }
      return { status: "ok", events: [], next_cursor: this.totalSeen };
    }

    const oldest = this.events[0].seq;
    if (cursor + 1 < oldest) {
      return { status: "expired", cursor, earliest_replayable_cursor: oldest - 1 };
    }

    const events = this.events.filter((event) => event.seq > cursor);
    return {
      status: "ok",
      events,
      next_cursor: events.length > 0
        ? events[events.length - 1].seq
        : Math.max(cursor, this.totalSeen),
    };
  }
}
