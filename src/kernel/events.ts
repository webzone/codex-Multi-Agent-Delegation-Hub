import { AgentHubError } from "../errors.js";
import type { EventBody, SessionEvent } from "./contracts.js";

/**
 * Bounded per-session event ring for the interaction kernel.
 *
 * Producers (the pump) call `push` with kernel-stamped events whose seq
 * continues without gaps — across a `resume()` the ring is seeded with the
 * durable cursor so replay never re-consumes events. Consumers call
 * `readAfter` with the last seq they saw; a cursor with provably-evicted
 * holes behind it gets an honest `expired` verdict, never a dishonestly
 * short replay.
 */

export const RING_MAX_EVENTS = 4096;
export const RING_MAX_BYTES = 8 * 1024 * 1024;
export const EVENT_MAX_BYTES = 256 * 1024;

/** Serialized size of an event as the ring counts it. */
export function eventBytes(event: SessionEvent): number {
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
 * A shrinkable text position inside a cloned event body. `slot` is the
 * actual `BoundedText` object when one exists: writing through it sets the
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
function textLeaves(body: EventBody): TextLeaf[] {
  switch (body.kind) {
    case "status": {
      if (body.note === null) {
        return [];
      }
      // `note` is a plain kernel-carried string with no flag slot; shrinkage
      // is observable through the publish result instead.
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
      // Structured error messages are kernel-generated; shrinking stays
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

export interface EventPublishResult {
  /** The event as it must be stored: guaranteed within the per-event bound. */
  event: SessionEvent;
  /** True when anything was cut to fit — never silent. */
  truncated: boolean;
  bytes: number;
}

/**
 * Enforce the per-event byte bound by shrinking text-bearing fields. The
 * largest leaf is cut first, repeatedly, so a single huge field absorbs the
 * damage instead of every field losing the same absolute amount. The
 * caller's event is never mutated; truncation happens on a copied body.
 *
 * Throws `EVENT_TOO_LARGE` only when every text field is already empty and
 * the event still does not fit — a structurally oversized event is a
 * producer (transport) bug, not something to pad into compliance.
 */
export function boundEvent(event: SessionEvent, maxBytes = EVENT_MAX_BYTES): EventPublishResult {
  const initial = eventBytes(event);
  if (initial <= maxBytes) {
    return { event, truncated: false, bytes: initial };
  }

  const shrunk: SessionEvent = {
    session_id: event.session_id,
    seq: event.seq,
    transport: event.transport,
    occurred_at: event.occurred_at,
    body: structuredClone(event.body),
  };
  const leaves = textLeaves(shrunk.body);
  if (leaves.length === 0) {
    throw new AgentHubError(
      "EVENT_TOO_LARGE",
      `event (kind ${event.body.kind}) is ${initial} bytes with no truncatable text fields; the transport must bound this body itself`,
    );
  }

  let bytes = eventBytes(shrunk);
  while (bytes > maxBytes) {
    const excess = bytes - maxBytes;
    const candidates = leaves.filter((leaf) => leaf.size() > 0);
    if (candidates.length === 0) {
      throw new AgentHubError(
        "EVENT_TOO_LARGE",
        `event (kind ${event.body.kind}) remains ${bytes} bytes above the bound after every text field was emptied`,
      );
    }
    const leaf = candidates.reduce((best, current) =>
      current.size() > best.size() ? current : best,
    );
    leaf.write(truncateUtf8(leaf.read(), Math.max(0, leaf.size() - excess)));
    leaf.markTruncated();
    bytes = eventBytes(shrunk);
  }

  return { event: shrunk, truncated: true, bytes };
}

export interface EventRingOptions {
  maxEvents?: number;
  maxBytes?: number;
  maxEventBytes?: number;
  /**
   * Highest seq already durably consumed before a resume. The ring starts
   * gaplessly after it: `readAfter(cursor ≤ seed)` with nothing buffered is
   * a caught-up consumer, `cursor < seed` is an honest expiry.
   */
  seedCursor?: number;
}

export type EventReplay =
  | { status: "ok"; events: SessionEvent[]; next_cursor: number }
  | { status: "expired"; cursor: number; earliest_replayable_cursor: number };

export class EventRing {
  private readonly maxEvents: number;
  private readonly maxBytes: number;
  private readonly maxEventBytes: number;
  private events: SessionEvent[] = [];
  private bytes = 0;
  private totalSeen: number;

  constructor(options: EventRingOptions = {}) {
    this.maxEvents = options.maxEvents ?? RING_MAX_EVENTS;
    this.maxBytes = options.maxBytes ?? RING_MAX_BYTES;
    this.maxEventBytes = options.maxEventBytes ?? EVENT_MAX_BYTES;
    this.totalSeen = options.seedCursor ?? 0;
    if (!Number.isInteger(this.totalSeen) || this.totalSeen < 0) {
      throw new AgentHubError(
        "EVENT_CURSOR_INVALID",
        `seed cursor must be a non-negative integer, got ${String(options.seedCursor)}`,
      );
    }
  }

  /** Seq the next pushed event must carry. */
  get nextSeq(): number {
    return this.totalSeen + 1;
  }

  /** Highest seq the ring has stamped (the authoritative live cursor). */
  get latest(): number {
    return this.totalSeen;
  }

  /** Oldest seq still buffered, or null when nothing is buffered. */
  get oldestSeq(): number | null {
    return this.events.length > 0 ? this.events[0].seq : null;
  }

  push(event: SessionEvent): EventPublishResult {
    if (event.seq !== this.totalSeen + 1) {
      throw new AgentHubError(
        "EVENT_SEQ_GAP",
        `ring expected seq ${this.totalSeen + 1} but received ${event.seq}; event seqs must continue without gaps`,
      );
    }

    const bounded = boundEvent(event, this.maxEventBytes);
    if (bounded.bytes > this.maxBytes) {
      throw new AgentHubError(
        "EVENT_TOO_LARGE",
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
      this.bytes -= eventBytes(evicted);
    }

    return bounded;
  }

  /**
   * Replay everything after `cursor` (a seq the consumer already saw). A
   * cursor with provably-evicted (or pre-resume-consumed) events behind it
   * returns `expired` rather than a dishonestly short replay.
   */
  readAfter(cursor: number): EventReplay {
    if (!Number.isInteger(cursor) || cursor < 0) {
      throw new AgentHubError(
        "EVENT_CURSOR_INVALID",
        `cursor must be a non-negative integer seq, got ${String(cursor)}`,
      );
    }

    if (this.events.length === 0) {
      // Nothing is buffered. A consumer caught up to (or past) the durable
      // cursor is honestly satisfied; anything behind it refers to events
      // this ring never will have again.
      if (cursor < this.totalSeen) {
        return { status: "expired", cursor, earliest_replayable_cursor: this.totalSeen };
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

/**
 * One live tail of the ring. `emit`/`complete` are called by the pump;
 * iteration ends cleanly on completion and throws `EVENT_CURSOR_EXPIRED`
 * when the consumer fell so far behind that events were honestly dropped.
 */
export class EventSubscription {
  private readonly queue: SessionEvent[] = [];
  private waiting: ((result: IteratorResult<SessionEvent>) => void) | null = null;
  private failure: AgentHubError | null = null;
  private done = false;

  constructor(private readonly maxQueued: number) {}

  emit(event: SessionEvent): void {
    if (this.done) {
      return;
    }
    const waiting = this.waiting;
    if (waiting !== null) {
      this.waiting = null;
      waiting({ value: event, done: false });
      return;
    }
    if (this.queue.length >= this.maxQueued) {
      this.fail(
        new AgentHubError(
          "EVENT_CURSOR_EXPIRED",
          `event consumer fell ${this.maxQueued}+ events behind; resynchronize through eventsAfter with an explicit cursor`,
        ),
      );
      return;
    }
    this.queue.push(event);
  }

  complete(): void {
    if (this.done) {
      return;
    }
    this.done = true;
    const waiting = this.waiting;
    if (waiting !== null) {
      this.waiting = null;
      waiting({ value: undefined as never, done: true });
    }
  }

  fail(error: AgentHubError): void {
    if (this.done) {
      return;
    }
    this.done = true;
    this.failure = error;
    const waiting = this.waiting;
    if (waiting !== null) {
      this.waiting = null;
      waiting({ value: undefined as never, done: true });
    }
  }

  [Symbol.asyncIterator](): AsyncIterator<SessionEvent> {
    return {
      next: () => {
        if (this.queue.length > 0) {
          const event = this.queue.shift() as SessionEvent;
          return Promise.resolve({ value: event, done: false });
        }
        if (this.failure !== null) {
          return Promise.reject(this.failure);
        }
        if (this.done) {
          return Promise.resolve({ value: undefined as never, done: true });
        }
        return new Promise<IteratorResult<SessionEvent>>((resolve) => {
          this.waiting = resolve;
        });
      },
      return: () => {
        this.done = true;
        const waiting = this.waiting;
        if (waiting !== null) {
          this.waiting = null;
          waiting({ value: undefined as never, done: true });
        }
        return Promise.resolve({ value: undefined as never, done: true });
      },
    };
  }
}
