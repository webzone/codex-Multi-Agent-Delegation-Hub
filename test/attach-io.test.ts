import { describe, expect, it } from "vitest";

import {
  AttachInputPump,
  ATTACH_MAX_CHUNK_BYTES,
  ATTACH_MAX_LINE_BYTES,
  ATTACH_QUEUE_MAX_COMMANDS,
  ATTACH_QUEUE_MAX_BYTES,
} from "../src/hub/attach-io.js";

// ---------------------------------------------------------------------------
// Controllable stdin: the test pushes chunks, ends input at will, and every
// reader release (iterator.return) is counted. No wall-clock waits anywhere:
// each pull resolves exactly when the test pushes.
// ---------------------------------------------------------------------------

interface Harness {
  push(chunk: Uint8Array | string): void;
  end(): void;
  returnCalls(): number;
  /** Resolves once the pump has requested its first chunk. */
  firstPull(): Promise<void>;
  iterable: AsyncIterable<Uint8Array | string>;
}

function makeSource(): Harness {
  const chunks: (Uint8Array | string)[] = [];
  const waiters: ((result: IteratorResult<Uint8Array | string>) => void)[] = [];
  let ended = false;
  let returnCalls = 0;
  let firstPull: (() => void) | null = null;
  const firstPullPromise = new Promise<void>((resolve) => {
    firstPull = resolve;
  });
  const iterable: AsyncIterable<Uint8Array | string> = {
    [Symbol.asyncIterator]: () => ({
      next(): Promise<IteratorResult<Uint8Array | string>> {
        firstPull?.();
        const chunk = chunks.shift();
        if (chunk !== undefined) return Promise.resolve({ done: false, value: chunk });
        if (ended) return Promise.resolve({ done: true, value: undefined });
        return new Promise((resolve) => waiters.push(resolve));
      },
      return(): Promise<IteratorResult<Uint8Array | string>> {
        returnCalls += 1;
        ended = true;
        for (const resolve of waiters.splice(0)) resolve({ done: true, value: undefined });
        return Promise.resolve({ done: true, value: undefined });
      },
    }),
  };
  return {
    push(chunk) {
      const resolve = waiters.shift();
      if (resolve) resolve({ done: false, value: chunk });
      else chunks.push(chunk);
    },
    end() {
      ended = true;
      for (const resolve of waiters.splice(0)) resolve({ done: true, value: undefined });
    },
    returnCalls: () => returnCalls,
    firstPull: () => firstPullPromise,
    iterable,
  };
}

async function collect(pump: AttachInputPump): Promise<Array<string | "eof">> {
  const events: Array<string | "eof"> = [];
  for (;;) {
    const event = await pump.next();
    if (event === null) return events;
    events.push(event.kind === "eof" ? "eof" : event.value);
  }
}

function lines(count: number, filler: number): string {
  return (
    Array.from({ length: count }, (_, index) => `${"x".repeat(filler)}#${index}`)
      .join("\n") + "\n"
  );
}

describe("AttachInputPump framing", () => {
  it("frames LF/CRLF lines and survives UTF-8 codepoints split across chunks", async () => {
    const source = makeSource();
    const pump = new AttachInputPump(source.iterable);
    const bytes = Buffer.from('{"t":"café — ✓"}\r\n{"t":"plain"}\n', "utf8");
    for (const byte of bytes) source.push(Buffer.from([byte]));
    source.end();
    expect(await collect(pump)).toEqual(['{"t":"café — ✓"}', '{"t":"plain"}', "eof"]);
    expect(pump.readError).toBeNull();
  });

  it("delivers an immediate-EOF trailing line that has no newline", async () => {
    const source = makeSource();
    const pump = new AttachInputPump(source.iterable);
    source.push(Buffer.from("first\nsecond-partial", "utf8"));
    source.end();
    expect(await collect(pump)).toEqual(["first", "second-partial", "eof"]);
  });
});

describe("AttachInputPump hard bounds", () => {
  it("refuses a chunk above the hard chunk limit before enqueueing anything", async () => {
    const source = makeSource();
    const pump = new AttachInputPump(source.iterable);
    source.push(Buffer.alloc(ATTACH_MAX_CHUNK_BYTES + 1, 0x61));
    expect(await pump.next()).toEqual({ kind: "eof" });
    expect(await pump.next()).toBeNull();
    expect(pump.readError?.code).toBe("ATTACH_INPUT_CHUNK_TOO_LARGE");
    expect(pump.queuedCommands).toBe(0);
    expect(source.returnCalls()).toBe(1);
    await pump.settled();
  });

  it("holds a line-dense chunk at the count bound and delivers every line exactly once, in order", async () => {
    const total = ATTACH_QUEUE_MAX_COMMANDS + 172;
    const body = lines(total, 4);
    expect(Buffer.byteLength(body, "utf8")).toBeLessThan(ATTACH_MAX_CHUNK_BYTES);
    const source = makeSource();
    const pump = new AttachInputPump(source.iterable);
    source.push(Buffer.from(body, "utf8"));
    source.end();
    const first = await pump.next();
    // The whole 300-line chunk arrived at once; the queue stopped at the cap.
    expect(first?.kind === "line" && /^x+#0$/.test(first.value)).toBe(true);
    expect(pump.queuedCommands).toBe(ATTACH_QUEUE_MAX_COMMANDS - 1);
    const events = await collect(pump);
    const expected = Array.from({ length: total }, (_, index) => `xxxx#${index}`);
    expect([first?.value, ...events.slice(0, total - 1)]).toEqual(expected);
    expect(events[events.length - 1]).toBe("eof");
    expect(events.length).toBe(total); // 299 remaining lines + eof
    expect(pump.readError).toBeNull();
  });

  it("enforces the byte bound below the count bound", async () => {
    // 16 × 64 KiB lines hit the 1 MiB queue byte cap exactly; a second chunk
    // arrives while the queue is held. The count bound (128) is nowhere near
    // — only the byte bound can be stopping enqueue.
    const oneLine = `${"x".repeat(64 * 1024 - 1)}\n`;
    const block = oneLine.repeat(16); // exactly 1 MiB
    expect(Buffer.byteLength(block, "utf8")).toBe(ATTACH_MAX_CHUNK_BYTES);
    const total = 21; // 16 accepted into the queue + 5 held pending
    const source = makeSource();
    const pump = new AttachInputPump(source.iterable);
    source.push(Buffer.from(block, "utf8"));
    await source.firstPull();
    source.push(oneLine.repeat(5));
    source.end();
    await pump.next();
    expect(pump.queuedCommands).toBeLessThan(ATTACH_QUEUE_MAX_COMMANDS);
    expect(pump.queuedBytes).toBeLessThanOrEqual(ATTACH_QUEUE_MAX_BYTES);
    const events = await collect(pump);
    expect(events[events.length - 1]).toBe("eof");
    expect(events.length).toBe(total); // 20 remaining lines + eof
  });
  it("fails closed on a completed line above the hard line limit", async () => {
    const source = makeSource();
    const pump = new AttachInputPump(source.iterable);
    // A maximal chunk with no newline, then a chunk that COMPLETES the line:
    // the completed line exceeds the hard per-line limit (neither chunk did).
    source.push(Buffer.alloc(ATTACH_MAX_LINE_BYTES, 0x61));
    await source.firstPull();
    source.push(Buffer.from("a\n", "utf8"));
    source.end();
    expect(await pump.next()).toEqual({ kind: "eof" });
    expect(pump.readError?.code).toBe("ATTACH_INPUT_LINE_TOO_LARGE");
    expect(pump.queuedCommands).toBe(0);
  });

  it("fails closed on an unterminated partial line above the hard line limit", async () => {
    const source = makeSource();
    const pump = new AttachInputPump(source.iterable);
    source.push(Buffer.alloc(600 * 1024, 0x62));
    await source.firstPull();
    source.push(Buffer.alloc(600 * 1024, 0x62));
    source.end();
    expect(await pump.next()).toEqual({ kind: "eof" });
    expect(pump.readError?.code).toBe("ATTACH_INPUT_PARTIAL_LINE_TOO_LARGE");
    expect(pump.queuedCommands).toBe(0);
  });
});

describe("AttachInputPump dispose", () => {
  it("settles without EOF while a live reader still holds stdin", async () => {
    const source = makeSource();
    const pump = new AttachInputPump(source.iterable);
    await source.firstPull(); // stdin open, writer never writes, never ends
    pump.dispose();
    await pump.settled(); // must not hang on the open stream
    expect(await pump.next()).toBeNull();
    expect(source.returnCalls()).toBe(1);
  });

  it("releases a full-queue hold and refuses the rest of the backlog", async () => {
    const source = makeSource();
    const pump = new AttachInputPump(source.iterable);
    source.push(Buffer.from(lines(ATTACH_QUEUE_MAX_COMMANDS + 10, 2), "utf8"));
    await pump.next();
    pump.dispose();
    expect(await pump.next()).toBeNull();
    await pump.settled();
    expect(source.returnCalls()).toBe(1);
  });

  it("is idempotent and resolves waitForBufferedCommand as false", async () => {
    const source = makeSource();
    const pump = new AttachInputPump(source.iterable);
    const buffered = pump.waitForBufferedCommand();
    pump.dispose();
    pump.dispose();
    expect(await buffered).toBe(false);
    await pump.settled();
  });

  it("resolves waitForBufferedCommand true for a line queued before the session exists", async () => {
    const source = makeSource();
    const pump = new AttachInputPump(source.iterable);
    const buffered = pump.waitForBufferedCommand();
    source.push(Buffer.from('{"action":"prompt","text":"early"}\n', "utf8"));
    expect(await buffered).toBe(true);
    expect((await pump.next())?.value).toBe('{"action":"prompt","text":"early"}');
    pump.dispose();
    await pump.settled();
  });
});

