import { StringDecoder } from "node:string_decoder";
import { deferred, type Deferred } from "../deferred.js";

/**
 * Eager attach input — the shared, provider-neutral stdin lifecycle for
 * attached hub sessions (CLI wire; anything else that hosts one).
 *
 * The single consumer of the process's stdin stream attaches BEFORE the
 * provider and workspace startup, so a command written during the handshake
 * (or before the process even resolved its repository) is queued, framed,
 * and delivered exactly once after the session exists. One pump per run
 * means pipe, FIFO, and redirected-file stdin all flow through the same
 * reader — there is never a second consumer to steal bytes.
 *
 * Framing is incremental UTF-8 (`StringDecoder`, so codepoints split across
 * chunks survive) plus newline splitting; CRLF and LF both terminate lines.
 * Every bound is HARD and checked BEFORE anything is enqueued:
 *   - per chunk: a single read larger than `ATTACH_MAX_CHUNK_BYTES` is
 *     refused without being inspected;
 *   - per (partial) line: neither a completed command nor an unterminated
 *     partial may exceed `ATTACH_MAX_LINE_BYTES`. The check runs at every
 *     chunk boundary, so a giant chunk cannot grow an unbounded partial:
 *     between pulls the pending text is always one pure partial line
 *     (≤ the line limit — holds at queue bounds end at line boundaries
 *     and block further pulls), plus the single chunk being framed.
 *   - per queue: `ATTACH_QUEUE_MAX_COMMANDS` and `ATTACH_QUEUE_MAX_BYTES`,
 *     enforced line-by-line during framing, not after a whole chunk lands —
 *     one huge chunk can therefore never enqueue unlimited lines.
 *
 * When the queue is full the reader PAUSES: it stops pulling chunks and
 * holds the already-read remainder at a line boundary until the consumer
 * drains. A bound that cannot be honoured (chunk/line/partial overflow)
 * FAILS CLOSED: the pump stops reading, records `readError`, and never
 * buffers the offending bytes — the wire surfaces the error and exits 1.
 *
 * `dispose()` is cancellable: it releases the underlying stream reader,
 * wakes every waiter, and never awaits stdin EOF or the reader's own
 * teardown, so closing the wire cannot hang on a TTY/FIFO writer that
 * still holds stdin open.
 */

export type AttachInputEvent =
  | { kind: "line"; value: string }
  | { kind: "eof" };

export const ATTACH_QUEUE_MAX_COMMANDS = 128;
export const ATTACH_QUEUE_MAX_BYTES = 1_048_576;
export const ATTACH_MAX_LINE_BYTES = 1_048_576;
export const ATTACH_MAX_CHUNK_BYTES = 1_048_576;
/** Canonical names used by the public hub façade. */
export const ATTACH_LINE_MAX_BYTES = ATTACH_MAX_LINE_BYTES;
export const ATTACH_CHUNK_MAX_BYTES = ATTACH_MAX_CHUNK_BYTES;

const DISPOSED = Symbol("attach-input-disposed");

export class AttachInputPump {
  private readonly lines: string[] = [];
  private readonly waiters = new Set<() => void>();
  private bufferedBytes = 0;
  private pending = "";
  private ended = false;
  private endDelivered = false;
  private holdingForSpace = false;
  private disposed = false;
  private failed = false;
  private readonly iterator: AsyncIterator<Uint8Array | string>;
  private readonly disposedSignal: Deferred<typeof DISPOSED> = deferred();
  private readonly reading: Promise<void>;
  readError: { code: string; message: string } | null = null;

  constructor(stdin: AsyncIterable<Uint8Array | string>) {
    this.iterator = stdin[Symbol.asyncIterator]();
    this.reading = this.run();
  }

  /** True while the queue is at its bound and the pump is holding stdin. */
  get pausedForSpace(): boolean {
    return this.holdingForSpace;
  }

  /** Commands currently queued — the count bound is externally observable. */
  get queuedCommands(): number {
    return this.lines.length;
  }

  /** Queued bytes currently held — the byte bound is externally observable. */
  get queuedBytes(): number {
    return this.bufferedBytes;
  }

  /**
   * Resolves true once a complete command line is queued (possibly before
   * the session even exists), false when stdin ended or was disposed with
   * nothing buffered. Lets hosts prove handshake-ordering deterministically.
   */
  async waitForBufferedCommand(): Promise<boolean> {
    for (;;) {
      if (this.lines.length > 0) return true;
      if (this.ended || this.disposed) return false;
      await this.once();
    }
  }

  /** Next event in arrival order; the EOF event only follows all lines. */
  async next(): Promise<AttachInputEvent | null> {
    for (;;) {
      if (this.disposed) return null;
      if (this.lines.length > 0) {
        const value = this.lines.shift() as string;
        this.bufferedBytes -= Buffer.byteLength(value, "utf8");
        this.notify();
        return { kind: "line", value };
      }
      if (this.ended) {
        if (this.endDelivered) return null;
        this.endDelivered = true;
        return { kind: "eof" };
      }
      await this.once();
    }
  }

  /** Resolves when the pump is done reading: EOF, failure, or dispose. */
  settled(): Promise<void> {
    return this.reading;
  }

  /**
   * Cancel reading immediately. Releases the underlying reader and wakes
   * every waiter; it NEVER awaits stdin EOF or the reader's teardown, so an
   * explicit close cannot hang with a TTY/FIFO writer still holding stdin.
   * Idempotent. Lines still queued after a dispose are abandoned by design:
   * a closed wire stops accepting new commands (already-dispatched work is
   * the host's `inFlight` concern, and it settles normally).
   */
  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    try {
      const closing = this.iterator.return?.(undefined);
      if (closing && typeof (closing as Promise<unknown>).catch === "function") {
        void (closing as Promise<unknown>).catch(() => undefined);
      }
    } catch {
      // The reader is already gone; the pump is stopping regardless.
    }
    this.disposedSignal.resolve(DISPOSED);
    this.notify();
  }

  private once(): Promise<void> {
    return new Promise((resolve) => this.waiters.add(resolve));
  }

  private notify(): void {
    const waiters = [...this.waiters];
    this.waiters.clear();
    for (const resolve of waiters) resolve();
  }

  /** Overflow policy: stop reading now, surface honestly, buffer nothing. */
  private failClosed(code: string, detail: string): void {
    this.pending = "";
    this.failed = true;
    this.readError = { code, message: detail };
    try {
      const closing = this.iterator.return?.(undefined);
      if (closing && typeof (closing as Promise<unknown>).catch === "function") {
        void (closing as Promise<unknown>).catch(() => undefined);
      }
    } catch {
      // As above: nothing left to release.
    }
    this.notify();
  }

  private async run(): Promise<void> {
    const decoder = new StringDecoder("utf8");
    try {
      for (;;) {
        if (this.disposed || this.failed) break;
        const step: IteratorResult<Uint8Array | string> | typeof DISPOSED =
          await Promise.race([this.iterator.next(), this.disposedSignal.promise]);
        if (step === DISPOSED || this.disposed || this.failed) break;
        if (step.done) break;
        const chunk = step.value;
        const chunkBytes =
          typeof chunk === "string" ? Buffer.byteLength(chunk, "utf8") : chunk.byteLength;
        if (chunkBytes > ATTACH_MAX_CHUNK_BYTES) {
          this.failClosed(
            "ATTACH_INPUT_CHUNK_TOO_LARGE",
            `a stdin chunk of ${chunkBytes} bytes exceeds the ${ATTACH_MAX_CHUNK_BYTES}-byte hard chunk limit; the wire fails closed`,
          );
          break;
        }
        this.frame(typeof chunk === "string" ? chunk : decoder.write(Buffer.from(chunk)));
        if (this.failed) break;
        await this.drain();
        if (this.disposed || this.failed) break;
      }
    } catch (error) {
      // A failed stdin is EOF for the wire; the failure is surfaced, never swallowed.
      if (!this.failed) {
        this.readError = {
          code: "ATTACH_INPUT_FAILED",
          message: error instanceof Error ? error.message : String(error),
        };
      }
    }
    if (!this.disposed && !this.failed) {
      this.frame(decoder.end());
      if (!this.failed) {
        // The stream ended: hold chunk pulls, but every framed command and
        // the final unterminated line must still be deliverable.
        await this.drain();
        const tail = this.pending.trim();
        this.pending = "";
        if (tail.length > 0) {
          const tailBytes = Buffer.byteLength(tail, "utf8");
          if (tailBytes > ATTACH_MAX_LINE_BYTES) {
            this.failed = true;
            this.readError = {
              code: "ATTACH_INPUT_LINE_TOO_LARGE",
              message: `the final unterminated ${tailBytes}-byte line exceeds the ${ATTACH_MAX_LINE_BYTES}-byte hard line limit; it is not delivered`,
            };
          } else {
            await this.awaitSpace();
            if (!this.disposed) {
              this.lines.push(tail);
              this.bufferedBytes += tailBytes;
              this.notify();
            }
          }
        }
      }
    }
    this.ended = true;
    this.notify();
  }

  /** Frame every line the bounds allow, pausing for consumer space. */
  private async drain(): Promise<void> {
    for (;;) {
      this.frame("");
      if (this.failed || this.disposed) return;
      if (!this.full()) return;
      await this.awaitSpace();
    }
  }

  /** Pause chunk pulls while the queue is at its bound. */
  private async awaitSpace(): Promise<void> {
    while (this.full() && !this.disposed && !this.failed) {
      this.holdingForSpace = true;
      await Promise.race([this.once(), this.disposedSignal.promise]);
      this.holdingForSpace = false;
    }
  }

  private frame(text: string): void {
    if (text.length > 0) this.pending += text;
    for (;;) {
      const index = this.pending.indexOf("\n");
      if (index < 0) break;
      const line = this.pending.slice(0, index).trim();
      const lineBytes = Buffer.byteLength(line, "utf8");
      if (lineBytes > ATTACH_MAX_LINE_BYTES) {
        this.failClosed(
          "ATTACH_INPUT_LINE_TOO_LARGE",
          `a ${lineBytes}-byte command exceeds the ${ATTACH_MAX_LINE_BYTES}-byte hard line limit; the wire fails closed`,
        );
        return;
      }
      if (line.length > 0 && this.full()) break;
      this.pending = this.pending.slice(index + 1);
      if (line.length > 0) {
        this.lines.push(line);
        this.bufferedBytes += lineBytes;
        this.notify();
      }
    }
    if (
      this.pending.length > 0 &&
      this.pending.indexOf("\n") < 0 &&
      Buffer.byteLength(this.pending, "utf8") > ATTACH_MAX_LINE_BYTES
    ) {
      this.failClosed(
        "ATTACH_INPUT_PARTIAL_LINE_TOO_LARGE",
        `an unterminated command line already exceeds the ${ATTACH_MAX_LINE_BYTES}-byte hard line limit; the wire fails closed`,
      );
    }
  }

  private full(): boolean {
    return (
      this.lines.length >= ATTACH_QUEUE_MAX_COMMANDS ||
      this.bufferedBytes >= ATTACH_QUEUE_MAX_BYTES
    );
  }
}
