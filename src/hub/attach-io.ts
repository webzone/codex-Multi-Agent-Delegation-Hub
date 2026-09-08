import { StringDecoder } from "node:string_decoder";

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
 * Bounds, all enforced BEFORE enqueue:
 *   - one stdin chunk larger than `ATTACH_CHUNK_MAX_BYTES` fails closed;
 *   - one line larger than `ATTACH_LINE_MAX_BYTES` before its newline fails
 *     closed (no unbounded partial-line buffering, ever);
 *   - the queue is hard-bounded by count AND bytes; when full the pump
 *     stops pulling chunks — producer backpressure, nothing dropped,
 *     nothing buffered beyond the bound.
 *
 * Framing is incremental UTF-8 (`StringDecoder`) with LF or CRLF line
 * endings. `dispose()` cancels intake without waiting for EOF: a TTY or
 * FIFO whose writer stays open cannot hold the process — hosts that close
 * explicitly dispose the pump instead of waiting out stdin.
 */

export type AttachInputEvent =
  | { kind: "line"; value: string }
  | { kind: "eof" };

/** Hard count bound on queued commands. */
export const ATTACH_QUEUE_MAX_COMMANDS = 128;
/** Hard byte bound on queued command text. */
export const ATTACH_QUEUE_MAX_BYTES = 1_048_576;
/** Hard bound on one command line, checked before its newline lands. */
export const ATTACH_LINE_MAX_BYTES = 262_144;
/** Hard bound on one inbound stdin chunk. */
export const ATTACH_CHUNK_MAX_BYTES = 1_048_576;

interface ByteStream {
  pause?(): void;
  unref?(): void;
}

export class AttachInputPump {
  private readonly lines: string[] = [];
  private readonly waiters = new Set<() => void>();
  private bufferedBytes = 0;
  private pending = "";
  private ended = false;
  private endDelivered = false;
  private disposed = false;
  private holdingForSpace = false;
  private readonly reading: Promise<void>;
  private readonly stream: ByteStream | null;
  readError: { code: string; message: string } | null = null;

  constructor(stdin: AsyncIterable<Uint8Array | string>) {
    this.stream = typeof (stdin as ByteStream).pause === "function" ||
      typeof (stdin as ByteStream).unref === "function"
      ? (stdin as ByteStream)
      : null;
    this.reading = this.pump(stdin);
  }

  /** True while the queue is at its bound and the pump is holding stdin. */
  get pausedForSpace(): boolean {
    return this.holdingForSpace;
  }

  /**
   * Resolves true once a complete command line is queued (possibly before
   * the session even exists), false when stdin ended with nothing buffered.
   * Lets hosts prove handshake-ordering deterministically.
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

  /** Resolves when intake is finished — EOF, failure, overflow, or dispose. */
  settled(): Promise<void> {
    return this.reading;
  }

  /**
   * Cancel intake without awaiting EOF: the reader stops pulling, the
   * underlying stream (if it is one) is paused and unreferenced so an
   * open TTY/FIFO writer cannot keep the process alive, and `settled()`
   * resolves. Queued-but-unread lines are dropped — they were never
   * accepted as commands.
   */
  dispose(): void {
    if (this.disposed) return;
    this.disposed = true;
    try {
      this.stream?.pause?.();
      this.stream?.unref?.();
    } catch {
      // A stream that refuses pause/unref is not a reason to hang the host.
    }
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

  private async pump(stdin: AsyncIterable<Uint8Array | string>): Promise<void> {
    const decoder = new StringDecoder("utf8");
    try {
      for await (const chunk of stdin) {
        if (this.disposed) break;
        const bytes =
          typeof chunk === "string" ? Buffer.byteLength(chunk, "utf8") : (chunk as Uint8Array).byteLength;
        if (bytes > ATTACH_CHUNK_MAX_BYTES) {
          this.overflow(`a single stdin chunk of ${bytes} bytes exceeds the ${ATTACH_CHUNK_MAX_BYTES}-byte attach chunk bound`);
          break;
        }
        this.frame(typeof chunk === "string" ? chunk : decoder.write(Buffer.from(chunk as Uint8Array)));
        if (this.readError !== null || this.disposed) break;
        // Enqueue whatever fits, then hold stdin while framed commands wait
        // for queue space. The queue bound is checked before every push.
        for (;;) {
          this.drain();
          if (this.pending.indexOf("\n") < 0 || this.disposed) break;
          await this.once();
        }
        if (this.disposed) break;
      }
      if (!this.disposed && this.readError === null) {
        this.frame(decoder.end());
        this.drain();
        // A trailing unterminated line at EOF is still one accepted command.
        const tail = this.pending.replace(/\r$/, "").trim();
        this.pending = "";
        if (tail.length > 0 && Buffer.byteLength(tail, "utf8") <= ATTACH_LINE_MAX_BYTES) {
          this.accept(tail);
        } else if (tail.length > 0) {
          this.overflow(`the final stdin line exceeds the ${ATTACH_LINE_MAX_BYTES}-byte attach line bound`);
        }
      }
    } catch (error) {
      // A failed stdin is EOF for the wire; the failure is surfaced, never swallowed.
      this.readError = {
        code: "ATTACH_INPUT_FAILED",
        message: error instanceof Error ? error.message : String(error),
      };
    }
    this.ended = true;
    this.notify();
  }

  private frame(text: string): void {
    if (text.length === 0 || this.disposed) return;
    this.pending += text;
    this.guardPartialLine();
  }

  /** Extract as many complete bounded lines as the queue bounds allow. */
  private drain(): void {
    for (;;) {
      if (this.disposed || this.readError !== null) return;
      const index = this.pending.indexOf("\n");
      if (index < 0) {
        this.guardPartialLine();
        return;
      }
      const line = this.pending.slice(0, index).replace(/\r$/, "").trim();
      const bytes = Buffer.byteLength(line, "utf8");
      if (bytes > ATTACH_LINE_MAX_BYTES) {
        this.overflow(`a stdin command line of ${bytes} bytes exceeds the ${ATTACH_LINE_MAX_BYTES}-byte attach line bound`);
        return;
      }
      if (line.length > 0 && !this.hasRoomFor(bytes)) {
        return; // hard queue bound reached: the line stays framed; stdin holds.
      }
      this.pending = this.pending.slice(index + 1);
      if (line.length > 0) this.accept(line);
    }
  }

  private guardPartialLine(): void {
    if (this.pending.indexOf("\n") >= 0) return;
    if (Buffer.byteLength(this.pending, "utf8") > ATTACH_LINE_MAX_BYTES) {
      const bytes = Buffer.byteLength(this.pending, "utf8");
      this.pending = "";
      this.overflow(
        `stdin input grew to ${bytes} bytes without a line terminator, past the ${ATTACH_LINE_MAX_BYTES}-byte attach line bound`,
      );
    }
  }

  private hasRoomFor(bytes: number): boolean {
    return (
      this.lines.length < ATTACH_QUEUE_MAX_COMMANDS &&
      this.bufferedBytes + bytes <= ATTACH_QUEUE_MAX_BYTES
    );
  }

  private accept(line: string): void {
    this.lines.push(line);
    this.bufferedBytes += Buffer.byteLength(line, "utf8");
    this.notify();
  }

  /** Fail closed: report, stop buffering, end intake. */
  private overflow(message: string): void {
    this.readError = { code: "ATTACH_INPUT_OVERFLOW", message };
    this.pending = "";
    this.ended = true;
    this.notify();
  }
}
