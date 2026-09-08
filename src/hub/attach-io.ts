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
 * Framing is incremental UTF-8 (`StringDecoder`) plus newline splitting;
 * the queue is bounded by count AND bytes. When full, the pump stops
 * pulling chunks — producer backpressure is the honest overflow behavior:
 * nothing is dropped and nothing unbounded is buffered.
 */

export type AttachInputEvent =
  | { kind: "line"; value: string }
  | { kind: "eof" };

export const ATTACH_QUEUE_MAX_COMMANDS = 128;
export const ATTACH_QUEUE_MAX_BYTES = 1_048_576;

/** How long an EOF-drain waits for already-dispatched commands to settle. */
export const ATTACH_CLOSE_DRAIN_DEFAULT_MS = 5_000;

export class AttachInputPump {
  private readonly lines: string[] = [];
  private readonly waiters = new Set<() => void>();
  private bufferedBytes = 0;
  private pending = "";
  private ended = false;
  private endDelivered = false;
  private holdingForSpace = false;
  private readonly reading: Promise<void>;
  readError: { code: string; message: string } | null = null;

  constructor(stdin: AsyncIterable<Uint8Array | string>) {
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
      if (this.ended) return false;
      await this.once();
    }
  }

  /** Next event in arrival order; the EOF event only follows all lines. */
  async next(): Promise<AttachInputEvent | null> {
    for (;;) {
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

  /** Resolves when the stdin stream is fully consumed (or failed). */
  settled(): Promise<void> {
    return this.reading;
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
        this.frame(
          typeof chunk === "string" ? chunk : decoder.write(Buffer.from(chunk as Uint8Array)),
        );
        while (this.full()) {
          this.holdingForSpace = true;
          await this.once();
          this.holdingForSpace = false;
        }
      }
    } catch (error) {
      // A failed stdin is EOF for the wire; the failure is surfaced, never swallowed.
      this.readError = {
        code: "ATTACH_INPUT_FAILED",
        message: error instanceof Error ? error.message : String(error),
      };
    }
    this.frame(decoder.end());
    const tail = this.pending.trim();
    this.pending = "";
    if (tail.length > 0) this.lines.push(tail);
    this.ended = true;
    this.notify();
  }

  private frame(text: string): void {
    if (text.length === 0) return;
    this.pending += text;
    let index = this.pending.indexOf("\n");
    while (index >= 0) {
      const line = this.pending.slice(0, index).trim();
      this.pending = this.pending.slice(index + 1);
      if (line.length > 0) {
        this.lines.push(line);
        this.bufferedBytes += Buffer.byteLength(line, "utf8");
        this.notify();
      }
      index = this.pending.indexOf("\n");
    }
  }

  private full(): boolean {
    return (
      this.lines.length >= ATTACH_QUEUE_MAX_COMMANDS ||
      this.bufferedBytes >= ATTACH_QUEUE_MAX_BYTES
    );
  }
}
