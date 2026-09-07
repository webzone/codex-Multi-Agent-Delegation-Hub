import { StringDecoder } from "node:string_decoder";
import type { Readable } from "node:stream";

import { AgentHubError } from "../errors.js";

/** Bounds for commands received before the provider handshake is complete. */
export const LIVE_STDIN_MAX_LINE_BYTES = 64 * 1024;
export const LIVE_STDIN_MAX_QUEUE_LINES = 64;
export const LIVE_STDIN_MAX_QUEUE_BYTES = 1024 * 1024;

interface PendingRead {
  resolve: (result: IteratorResult<string>) => void;
  reject: (error: unknown) => void;
}

/**
 * Eager, single-owner line reader for the public live CLI.
 *
 * The reader attaches its data handler in the constructor, before provider
 * provisioning starts.  This matters for a controlling TTY: commands typed
 * during a slow provider handshake must be captured just as reliably as
 * commands written to a pipe or FIFO.  Dispatch remains the runner's job, so
 * eager reading never sends a command before the session exists.
 */
export class LiveStdinReader implements AsyncIterable<string>, AsyncIterator<string> {
  private readonly decoder = new StringDecoder("utf8");
  private readonly queue: string[] = [];
  private readonly waiters: PendingRead[] = [];
  private readonly maxLineBytes: number;
  private readonly maxQueueLines: number;
  private readonly maxQueueBytes: number;
  private pending = "";
  private queueBytes = 0;
  private ended = false;
  private paused = false;
  private disposed = false;
  private iteratorClaimed = false;
  private terminalError: unknown = null;

  private readonly onData = (chunk: Buffer | string | Uint8Array): void => {
    if (this.ended || this.disposed) {
      return;
    }
    try {
      const bytes = typeof chunk === "string" ? Buffer.from(chunk, "utf8") : Buffer.from(chunk);
      this.acceptText(this.decoder.write(bytes));
    } catch (error) {
      this.fail(error);
    }
  };

  private readonly onEnd = (): void => {
    if (this.ended || this.disposed) {
      return;
    }
    try {
      this.acceptText(this.decoder.end());
      if (this.pending.length > 0) {
        this.enqueue(this.pending.replace(/\r$/, ""));
        this.pending = "";
      }
      this.finish();
    } catch (error) {
      this.fail(error);
    }
  };

  private readonly onError = (error: unknown): void => {
    this.fail(
      error instanceof AgentHubError
        ? error
        : new AgentHubError(
            "LIVE_STDIN_ERROR",
            `live stdin failed: ${error instanceof Error ? error.message : String(error)}`,
          ),
    );
  };

  constructor(
    private readonly source: Readable,
    options: {
      maxLineBytes?: number;
      maxQueueLines?: number;
      maxQueueBytes?: number;
    } = {},
  ) {
    this.maxLineBytes = options.maxLineBytes ?? LIVE_STDIN_MAX_LINE_BYTES;
    this.maxQueueLines = options.maxQueueLines ?? LIVE_STDIN_MAX_QUEUE_LINES;
    this.maxQueueBytes = options.maxQueueBytes ?? LIVE_STDIN_MAX_QUEUE_BYTES;
    if (!Number.isSafeInteger(this.maxLineBytes) || this.maxLineBytes < 1) {
      throw new Error("maxLineBytes must be a positive safe integer");
    }
    if (!Number.isSafeInteger(this.maxQueueLines) || this.maxQueueLines < 1) {
      throw new Error("maxQueueLines must be a positive safe integer");
    }
    if (!Number.isSafeInteger(this.maxQueueBytes) || this.maxQueueBytes < 1) {
      throw new Error("maxQueueBytes must be a positive safe integer");
    }

    // Register the one data consumer immediately.  Do not replace this with
    // a lazy async-generator wrapper: that was the source of the TTY race.
    source.on("data", this.onData);
    source.once("end", this.onEnd);
    source.once("error", this.onError);
  }

  [Symbol.asyncIterator](): AsyncIterator<string> {
    if (this.iteratorClaimed) {
      throw new AgentHubError(
        "LIVE_STDIN_READER_ALREADY_CONSUMED",
        "live stdin has a single async consumer",
      );
    }
    this.iteratorClaimed = true;
    return this;
  }

  next(): Promise<IteratorResult<string>> {
    if (this.queue.length > 0) {
      const value = this.queue.shift() as string;
      this.queueBytes -= Buffer.byteLength(value, "utf8");
      this.resumeIfNeeded();
      return Promise.resolve({ value, done: false });
    }
    if (this.terminalError !== null) {
      return Promise.reject(this.terminalError);
    }
    if (this.ended) {
      return Promise.resolve({ value: undefined, done: true });
    }
    return new Promise<IteratorResult<string>>((resolve, reject) => {
      this.waiters.push({ resolve, reject });
    });
  }

  async return(): Promise<IteratorResult<string>> {
    this.dispose();
    return { value: undefined, done: true };
  }

  /** Stop listening without destroying the caller-owned stdin stream. */
  dispose(): void {
    if (this.disposed) {
      return;
    }
    this.disposed = true;
    this.ended = true;
    this.source.pause();
    this.source.removeListener("data", this.onData);
    this.source.removeListener("end", this.onEnd);
    this.source.removeListener("error", this.onError);
    const error = new AgentHubError("LIVE_STDIN_DISPOSED", "live stdin reader was disposed");
    for (const waiter of this.waiters.splice(0)) {
      waiter.reject(error);
    }
  }

  private acceptText(text: string): void {
    this.pending += text;
    for (;;) {
      const newline = this.pending.indexOf("\n");
      if (newline < 0) {
        this.assertPendingBound();
        return;
      }
      const line = this.pending.slice(0, newline).replace(/\r$/, "");
      this.pending = this.pending.slice(newline + 1);
      this.enqueue(line);
    }
  }

  private assertPendingBound(): void {
    if (Buffer.byteLength(this.pending, "utf8") > this.maxLineBytes) {
      throw new AgentHubError(
        "LIVE_STDIN_LINE_TOO_LARGE",
        `live stdin line exceeds ${this.maxLineBytes} bytes`,
      );
    }
  }

  private enqueue(line: string): void {
    const bytes = Buffer.byteLength(line, "utf8");
    if (bytes > this.maxLineBytes) {
      throw new AgentHubError(
        "LIVE_STDIN_LINE_TOO_LARGE",
        `live stdin line exceeds ${this.maxLineBytes} bytes`,
      );
    }
    const waiter = this.waiters.shift();
    if (waiter !== undefined) {
      waiter.resolve({ value: line, done: false });
      return;
    }
    if (
      this.queue.length >= this.maxQueueLines ||
      this.queueBytes + bytes > this.maxQueueBytes
    ) {
      throw new AgentHubError(
        "LIVE_STDIN_QUEUE_FULL",
        `live stdin queue exceeded ${this.maxQueueLines} lines or ${this.maxQueueBytes} bytes`,
      );
    }
    this.queue.push(line);
    this.queueBytes += bytes;
    if (this.queue.length >= this.maxQueueLines || this.queueBytes >= this.maxQueueBytes) {
      this.source.pause();
      this.paused = true;
    }
  }

  private resumeIfNeeded(): void {
    if (!this.paused || this.queue.length > Math.floor(this.maxQueueLines / 2)) {
      return;
    }
    this.paused = false;
    this.source.resume();
  }

  private finish(): void {
    this.ended = true;
    this.resolveDoneIfDrained();
  }

  private fail(error: unknown): void {
    if (this.ended || this.disposed) {
      return;
    }
    this.terminalError = error;
    this.ended = true;
    this.source.pause();
    this.source.removeListener("data", this.onData);
    for (const waiter of this.waiters.splice(0)) {
      waiter.reject(error);
    }
  }

  private resolveDoneIfDrained(): void {
    if (!this.ended || this.queue.length > 0 || this.terminalError !== null) {
      return;
    }
    for (const waiter of this.waiters.splice(0)) {
      waiter.resolve({ value: undefined, done: true });
    }
  }
}

