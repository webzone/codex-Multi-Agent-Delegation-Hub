import { Buffer } from "node:buffer";

import { AgentHubError } from "../errors.js";

/**
 * Strict byte-level LF JSONL framing (v3 live, Package 1).
 *
 * A frame is a single JSON document followed by exactly one LF byte (0x0A).
 * Framing operates on raw bytes: the split point is the byte 0x0A, never a
 * decoded character, so a multi-byte code point split across transport chunks
 * can never be mistaken for a boundary, and invalid UTF-8 inside a frame
 * fails JSON parsing rather than being silently repaired at the framing layer.
 *
 * Nothing about the frame *contents* is interpreted here — this module is
 * provider-neutral by construction.
 *
 * Honesty rules:
 *   - CR is never a terminator: it stays inside the frame (JSON.parse's
 *     tolerance of surrounding whitespace is JSON's business, not a frame
 *     boundary this framer would invent).
 *   - An *unfinished* frame crossing `maxFrameBytes` without an LF is
 *     reported once as TOO_LARGE, then bytes are discarded until the next LF
 *     so subsequent frames still frame cleanly (observable, resynchronizing,
 *     bounded). A *completed* oversized frame is reported as TOO_LARGE
 *     without entering discard mode — it already terminated.
 *   - Unterminated trailing bytes at end-of-input are an UNTERMINATED error;
 *     a partial frame is never parsed "just in case".
 *   - Raw frame bytes are never echoed into error messages — oversized or
 *     malformed provider traffic must not become hub-reported content.
 */

/** Frames above this size are reported and dropped by default. */
export const LIVE_MAX_FRAME_BYTES = 256 * 1024;

export type LiveFramingCode =
  | "LIVE_JSONL_FRAME_TOO_LARGE"
  | "LIVE_JSONL_FRAME_INVALID_JSON"
  | "LIVE_JSONL_FRAME_UNTERMINATED";

export interface LiveFramingError {
  code: LiveFramingCode;
  message: string;
  /** Bytes observed for the offending frame; null when the true size is unknown. */
  frame_bytes: number | null;
}

export type LiveJsonlFrame =
  | { kind: "frame"; value: unknown; bytes: number }
  | { kind: "error"; error: LiveFramingError };

function framingFailure(code: LiveFramingCode, message: string, frameBytes: number | null): never {
  // `frame_bytes` rides alongside the standard code/message pair so callers
  // can size the failure without ever seeing the frame content itself.
  throw Object.assign(
    new AgentHubError(
      code,
      frameBytes === null ? message : `${message} (frame_bytes: ${frameBytes})`,
    ),
    { frame_bytes: frameBytes },
  );
}

const LF = 0x0a;

/**
 * Serialize one value to a single JSONL frame (compact JSON + one LF).
 * Throws `LIVE_JSONL_FRAME_TOO_LARGE` when the frame would exceed
 * `maxFrameBytes` — callers must not be able to write an unbounded frame.
 */
export function encodeJsonlFrame(value: unknown, maxFrameBytes = LIVE_MAX_FRAME_BYTES): Buffer {
  let json: string;
  try {
    json = JSON.stringify(value) as string;
  } catch (error) {
    framingFailure(
      "LIVE_JSONL_FRAME_INVALID_JSON",
      `frame value is not JSON-serializable: ${error instanceof Error ? error.message : String(error)}`,
      null,
    );
  }
  const frame = Buffer.from(`${json}\n`, "utf8");
  if (frame.byteLength > maxFrameBytes) {
    framingFailure(
      "LIVE_JSONL_FRAME_TOO_LARGE",
      "encoded frame is above the byte bound",
      frame.byteLength,
    );
  }
  return frame;
}

export interface LiveJsonlFramerOptions {
  /** Maximum bytes allowed between LFs before the frame is abandoned. */
  maxFrameBytes?: number;
}

/**
 * Incremental LF framer over a byte stream. Feed transport chunks in; frames
 * and framing errors come out in stream order. The framer never retains more
 * than `maxFrameBytes` of unfinished frame: crossing the bound flips it into
 * discard mode until the next LF resynchronizes it.
 */
export class LiveJsonlFramer {
  private readonly maxFrameBytes: number;
  private chunks: Buffer[] = [];
  private buffered = 0;
  private discarding = false;

  constructor(options: LiveJsonlFramerOptions = {}) {
    this.maxFrameBytes = options.maxFrameBytes ?? LIVE_MAX_FRAME_BYTES;
    if (!Number.isInteger(this.maxFrameBytes) || this.maxFrameBytes < 1) {
      framingFailure(
        "LIVE_JSONL_FRAME_TOO_LARGE",
        `maxFrameBytes must be a positive integer, got ${String(this.maxFrameBytes)}`,
        null,
      );
    }
  }

  /** Bytes currently held as an unfinished frame (never exceeds the bound). */
  get pendingBytes(): number {
    return this.buffered;
  }

  feed(chunk: Buffer): LiveJsonlFrame[] {
    const out: LiveJsonlFrame[] = [];

    // Concatenate first: frame splitting must see a contiguous byte buffer,
    // so the LF search is byte-exact across chunk boundaries.
    let buffer: Buffer;
    if (this.chunks.length === 0) {
      buffer = chunk;
    } else {
      this.chunks.push(chunk);
      buffer = Buffer.concat(this.chunks, this.buffered + chunk.byteLength);
      this.chunks = [];
    }

    let offset = 0;
    while (offset < buffer.byteLength) {
      const lf = buffer.indexOf(LF, offset);

      if (this.discarding) {
        if (lf === -1) {
          break; // Still inside the discarded run; nothing left to observe.
        }
        this.discarding = false;
        this.buffered = 0;
        offset = lf + 1;
        continue;
      }

      if (lf === -1) {
        const pending = buffer.byteLength - offset;
        if (pending > this.maxFrameBytes) {
          out.push(this.beginDiscard(pending));
        } else {
          this.chunks.push(buffer.subarray(offset));
          this.buffered = pending;
        }
        break;
      }

      const line = buffer.subarray(offset, lf);
      offset = lf + 1;
      this.buffered = 0;
      this.chunks = [];
      this.completeFrame(line, out);
    }

    return out;
  }

  /**
   * Signal end-of-input. Any unfinished frame is reported as UNTERMINATED
   * (never parsed); a framer in discard mode has already reported its error.
   */
  end(): LiveJsonlFrame[] {
    const out: LiveJsonlFrame[] = [];
    if (!this.discarding && this.buffered > 0) {
      out.push({
        kind: "error",
        error: {
          code: "LIVE_JSONL_FRAME_UNTERMINATED",
          message:
            `input ended with ${this.buffered} unterminated frame bytes; ` +
            "strict LF framing requires every frame to end with a newline",
          frame_bytes: this.buffered,
        },
      });
    }
    this.chunks = [];
    this.buffered = 0;
    this.discarding = false;
    return out;
  }

  reset(): void {
    this.chunks = [];
    this.buffered = 0;
    this.discarding = false;
  }

  /** The one error emitted when an *unfinished* frame crosses the bound. */
  private beginDiscard(observed: number): LiveJsonlFrame {
    this.discarding = true;
    this.chunks = [];
    this.buffered = 0;
    return {
      kind: "error",
      error: {
        code: "LIVE_JSONL_FRAME_TOO_LARGE",
        message:
          `unfinished frame crossed the ${this.maxFrameBytes}-byte bound without an LF; ` +
          "discarding until the next newline",
        frame_bytes: observed,
      },
    };
  }

  private completeFrame(line: Buffer, out: LiveJsonlFrame[]): void {
    if (line.byteLength > this.maxFrameBytes) {
      // A terminated oversized frame needs no discard mode: it is over.
      out.push({
        kind: "error",
        error: {
          code: "LIVE_JSONL_FRAME_TOO_LARGE",
          message: `completed frame exceeds the ${this.maxFrameBytes}-byte bound`,
          frame_bytes: line.byteLength,
        },
      });
      return;
    }
    if (line.byteLength === 0) {
      out.push({
        kind: "error",
        error: {
          code: "LIVE_JSONL_FRAME_INVALID_JSON",
          message: "empty frame: a blank line is not a JSON document",
          frame_bytes: 0,
        },
      });
      return;
    }
    try {
      out.push({ kind: "frame", value: JSON.parse(line.toString("utf8")), bytes: line.byteLength });
    } catch {
      out.push({
        kind: "error",
        error: {
          code: "LIVE_JSONL_FRAME_INVALID_JSON",
          message: "frame is not a single valid JSON document (raw bytes are withheld by contract)",
          frame_bytes: line.byteLength,
        },
      });
    }
  }
}
