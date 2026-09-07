import { describe, expect, it } from "vitest";
import {
  OmpRpcCodecError,
  OmpRpcFrameDecoder,
  encodeOmpRpcFrames,
} from "../src/live/transports/omp-rpc-codec.js";

const LIMITS = {
  maxFrameBytes: 256,
  maxReassembledBytes: 1024,
  chunkBytes: 32,
};

function parse(lines: readonly string[]): Record<string, unknown>[] {
  return lines.map((line) => JSON.parse(line) as Record<string, unknown>);
}

describe("OMP RPC v2 codec", () => {
  it("reassembles a bounded UTF-8 logical message in order", () => {
    const frame = { type: "message_update", text: "🙂".repeat(160) };
    const physical = parse(encodeOmpRpcFrames(frame, LIMITS.maxFrameBytes, LIMITS.maxReassembledBytes, LIMITS.chunkBytes));
    expect(physical.length).toBeGreaterThan(1);
    const decoder = new OmpRpcFrameDecoder(
      LIMITS.maxFrameBytes,
      LIMITS.maxReassembledBytes,
      LIMITS.chunkBytes,
    );
    expect(physical.slice(0, -1).every((chunk) => decoder.push(chunk) === null)).toBe(true);
    expect(decoder.push(physical.at(-1)!)).toEqual(frame);
  });

  it("rejects an out-of-order, duplicate, or interrupted sequence", () => {
    const physical = parse(
      encodeOmpRpcFrames(
        { type: "message_update", text: "x".repeat(400) },
        LIMITS.maxFrameBytes,
        LIMITS.maxReassembledBytes,
        LIMITS.chunkBytes,
      ),
    );
    expect(physical.length).toBeGreaterThan(2);
    const outOfOrder = new OmpRpcFrameDecoder(LIMITS.maxFrameBytes, LIMITS.maxReassembledBytes, LIMITS.chunkBytes);
    expect(() => outOfOrder.push(physical[1]!)).toThrowError(OmpRpcCodecError);

    const duplicate = new OmpRpcFrameDecoder(LIMITS.maxFrameBytes, LIMITS.maxReassembledBytes, LIMITS.chunkBytes);
    duplicate.push(physical[0]!);
    expect(() => duplicate.push(physical[0]!)).toThrowError(OmpRpcCodecError);

    const interrupted = new OmpRpcFrameDecoder(LIMITS.maxFrameBytes, LIMITS.maxReassembledBytes, LIMITS.chunkBytes);
    interrupted.push(physical[0]!);
    expect(() => interrupted.push({ type: "notice" })).toThrowError(OmpRpcCodecError);
  });

  it("rejects malformed base64 and a message that exceeds the declared length", () => {
    const physical = parse(
      encodeOmpRpcFrames(
        { type: "message_update", text: "x".repeat(400) },
        LIMITS.maxFrameBytes,
        LIMITS.maxReassembledBytes,
        LIMITS.chunkBytes,
      ),
    );
    const badBase64 = { ...physical[0], data: "%%%=" };
    const decoder = new OmpRpcFrameDecoder(LIMITS.maxFrameBytes, LIMITS.maxReassembledBytes, LIMITS.chunkBytes);
    expect(() => decoder.push(badBase64)).toThrowError(OmpRpcCodecError);

    const badLength = physical.map((chunk) => ({ ...chunk, byteLength: (chunk.byteLength as number) - 1 }));
    const lengthDecoder = new OmpRpcFrameDecoder(LIMITS.maxFrameBytes, LIMITS.maxReassembledBytes, LIMITS.chunkBytes);
    expect(() => badLength.forEach((chunk) => lengthDecoder.push(chunk))).toThrowError(OmpRpcCodecError);
  });

  it("refuses reassembly of a message declared beyond the reassembly limit", () => {
    const physical = parse(encodeOmpRpcFrames({ type: "message_update", text: "x".repeat(900) }, 256, 65536, 32));
    expect(physical.length).toBeGreaterThan(2);
    const decoder = new OmpRpcFrameDecoder(256, 640, 32);
    // The declaration itself crosses the session's reassembly bound: the
    // sequence dies at its first chunk, not after the bytes accumulate.
    expect(() => decoder.push(physical[0]!)).toThrowError(OmpRpcCodecError);
  });

  it("shrinks chunks so an oversized message stays sendable under a small frame cap", () => {
    const lines = encodeOmpRpcFrames({ type: "prompt", message: "y".repeat(1500) }, 512, 65536);
    expect(lines.length).toBeGreaterThan(2);
    for (const line of lines) {
      expect(Buffer.byteLength(line, "utf8")).toBeLessThanOrEqual(512);
    }
    const chunks = parse(lines);
    expect(chunks.map((c) => c.index)).toEqual(chunks.map((_, i) => i));
    const joined = Buffer.concat(chunks.map((c) => Buffer.from(c.data as string, "base64")));
    expect(JSON.parse(joined.toString("utf8"))).toEqual({ type: "prompt", message: "y".repeat(1500) });
  });
});
