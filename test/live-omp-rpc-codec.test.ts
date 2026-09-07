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
});
