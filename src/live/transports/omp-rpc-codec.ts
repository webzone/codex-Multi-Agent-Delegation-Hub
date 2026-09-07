import { TextDecoder } from "node:util";
import type { RpcFrame } from "./rpc-base.js";

/** The physical and logical bounds advertised by OMP 18.1.13 RPC v2. */
export const OMP_RPC_MAX_FRAME_BYTES = 1024 * 1024;
export const OMP_RPC_MAX_REASSEMBLED_BYTES = 64 * 1024 * 1024;
export const OMP_RPC_CHUNK_BYTES = 256 * 1024;

export type OmpRpcCodecErrorCode = "OMP_RPC_FRAME_INVALID" | "OMP_RPC_MESSAGE_INVALID";

export class OmpRpcCodecError extends Error {
  readonly code: OmpRpcCodecErrorCode;

  constructor(code: OmpRpcCodecErrorCode, message: string) {
    super(message);
    this.name = "OmpRpcCodecError";
    this.code = code;
  }
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function jsonLineBytes(value: string): number {
  return Buffer.byteLength(value, "utf8") + 1;
}

function encodeJson(value: RpcFrame): string {
  const json = JSON.stringify(value);
  if (json === undefined) {
    throw new OmpRpcCodecError("OMP_RPC_FRAME_INVALID", "the RPC frame could not be encoded as JSON");
  }
  return json;
}

/**
 * Encode a logical OMP v2 frame. Small frames stay ordinary JSONL; larger
 * frames are split into the exact ordered rpc_chunk contract used by OMP.
 */
export function encodeOmpRpcFrames(
  frame: RpcFrame,
  maxFrameBytes = OMP_RPC_MAX_FRAME_BYTES,
  maxReassembledBytes = OMP_RPC_MAX_REASSEMBLED_BYTES,
  chunkBytes = OMP_RPC_CHUNK_BYTES,
): string[] {
  const json = encodeJson(frame);
  const byteLength = Buffer.byteLength(json, "utf8");
  if (jsonLineBytes(json) <= maxFrameBytes) {
    return [`${json}\n`];
  }
  if (byteLength > maxReassembledBytes) {
    throw new OmpRpcCodecError("OMP_RPC_MESSAGE_INVALID", "the logical RPC message exceeded the reassembly limit");
  }
  // A chunk must fit one PHYSICAL frame together with its envelope: base64
  // inflates 4/3 and the envelope overhead is bounded (~200 bytes). When the
  // advertised frame limit is below the default transport chunk size, chunks
  // shrink so an oversized logical message stays sendable at any bounds the
  // provider honestly advertised instead of becoming unsplittable.
  const envelopeHeadroomBytes = 200;
  const effectiveChunkBytes = Math.max(
    1,
    Math.min(chunkBytes, Math.floor(((maxFrameBytes - envelopeHeadroomBytes) * 3) / 4)),
  );
  const bytes = Buffer.from(json, "utf8");
  const count = Math.ceil(byteLength / effectiveChunkBytes);
  if (count < 2 || count > Math.ceil(maxReassembledBytes / effectiveChunkBytes)) {
    throw new OmpRpcCodecError("OMP_RPC_MESSAGE_INVALID", "the logical RPC message has an invalid chunk count");
  }
  const chunkId = `hub-${crypto.randomUUID()}`;
  const lines: string[] = [];
  for (let index = 0; index < count; index += 1) {
    const chunk = {
      type: "rpc_chunk",
      chunkId,
      index,
      count,
      byteLength,
      data: bytes.subarray(index * effectiveChunkBytes, (index + 1) * effectiveChunkBytes).toString("base64"),
    } satisfies RpcFrame;
    const line = `${JSON.stringify(chunk)}\n`;
    if (jsonLineBytes(line.slice(0, -1)) > maxFrameBytes) {
      throw new OmpRpcCodecError("OMP_RPC_FRAME_INVALID", "an RPC chunk exceeded the physical frame limit");
    }
    lines.push(line);
  }
  return lines;
}

function decodeBase64(value: unknown): Buffer {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    !/^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/.test(value)
  ) {
    throw new OmpRpcCodecError("OMP_RPC_FRAME_INVALID", "an RPC chunk carried invalid base64 data");
  }
  const bytes = Buffer.from(value, "base64");
  if (bytes.toString("base64") !== value) {
    throw new OmpRpcCodecError("OMP_RPC_FRAME_INVALID", "an RPC chunk carried non-canonical base64 data");
  }
  return bytes;
}

interface PartialSequence {
  chunkId: string;
  count: number;
  byteLength: number;
  nextIndex: number;
  chunks: Buffer[];
  receivedBytes: number;
}

/** Bounded, ordered, single-sequence reassembly for negotiated OMP RPC v2. */
export class OmpRpcFrameDecoder {
  private partial: PartialSequence | null = null;

  constructor(
    private readonly maxFrameBytes = OMP_RPC_MAX_FRAME_BYTES,
    private readonly maxReassembledBytes = OMP_RPC_MAX_REASSEMBLED_BYTES,
    private readonly chunkBytes = OMP_RPC_CHUNK_BYTES,
  ) {}

  push(frame: RpcFrame): RpcFrame | null {
    if (frame.type !== "rpc_chunk") {
      if (this.partial) {
        throw new OmpRpcCodecError("OMP_RPC_MESSAGE_INVALID", "the RPC chunk sequence was interrupted");
      }
      return frame;
    }

    const chunkId = frame.chunkId;
    const index = frame.index;
    const count = frame.count;
    const byteLength = frame.byteLength;
    if (
      typeof chunkId !== "string" ||
      chunkId.length === 0 ||
      chunkId.length > 128 ||
      typeof index !== "number" ||
      typeof count !== "number" ||
      typeof byteLength !== "number" ||
      !Number.isSafeInteger(index) ||
      !Number.isSafeInteger(count) ||
      !Number.isSafeInteger(byteLength) ||
      index < 0 ||
      count < 2 ||
      count > Math.ceil(this.maxReassembledBytes / this.chunkBytes) ||
      index >= count ||
      byteLength < this.maxFrameBytes ||
      byteLength > this.maxReassembledBytes
    ) {
      throw new OmpRpcCodecError("OMP_RPC_FRAME_INVALID", "an RPC chunk carried invalid metadata");
    }

    const bytes = decodeBase64(frame.data);
    if (bytes.byteLength > this.chunkBytes) {
      throw new OmpRpcCodecError("OMP_RPC_FRAME_INVALID", "an RPC chunk payload exceeded the transport limit");
    }

    if (!this.partial) {
      if (index !== 0) {
        throw new OmpRpcCodecError("OMP_RPC_MESSAGE_INVALID", "an RPC chunk sequence did not start at index zero");
      }
      this.partial = { chunkId, count, byteLength, nextIndex: 0, chunks: [], receivedBytes: 0 };
    }

    const sequence = this.partial;
    if (!sequence) {
      throw new OmpRpcCodecError("OMP_RPC_MESSAGE_INVALID", "the RPC chunk sequence was not initialized");
    }
    if (
      sequence.chunkId !== chunkId ||
      sequence.count !== count ||
      sequence.byteLength !== byteLength ||
      sequence.nextIndex !== index
    ) {
      throw new OmpRpcCodecError("OMP_RPC_MESSAGE_INVALID", "an RPC chunk sequence was out of order or inconsistent");
    }

    sequence.chunks.push(bytes);
    sequence.receivedBytes += bytes.byteLength;
    sequence.nextIndex += 1;
    if (sequence.receivedBytes > sequence.byteLength || sequence.receivedBytes > this.maxReassembledBytes) {
      throw new OmpRpcCodecError("OMP_RPC_MESSAGE_INVALID", "an RPC chunk sequence exceeded its declared length");
    }
    if (sequence.nextIndex < sequence.count) {
      return null;
    }
    if (sequence.receivedBytes !== sequence.byteLength) {
      throw new OmpRpcCodecError("OMP_RPC_MESSAGE_INVALID", "an RPC chunk sequence length did not match its declaration");
    }

    this.partial = null;
    let json: string;
    try {
      json = new TextDecoder("utf-8", { fatal: true }).decode(Buffer.concat(sequence.chunks));
    } catch {
      throw new OmpRpcCodecError("OMP_RPC_MESSAGE_INVALID", "the reassembled RPC message was not valid UTF-8");
    }
    let value: unknown;
    try {
      value = JSON.parse(json);
    } catch {
      throw new OmpRpcCodecError("OMP_RPC_MESSAGE_INVALID", "the reassembled RPC message was not valid JSON");
    }
    if (!isObject(value)) {
      throw new OmpRpcCodecError("OMP_RPC_MESSAGE_INVALID", "the reassembled RPC message was not an object");
    }
    return value;
  }
}
