import { PassThrough } from "node:stream";

import { describe, expect, it } from "vitest";

import { deferred } from "../src/deferred.js";
import { LiveStdinReader, runLiveSession } from "../src/live/index.js";

describe("LiveStdinReader", () => {
  it("captures commands before the consumer starts and preserves split UTF-8", async () => {
    const input = new PassThrough();
    const reader = new LiveStdinReader(input);
    const encoded = Buffer.from('{"action":"prompt","text":"你好"}\n', "utf8");

    input.write(encoded.subarray(0, encoded.length - 2));
    input.write(encoded.subarray(encoded.length - 2));

    const iterator = reader[Symbol.asyncIterator]();
    expect((await iterator.next()).value).toBe('{"action":"prompt","text":"你好"}');
    input.end();
    expect((await iterator.next()).done).toBe(true);
    reader.dispose();
  });

  it("fails closed instead of growing an unbounded command queue", async () => {
    const input = new PassThrough();
    const reader = new LiveStdinReader(input, { maxQueueLines: 1, maxQueueBytes: 128 });

    input.write('{"action":"status"}\n{"action":"status"}\n');
    const iterator = reader[Symbol.asyncIterator]();
    expect((await iterator.next()).value).toBe('{"action":"status"}');
    await expect(iterator.next()).rejects.toMatchObject({ code: "LIVE_STDIN_QUEUE_FULL" });
    reader.dispose();
  });

  it("turns EOF into a clean end after delivering a final partial line", async () => {
    const input = new PassThrough();
    const reader = new LiveStdinReader(input);
    input.end('{"action":"close"}');

    const iterator = reader[Symbol.asyncIterator]();
    expect((await iterator.next()).value).toBe('{"action":"close"}');
    expect((await iterator.next()).done).toBe(true);
    reader.dispose();
  });
});

describe("live stdin handshake ordering", () => {
  it("queues an early prompt, dispatches it once after start, and drains it on EOF", async () => {
    const input = new PassThrough();
    const stdin = new LiveStdinReader(input);
    const startGate = deferred<void>();
    const promptTexts: string[] = [];
    let status = "idle";
    const state = () =>
      ({
        live_session_id: "live-stdin-test",
        provider: "pi",
        transport: "pi-rpc",
        status,
        session_id: null,
        base_commit: "0".repeat(40),
        current_commit: "0".repeat(40),
        capabilities: {},
      }) as any;
    const turnResult = () =>
      ({
        kind: "prompt",
        live_session_id: "live-stdin-test",
        outcome: "succeeded",
        final_text: null,
        error: null,
        checkpoint: null,
        usage: null,
      }) as any;
    const manager = {
      start: async () => {
        await startGate.promise;
        return { live_session_id: "live-stdin-test", state: state(), workspace: "/worktree" };
      },
      resumeFromState: async () => ({
        live_session_id: "live-stdin-test",
        state: state(),
        workspace: "/worktree",
      }),
      prompt: async (_id: string, text: string) => {
        promptTexts.push(text);
        return turnResult();
      },
      followUp: async () => turnResult(),
      steer: async () => turnResult(),
      cancel: async () => turnResult(),
      requestStatus: async () => turnResult(),
      respondPermission: async () => turnResult(),
      view: () => state(),
      eventsAfter: () => ({ events: [], next_cursor: 0 }),
      eventCursor: () => 0,
      close: async () => {
        status = "closed";
        return {
          state: state(),
          stop: { status: "closed", exit_code: 0, exit_signal: null, waited_ms: 0 },
          checkpoint_taken: false,
          cleanup_errors: [],
        };
      },
    };

    const running = runLiveSession(
      { provider: "pi", resumeId: null, workspace: "/repo" },
      {
        stdin,
        stdout: () => {},
        stderr: () => {},
      },
      { manager },
    );

    input.write('{"action":"prompt","text":"first turn"}\n');
    await Promise.resolve();
    expect(promptTexts).toEqual([]);

    startGate.resolve();
    input.end();
    expect(await running).toBe(0);
    expect(promptTexts).toEqual(["first turn"]);
    stdin.dispose();
  });
});

