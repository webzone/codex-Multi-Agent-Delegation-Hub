import { chmod, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";

import { deferred, type Deferred } from "../src/deferred.js";
import { AgyStreamJsonTransport } from "../src/live/transports/agy-stream-json.js";
import { probeAgy } from "../src/live/probes/agy.js";
import type {
  AgyResumeState,
  LiveEvent,
  LiveEventBody,
  LiveFollowUpCommand,
  LivePermissionResponseCommand,
  LivePromptCommand,
  LiveSteerCommand,
  LiveStatusCommand,
} from "../src/live/types.js";
import { removeDirectory } from "./helpers.js";
import {
  realProcessAlive,
  runLeaderFirstSurvivorScenario,
} from "./live-stop-authority.js";

/**
 * Fake-protocol tests for the AGY 1.1.26 stream-json contract. The fake is a
 * real child process speaking the exact envelopes (real timers are deliberate
 * here: fake clocks cannot drive an OS subprocess); every stdin line, argv,
 * emitted envelope, and signal it observes is journaled to a JSONL file so
 * tests assert the wire, not just the normalized events.
 */

interface FakeJournalEntry {
  m: string;
  argv?: string[];
  line?: string;
  event?: string;
  pid?: number;
}

const HELP_WITH_RESUME =
  "usage: agy [--input-format fmt] [--output-format fmt] [--conversation <id>] [--print]\n";
const HELP_WITHOUT_RESUME = "usage: agy [--input-format fmt] [--output-format fmt] [--print]\n";

const SCENARIOS: Record<string, string> = {
  echo: `
const rl = require('node:readline').createInterface({ input: process.stdin });
out({ event: 'init', conversation_id: process.env.AGY_FAKE_CONV_ID || 'conv-1' });
rl.on('line', (line) => {
  journal({ m: 'stdin', line });
  out({ event: 'step_update', step_id: 's-1', step_type: 'text', text: 'done: ok' });
  out({ event: 'result', subtype: 'success', usage: { input_tokens: 7, output_tokens: 3 } });
});
process.on('SIGINT', () => { journal({ m: 'sigint' }); process.exit(0); });
`,
  rich: `
const rl = require('node:readline').createInterface({ input: process.stdin });
out({ event: 'init', conversation_id: 'conv-rich' });
rl.on('line', (line) => {
  journal({ m: 'stdin', line });
  out({ event: 'step_update', step_id: 'r-1', step_type: 'reasoning', text: 'pondering deeply about this' });
  out({ event: 'step_update', step_id: 't-1', step_type: 'text', text: 'partial ' });
  out({ event: 'step_update', step_id: 't-1', step_type: 'text', text: 'final' });
  out({ event: 'step_update', step_type: 'tool_start', tool: 'bash', call_id: 'c-9', input_preview: 'npm test' });
  out({ event: 'step_update', step_type: 'tool_end', tool: 'bash', call_id: 'c-9', ok: true, output_preview: '23 passed' });
  out({ event: 'step_update', step_type: 'hologram', whatever: true });
  out({ event: 'result', subtype: 'success', usage: { input_tokens: 11, output_tokens: 0, cost_usd: 0.25 } });
});
process.on('SIGINT', () => process.exit(0));
`,
  slow: `
const rl = require('node:readline').createInterface({ input: process.stdin });
out({ event: 'init', conversation_id: 'conv-slow' });
rl.on('line', (line) => {
  journal({ m: 'stdin', line });
  setTimeout(() => {
    out({ event: 'step_update', step_id: 't-1', step_type: 'text', text: 'late answer' });
    out({ event: 'result', subtype: 'success' });
  }, 300);
});
process.on('SIGINT', () => process.exit(0));
`,
  sigint: `
const rl = require('node:readline').createInterface({ input: process.stdin });
out({ event: 'init', conversation_id: 'conv-cancel' });
rl.on('line', (line) => { journal({ m: 'stdin', line }); });
process.on('SIGINT', () => {
  journal({ m: 'sigint' });
  out({ event: 'result', subtype: 'cancelled' });
  setTimeout(() => process.exit(0), 50);
});
`,
  die: `
const rl = require('node:readline').createInterface({ input: process.stdin });
out({ event: 'init', conversation_id: 'conv-die' });
rl.on('line', (line) => { journal({ m: 'stdin', line }); process.exit(4); });
`,
  malformed: `
const rl = require('node:readline').createInterface({ input: process.stdin });
out({ event: 'init', conversation_id: 'conv-bad' });
rl.on('line', (line) => {
  journal({ m: 'stdin', line });
  process.stdout.write('this is not a json envelope\\n');
});
`,
  unknown: `
const rl = require('node:readline').createInterface({ input: process.stdin });
out({ event: 'init', conversation_id: 'conv-unknown' });
rl.on('line', (line) => {
  journal({ m: 'stdin', line });
  out({ event: 'telepathy', payload: 'control frame the hub must never invent' });
});
`,
  orphan: `
const { spawn } = require('node:child_process');
out({ event: 'init', conversation_id: 'conv-orphan' });
// An owned-group helper that shrugs off everything but SIGKILL, then the
// leader exits FIRST — before the hub ever requests a shutdown.
const helper = spawn('/bin/sh', ['-c', "trap '' TERM INT HUP; while :; do sleep 1; done"], { stdio: 'ignore' });
journal({ m: 'helper', pid: helper.pid });
setTimeout(() => process.exit(0), 150);
`,
};

function fakeAgySource(scenario: string, help: string): string {
  return [
    "#!/usr/bin/env node",
    'const fs = require("node:fs");',
    "const J = process.env.AGY_FAKE_JOURNAL;",
    "const journal = (o) => { if (J) fs.appendFileSync(J, JSON.stringify(o) + '\\n'); };",
    "const out = (o) => { journal({ m: 'sent', event: o.event }); process.stdout.write(JSON.stringify(o) + '\\n'); };",
    "journal({ m: 'argv', argv: process.argv.slice(2) });",
    "if (process.argv.includes('--version')) { process.stdout.write('agy 1.1.26\\n'); process.exit(0); }",
    "if (process.argv.includes('--help')) { process.stdout.write(" + JSON.stringify(help) + "); process.exit(0); }",
    SCENARIOS[scenario],
  ].join("\n");
}

interface FakeHandle {
  dir: string;
  bin: string;
  journalPath: string;
  environment: NodeJS.ProcessEnv;
}

async function makeFakeAgy(scenario: string, help: string = HELP_WITH_RESUME): Promise<FakeHandle> {
  const dir = await mkdtemp(join(tmpdir(), "agent-hub-agy-"));
  const bin = join(dir, "agy");
  await writeFile(bin, fakeAgySource(scenario, help));
  await chmod(bin, 0o755);
  const journalPath = join(dir, "journal.jsonl");
  return {
    dir,
    bin,
    journalPath,
    environment: { ...process.env, AGY_FAKE_JOURNAL: journalPath },
  };
}

async function readJournal(handle: FakeHandle): Promise<FakeJournalEntry[]> {
  const raw = await readFile(handle.journalPath, "utf8");
  return raw
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line) as FakeJournalEntry);
}

/**
 * Drains the pump into an array; waiters wake on the real event (no polling
 * loop), with a single deadline timer per wait as the failure guard.
 */
class EventRecorder {
  readonly events: LiveEvent[] = [];
  done = false;
  private gate: Deferred<void> = deferred<void>();

  constructor(iterable: AsyncIterable<LiveEvent>) {
    void (async () => {
      for await (const event of iterable) {
        this.events.push(event);
        this.gate.resolve();
        this.gate = deferred<void>();
      }
      this.done = true;
      this.gate.resolve();
    })();
  }

  async waitUntil(label: string, predicate: (events: LiveEvent[]) => boolean, timeoutMs = 5000): Promise<LiveEvent[]> {
    const deadline = Date.now() + timeoutMs;
    while (!predicate(this.events)) {
      const remaining = deadline - Date.now();
      if (remaining <= 0) {
        break;
      }
      const gate = this.gate;
      await Promise.race([gate.promise, sleep(remaining)]);
    }
    if (!predicate(this.events)) {
      throw new Error(
        `timeout waiting for ${label}; saw ${JSON.stringify(
          this.events.map((e) => (e.body.kind === "status" ? `status:${e.body.status}` : e.body.kind)),
        )}`,
      );
    }
    return this.events;
  }

  async waitDone(timeoutMs = 6000): Promise<void> {
    await this.waitUntil("pump end", () => this.done, timeoutMs);
  }

  bodiesOf<K extends LiveEventBody["kind"]>(kind: K): Extract<LiveEventBody, { kind: K }>[] {
    return this.events
      .filter((event): event is LiveEvent & { body: Extract<LiveEventBody, { kind: K }> } => event.body.kind === kind)
      .map((event) => event.body);
  }
}

function sleep(ms: number): Promise<void> {
  const { promise, resolve } = deferred<void>();
  setTimeout(resolve, ms);
  return promise;
}

function describeEvent(event: LiveEvent): string {
  return event.body.kind === "status" ? `status:${event.body.status}` : event.body.kind;
}

let commandSeq = 0;
function commandBase() {
  commandSeq += 1;
  return {
    command_id: `cmd-${commandSeq}`,
    live_session_id: "ls-agy",
    issued_at: new Date().toISOString(),
  };
}
function promptCommand(text: string): LivePromptCommand {
  return { ...commandBase(), kind: "prompt", text };
}
function followUpCommand(text: string): LiveFollowUpCommand {
  return { ...commandBase(), kind: "follow_up", text };
}
function steerCommand(text: string): LiveSteerCommand {
  return { ...commandBase(), kind: "steer", text };
}
function statusCommand(): LiveStatusCommand {
  return { ...commandBase(), kind: "status" };
}
function permissionResponse(requestId: string, decision: "allow_once" | "allow_session" | "deny"): LivePermissionResponseCommand {
  return { ...commandBase(), kind: "permission_response", request_id: requestId, decision, note: null };
}
function cancelCommand(reason: string | null) {
  return { ...commandBase(), kind: "cancel" as const, reason };
}

function launchRequest(workspace: string, resume: AgyResumeState | null = null) {
  return { live_session_id: "ls-agy", workspace, max_text_bytes: 256, resume };
}

function idleCount(events: LiveEvent[]): number {
  return events.filter((e) => e.body.kind === "status" && e.body.status === "idle").length;
}

describe("agy stream-json probe", () => {
  it("reads version and resume-argv support from the installed command", async () => {
    const fake = await makeFakeAgy("echo");
    try {
      const probe = await probeAgy({ command: fake.bin, environment: fake.environment });
      expect(probe.found).toBe(true);
      expect(probe.version).toBe("1.1.26");
      expect(probe.resume_argv_verified).toBe(true);
      expect(probe.detail).toContain("--version");
    } finally {
      await removeDirectory(fake.dir);
    }
  });

  it("reports resume_argv_verified false when --help lacks --conversation", async () => {
    const fake = await makeFakeAgy("echo", HELP_WITHOUT_RESUME);
    try {
      const probe = await probeAgy({ command: fake.bin, environment: fake.environment });
      expect(probe.found).toBe(true);
      expect(probe.resume_argv_verified).toBe(false);
    } finally {
      await removeDirectory(fake.dir);
    }
  });

  it("reports found=false for a missing command", async () => {
    const probe = await probeAgy({ command: "definitely-not-installed-agy-xyz", environment: process.env });
    expect(probe.found).toBe(false);
    expect(probe.version).toBeNull();
    expect(probe.resume_argv_verified).toBe(false);
  });
});

describe("agy stream-json transport", () => {
  it("launches the exact stream-json argv and writes only exact user envelopes", async () => {
    const fake = await makeFakeAgy("echo");
    const transport = new AgyStreamJsonTransport({ command: fake.bin, environment: fake.environment });
    try {
      const recorder = new EventRecorder(transport.events());
      const report = await transport.open(launchRequest(fake.dir));
      expect(report.pid).not.toBeNull();
      expect(report.provider_session_id).toBe("conv-1");

      await transport.send(promptCommand('fix bug $(rm -rf "x")'));
      await recorder.waitUntil("first turn closed", (events) => idleCount(events) >= 2);
      await transport.send(followUpCommand("second turn"));
      await recorder.waitUntil("second turn closed", (events) => idleCount(events) >= 3);

      const stopped = await transport.stop("graceful");
      expect(stopped.status).toBe("closed");

      const journal = await readJournal(fake);
      const launchArgv = journal.filter((e) => e.m === "argv" && e.argv?.includes("--input-format"));
      expect(launchArgv).toHaveLength(1);
      expect(launchArgv[0].argv).toEqual([
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
      ]);
      const stdinLines = journal.filter((e) => e.m === "stdin").map((e) => e.line);
      expect(stdinLines).toEqual([
        '{"event":"user","message":"fix bug $(rm -rf \\"x\\")"}',
        '{"event":"user","message":"second turn"}',
      ]);
      // The follow-up line may appear only after the first result went out.
      const firstResultIndex = journal.findIndex((e) => e.m === "sent" && e.event === "result");
      const secondStdinIndex = journal.findIndex((e, i) => e.m === "stdin" && i > firstResultIndex);
      expect(secondStdinIndex).toBeGreaterThan(firstResultIndex);
    } finally {
      await transport.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });

  it("normalizes init/step_update/result into bounded events with a final marker and usage", async () => {
    const fake = await makeFakeAgy("rich");
    const transport = new AgyStreamJsonTransport({ command: fake.bin, environment: fake.environment });
    try {
      const recorder = new EventRecorder(transport.events());
      await transport.open(launchRequest(fake.dir));
      await transport.send(promptCommand("go"));
      await recorder.waitUntil("idle after usage", (events) => {
        const usageIndex = events.findIndex((e) => e.body.kind === "usage");
        return usageIndex >= 0 && events.some((e, i) => i > usageIndex && e.body.kind === "status" && e.body.status === "idle");
      });

      const bodies = recorder.events.map((e) => e.body);
      expect(bodies[0]).toMatchObject({ kind: "status", status: "idle" });
      expect(bodies).toContainEqual(expect.objectContaining({ kind: "status", status: "running" }));

      const texts = recorder.bodiesOf("text");
      expect(texts.map((t) => t.role)).toEqual(["reasoning", "assistant", "assistant", "assistant"]);
      expect(texts[1].text).toEqual({ text: "partial ", truncated: false });
      expect(texts[3].final).toBe(true);
      expect(texts[1].stream_id).toBe(texts[2].stream_id);

      const toolStarts = recorder.bodiesOf("tool_start");
      expect(toolStarts[0]).toMatchObject({ call_id: "c-9", tool: "bash", input_preview: { text: "npm test", truncated: false } });
      const toolEnds = recorder.bodiesOf("tool_end");
      expect(toolEnds[0]).toMatchObject({ call_id: "c-9", tool: "bash", ok: true, output_preview: { text: "23 passed", truncated: false } });

      expect(recorder.bodiesOf("unrecognized")).toEqual([
        { kind: "unrecognized", transport_kind: "hologram", bytes: expect.any(Number) },
      ]);

      // reported-zero and unreported stay distinct: output_tokens 0 vs cached null.
      expect(recorder.bodiesOf("usage")).toEqual([
        { kind: "usage", usage: { input_tokens: 11, output_tokens: 0, cached_tokens: null, cost_usd: 0.25 } },
      ]);

      // gapless per-session seq starting at 1
      expect(recorder.events.map((e) => e.seq)).toEqual(recorder.events.map((_, i) => i + 1));

      await transport.stop("graceful");
    } finally {
      await transport.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });

  it("bounds text at the session byte cap and flags truncation", async () => {
    const fake = await makeFakeAgy("rich");
    const transport = new AgyStreamJsonTransport({ command: fake.bin, environment: fake.environment });
    try {
      const recorder = new EventRecorder(transport.events());
      await transport.open({ live_session_id: "ls-agy", workspace: fake.dir, max_text_bytes: 8, resume: null });
      await transport.send(promptCommand("go"));
      await recorder.waitUntil("usage seen", (events) => events.some((e) => e.body.kind === "usage"));
      const texts = recorder.bodiesOf("text");
      expect(texts.filter((t) => t.text.truncated).length).toBeGreaterThan(0);
      for (const t of texts) {
        expect(Buffer.byteLength(t.text.text, "utf8")).toBeLessThanOrEqual(8);
      }
      await transport.stop("graceful");
    } finally {
      await transport.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });

  it("queues follow_up while running and releases it only after the result", async () => {
    const fake = await makeFakeAgy("slow");
    const transport = new AgyStreamJsonTransport({ command: fake.bin, environment: fake.environment });
    try {
      const recorder = new EventRecorder(transport.events());
      await transport.open(launchRequest(fake.dir));
      await transport.send(promptCommand("one"));
      await transport.send(followUpCommand("two"));

      await recorder.waitUntil("second turn closed", (events) => idleCount(events) >= 3);
      const journal = await readJournal(fake);
      const stdinLines = journal.filter((e) => e.m === "stdin").map((e) => e.line);
      expect(stdinLines).toEqual([
        '{"event":"user","message":"one"}',
        '{"event":"user","message":"two"}',
      ]);
      // Queued, not eager: the second user line must land after turn 1's result.
      const firstResultIndex = journal.findIndex((e) => e.m === "sent" && e.event === "result");
      const secondStdinIndex = journal.findIndex((e, i) => e.m === "stdin" && i > firstResultIndex);
      expect(secondStdinIndex).toBeGreaterThan(firstResultIndex);
    } finally {
      await transport.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });

  it("cancels with SIGINT to the process group and writes no control frame", async () => {
    const fake = await makeFakeAgy("sigint");
    const transport = new AgyStreamJsonTransport({ command: fake.bin, environment: fake.environment });
    try {
      const recorder = new EventRecorder(transport.events());
      await transport.open(launchRequest(fake.dir));
      await transport.send(promptCommand("unfinishable"));
      await recorder.waitUntil("running", (events) => events.some((e) => e.body.kind === "status" && e.body.status === "running"));
      await transport.send(cancelCommand("user asked"));
      await recorder.waitUntil("sigint exit", (events) => events.some((e) => e.body.kind === "exit"));

      const bodies = recorder.events.map((e) => e.body);
      expect(bodies).toContainEqual(expect.objectContaining({ kind: "status", status: "cancelling", note: "user asked" }));
      expect(bodies).toContainEqual(expect.objectContaining({ kind: "status", status: "idle" }));
      const exit = recorder.bodiesOf("exit")[0];
      expect(exit.intentional).toBe(true);

      const journal = await readJournal(fake);
      expect(journal.filter((e) => e.m === "stdin")).toHaveLength(1);
      expect(journal.some((e) => e.m === "sigint")).toBe(true);
      // no invented frames: every stdin line is a user envelope
      expect(journal.filter((e) => e.m === "stdin").every((e) => e.line?.startsWith('{"event":"user"'))).toBe(true);
      await transport.stop("graceful");
    } finally {
      await transport.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });

  it("treats exit before the result envelope as an error with exit evidence", async () => {
    const fake = await makeFakeAgy("die");
    const transport = new AgyStreamJsonTransport({ command: fake.bin, environment: fake.environment });
    try {
      const recorder = new EventRecorder(transport.events());
      await transport.open(launchRequest(fake.dir));
      await transport.send(promptCommand("doomed"));
      await recorder.waitDone();

      const errors = recorder.bodiesOf("error");
      expect(errors[0].error).toMatchObject({ code: "LIVE_AGY_EXITED_BEFORE_RESULT", stage: "provider" });
      const exit = recorder.bodiesOf("exit")[0];
      expect(exit).toMatchObject({ intentional: false, exit_code: 4 });
      expect(recorder.bodiesOf("status").map((s) => s.status)).toContain("error");

      const stopped = await transport.stop("terminate");
      expect(stopped.status).toBe("closed");
      expect(stopped.exit_code).toBe(4);
    } finally {
      await transport.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });

  it("kills the stream on a malformed envelope", async () => {
    const fake = await makeFakeAgy("malformed");
    const transport = new AgyStreamJsonTransport({ command: fake.bin, environment: fake.environment });
    try {
      const recorder = new EventRecorder(transport.events());
      await transport.open(launchRequest(fake.dir));
      await transport.send(promptCommand("poison"));
      await recorder.waitDone(8000);

      const errors = recorder.bodiesOf("error");
      expect(errors[0].error).toMatchObject({ code: "LIVE_AGY_MALFORMED_ENVELOPE", stage: "protocol", retryable: false });
      expect(recorder.bodiesOf("status").map((s) => s.status)).toContain("error");

      const stopped = await transport.stop("graceful");
      expect(stopped.status).toBe("closed");
    } finally {
      await transport.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });

  it("kills the stream on an unknown event envelope (no invented controls understood)", async () => {
    const fake = await makeFakeAgy("unknown");
    const transport = new AgyStreamJsonTransport({ command: fake.bin, environment: fake.environment });
    try {
      const recorder = new EventRecorder(transport.events());
      await transport.open(launchRequest(fake.dir));
      await transport.send(promptCommand("telepathy"));
      await recorder.waitDone(8000);
      expect(recorder.bodiesOf("error")[0].error.code).toBe("LIVE_AGY_MALFORMED_ENVELOPE");
    } finally {
      await transport.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });

  it("accepts the prompt exactly once and rejects steer/status/permission without touching stdin", async () => {
    const fake = await makeFakeAgy("echo");
    const transport = new AgyStreamJsonTransport({ command: fake.bin, environment: fake.environment });
    try {
      const recorder = new EventRecorder(transport.events());
      await transport.open(launchRequest(fake.dir));

      await transport.send(promptCommand("first"));
      await recorder.waitUntil("first idle", (events) => idleCount(events) >= 2);

      await expect(transport.send(promptCommand("again"))).rejects.toThrow(/exactly once/);
      await expect(transport.send(steerCommand("nudge"))).rejects.toThrow(/steer/);
      await expect(transport.send(statusCommand())).rejects.toThrow(/status/);
      await expect(transport.send(permissionResponse("perm-1", "allow_once"))).rejects.toThrow(/permission/);

      const journal = await readJournal(fake);
      expect(journal.filter((e) => e.m === "stdin")).toHaveLength(1);
      await transport.stop("graceful");
    } finally {
      await transport.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });

  it("requires verified --conversation argv and identity echo for resume", async () => {
    const unverified = await makeFakeAgy("echo");
    try {
      const transport = new AgyStreamJsonTransport({ command: unverified.bin, environment: unverified.environment });
      const resume: AgyResumeState = {
        provider: "agy",
        provider_session_id: "conv-9",
        resume_argv_verified: false,
        verified: false,
        verified_via: null,
      };
      await expect(transport.open(launchRequest(unverified.dir, resume))).rejects.toThrow(/resume requires/i);
      // Never even spawned: no journal exists.
      await expect(readJournal(unverified)).rejects.toThrow();
    } finally {
      await removeDirectory(unverified.dir);
    }

    const match = await makeFakeAgy("echo");
    try {
      match.environment.AGY_FAKE_CONV_ID = "conv-9";
      const transport = new AgyStreamJsonTransport({ command: match.bin, environment: match.environment });
      const resume: AgyResumeState = {
        provider: "agy",
        provider_session_id: "conv-9",
        resume_argv_verified: true,
        verified: false,
        verified_via: null,
      };
      const report = await transport.open(launchRequest(match.dir, resume));
      expect(report.provider_session_id).toBe("conv-9");
      expect(transport.resumeState()).toMatchObject({
        provider: "agy",
        provider_session_id: "conv-9",
        resume_argv_verified: true,
        verified: true,
        verified_via: expect.stringContaining("--conversation"),
      });
      const journal = await readJournal(match);
      const launchArgv = journal.filter((e) => e.m === "argv" && e.argv?.includes("--conversation"));
      expect(launchArgv).toHaveLength(1);
      expect(launchArgv[0].argv).toEqual([
        "--input-format",
        "stream-json",
        "--output-format",
        "stream-json",
        "--conversation",
        "conv-9",
      ]);
      await transport.stop("graceful");
    } finally {
      await removeDirectory(match.dir);
    }

    const mismatch = await makeFakeAgy("echo");
    try {
      mismatch.environment.AGY_FAKE_CONV_ID = "conv-somebody-elses";
      const transport = new AgyStreamJsonTransport({ command: mismatch.bin, environment: mismatch.environment });
      const resume: AgyResumeState = {
        provider: "agy",
        provider_session_id: "conv-9",
        resume_argv_verified: true,
        verified: false,
        verified_via: null,
      };
      await expect(transport.open(launchRequest(mismatch.dir, resume))).rejects.toThrow(/different conversation/i);
      const recorder = new EventRecorder(transport.events());
      await recorder.waitUntil("error surfaced", (events) => events.some((e) => e.body.kind === "error"));
      expect(recorder.bodiesOf("error")[0].error.code).toBe("LIVE_AGY_RESUME_IDENTITY_MISMATCH");
      const stopped = await transport.stop("terminate");
      expect(stopped.status).toBe("closed");
    } finally {
      await removeDirectory(mismatch.dir);
    }
  });

  it("declares contract capabilities backed by probed evidence", async () => {
    const fake = await makeFakeAgy("echo");
    try {
      const transport = new AgyStreamJsonTransport({ command: fake.bin, environment: fake.environment });
      const descriptor = await transport.describe();
      expect(descriptor.transport).toBe("agy-stream-json");
      expect(descriptor.provider).toBe("agy");
      const caps = descriptor.capabilities;
      expect(caps.prompt.support).toBe("native");
      expect(caps.prompt.evidence).toContain("1.1.26");
      expect(caps.follow_up.support).toBe("hub-queued");
      expect(caps.steer).toEqual({ support: "unsupported", evidence: null });
      expect(caps.cancel.support).toBe("signal");
      expect(caps.status.support).toBe("derived");
      expect(caps.permission_response).toEqual({ support: "unsupported", evidence: null });
      expect(caps.resume.support).toBe("native");
      expect(caps.resume.evidence).toContain("--conversation");
      expect(caps.checkpoint).toEqual({ support: "unsupported", evidence: null });
      expect(caps.usage_reporting.support).toBe("derived");
    } finally {
      await removeDirectory(fake.dir);
    }
  });

  it(
    "leader exits first, helper survives: graceful orphaned with no group signal; terminate kills and closes",
    async () => {
      const fake = await makeFakeAgy("orphan");
      const transport = new AgyStreamJsonTransport({ command: fake.bin, environment: fake.environment });
      try {
        const recorder = new EventRecorder(transport.events());
        await transport.open(launchRequest(fake.dir));
        await recorder.waitUntil("leader exit", (events) =>
          events.some((e) => e.body.kind === "exit"),
        );

        const journal = await readJournal(fake);
        const helperPid = journal.find((entry) => entry.m === "helper")?.pid;
        expect(typeof helperPid).toBe("number");
        expect(realProcessAlive(helperPid as number)).toBe(true);

        await runLeaderFirstSurvivorScenario({
          stop: (mode) => transport.stop(mode),
          survivorAlive: () => realProcessAlive(helperPid as number),
        });
      } finally {
        await transport.stop("terminate");
        await removeDirectory(fake.dir);
      }
    },
    30_000,
  );
});

