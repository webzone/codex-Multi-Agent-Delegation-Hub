import { chmod, mkdtemp, readFile, writeFile } from "node:fs/promises";
import { createRequire } from "node:module";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { describe, expect, it } from "vitest";

import { deferred, type Deferred } from "../src/deferred.js";
import { HermesAcpTransport, type HermesTransportOptions } from "../src/live/transports/hermes-acp.js";
import { probeHermes } from "../src/live/probes/hermes.js";
import type {
  HermesResumeState,
  LiveEvent,
  LiveEventBody,
  LiveFollowUpCommand,
  LivePermissionDecision,
  LivePromptCommand,
  LiveSteerCommand,
  LiveStatusCommand,
} from "../src/live/types.js";
import { removeDirectory } from "./helpers.js";

/**
 * Fake-protocol tests for the Hermes ACP transport (Hermes 0.20.5, ACP v1).
 * The fake speaks real ACP over stdio using the *official SDK's* agent side
 * (AgentSideConnection), so the runtime handshake, session/new, session/load,
 * prompt/update, cancel and permission round-trips are exercised against the
 * same library the transport uses. Every request the fake receives is
 * journaled to JSONL. Real timers appear only in genuine subprocess / timeout
 * integration cases (fake clocks cannot drive an OS process).
 */

const require = createRequire(import.meta.url);
const sdkEntry = pathToFileURL(require.resolve("@zed-industries/agent-client-protocol")).href;

interface FakeJournalEntry {
  m: string;
  argv?: string[];
  method?: string;
  params?: Record<string, unknown>;
  response?: unknown;
  [key: string]: unknown;
}

function fakeHermesSource(): string {
  return [
    "#!/usr/bin/env node",
    `import(${JSON.stringify(sdkEntry)}).then(async (acp) => {`,
    "  const fs = await import('node:fs');",
    "  const { Readable, Writable } = await import('node:stream');",
    "  const J = process.env.HERMES_FAKE_JOURNAL;",
    "  const mode = process.env.HERMES_FAKE_MODE || 'default';",
    "  const journal = (o) => { if (J) fs.appendFileSync(J, JSON.stringify(o) + '\\n'); };",
    "  journal({ m: 'argv', argv: process.argv.slice(2) });",
    "  if (process.argv.includes('--version')) { process.stdout.write('hermes 0.20.5\\n'); process.exit(0); }",
    "  let resolveCancel = null;",
    "  const cancelled = new Promise((r) => { resolveCancel = r; });",
    "  const conn = new acp.AgentSideConnection(() => ({",
    "    async initialize(params) {",
    "      journal({ m: 'initialize', params });",
    "      if (mode === 'bad-version') return { protocolVersion: 2, agentCapabilities: {} };",
    "      return { protocolVersion: 1, agentCapabilities: { loadSession: mode !== 'no-load' } };",
    "    },",
    "    async newSession(params) { journal({ m: 'new', params }); return { sessionId: 'hermes-sess-1' }; },",
    "    async loadSession(params) {",
    "      journal({ m: 'load', params });",
    "      await conn.sessionUpdate({ sessionId: params.sessionId, update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: 'restored' } } });",
    "      return {};",
    "    },",
    "    async prompt(params) {",
    "      journal({ m: 'prompt', params });",
    "      const text = params.prompt.map((b) => b.text || '').join('');",
    "      if (text.includes('DIE')) process.exit(3);",
    "      if (text.includes('PERM')) {",
    "        const answer = await conn.requestPermission({",
    "          sessionId: params.sessionId,",
    "          toolCall: { toolCallId: 'call-perm', title: 'delete files', kind: 'execute' },",
    "          options: [",
    "            { optionId: 'opt-allow', kind: 'allow_once', name: 'Allow once' },",
    "            { optionId: 'opt-always', kind: 'allow_always', name: 'Always allow' },",
    "            { optionId: 'opt-reject', kind: 'reject_once', name: 'Deny' },",
    "          ],",
    "        });",
    "        journal({ m: 'permission', response: answer });",
    "      }",
    "      await conn.sessionUpdate({ sessionId: params.sessionId, update: { sessionUpdate: 'agent_thought_chunk', content: { type: 'text', text: 'thinking' } } });",
    "      await conn.sessionUpdate({ sessionId: params.sessionId, update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: 'answer ' } } });",
    "      await conn.sessionUpdate({ sessionId: params.sessionId, update: { sessionUpdate: 'agent_message_chunk', content: { type: 'text', text: 'done' } } });",
    "      await conn.sessionUpdate({ sessionId: params.sessionId, update: { sessionUpdate: 'tool_call', toolCallId: 'tc-1', title: 'bash npm test', kind: 'execute', status: 'pending', rawInput: { cmd: 'npm test' } } });",
    "      await conn.sessionUpdate({ sessionId: params.sessionId, update: { sessionUpdate: 'tool_call_update', toolCallId: 'tc-1', status: 'completed', rawOutput: { code: 0 } } });",
    "      await conn.sessionUpdate({ sessionId: params.sessionId, update: { sessionUpdate: 'plan', entries: [{ content: 'x', status: 'pending', priority: 'high' }] } });",
    "      if (text.includes('HANG')) { await cancelled; return { stopReason: 'cancelled' }; }",
    "      return { stopReason: 'end_turn' };",
    "    },",
    "    async cancel(params) { journal({ m: 'cancel', params }); if (resolveCancel) resolveCancel(); return {}; },",
    "  }), acp.ndJsonStream(Writable.toWeb(process.stdout), Readable.toWeb(process.stdin)));",
    "  process.on('SIGTERM', () => process.exit(0));",
    "  process.on('SIGINT', () => process.exit(0));",
    "}).catch((e) => { process.stderr.write(String(e && e.stack || e)); process.exit(1); });",
  ].join("\n");
}

interface FakeHandle {
  dir: string;
  bin: string;
  journalPath: string;
  environment: NodeJS.ProcessEnv;
}

async function makeFakeHermes(mode: string = "default", extra: NodeJS.ProcessEnv = {}): Promise<FakeHandle> {
  const dir = await mkdtemp(join(tmpdir(), "agent-hub-hermes-"));
  const bin = join(dir, "hermes");
  await writeFile(bin, fakeHermesSource());
  await chmod(bin, 0o755);
  const journalPath = join(dir, "journal.jsonl");
  return {
    dir,
    bin,
    journalPath,
    environment: { ...process.env, HERMES_FAKE_JOURNAL: journalPath, HERMES_FAKE_MODE: mode, ...extra },
  };
}

async function readJournal(handle: FakeHandle): Promise<FakeJournalEntry[]> {
  const raw = await readFile(handle.journalPath, "utf8");
  return raw
    .split("\n")
    .filter(Boolean)
    .map((line) => JSON.parse(line) as FakeJournalEntry);
}

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

  async waitUntil(label: string, predicate: (events: LiveEvent[]) => boolean, timeoutMs = 6000): Promise<LiveEvent[]> {
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

  async waitDone(timeoutMs = 8000): Promise<void> {
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

let commandSeq = 0;
function commandBase() {
  commandSeq += 1;
  return { command_id: `hcmd-${commandSeq}`, live_session_id: "ls-hermes", issued_at: new Date().toISOString() };
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
function cancelCommand(reason: string | null) {
  return { ...commandBase(), kind: "cancel" as const, reason };
}
function permissionResponse(requestId: string, decision: LivePermissionDecision): LivePermissionResponseCommand {
  return { ...commandBase(), kind: "permission_response", request_id: requestId, decision, note: null };
}

function launchRequest(workspace: string, resume: HermesResumeState | null = null) {
  return { live_session_id: "ls-hermes", workspace, max_text_bytes: 256, resume };
}

function transport(fake: FakeHandle, options: Omit<HermesTransportOptions, "command" | "environment"> = {}): HermesAcpTransport {
  return new HermesAcpTransport({ command: fake.bin, environment: fake.environment, ...options });
}

function idleCount(events: LiveEvent[]): number {
  return events.filter((e) => e.body.kind === "status" && e.body.status === "idle").length;
}

describe("hermes acp probe", () => {
  it("reads the installed version and defers the handshake to open", async () => {
    const fake = await makeFakeHermes();
    try {
      const probe = await probeHermes({ command: fake.bin, environment: fake.environment });
      expect(probe.found).toBe(true);
      expect(probe.version).toBe("0.20.5");
      expect(probe.detail).toContain("handshake");
    } finally {
      await removeDirectory(fake.dir);
    }
  });

  it("reports found=false for a missing command", async () => {
    const probe = await probeHermes({ command: "definitely-not-installed-hermes-xyz", environment: process.env });
    expect(probe.found).toBe(false);
    expect(probe.version).toBeNull();
  });
});

describe("hermes acp transport", () => {
  it("verifies the ACP v1 handshake and session/new over stdio", async () => {
    const fake = await makeFakeHermes();
    const t = transport(fake);
    try {
      const recorder = new EventRecorder(t.events());
      const report = await t.open(launchRequest(fake.dir));
      expect(report.provider_session_id).toBe("hermes-sess-1");
      expect(report.pid).not.toBeNull();
      await recorder.waitUntil("launch idle event", (events) => events.length > 0);
      expect(recorder.events[0].body).toMatchObject({ kind: "status", status: "idle" });

      const journal = await readJournal(fake);
      expect(journal.find((e) => e.m === "argv")?.argv).toEqual(["acp"]);
      const init = journal.find((e) => e.m === "initialize");
      expect(init?.params).toMatchObject({ protocolVersion: 1 });
      expect(journal.find((e) => e.m === "new")?.params).toMatchObject({ cwd: fake.dir, mcpServers: [] });

      const stopped = await t.stop("graceful");
      expect(stopped.status).toBe("closed");
    } finally {
      await t.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });

  it("refuses a non-v1 protocol negotiation", async () => {
    const fake = await makeFakeHermes("bad-version");
    const t = transport(fake);
    try {
      await expect(t.open(launchRequest(fake.dir))).rejects.toThrow(/protocolVersion 2|speaks v1/i);
      const stopped = await t.stop("terminate");
      expect(stopped.status).toBe("closed");
    } finally {
      await removeDirectory(fake.dir);
    }
  });

  it("normalizes prompt/update/cancel with the turn boundary at the prompt response", async () => {
    const fake = await makeFakeHermes();
    const t = transport(fake);
    try {
      const recorder = new EventRecorder(t.events());
      await t.open(launchRequest(fake.dir));
      await t.send(promptCommand("explain this"));
      await recorder.waitUntil("turn closed", (events) => idleCount(events) >= 2);

      const texts = recorder.bodiesOf("text");
      expect(texts.map((x) => x.role)).toEqual(["reasoning", "assistant", "assistant", "assistant"]);
      expect(texts[3].final).toBe(true);
      expect(texts[1].text.text + texts[2].text.text).toBe("answer done");

      expect(recorder.bodiesOf("tool_start")[0]).toMatchObject({ call_id: "tc-1", input_preview: { text: '{"cmd":"npm test"}' } });
      expect(recorder.bodiesOf("tool_end")[0]).toMatchObject({ call_id: "tc-1", ok: true });
      // plan carries no normalized body: kind + bytes only, no raw payload.
      expect(recorder.bodiesOf("unrecognized")).toEqual([
        { kind: "unrecognized", transport_kind: "plan", bytes: expect.any(Number) },
      ]);
      expect(t.resumeState()).toMatchObject({ verified: false, session_load_advertised: true });

      await t.stop("graceful");
    } finally {
      await t.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });

  it("delivers session/cancel natively and concludes the turn cancelled", async () => {
    const fake = await makeFakeHermes();
    const t = transport(fake);
    try {
      const recorder = new EventRecorder(t.events());
      await t.open(launchRequest(fake.dir));
      await t.send(promptCommand("HANG forever"));
      await recorder.waitUntil("running", (events) => events.some((e) => e.body.kind === "status" && e.body.status === "running"));
      await t.send(cancelCommand("stop it"));
      await recorder.waitUntil("cancelled idle", (events) => idleCount(events) >= 2);

      expect(recorder.bodiesOf("status").map((s) => s.status)).toContain("cancelling");
      const journal = await readJournal(fake);
      expect(journal.some((e) => e.m === "cancel")).toBe(true);
      expect(journal.some((e) => e.m === "prompt")).toBe(true);
    } finally {
      await t.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });

  it("queues follow_up until the prompt response resolves", async () => {
    const fake = await makeFakeHermes();
    const t = transport(fake);
    try {
      const recorder = new EventRecorder(t.events());
      await t.open(launchRequest(fake.dir));
      await t.send(promptCommand("HANG first"));
      await recorder.waitUntil("running", (events) => events.some((e) => e.body.kind === "status" && e.body.status === "running"));
      await t.send(followUpCommand("second"));
      await t.send(cancelCommand(null));
      await recorder.waitUntil("two turns settled", (events) => idleCount(events) >= 3);

      const journal = await readJournal(fake);
      const prompts = journal.filter((e) => e.m === "prompt").map((e) => (e.params?.prompt as { text: string }[])[0].text);
      expect(prompts).toEqual(["HANG first", "second"]);
      // follow_up was not sent while the first turn was mid-flight.
      const firstCancelIndex = journal.findIndex((e) => e.m === "cancel");
      const secondPromptIndex = journal.findIndex((e, i) => e.m === "prompt" && i > 0 && journal.slice(0, i).some((x) => x.m === "prompt"));
      expect(secondPromptIndex).toBeGreaterThan(firstCancelIndex);
    } finally {
      await t.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });

  it("resumes via session/load when the agent advertises loadSession and echoes identity", async () => {
    const fake = await makeFakeHermes();
    const t = transport(fake);
    try {
      const recorder = new EventRecorder(t.events());
      const resume: HermesResumeState = {
        provider: "hermes",
        provider_session_id: "hermes-sess-1",
        session_load_advertised: true,
        verified: false,
        verified_via: null,
      };
      const report = await t.open(launchRequest(fake.dir, resume));
      expect(report.provider_session_id).toBe("hermes-sess-1");
      await recorder.waitUntil("restored", (events) => events.some((e) => e.body.kind === "text"));
      expect(t.resumeState()).toMatchObject({
        verified: true,
        session_load_advertised: true,
        verified_via: expect.stringContaining("session/load"),
      });
      const journal = await readJournal(fake);
      expect(journal.find((e) => e.m === "load")?.params).toMatchObject({ sessionId: "hermes-sess-1" });
      expect(journal.some((e) => e.m === "new")).toBe(false);
      await t.stop("graceful");
    } finally {
      await t.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });

  it("refuses to start a fresh session under a resume when loadSession is not advertised", async () => {
    const fake = await makeFakeHermes("no-load");
    const t = transport(fake);
    try {
      const resume: HermesResumeState = {
        provider: "hermes",
        provider_session_id: "hermes-sess-1",
        session_load_advertised: false,
        verified: false,
        verified_via: null,
      };
      await expect(t.open(launchRequest(fake.dir, resume))).rejects.toThrow(/loadSession|resume/i);
      const journal = await readJournal(fake);
      // Never fell back to session/new to fake a resume.
      expect(journal.some((e) => e.m === "new")).toBe(false);
      expect(journal.some((e) => e.m === "load")).toBe(false);
      const stopped = await t.stop("terminate");
      expect(stopped.status).toBe("closed");
    } finally {
      await removeDirectory(fake.dir);
    }
  });

  it("headless denies every permission with reject_once, never allow_always", async () => {
    const fake = await makeFakeHermes();
    const t = transport(fake); // not interactive
    try {
      const recorder = new EventRecorder(t.events());
      await t.open(launchRequest(fake.dir));
      await t.send(promptCommand("PERM now"));
      await recorder.waitUntil("turn closed", (events) => idleCount(events) >= 2);

      expect(recorder.bodiesOf("permission_request").length).toBe(1);
      const journal = await readJournal(fake);
      const answer = journal.find((e) => e.m === "permission")?.response as {
        outcome: { outcome: string; optionId?: string };
      };
      expect(answer.outcome.outcome).toBe("selected");
      expect(answer.outcome.optionId).toBe("opt-reject");
      await t.stop("graceful");
    } finally {
      await t.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });

  it("interactive allow_once selects the allow_once option only", async () => {
    const fake = await makeFakeHermes();
    const t = transport(fake, { interactive: true });
    try {
      const recorder = new EventRecorder(t.events());
      await t.open(launchRequest(fake.dir));
      await t.send(promptCommand("PERM now"));
      await recorder.waitUntil("permission request", (events) => events.some((e) => e.body.kind === "permission_request"));
      const request = recorder.bodiesOf("permission_request")[0];
      await t.send(permissionResponse(request.request_id, "allow_once"));
      await recorder.waitUntil("turn closed", (events) => idleCount(events) >= 2);

      const journal = await readJournal(fake);
      const answer = journal.find((e) => e.m === "permission")?.response as { outcome: { outcome: string; optionId?: string } };
      expect(answer.outcome).toEqual({ outcome: "selected", optionId: "opt-allow" });
      await t.stop("graceful");
    } finally {
      await t.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });

  it("interactive answers speak only allow_once/deny; a widened decision never reaches allow_always", async () => {
    // Typed surface: LivePermissionDecision is exactly "allow_once" | "deny".
    // This assignment fails typecheck if a wider decision ever returns to the union.
    // @ts-expect-error allow_session was removed from LivePermissionDecision
    const widened: LivePermissionDecision = "allow_session";

    const fake = await makeFakeHermes();
    const t = transport(fake, { interactive: true });
    try {
      const recorder = new EventRecorder(t.events());
      await t.open(launchRequest(fake.dir));

      // Typed deny: selects the agent's own reject_once, never allow_always.
      await t.send(promptCommand("PERM first"));
      await recorder.waitUntil("first permission", (events) => events.some((e) => e.body.kind === "permission_request"));
      const first = recorder.bodiesOf("permission_request")[0];
      await t.send(permissionResponse(first.request_id, "deny"));
      await recorder.waitUntil("first turn closed", (events) => idleCount(events) >= 2);

      // The widened value cannot be constructed typed; cast across the
      // manager-free transport boundary it still cannot be honored: anything
      // but allow_once denies.
      await t.send(promptCommand("PERM second"));
      await recorder.waitUntil("second permission", () => recorder.bodiesOf("permission_request").length >= 2);
      const second = recorder.bodiesOf("permission_request")[1];
      await t.send(permissionResponse(second.request_id, widened));
      await recorder.waitUntil("second turn closed", (events) => idleCount(events) >= 3);

      const journal = await readJournal(fake);
      const answers = journal
        .filter((e) => e.m === "permission")
        .map((e) => e.response as { outcome: { outcome: string; optionId?: string } });
      expect(answers).toHaveLength(2);
      expect(answers[0].outcome).toEqual({ outcome: "selected", optionId: "opt-reject" });
      expect(answers[1].outcome).toEqual({ outcome: "selected", optionId: "opt-reject" });
      expect(answers.some((a) => a.outcome.optionId === "opt-always")).toBe(false);
      await t.stop("graceful");
    } finally {
      await t.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });

  it("interactive permission times out to deny", async () => {
    const fake = await makeFakeHermes();
    // Real timer: this asserts the transport's own bounded permission wait.
    const t = transport(fake, { interactive: true, permission_timeout_ms: 200 });
    try {
      const recorder = new EventRecorder(t.events());
      await t.open(launchRequest(fake.dir));
      await t.send(promptCommand("PERM now"));
      await recorder.waitUntil("turn closed after timeout", (events) => idleCount(events) >= 2, 5000);

      const journal = await readJournal(fake);
      const answer = journal.find((e) => e.m === "permission")?.response as { outcome: { outcome: string; optionId?: string } };
      expect(answer.outcome.outcome).toBe("selected");
      expect(answer.outcome.optionId).toBe("opt-reject");
      expect(recorder.bodiesOf("log").some((l) => l.text.text.includes("timed out"))).toBe(true);
    } finally {
      await t.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });

  it("treats agent exit mid-turn as an error with exit evidence", async () => {
    const fake = await makeFakeHermes();
    const t = transport(fake);
    try {
      const recorder = new EventRecorder(t.events());
      await t.open(launchRequest(fake.dir));
      await t.send(promptCommand("DIE now"));
      await recorder.waitDone();

      expect(recorder.bodiesOf("error")[0].error).toMatchObject({ code: "LIVE_ACP_AGENT_EXITED", stage: "provider" });
      const exit = recorder.bodiesOf("exit")[0];
      expect(exit).toMatchObject({ intentional: false, exit_code: 3 });
      const stopped = await t.stop("terminate");
      expect(stopped.exit_code).toBe(3);
    } finally {
      await t.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });

  it("rejects steer and status without touching the wire", async () => {
    const fake = await makeFakeHermes();
    const t = transport(fake);
    try {
      await t.open(launchRequest(fake.dir));
      await expect(t.send(steerCommand("nudge"))).rejects.toThrow(/steer/);
      await expect(t.send(statusCommand())).rejects.toThrow(/status/);
      await t.stop("graceful");
      const journal = await readJournal(fake);
      expect(journal.some((e) => e.m === "prompt")).toBe(false);
    } finally {
      await t.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });

  it("declares contract capabilities backed by the negotiated handshake", async () => {
    const fake = await makeFakeHermes();
    const t = transport(fake);
    try {
      await t.open(launchRequest(fake.dir));
      const descriptor = await t.describe();
      expect(descriptor.transport).toBe("hermes-acp");
      expect(descriptor.provider).toBe("hermes");
      const caps = descriptor.capabilities;
      expect(caps.prompt.support).toBe("native");
      expect(caps.prompt.evidence).toContain("0.4.5");
      expect(caps.follow_up.support).toBe("hub-queued");
      expect(caps.steer).toEqual({ support: "unsupported", evidence: null });
      expect(caps.cancel.support).toBe("native");
      expect(caps.status.support).toBe("derived");
      expect(caps.permission_response.support).toBe("native");
      // loadSession was advertised in the handshake we just completed.
      expect(caps.resume.support).toBe("native");
      // ACP v1 prompt responses carry no usage counters.
      expect(caps.usage_reporting).toEqual({ support: "unsupported", evidence: null });
      expect(caps.checkpoint).toEqual({ support: "unsupported", evidence: null });
    } finally {
      await t.stop("terminate");
      await removeDirectory(fake.dir);
    }
  });
});
