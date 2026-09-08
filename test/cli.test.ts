import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { parseCliCommand, runCli, type CliCommand, type CliIo } from "../src/cli.js";
import { AgentHubSupervisor } from "../src/hub/supervisor.js";
import type { AttachInputPump } from "../src/hub/attach-io.js";
import { PassThrough } from "node:stream";
import type { AgentHubOptions } from "../src/hub/agent-hub.js";
import { bridgeTransportFactory } from "../src/hub/transport-adapter.js";
import {
  fullHubCapabilities,
  hubFakeProbes,
  hubFakeProviderFactory,
  HubFakeFactory,
  type HubFakeTurnBehavior,
} from "./hub-fakes.js";
import { createGitRepository, removeDirectory } from "./helpers.js";

// ---------------------------------------------------------------------------
// Harness: CLI over injected fake transports
// ---------------------------------------------------------------------------

interface CliHarness {
  repository: string;
  hubOptions: AgentHubOptions;
  factory: HubFakeFactory;
  run: (
    argv: string[],
    stdin?: AsyncIterable<string>,
  ) => Promise<{ code: number; out: string; err: string }>;
  cleanup: () => Promise<void>;
}

async function cliHarness(
  options: {
    turn?: HubFakeTurnBehavior;
    resumeEcho?: boolean;
    probeFound?: boolean;
    hubOptions?: AgentHubOptions;
  } = {},
): Promise<CliHarness> {
  const repository = await createGitRepository();
  const tmpRoot = await mkdtemp(join(tmpdir(), "agent-hub-cli-"));
  const factory = new HubFakeFactory(
    () => fullHubCapabilities(),
    { resumeState: options.resumeEcho ? "echo" : undefined },
  );
  factory.defaultTurn = options.turn ?? { writes: { "done.md": "worked\n" } };
  if (options.probeFound === false) {
    factory.probeResult = { found: false, version: null, detail: "no RPC v2 evidence" };
  }
  const hubOptions: AgentHubOptions = {
    transportFactories: [bridgeTransportFactory(factory)],
    providerFactories: [hubFakeProviderFactory],
    tmpRoot,
    probes: hubFakeProbes(),
    ...options.hubOptions,
  };
  return {
    repository,
    hubOptions,
    factory,
    async run(argv, stdin = emptyLines) {
      let out = "";
      let err = "";
      const io: CliIo = {
        stdin: (async function* lines() {
          for await (const line of stdin) yield `${line}\n`;
        })(),
        stdout: { write: (chunk: string) => (out += chunk) },
        stderr: { write: (chunk: string) => (err += chunk) },
      };
      const code = await runCli(argv, io, { hubOptions });
      return { code, out, err };
    },
    cleanup: () => removeDirectory(repository),
  };
}

/** Stream-safe scanner: returns every top-level JSON document in order. */
function parseJsonDocuments(text: string): unknown[] {
  const documents: unknown[] = [];
  let depth = 0;
  let start = -1;
  let inString = false;
  let escaped = false;
  for (let index = 0; index < text.length; index += 1) {
    const char = text[index];
    if (inString) {
      if (escaped) escaped = false;
      else if (char === "\\") escaped = true;
      else if (char === '"') inString = false;
      continue;
    }
    if (char === '"') {
      if (depth > 0) inString = true;
      continue;
    }
    if (char === "{") {
      if (depth === 0) start = index;
      depth += 1;
    } else if (char === "}") {
      depth -= 1;
      if (depth === 0 && start >= 0) {
        documents.push(JSON.parse(text.slice(start, index + 1)) as unknown);
        start = -1;
      }
    }
  }
  return documents;
}

const emptyLines: AsyncIterable<string> = {
  [Symbol.asyncIterator]: () => ({ next: () => Promise.resolve({ done: true, value: undefined }) }),
};

/** Scripted stdin: yield each line; awaits between runs via hooks. */
async function* scripted(lines: Array<string | (() => Promise<void>)>): AsyncGenerator<string> {
  for (const line of lines) {
    if (typeof line === "string") yield line;
    else await line();
  }
}

describe("CLI parsing", () => {
  const parse = parseCliCommand;
  it("defaults to help", () => {
    expect(parse([])).toEqual({ kind: "help" });
    expect(parse(["--help"])).toEqual({ kind: "help" });
  });
  it("requires a provider for start", () => {
    expect(() => parse(["start"])).toThrow(/--provider/);
    expect(() => parse(["start", "--task", "t"])).toThrow(/--provider/);
  });
  it("rejects unknown commands, flags, and probe providers", () => {
    expect(() => parse(["delegate", "--agent", "omp"])).toThrow(/unknown command/);
    expect(() => parse(["fanout"])).toThrow(/unknown command/);
    expect(() => parse(["live"])).toThrow(/unknown command/);
    expect(() => parse(["start", "--provider", "omp", "--json"])).toThrow(/unknown flag/);
    expect(() => parse(["probe", "grok"])).toThrow(/unknown provider/);
  });
  it("requires a session id for resume and handoff", () => {
    expect(() => parse(["resume"])).toThrow(/session-id/);
    expect(() => parse(["handoff", "--workspace", "/x"])).toThrow(/session-id/);
  });
  it("parses the full start surface", () => {
    const command = parse([
      "start",
      "--provider",
      "hermes",
      "--permission-policy",
      "interactive",
      "--max-output-bytes",
      "4096",
      "--allow-dirty",
      "--workspace",
      "/tmp/wherever",
      "--task",
      "hello",
      "--attach",
      "--attach-close-drain-ms",
      "1000",
    ]) as CliCommand & { kind: "start" };
    expect(command.kind).toBe("start");
    expect(command.start).toEqual({
      provider: "hermes",
      permission_policy: "interactive",
      max_text_bytes: 4096,
      allow_dirty: true,
    });
    expect(command.task).toBe("hello");
    expect(command.attach).toBe(true);
    expect(command.attach_close_drain_ms).toBe(1000);
    expect(command.workspace).toBe("/tmp/wherever");
  });
  it("rejects bad values structurally", () => {
    expect(() => parse(["start", "--provider", "omp", "--permission-policy", "always"])).toThrow(
      /deny or interactive/,
    );
    expect(() => parse(["start", "--provider", "omp", "--max-output-bytes", "0"])).toThrow(
      /positive integer/,
    );
    expect(() => parse(["gc", "--dry-run", "extra"])).toThrow(/unknown argument/);
  });
});

describe("CLI one-shot commands", () => {
  it("runs start → prompt → close → handoff and exits 0", async () => {
    const world = await cliHarness({ turn: { writes: { "fixed.md": "patched\n" } } });
    const result = await world.run([
      "start",
      "--provider",
      "omp",
      "--task",
      "fix it",
      "--workspace",
      world.repository,
    ]);
    expect(result.code).toBe(0);
    const [document] = parseJsonDocuments(result.out) as Array<Record<string, never>>;
    const typed = document as unknown as {
      session: { session_id: string; probe: { found: boolean } };
      turn: { outcome: string; checkpoint: unknown };
      close: { cleanup_errors: unknown[] };
      handoff: { changed_files: string[]; apply_hint: string };
    };
    expect(typed.session.probe.found).toBe(true);
    expect(typed.turn.outcome).toBe("succeeded");
    expect(typed.turn.checkpoint).not.toBeNull();
    expect(typed.close.cleanup_errors).toEqual([]);
    expect(typed.handoff.changed_files).toEqual(["fixed.md"]);
    expect(typed.handoff.apply_hint).toContain("git cherry-pick");

    // The durable session is now visible to a fresh command process.
    const status = await world.run(["status", typed.session.session_id, "--workspace", world.repository]);
    expect(status.code).toBe(0);
    const [statusDoc] = parseJsonDocuments(status.out) as Array<Record<string, unknown>>;
    expect((statusDoc as { state: { status: string } }).state.status).toBe("closed");
    expect((statusDoc as { attached_here: boolean }).attached_here).toBe(false);

    const list = await world.run(["status", "--workspace", world.repository]);
    const [listDoc] = parseJsonDocuments(list.out) as Array<{ sessions: unknown[] }>;
    expect(listDoc.sessions).toHaveLength(1);

    const handoff = await world.run(["handoff", typed.session.session_id, "--workspace", world.repository]);
    expect(handoff.code).toBe(0);

    // Resume: the durable line continues (transport echoes identity back).
    await world.cleanup();
  });

  it("requires --task for a one-shot start", async () => {
    const world = await cliHarness();
    const result = await world.run(["start", "--provider", "omp", "--workspace", world.repository]);
    expect(result.code).toBe(1);
    const [document] = parseJsonDocuments(result.out) as Array<{
      error: { code: string; message: string };
    }>;
    expect(document.error.code).toBe("COMMAND_INVALID");
    expect(document.error.message).toContain("--task");
    await world.cleanup();
  });

  it("answers probe with honest documents and exit 0 even when not found", async () => {
    const world = await cliHarness({ probeFound: false });
    const result = await world.run(["probe", "omp"]);
    expect(result.code).toBe(0);
    const [document] = parseJsonDocuments(result.out) as Array<{
      probes: Array<{ provider: string; transport: string; found: boolean; detail: string | null }>;
    }>;
    expect(document.probes).toEqual([
      {
        provider: "omp",
        transport: "omp-rpc",
        found: false,
        version: null,
        detail: "no RPC v2 evidence",
      },
    ]);
    await world.cleanup();
  });

  it("resumes a closed session one-shot and continues the same id", async () => {
    const world = await cliHarness({ resumeEcho: true, turn: { writes: { "one.md": "1\n" } } });
    const first = await world.run([
      "start",
      "--provider",
      "omp",
      "--task",
      "first",
      "--workspace",
      world.repository,
    ]);
    expect(first.code).toBe(0);
    const [firstDoc] = parseJsonDocuments(first.out) as Array<{
      session: { session_id: string };
    }>;
    const sessionId = (firstDoc as { session: { session_id: string } }).session.session_id;

    world.factory.defaultTurn = { writes: { "two.md": "2\n" } };
    const second = await world.run([
      "resume",
      sessionId,
      "--task",
      "second",
      "--workspace",
      world.repository,
    ]);
    expect(second.code).toBe(0);
    const [secondDoc] = parseJsonDocuments(second.out) as Array<{
      session: { session_id: string };
      turn: { kind: string };
      handoff: { changed_files: string[] };
    }>;
    expect((secondDoc as { session: { session_id: string } }).session.session_id).toBe(sessionId);
    expect((secondDoc as { turn: { kind: string } }).turn.kind).toBe("follow_up");
    expect((secondDoc as { handoff: { changed_files: string[] } }).handoff.changed_files.sort()).toEqual([
      "one.md",
      "two.md",
    ]);
    await world.cleanup();
  });

  it("gc reports and exits non-zero only for manual outcomes", async () => {
    const world = await cliHarness();
    const clean = await world.run(["gc", "--workspace", world.repository]);
    expect(clean.code).toBe(0);
    const [report] = parseJsonDocuments(clean.out) as Array<{ scanned: number; sessions: unknown[] }>;
    expect((report as { scanned: number }).scanned).toBe(0);
    await world.cleanup();
  });
});

describe("CLI attach wire", () => {
  it("speaks NDJSON: session, events, results, close", async () => {
    const world = await cliHarness({ turn: { writes: { "wire.md": "spoken\n" } } });
    const result = await world.run(
      ["start", "--provider", "omp", "--attach", "--workspace", world.repository],
      scripted([
        '{"action":"prompt","text":"hello"}',
        // The pump reads eagerly (before startup finished): await the
        // transport the launch will create, then its turn settlement.
        () => world.factory.transportAt(0).then((t) => t.awaitTurnSettled()),
        '{"action":"follow_up","text":"and more"}',
        () => world.factory.transportAt(0).then((t) => t.awaitTurnSettled()),
      ]),
    );
    expect(result.code).toBe(0);
    const documents = parseJsonDocuments(result.out) as Array<Record<string, unknown>>;
    const types = documents.map((document) => document.type);
    expect(types[0]).toBe("session");
    expect(types).toContain("event");
    expect(types).toContain("result");
    expect(types[types.length - 1]).toBe("close");
    const results = documents.filter((document) => document.type === "result") as Array<{
      action: string;
      outcome: string;
      checkpoint: unknown;
    }>;
    expect(results.some((r) => r.action === "prompt" && r.outcome === "succeeded")).toBe(true);
    expect(results.some((r) => r.action === "follow_up" && r.outcome === "succeeded")).toBe(true);
    const promptResult = results.find((r) => r.action === "prompt");
    expect(promptResult?.checkpoint).not.toBeNull();
    await world.cleanup();
  });

  it("reports structured wire errors without killing the session", async () => {
    const world = await cliHarness();
    const result = await world.run(
      ["start", "--provider", "omp", "--attach", "--workspace", world.repository],
      scripted([
        "not json at all",
        '{"action":"permission","request_id":"x","decision":"allow_always"}',
        '{"action":"bogus"}',
      ]),
    );
    const documents = parseJsonDocuments(result.out) as Array<{ type: string; error?: { code: string } }>;
    const errors = documents.filter((document) => document.type === "error");
    expect(errors.length).toBe(3);
    expect(errors[1]?.error?.code).toBe("COMMAND_INVALID");
    expect(result.code).toBe(1); // errors seen → nonzero, but the session closed cleanly
    const last = documents[documents.length - 1];
    expect(last?.type).toBe("close");
    await world.cleanup();
  });

  it("closes an in-flight turn honestly on EOF", async () => {
    const world = await cliHarness({ turn: { hang: true } });
    const settle = await world.run(
      [
        "start",
        "--provider",
        "omp",
        "--attach",
        "--attach-close-drain-ms",
        "150",
        "--workspace",
        world.repository,
      ],
      // prompt hangs; the EOF drain window expires, then close cancels the turn.
      scripted(['{"action":"prompt","text":"slow one"}']),
    );
    const documents = parseJsonDocuments(settle.out) as Array<{
      type: string;
      outcome?: string;
      action?: string;
    }>;
    // The cancelled turn is reported honestly; an orderly close is not an error.
    const cancelled = documents.find(
      (document) => document.type === "result" && document.outcome === "cancelled",
    );
    expect(cancelled?.action).toBe("prompt");
    expect(documents[documents.length - 1]?.type).toBe("close");
    expect(settle.code).toBe(0);
    await world.cleanup();
  });
});

describe("CLI attach startup lifecycle", () => {
  it("delivers a prompt written before startup completes, exactly once after start", async () => {
    const world = await cliHarness({ turn: { writes: { "early.md": "x\n" } } });
    // A pipe fed (and closed) before the CLI has even built its hub: the
    // eager pump must capture the command during, not after, startup.
    const source = new PassThrough();
    source.write('{"action":"prompt","text":"early bird"}\n');
    source.end();

    const supervisor = new AgentHubSupervisor();
    const realLaunch = supervisor.launch.bind(supervisor);
    let pump: AttachInputPump | undefined;
    supervisor.launch = async (hub, run) => {
      // Gate the provider launch behind PROOF that the command is already
      // queued in the pump. With a lazy reader this never resolves.
      const buffered = await pump?.waitForBufferedCommand();
      expect(buffered).toBe(true);
      return realLaunch(hub, run);
    };

    let out = "";
    const code = await runCli(
      ["start", "--provider", "omp", "--attach", "--workspace", world.repository],
      {
        stdin: source,
        stdout: { write: (chunk: string) => (out += chunk) },
        stderr: { write: () => undefined },
      },
      {
        hubOptions: world.hubOptions,
        supervisor,
        onAttachPump: (attached) => {
          pump = attached;
        },
      },
    );
    expect(code).toBe(0);
    const transport = world.factory.created[0]!;
    // Delivered exactly once, AFTER the session existed…
    expect(transport.commands.filter((command) => command.kind === "prompt")).toHaveLength(1);
    const documents = parseJsonDocuments(out) as Array<{
      type: string;
      outcome?: string;
      action?: string;
    }>;
    const results = documents.filter((document) => document.type === "result");
    expect(results).toHaveLength(1);
    // …and NOT cancelled by the EOF that arrived long before startup finished.
    expect(results[0]?.outcome).toBe("succeeded");
    expect(documents[documents.length - 1]?.type).toBe("close");
    await world.cleanup();
  });
});

describe("CLI usage surface", () => {
  it("prints help on request and on parse errors with exit 2", async () => {
    const world = await cliHarness();
    const help = await world.run(["--help"]);
    expect(help.code).toBe(0);
    expect(help.out).toContain("agent-hub start");
    expect(help.out).toContain("NDJSON");
    expect(help.out).not.toContain("delegate");
    expect(help.out).not.toContain("fanout");

    const bad = await world.run(["nonsense"]);
    expect(bad.code).toBe(2);
    expect(bad.err).toContain("unknown command");
    await world.cleanup();
  });
});
