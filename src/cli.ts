#!/usr/bin/env node
import { realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { AgentHubError, asHubError } from "./errors.js";
import {
  AgentHub,
  HUB_PROVIDERS,
  type HandoffDecisionInput,
  type TurnDocument,
} from "./hub/agent-hub.js";
import type { AgentHubOptions } from "./hub/agent-hub.js";
import {
  AgentHubSupervisor,
  type HubOpen,
} from "./hub/supervisor.js";
import { AttachInputPump } from "./hub/attach-io.js";
import { productionBridgedFactories } from "./hub/transport-adapter.js";
import { isPermissionDecision } from "./kernel/contracts.js";
import type { PermissionPolicy } from "./kernel/contracts.js";
import type { HandoffDecision } from "./workspace/records.js";

/**
 * agent-hub — the command-line surface of the rewrite.
 *
 * Every command answers with ONE JSON document on stdout; human guidance
 * goes to stderr. Exit codes: 0 success, 1 structured operation failure
 * (the JSON document carries `error`), 2 usage/parse error.
 *
 * Real-time interaction (prompt, follow_up, steer, cancel, status,
 * permission) lives in `--attach` mode: long-lived NDJSON — one command
 * per line on stdin, `session`/`event`/`result`/`error`/`close` documents
 * on stdout. Sessions run in hub-owned isolated worktrees under
 * AGENT_HUB_HOME; closing never deletes anything — cleanup waits for an
 * explicit handoff decision and the retention window.
 */

const HELP = `agent-hub — provider-neutral agent sessions in durable, isolated workspaces

Providers (transport auto-selected per provider, honest probe gate; the
transport is reported as a fact and can never be pinned by callers):
  omp    omp-rpc          RPC v2 dialect ONLY — no v1 fallback, ever
  pi     pi-rpc           JSON-RPC over stdio
  agy    agy-stream-json  stream-json
  hermes hermes-acp       Agent Client Protocol (ACP)

Usage:
  agent-hub start --provider <p> [--task TEXT] [--workspace DIR]
                  [--permission-policy deny|interactive] [--max-output-bytes N]
                  [--attach]
      One-shot (requires --task): start → prompt → turn result → close,
      answering with {session, turn, close, next}. With --attach: keep the
      process attached and drive the session over the NDJSON wire (below);
      --task, when present, is issued as the first prompt.

  agent-hub resume <session-id> [--task TEXT] [--workspace DIR]
                  [--permission-policy ...] [--attach]
      Continue a closed session from its durable custody record (only while
      no handoff decision has been made). --task is delivered as a
      follow_up turn.

  agent-hub status [session-id] [--workspace DIR]
      One session's custody document (workspace record + runtime mirror +
      lease + worktree state), or every durable session when no id is given.

  agent-hub handoff <session-id> --decision accepted|discarded
                  --result-seq N --commit SHA [--consumer NAME] [--workspace DIR]
      The consumer's takeover decision. It must name the workspace's exact
      published head (result sequence + commit) from \`status\` or a turn
      document, or it is refused. This starts the retention clock (default
      24 h); nothing is deleted here. Until a decision exists, close/GC
      never removes the worktree, lease, results, or ref.

  agent-hub gc [--workspace DIR]
      Safe manual reconciliation: re-prove every lease, settle provably-dead
      orphans (deletes nothing), then collect ONLY workspaces whose exact
      handoff decision is on record and whose retention window has expired.
      Never deletes unacknowledged, orphaned, uncertain, or referenced
      work; every retained workspace names the precondition that failed.
      Exit 1 when a session needs manual review.

  agent-hub probe [provider ...]
      Honest probe documents (found/version/detail). Launches nothing;
      omp answers found=false unless its RPC v2 dialect evidence holds.

Attach wire (start/resume --attach):
  -> {"action":"prompt","text":"..."}            first task, exactly once
  -> {"action":"follow_up","text":"..."}         next turn (queued while running)
  -> {"action":"steer","text":"..."}             mid-turn guidance
  -> {"action":"cancel","reason":"..."}          abort the in-flight turn
  -> {"action":"status"}                         provider's authoritative progress
  -> {"action":"permission","request_id":"...","decision":"allow_once|deny","note":"..."}
  -> {"action":"close","mode":"graceful|terminate"}
  <- {"type":"session",...} | {"type":"event",event} | {"type":"result",...}
  <- {"type":"error",error} | {"type":"close",...}
  An explicit {"action":"close"} closes immediately (cancelling a running
  turn is the kernel's honest behavior for that instruction) and does NOT
  wait for stdin EOF — safe with an interactive TTY or FIFO writer left
  open. Closing stdin EOF is never a cancel: intake stops, every already
  accepted command settles normally, then the session closes. Stdin is read
  eagerly, from before provider startup through the whole session, by a
  single shared reader: 128 commands / 1 MiB queued (backpressure beyond
  that), 256 KiB per line, 1 MiB per chunk, with fail-closed overflow.

Workspace: --workspace names the Git checkout to bind (default: cwd).
Sessions run in hub-owned isolated worktrees under AGENT_HUB_HOME (default
~/.local/share/agent-hub; set AGENT_HUB_HOME); the caller checkout is never
used as the provider workspace.
`;

export interface CliIo {
  stdin: AsyncIterable<Uint8Array | string>;
  stdout: { write(chunk: string): unknown };
  stderr: { write(chunk: string): unknown };
}

export interface CliDependencies {
  /** Test seam: replaces the hub construction entirely. */
  openHub?: HubOpen;
  /** Options merged into every hub construction (injected factories etc.). */
  hubOptions?: AgentHubOptions;
  /** Test seam: replace the process supervisor. */
  supervisor?: AgentHubSupervisor;
  /**
   * Test seam: receives the eager stdin pump the moment an attached run
   * attaches it (before any hub construction), proving startup ordering.
   */
  onAttachPump?: (pump: AttachInputPump) => void;
}

export type CliCommand =
  | { kind: "help" }
  | {
      kind: "start";
      workspace: string;
      task: string | null;
      attach: boolean;
      start: StartArgs;
    }
  | {
      kind: "resume";
      workspace: string;
      session_id: string;
      task: string | null;
      attach: boolean;
      start: StartArgs;
    }
  | { kind: "status"; workspace: string; session_id: string | null }
  | { kind: "handoff"; workspace: string; session_id: string; decision: HandoffDecisionInput }
  | { kind: "gc"; workspace: string }
  | { kind: "probe"; providers: string[] };

interface StartArgs {
  provider: string;
  permission_policy: PermissionPolicy;
  max_text_bytes?: number;
}

export class UsageError extends Error {}

// ---------------------------------------------------------------------------
// Argument parsing (pure; exported for tests)
// ---------------------------------------------------------------------------

function takeValue(flag: string, argv: string[], index: number): string {
  const value = argv[index + 1];
  if (value === undefined || value.startsWith("-")) {
    throw new UsageError(`${flag} requires a value`);
  }
  return value;
}

function parseMaxBytes(flag: string, raw: string): number {
  const value = Number(raw);
  if (!Number.isInteger(value) || value <= 0) {
    throw new UsageError(`${flag} must be a positive integer`);
  }
  return value;
}

function parsePermissionPolicy(raw: string): PermissionPolicy {
  if (raw === "deny" || raw === "interactive") return raw;
  throw new UsageError(`--permission-policy must be deny or interactive, got "${raw}"`);
}

function parseHandoffDecision(raw: string): HandoffDecision {
  if (raw === "accepted" || raw === "discarded") return raw;
  throw new UsageError(`--decision must be accepted or discarded, got "${raw}"`);
}

function parseStartArgs(
  argv: string[],
  parseFrom: number,
  providerFlagRequired: boolean,
): { start: StartArgs; next: number } {
  const start: StartArgs = {
    provider: "",
    permission_policy: "deny",
  };
  let index = parseFrom;
  for (;;) {
    const arg = argv[index];
    if (arg === undefined) break;
    if (arg === "--provider" || arg === "-p") {
      start.provider = takeValue(arg, argv, index);
      index += 2;
    } else if (arg === "--permission-policy") {
      start.permission_policy = parsePermissionPolicy(takeValue(arg, argv, index));
      index += 2;
    } else if (arg === "--max-output-bytes") {
      start.max_text_bytes = parseMaxBytes(arg, takeValue(arg, argv, index));
      index += 2;
    } else if (arg === "--task" || arg === "-t") {
      break;
    } else if (arg === "--attach") {
      break;
    } else if (arg === "--workspace" || arg === "-w") {
      break;
    } else if (arg.startsWith("-")) {
      throw new UsageError(`unknown flag "${arg}"`);
    } else {
      break;
    }
  }
  if (providerFlagRequired && start.provider === "") {
    throw new UsageError(`start requires --provider (one of: ${HUB_PROVIDERS.join(", ")})`);
  }
  return { start, next: index };
}

export function parseCliCommand(argv: string[]): CliCommand {
  const [command, ...rest] = argv;
  if (command === undefined || command === "--help" || command === "-h" || command === "help") {
    return { kind: "help" };
  }
  if (command.startsWith("-")) {
    throw new UsageError(`unknown command/flag "${command}"`);
  }

  let workspace = process.cwd();
  let task: string | null = null;
  let attach = false;

  const consumeCommonTail = (index: number): void => {
    for (let i = index; i < rest.length; i += 1) {
      const arg = rest[i];
      if (arg === "--workspace" || arg === "-w") {
        workspace = takeValue(arg, rest, i);
        i += 1;
      } else if (arg === "--task" || arg === "-t") {
        task = takeValue(arg, rest, i);
        i += 1;
      } else if (arg === "--attach") {
        attach = true;
      } else if (arg.startsWith("-")) {
        throw new UsageError(`unknown flag "${arg}"`);
      } else {
        throw new UsageError(`unexpected argument "${arg}"`);
      }
    }
  };

  switch (command) {
    case "start": {
      const { start, next } = parseStartArgs(rest, 0, true);
      consumeCommonTail(next);
      return { kind: "start", workspace, task, attach, start };
    }
    case "resume": {
      const sessionId = rest[0];
      if (sessionId === undefined || sessionId.startsWith("-")) {
        throw new UsageError("resume requires <session-id>");
      }
      const { start, next } = parseStartArgs(rest, 1, false);
      consumeCommonTail(next);
      return {
        kind: "resume",
        workspace,
        session_id: sessionId,
        task,
        attach,
        // For resume the provider and transport come from the durable
        // record; neither flag is meaningful and neither is parsed.
        start: { ...start, provider: "" },
      };
    }
    case "status": {
      let sessionId: string | null = null;
      for (let i = 0; i < rest.length; i += 1) {
        const arg = rest[i];
        if (arg === "--workspace" || arg === "-w") {
          workspace = takeValue(arg, rest, i);
          i += 1;
        } else if (arg.startsWith("-")) {
          throw new UsageError(`unknown flag "${arg}"`);
        } else if (sessionId === null) {
          sessionId = arg;
        } else {
          throw new UsageError(`unexpected argument "${arg}"`);
        }
      }
      return { kind: "status", workspace, session_id: sessionId };
    }
    case "handoff": {
      const sessionId = rest[0];
      if (sessionId === undefined || sessionId.startsWith("-")) {
        throw new UsageError("handoff requires <session-id>");
      }
      let decision: HandoffDecision | null = null;
      let resultSeq: number | null = null;
      let commit: string | null = null;
      let consumer: string | null = null;
      for (let i = 1; i < rest.length; i += 1) {
        const arg = rest[i];
        if (arg === "--workspace" || arg === "-w") {
          workspace = takeValue(arg, rest, i);
          i += 1;
        } else if (arg === "--decision") {
          decision = parseHandoffDecision(takeValue(arg, rest, i));
          i += 1;
        } else if (arg === "--result-seq") {
          resultSeq = parseMaxBytes(arg, takeValue(arg, rest, i));
          i += 1;
        } else if (arg === "--commit") {
          commit = takeValue(arg, rest, i);
          i += 1;
        } else if (arg === "--consumer") {
          consumer = takeValue(arg, rest, i);
          i += 1;
        } else {
          throw new UsageError(`unknown argument "${arg}"`);
        }
      }
      if (decision === null) {
        throw new UsageError("handoff requires --decision accepted|discarded");
      }
      if (resultSeq === null || commit === null) {
        throw new UsageError(
          "handoff must name the exact result: --result-seq N --commit SHA (see `agent-hub status <session-id>`)",
        );
      }
      return {
        kind: "handoff",
        workspace,
        session_id: sessionId,
        decision: { decision, result_seq: resultSeq, commit, consumer },
      };
    }
    case "gc": {
      for (let i = 0; i < rest.length; i += 1) {
        const arg = rest[i];
        if (arg === "--workspace" || arg === "-w") {
          workspace = takeValue(arg, rest, i);
          i += 1;
        } else {
          throw new UsageError(`unknown argument "${arg}"`);
        }
      }
      return { kind: "gc", workspace };
    }
    case "probe": {
      const providers = rest.filter((arg) => arg !== "--");
      for (const provider of providers) {
        if (!(HUB_PROVIDERS as readonly string[]).includes(provider)) {
          throw new UsageError(
            `unknown provider "${provider}" (shipped: ${HUB_PROVIDERS.join(", ")})`,
          );
        }
      }
      return { kind: "probe", providers };
    }
    default:
      throw new UsageError(`unknown command "${command}" (see: agent-hub --help)`);
  }
}

// ---------------------------------------------------------------------------
// Attach wire
// ---------------------------------------------------------------------------

interface WireIo {
  out: (document: unknown) => void;
}

/**
 * The attached NDJSON wire, driven by the EAGER input pump that was
 * attached before the hub even started. Loop semantics:
 *
 *   - queued lines dispatch in arrival order, exactly once; commands
 *     received during the handshake run as the session's first commands;
 *   - an explicit `close` action closes immediately (a running turn
 *     settles `cancelled` — the kernel's honest close semantics) and the
 *     pump is DISPOSED, never awaited to EOF: an open TTY/FIFO writer
 *     must not hold the process;
 *   - EOF is NOT a cancel: intake stops, every already-dispatched command
 *     settles normally (no bounded drain timer), and only then does the
 *     session close gracefully.
 */
async function runAttachWire(
  pump: AttachInputPump,
  hub: AgentHub,
  sessionId: string,
  io: CliIo,
  autoTask: string | null,
): Promise<{ sawError: boolean }> {
  const wire: WireIo = {
    out: (document) => io.stdout.write(`${JSON.stringify(document)}\n`),
  };
  let sawError = false;
  let closeIssued = false;
  let clean = true;

  const eventsTail = (async () => {
    try {
      for await (const event of hub.streamEvents(sessionId)) {
        wire.out({ type: "event", event });
      }
    } catch (error) {
      sawError = true;
      wire.out({ type: "error", error: asHubError(error) });
    }
  })();

  const inFlight = new Set<Promise<void>>();
  const dispatch = (promise: Promise<TurnDocument>, action: string): void => {
    const tracked = promise
      .then((document) => wire.out({ type: "result", action, ...document }))
      .catch((error: unknown) => {
        sawError = true;
        wire.out({ type: "error", action, error: asHubError(error) });
      });
    inFlight.add(tracked);
    void tracked.finally(() => inFlight.delete(tracked));
  };

  const closeOnce = async (mode: "graceful" | "terminate"): Promise<boolean> => {
    closeIssued = true;
    try {
      const document = await hub.close(sessionId, mode);
      wire.out({ type: "close", ...document });
      return document.record.status === "closed";
    } catch (error) {
      sawError = true;
      wire.out({ type: "error", action: "close", error: asHubError(error) });
      return false;
    }
  };

  const handleLine = async (line: string): Promise<boolean> => {
    let command: Record<string, unknown>;
    try {
      const parsed: unknown = JSON.parse(line);
      if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
        throw new Error("not an object");
      }
      command = parsed as Record<string, unknown>;
    } catch (error) {
      sawError = true;
      wire.out({
        type: "error",
        error: { code: "COMMAND_INVALID", message: `stdin line is not a JSON object: ${String(error)}` },
      });
      return true;
    }
    const action = command.action;
    const text = command.text;
    try {
      switch (action) {
        case "prompt":
          if (typeof text !== "string") throw new AgentHubError("COMMAND_INVALID", "prompt requires text");
          dispatch(hub.prompt(sessionId, text), "prompt");
          return true;
        case "follow_up":
          if (typeof text !== "string") throw new AgentHubError("COMMAND_INVALID", "follow_up requires text");
          dispatch(hub.followUp(sessionId, text), "follow_up");
          return true;
        case "steer":
          if (typeof text !== "string") throw new AgentHubError("COMMAND_INVALID", "steer requires text");
          dispatch(hub.steer(sessionId, text), "steer");
          return true;
        case "cancel":
          dispatch(hub.cancel(sessionId, typeof command.reason === "string" ? command.reason : null), "cancel");
          return true;
        case "status":
          dispatch(hub.requestStatus(sessionId), "status");
          return true;
        case "permission": {
          if (typeof command.request_id !== "string") {
            throw new AgentHubError("COMMAND_INVALID", "permission requires request_id");
          }
          if (!isPermissionDecision(command.decision)) {
            throw new AgentHubError(
              "COMMAND_INVALID",
              `decision "${String(command.decision)}" is outside the contract vocabulary (allow_once, deny)`,
            );
          }
          dispatch(
            hub.respondPermission(
              sessionId,
              command.request_id,
              command.decision,
              typeof command.note === "string" ? command.note : null,
            ),
            "permission",
          );
          return true;
        }
        case "close":
          clean = (await closeOnce(command.mode === "terminate" ? "terminate" : "graceful")) && clean;
          return true;
        default:
          throw new AgentHubError(
            "COMMAND_INVALID",
            `unknown action "${String(action)}" (unknown action must be one of prompt, follow_up, steer, cancel, status, permission, close)`,
          );
      }
    } catch (error) {
      sawError = true;
      wire.out({ type: "error", action: String(action), error: asHubError(error) });
      return true;
    }
  };

  if (autoTask !== null) {
    dispatch(hub.prompt(sessionId, autoTask), "prompt");
  }

  for (;;) {
    const event = await pump.next();
    if (event === null || event.kind === "eof") break;
    clean = (await handleLine(event.value)) && clean;
    if (closeIssued) break;
  }
  if (pump.readError !== null) {
    sawError = true;
    wire.out({ type: "error", error: pump.readError });
    clean = false;
  }

  if (closeIssued) {
    // The host said close: release stdin right now — never await EOF from
    // a writer (TTY/FIFO) that may stay open forever.
    pump.dispose();
  } else {
    // EOF (or input failure) is not a cancel: every command already accepted
    // settles normally — no fixed drain window — and only then does the
    // session close.
    await Promise.allSettled([...inFlight]);
    clean = (await closeOnce("graceful")) && clean;
  }
  // Whatever the exit route, every dispatched command's document lands.
  await Promise.allSettled([...inFlight]);
  await eventsTail;
  pump.dispose();
  return { sawError: sawError || !clean };
}

// ---------------------------------------------------------------------------
// Command runners
// ---------------------------------------------------------------------------

function json(io: CliIo, document: unknown): void {
  io.stdout.write(`${JSON.stringify(document, null, 2)}\n`);
}

async function withHub<T>(
  command: { workspace: string },
  deps: CliDependencies,
  run: (hub: AgentHub, supervisor: AgentHubSupervisor) => Promise<T>,
): Promise<T> {
  const supervisor = deps.supervisor ?? new AgentHubSupervisor();
  const open: HubOpen =
    deps.openHub ??
    ((workspace, options) => AgentHub.open(workspace, { ...deps.hubOptions, ...options }));
  const hub = await supervisor.hubFor(command.workspace, deps.hubOptions ?? {}, open);
  return run(hub, supervisor);
}

async function runStartLike(
  kind: "start" | "resume",
  workspace: string,
  args: {
    session_id?: string;
    start: StartArgs;
    task: string | null;
    attach: boolean;
  },
  io: CliIo,
  deps: CliDependencies,
  pump: AttachInputPump | null,
): Promise<number> {
  return withHub({ workspace }, deps, async (hub, supervisor): Promise<number> => {
    const started =
      kind === "start"
        ? await supervisor.launch(hub, () =>
            hub.start({
              provider: args.start.provider,
              permission_policy: args.start.permission_policy,
              ...(args.start.max_text_bytes === undefined ? {} : { max_text_bytes: args.start.max_text_bytes }),
            }),
          )
        : await supervisor.launch(hub, () =>
            hub.resume(args.session_id as string, {
              permission_policy: args.start.permission_policy,
              ...(args.start.max_text_bytes === undefined ? {} : { max_text_bytes: args.start.max_text_bytes }),
            }),
          );
    const sessionId = started.session_id;
    try {
      if (args.attach) {
        io.stderr.write(
          `agent-hub: session ${sessionId} attached; speak NDJSON on stdin (see agent-hub --help), Ctrl-D to end.\n`,
        );
        json(io, { type: "session", ...started });
        const wire = await runAttachWire(pump as AttachInputPump, hub, sessionId, io, args.task);
        return wire.sawError ? 1 : 0;
      }
      if (args.task === null) {
        throw new AgentHubError(
          "COMMAND_INVALID",
          kind === "start"
            ? "one-shot start requires --task (or use --attach for the NDJSON wire)"
            : "one-shot resume requires --task (or use --attach for the NDJSON wire)",
        );
      }
      const turn =
        kind === "start"
          ? await hub.prompt(sessionId, args.task)
          : await hub.followUp(sessionId, args.task);
      const closed = await hub.close(sessionId);
      json(io, {
        session: started,
        turn,
        close: closed,
        next:
          turn.result === null
            ? {
                handoff_command: null,
                note: "this turn published no result identity; decide on the published head shown by `agent-hub status`",
              }
            : {
                handoff_command: `agent-hub handoff ${sessionId} --decision accepted --result-seq ${turn.result.seq} --commit ${turn.result.commit}`,
                discard_command: `agent-hub handoff ${sessionId} --decision discarded --result-seq ${turn.result.seq} --commit ${turn.result.commit}`,
              },
      });
      const cleanClose = closed.record.status === "closed";
      const turnOk =
        (turn.outcome === "succeeded" || turn.outcome === "cancelled") &&
        turn.publish_error === undefined &&
        turn.result !== null;
      return turnOk && cleanClose ? 0 : 1;
    } finally {
      supervisor.retireIdle(hub);
    }
  });
}

// ---------------------------------------------------------------------------
// Entry
// ---------------------------------------------------------------------------

/**
 * Attaches the single eager stdin reader the moment an attached command is
 * recognized — before hub construction, provider launch, or workspace
 * provisioning — so input written during the handshake is queued, not raced.
 */
function attachPump(
  attach: boolean,
  io: CliIo,
  deps: CliDependencies,
): AttachInputPump | null {
  if (!attach) return null;
  const pump = new AttachInputPump(io.stdin);
  deps.onAttachPump?.(pump);
  return pump;
}

export async function runCli(
  argv: string[],
  io: CliIo,
  deps: CliDependencies = {},
): Promise<number> {
  let command: CliCommand;
  try {
    command = parseCliCommand(argv);
  } catch (error) {
    io.stderr.write(`agent-hub: ${(error as Error).message}\n\n${HELP}`);
    return 2;
  }

  try {
    switch (command.kind) {
      case "help":
        io.stdout.write(HELP);
        return 0;
      case "start":
        return await runStartLike(
          "start",
          command.workspace,
          command,
          io,
          deps,
          attachPump(command.attach, io, deps),
        );
      case "resume":
        return await runStartLike(
          "resume",
          command.workspace,
          command,
          io,
          deps,
          attachPump(command.attach, io, deps),
        );
      case "status":
        return await withHub(command, deps, async (hub) => {
          if (command.session_id === null) {
            json(io, { sessions: await hub.list() });
          } else {
            json(io, await hub.status(command.session_id));
          }
          return 0;
        });
      case "handoff":
        return await withHub(command, deps, async (hub) => {
          json(io, await hub.handoff(command.session_id, command.decision));
          return 0;
        });
      case "gc":
        return await withHub(command, deps, async (hub) => {
          const report = await hub.cleanup();
          json(io, { recovery: report.recovery, cleanup: report.cleanup });
          const manual =
            report.recovery.inconsistencies.length > 0
            || report.recovery.unclaimed.unknown_segments.length > 0
            || report.cleanup.unclaimed.cleanup_errors.length > 0;
          return manual ? 1 : 0;
        });
      case "probe": {
        const bridges = deps.hubOptions?.transportFactories ?? productionBridgedFactories();
        const documents: unknown[] = [];
        for (const factory of bridges) {
          if (command.providers.length > 0 && !command.providers.includes(factory.provider)) {
            continue;
          }
          const probe = await factory.probe();
          documents.push({
            provider: factory.provider,
            transport: factory.transport,
            ...probe,
          });
        }
        json(io, { probes: documents });
        return 0;
      }
    }
  } catch (error) {
    json(io, { error: asHubError(error) });
    return 1;
  }
}

function isEntrypoint(): boolean {
  const invoked = process.argv[1];
  if (invoked === undefined) return false;
  try {
    return realpathSync(fileURLToPath(import.meta.url)) === realpathSync(invoked);
  } catch {
    return false;
  }
}

if (isEntrypoint()) {
  const code = await runCli(
    process.argv.slice(2),
    {
      stdin: process.stdin,
      stdout: process.stdout,
      stderr: process.stderr,
    },
    {},
  );
  process.exitCode = code;
}
