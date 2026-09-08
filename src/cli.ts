#!/usr/bin/env node
import { realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { AgentHubError, asDelegateError } from "./errors.js";
import {
  AgentHub,
  HUB_PROVIDERS,
  type TurnDocument,
} from "./hub/agent-hub.js";
import type { AgentHubOptions } from "./hub/agent-hub.js";
import {
  AgentHubSupervisor,
  type HubOpen,
} from "./hub/supervisor.js";
import {
  AttachInputPump,
  ATTACH_CLOSE_DRAIN_DEFAULT_MS,
} from "./hub/attach-io.js";
import { productionBridgedFactories } from "./hub/transport-adapter.js";
import { isPermissionDecision } from "./kernel/contracts.js";
import type { PermissionPolicy } from "./kernel/contracts.js";

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
 * on stdout. A session belongs to the process that launched it; after a
 * hub-process loss, reconcile with `gc` and continue with `resume`.
 */

const HELP = `agent-hub — provider-neutral agent sessions with durable, checkpointed workspaces

Providers (transport auto-selected, honest probe gate):
  omp    omp-rpc          RPC v2 dialect ONLY — no v1 fallback, ever
  pi     pi-rpc           JSON-RPC over stdio
  agy    agy-stream-json  stream-json
  hermes hermes-acp       Agent Client Protocol (ACP)

Usage:
  agent-hub start --provider <p> [--transport <t>] [--task TEXT] [--workspace DIR]
                  [--permission-policy deny|interactive] [--max-output-bytes N]
                  [--allow-dirty] [--attach]
      One-shot (requires --task): start → prompt → turn result → close,
      answering with {session, turn, close, handoff}. With --attach: keep the
      process attached and drive the session over the NDJSON wire (below);
      --task, when present, is issued as the first prompt.

  agent-hub resume <session-id> [--task TEXT] [--transport <t>] [--workspace DIR]
                  [--permission-policy ...] [--attach]
      Continue a terminal session from its durable record + checkpoint chain.
      --task is delivered as a follow_up turn.

  agent-hub status [session-id] [--workspace DIR]
      One session's durable document (record + state + lease), or every
      durable session in this repository when no id is given.

  agent-hub handoff <session-id> [--workspace DIR]
      The checkpoint chain as the deliverable: ref, commits, changed files,
      and the human review/adopt command. Never merges anything.

  agent-hub gc [--dry-run] [--workspace DIR]
      Safe reconciliation: re-prove every lease, reap provably-orphaned
      provider groups, pin surviving worktrees, rewrite to orphaned,
      release leases last, retry worktree removals, prune. Nothing
      unprovable is touched. Exit 1 when a session needs manual review.

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
  Closing stdin EOFs the wire: already-received commands (including one
  written before startup finished) are delivered and given the drain window
  (--attach-close-drain-ms, default 5000) to settle before the graceful
  close — EOF never cancels in-flight work that was received. An explicit
  {"action":"close"} closes immediately (cancelling a running turn is the
  kernel's honest behavior for that instruction). Stdin is read eagerly,
  from before provider startup through the whole session, by a single
  shared reader bounded to 128 commands / 1 MiB of queued input.

Workspace: --workspace names the Git checkout to bind (default: cwd).
Sessions run in hub-owned isolated worktrees; the caller checkout is never
used as the provider workspace. A dirty caller checkout is refused unless
--allow-dirty (the base is a commit either way).
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
      attach_close_drain_ms: number;
      start: StartArgs;
    }
  | {
      kind: "resume";
      workspace: string;
      session_id: string;
      task: string | null;
      attach: boolean;
      attach_close_drain_ms: number;
      start: StartArgs;
    }
  | { kind: "status"; workspace: string; session_id: string | null }
  | { kind: "handoff"; workspace: string; session_id: string }
  | { kind: "gc"; workspace: string; dry_run: boolean }
  | { kind: "probe"; providers: string[] };

interface StartArgs {
  provider: string;
  transport?: string;
  permission_policy: PermissionPolicy;
  max_text_bytes?: number;
  allow_dirty: boolean;
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

function parseStartArgs(
  argv: string[],
  parseFrom: number,
  providerFlagRequired: boolean,
): { start: StartArgs; next: number } {
  const start: StartArgs = {
    provider: "",
    permission_policy: "deny",
    allow_dirty: false,
  };
  let index = parseFrom;
  for (;;) {
    const arg = argv[index];
    if (arg === undefined) break;
    if (arg === "--provider" || arg === "-p") {
      start.provider = takeValue(arg, argv, index);
      index += 2;
    } else if (arg === "--transport") {
      start.transport = takeValue(arg, argv, index);
      index += 2;
    } else if (arg === "--permission-policy") {
      start.permission_policy = parsePermissionPolicy(takeValue(arg, argv, index));
      index += 2;
    } else if (arg === "--max-output-bytes") {
      start.max_text_bytes = parseMaxBytes(arg, takeValue(arg, argv, index));
      index += 2;
    } else if (arg === "--allow-dirty") {
      start.allow_dirty = true;
      index += 1;
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
  let attachDrainMs = ATTACH_CLOSE_DRAIN_DEFAULT_MS;

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
      } else if (arg === "--attach-close-drain-ms") {
        attachDrainMs = parseMaxBytes(arg, takeValue(arg, rest, i));
        i += 1;
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
      return {
        kind: "start",
        workspace,
        task,
        attach,
        attach_close_drain_ms: attachDrainMs,
        start,
      };
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
        attach_close_drain_ms: attachDrainMs,
        // For resume the provider comes from the durable record; a
        // --provider flag is not meaningful and is not parsed.
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
      for (let i = 1; i < rest.length; i += 1) {
        const arg = rest[i];
        if (arg === "--workspace" || arg === "-w") {
          workspace = takeValue(arg, rest, i);
          i += 1;
        } else {
          throw new UsageError(`unknown argument "${arg}"`);
        }
      }
      return { kind: "handoff", workspace, session_id: sessionId };
    }
    case "gc": {
      let dryRun = false;
      for (let i = 0; i < rest.length; i += 1) {
        const arg = rest[i];
        if (arg === "--dry-run") dryRun = true;
        else if (arg === "--workspace" || arg === "-w") {
          workspace = takeValue(arg, rest, i);
          i += 1;
        } else {
          throw new UsageError(`unknown argument "${arg}"`);
        }
      }
      return { kind: "gc", workspace, dry_run: dryRun };
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
 *     settles `cancelled` — the kernel's honest close semantics);
 *   - EOF is NOT a cancel: it stops intake, then already-dispatched
 *     commands get `drainMs` to settle before the graceful close, so a
 *     first prompt written before startup is never killed by the drain.
 */
async function runAttachWire(
  pump: AttachInputPump,
  hub: AgentHub,
  sessionId: string,
  io: CliIo,
  autoTask: string | null,
  drainMs: number,
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
      wire.out({ type: "error", error: asDelegateError(error) });
    }
  })();

  const inFlight = new Set<Promise<void>>();
  const dispatch = (promise: Promise<TurnDocument>, action: string): void => {
    const tracked = promise
      .then((document) => wire.out({ type: "result", action, ...document }))
      .catch((error: unknown) => {
        sawError = true;
        wire.out({ type: "error", action, error: asDelegateError(error) });
      });
    inFlight.add(tracked);
    void tracked.finally(() => inFlight.delete(tracked));
  };

  const closeOnce = async (mode: "graceful" | "terminate"): Promise<boolean> => {
    closeIssued = true;
    try {
      const document = await hub.close(sessionId, mode);
      wire.out({ type: "close", ...document });
      return document.cleanup_errors.length === 0;
    } catch (error) {
      sawError = true;
      wire.out({ type: "error", action: "close", error: asDelegateError(error) });
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
            `unknown action "${String(action)}" (prompt, follow_up, steer, cancel, status, permission, close)`,
          );
      }
    } catch (error) {
      sawError = true;
      wire.out({ type: "error", action: String(action), error: asDelegateError(error) });
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

  if (!closeIssued) {
    // EOF (or input failure): commands already received must not be
    // cancelled by the close. Give dispatched work the drain window, then
    // close gracefully whatever the drain proved.
    if (inFlight.size > 0) {
      await Promise.race([
        Promise.allSettled([...inFlight]),
        new Promise<void>((resolve) => {
          const timer = setTimeout(resolve, drainMs);
          timer.unref?.();
        }),
      ]);
    }
    clean = (await closeOnce("graceful")) && clean;
  }
  // Whatever the exit route, every dispatched command's document lands.
  await Promise.allSettled([...inFlight]);
  await eventsTail;
  await pump.settled().catch(() => undefined);
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
    attach_close_drain_ms: number;
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
              transport: args.start.transport,
              permission_policy: args.start.permission_policy,
              max_text_bytes: args.start.max_text_bytes,
              allow_dirty: args.start.allow_dirty,
            }),
          )
        : await supervisor.launch(hub, () =>
            hub.resume(args.session_id as string, {
              transport: args.start.transport,
              permission_policy: args.start.permission_policy,
              max_text_bytes: args.start.max_text_bytes,
              allow_dirty: args.start.allow_dirty,
            }),
          );
    const sessionId = started.session_id;
    try {
      if (args.attach) {
        io.stderr.write(
          `agent-hub: session ${sessionId} attached; speak NDJSON on stdin (see agent-hub --help), Ctrl-D to end.\n`,
        );
        json(io, { type: "session", ...started });
        const wire = await runAttachWire(
          pump as AttachInputPump,
          hub,
          sessionId,
          io,
          args.task,
          args.attach_close_drain_ms,
        );
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
      let handoff: unknown = null;
      let handoffError: { code: string; message: string } | null = null;
      try {
        handoff = await hub.handoff(sessionId);
      } catch (error) {
        handoffError = asDelegateError(error);
      }
      json(io, {
        session: started,
        turn,
        close: closed,
        handoff,
        ...(handoffError !== null ? { handoff_error: handoffError } : {}),
      });
      const cleanClose = closed.cleanup_errors.length === 0;
      const turnOk =
        (turn.outcome === "succeeded" || turn.outcome === "cancelled") &&
        turn.checkpoint_error === undefined;
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
 * reservation — so input written during the handshake is queued, not raced.
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
          json(io, await hub.handoff(command.session_id));
          return 0;
        });
      case "gc":
        return await withHub(command, deps, async (hub) => {
          const report = await hub.gc({ dry_run: command.dry_run });
          json(io, report);
          return report.sessions.some((session) => session.outcome === "manual") ? 1 : 0;
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
    json(io, { error: asDelegateError(error) });
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
