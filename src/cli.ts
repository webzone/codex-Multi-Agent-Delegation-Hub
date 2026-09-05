#!/usr/bin/env node

import { delegate } from "./delegate.js";
import { runCompetition, type CompetitionResult } from "./competition.js";
import { fanOut } from "./fanout.js";
import { autoMerge } from "./merge.js";
import { releaseFanOutArtifactRefs } from "./artifacts.js";
import { createSession, resumeSession } from "./session.js";
import { asDelegateError } from "./errors.js";
import { supportedAgents } from "./adapters/index.js";
import type {
  DelegateError,
  DelegateRequest,
  ExecutionMode,
  FanOutCandidateSpec,
  FanOutRequest,
  FanOutResult,
  MergeOutcome,
} from "./types.js";
import type { CreateSessionRequest, ResumeSessionRequest } from "./session.js";
import { realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";

const usage = `Usage:
  agent-hub delegate --agent <omp|agy|grok> --mode <direct|isolated> [options] <task>
  agent-hub fanout --agent <a> [--agent <a>...] --task "<text>" [--task "<text>"...] [options]
  agent-hub session create --agent <a> --task "<text>" [options]
  agent-hub session resume <session-id> --task "<text>" [options]

Options:
  --workspace <path>   Workspace to operate on (default: current directory)
  --allow-dirty        Allow execution when the workspace has local changes
  --max-output-bytes   Limit captured stdout, stderr, and diff output
  --json               Emit the unified JSON result (the default)
  --help               Show this help

Fan-out options (fanout):
  --agent <a>          Candidate agent; repeat for one candidate per agent
  --task "<text>"      Candidate task; repeat to pair 1:1 with --agent in order,
                       or pass once to give every agent the same task
  --concurrency <n>    Maximum candidates in flight (1..8)
  --judge <agent>      fanout only: judge the candidates after fan-out
  --auto-merge         fanout only (default off): opt in to adopting the
                       competition winner into the primary checkout by
                       verified fast-forward; requires --judge

Session options (session create/resume):
  --task "<text>"      Required agent turn; resume requires a new task
  --agent <a>          Agent for session create

Exit codes: 0 success, 1 structured operation failure (JSON on stdout),
2 parse or usage error (message plus usage on stderr).
`;

export interface CliOptions {
  request: DelegateRequest;
  help: boolean;
}

export interface DelegateInvocation {
  kind: "delegate";
  options: CliOptions;
}

export interface FanoutInvocation {
  kind: "fanout";
  request: FanOutRequest;
  /** null when no competition was requested. */
  judge: string | null;
  autoMerge: boolean;
}

export interface SessionCreateInvocation {
  kind: "session-create";
  request: CreateSessionRequest;
}

export interface SessionResumeInvocation {
  kind: "session-resume";
  request: ResumeSessionRequest;
}

export interface HelpInvocation {
  kind: "help";
}

export type CliInvocation =
  | DelegateInvocation
  | FanoutInvocation
  | SessionCreateInvocation
  | SessionResumeInvocation
  | HelpInvocation;

function requireValue(argv: string[], index: number, option: string): string {
  const value = argv[index + 1];
  if (!value || value.startsWith("--")) {
    throw new Error(`${option} requires a value`);
  }
  return value;
}

export function parseCliArgs(argv: string[]): CliOptions {
  const args = argv[0] === "delegate" ? argv.slice(1) : argv;
  if (args.includes("--help") || args.length === 0) {
    return {
      help: true,
      request: {
        task: "",
        agent: "",
        mode: "isolated",
        workspace: process.cwd(),
      },
    };
  }

  let agent = "";
  let mode: ExecutionMode | "" = "";
  let workspace = process.cwd();
  let allowDirty = false;
  let maxOutputBytes: number | undefined;
  const taskParts: string[] = [];

  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    switch (argument) {
      case "--agent":
        agent = requireValue(args, index, argument);
        index += 1;
        break;
      case "--mode": {
        const value = requireValue(args, index, argument);
        if (value !== "direct" && value !== "isolated") {
          throw new Error(`--mode must be direct or isolated`);
        }
        mode = value;
        index += 1;
        break;
      }
      case "--workspace":
        workspace = requireValue(args, index, argument);
        index += 1;
        break;
      case "--allow-dirty":
        allowDirty = true;
        break;
      case "--max-output-bytes": {
        const value = requireValue(args, index, argument);
        maxOutputBytes = Number(value);
        if (!Number.isInteger(maxOutputBytes) || maxOutputBytes < 1) {
          throw new Error(`--max-output-bytes must be a positive integer`);
        }
        index += 1;
        break;
      }
      case "--json":
        break;
      default:
        if (argument.startsWith("--")) {
          throw new Error(`Unknown option: ${argument}`);
        }
        taskParts.push(argument);
    }
  }

  if (!agent) {
    throw new Error(`--agent is required (${supportedAgents.join(", ")})`);
  }
  if (!mode) {
    throw new Error(`--mode is required (direct or isolated)`);
  }
  if (taskParts.length === 0) {
    throw new Error(`task is required`);
  }

  return {
    help: false,
    request: {
      agent,
      mode,
      task: taskParts.join(" "),
      workspace,
      allowDirty,
      maxOutputBytes,
    },
  };
}

function assertSupportedAgent(agent: string, option: string): void {
  if (!(supportedAgents as readonly string[]).includes(agent)) {
    throw new Error(`${option} must be one of: ${supportedAgents.join(", ")}`);
  }
}

interface FanoutParse {
  request: FanOutRequest;
  judge: string | null;
  autoMerge: boolean;
}

/** Grammar for the isolated fan-out command. */
function parseFanoutArgs(args: string[], judgeAllowed: boolean): FanoutParse {
  const agents: string[] = [];
  const tasks: string[] = [];
  let workspace = process.cwd();
  let allowDirty = false;
  let maxOutputBytes: number | undefined;
  let concurrency: number | undefined;
  let judge: string | null = null;
  let autoMerge = false;

  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    switch (argument) {
      case "--agent":
        agents.push(requireValue(args, index, argument));
        index += 1;
        break;
      case "--task":
        tasks.push(requireValue(args, index, argument));
        index += 1;
        break;
      case "--workspace":
        workspace = requireValue(args, index, argument);
        index += 1;
        break;
      case "--concurrency": {
        const value = requireValue(args, index, argument);
        concurrency = Number(value);
        if (!Number.isInteger(concurrency) || concurrency < 1) {
          throw new Error(`--concurrency must be a positive integer`);
        }
        index += 1;
        break;
      }
      case "--allow-dirty":
        allowDirty = true;
        break;
      case "--max-output-bytes": {
        const value = requireValue(args, index, argument);
        maxOutputBytes = Number(value);
        if (!Number.isInteger(maxOutputBytes) || maxOutputBytes < 1) {
          throw new Error(`--max-output-bytes must be a positive integer`);
        }
        index += 1;
        break;
      }
      case "--judge":
        judge = requireValue(args, index, argument);
        index += 1;
        break;
      case "--auto-merge":
        autoMerge = true;
        break;
      case "--json":
        break;
      default:
        if (argument.startsWith("--")) {
          throw new Error(`Unknown option: ${argument}`);
        }
        throw new Error(`unexpected argument "${argument}"; candidate tasks must use --task`);
    }
  }

  if (agents.length === 0) {
    throw new Error(`--agent is required at least once (${supportedAgents.join(", ")})`);
  }
  for (const agent of agents) {
    assertSupportedAgent(agent, "--agent");
  }
  if (tasks.length === 0) {
    throw new Error(`--task is required`);
  }

  let candidates: FanOutCandidateSpec[];
  if (tasks.length === agents.length) {
    candidates = agents.map((agent, position) => ({ agent, task: tasks[position] }));
  } else if (tasks.length === 1) {
    candidates = agents.map((agent) => ({ agent, task: tasks[0] }));
  } else {
    throw new Error(`--task must be given once (shared task) or exactly once per --agent`);
  }

  if (judge !== null) {
    if (!judgeAllowed) {
      throw new Error(`--judge is only supported for the fanout command`);
    }
    assertSupportedAgent(judge, "--judge");
  }
  if (autoMerge && judge === null) {
    throw new Error(`--auto-merge requires --judge: adoption only follows an internal selection`);
  }

  return {
    request: {
      workspace,
      candidates,
      maxConcurrency: concurrency,
      allowDirty,
      maxOutputBytes,
    },
    judge,
    autoMerge: judgeAllowed ? autoMerge : false,
  };
}

function parseSessionCreateArgs(args: string[]): CreateSessionRequest {
  let agent = "";
  let workspace = process.cwd();
  let task = "";
  let allowDirty = false;
  let maxOutputBytes: number | undefined;

  for (let index = 0; index < args.length; index += 1) {
    const argument = args[index];
    switch (argument) {
      case "--agent":
        if (agent) {
          throw new Error("--agent may be specified only once for a session");
        }
        agent = requireValue(args, index, argument);
        index += 1;
        break;
      case "--task":
        if (task) {
          throw new Error("--task may be specified only once for a session");
        }
        task = requireValue(args, index, argument);
        index += 1;
        break;
      case "--workspace":
        workspace = requireValue(args, index, argument);
        index += 1;
        break;
      case "--allow-dirty":
        allowDirty = true;
        break;
      case "--max-output-bytes": {
        const value = requireValue(args, index, argument);
        maxOutputBytes = Number(value);
        if (!Number.isInteger(maxOutputBytes) || maxOutputBytes < 1) {
          throw new Error("--max-output-bytes must be a positive integer");
        }
        index += 1;
        break;
      }
      case "--json":
        break;
      default:
        if (argument.startsWith("--")) {
          throw new Error(`Unknown option: ${argument}`);
        }
        throw new Error(`unexpected argument "${argument}"; session tasks must use --task`);
    }
  }

  if (!agent) {
    throw new Error(`--agent is required (${supportedAgents.join(", ")})`);
  }
  assertSupportedAgent(agent, "--agent");
  if (!task) {
    throw new Error("--task is required");
  }

  return { agent, task, workspace, allowDirty, maxOutputBytes };
}

export function parseCliCommand(argv: string[]): CliInvocation {
  const command = argv[0];

  if (command === "fanout") {
    if (argv.includes("--help")) {
      return { kind: "help" };
    }
    const parsed = parseFanoutArgs(argv.slice(1), true);
    return { kind: "fanout", request: parsed.request, judge: parsed.judge, autoMerge: parsed.autoMerge };
  }

  if (command === "session") {
    const action = argv[1];
    if (argv.includes("--help")) {
      return { kind: "help" };
    }

    if (action === "create") {
      return { kind: "session-create", request: parseSessionCreateArgs(argv.slice(2)) };
    }

    if (action === "resume") {
      const args = argv.slice(2);
      let sessionId = "";
      let workspace = process.cwd();
      let task = "";
      let maxOutputBytes: number | undefined;

      for (let index = 0; index < args.length; index += 1) {
        const argument = args[index];
        switch (argument) {
          case "--task":
            if (task) {
              throw new Error("--task may be specified only once for a session resume");
            }
            task = requireValue(args, index, argument);
            index += 1;
            break;
          case "--workspace":
            workspace = requireValue(args, index, argument);
            index += 1;
            break;
          case "--max-output-bytes": {
            const value = requireValue(args, index, argument);
            maxOutputBytes = Number(value);
            if (!Number.isInteger(maxOutputBytes) || maxOutputBytes < 1) {
              throw new Error("--max-output-bytes must be a positive integer");
            }
            index += 1;
            break;
          }
          case "--json":
            break;
          default:
            if (argument.startsWith("--")) {
              throw new Error(`Unknown option: ${argument}`);
            }
            if (sessionId) {
              throw new Error(`unexpected argument "${argument}"`);
            }
            sessionId = argument;
        }
      }

      if (!sessionId) {
        throw new Error(`session resume requires <session-id>`);
      }
      if (!task) {
        throw new Error("session resume requires --task");
      }
      return { kind: "session-resume", request: { session_id: sessionId, task, workspace, maxOutputBytes } };
    }

    throw new Error(`session requires "create" or "resume", got "${action ?? ""}"`);
  }

  if (command === "compete") {
    throw new Error("compete is not a persisted-session command; use fanout with --judge");
  }

  return { kind: "delegate", options: parseCliArgs(argv) };
}

export async function runCli(
  argv: string[],
  output: { stdout: (value: string) => void; stderr: (value: string) => void } = {
    stdout: (value) => process.stdout.write(value),
    stderr: (value) => process.stderr.write(value),
  },
): Promise<number> {
  let invocation: CliInvocation;
  try {
    invocation = parseCliCommand(argv);
  } catch (error) {
    output.stderr(`${error instanceof Error ? error.message : String(error)}\n\n${usage}`);
    return 2;
  }

  if (invocation.kind === "help") {
    output.stdout(usage);
    return 0;
  }

  try {
    return await execute(invocation, output.stdout);
  } catch (error) {
    const failure = asDelegateError(error);
    output.stdout(`${JSON.stringify({ error: failure }, null, 2)}\n`);
    return 1;
  }
}

async function execute(
  invocation: Exclude<CliInvocation, HelpInvocation>,
  emitLine: (value: string) => void,
): Promise<number> {
  const emit = (document: unknown) => emitLine(`${JSON.stringify(document, null, 2)}\n`);

  switch (invocation.kind) {
    case "delegate": {
      if (invocation.options.help) {
        emitLine(usage);
        return 0;
      }
      const result = await delegate(invocation.options.request);
      emit(result);
      return result.status === "success" ? 0 : 1;
    }

    case "fanout": {
      const fan = await fanOut(invocation.request);
      let document:
        | (FanOutResult & { ref_cleanup_errors?: DelegateError[] })
        | {
            fan_out: FanOutResult;
            competition: CompetitionResult;
            merge?: MergeOutcome;
            ref_cleanup_errors?: DelegateError[];
          }
        | undefined = undefined;
      let failure: boolean;
      try {
        if (invocation.judge === null) {
          document = fan;
          // A partial or fully-failed fan-out is an operation failure.
          failure = fan.status !== "success";
        } else {
          const competition = await runCompetition({
            fan_out: fan,
            strategy: "judge",
            judge_agent: invocation.judge,
            workspace: invocation.request.workspace,
            maxOutputBytes: invocation.request.maxOutputBytes,
          });
          if (!invocation.autoMerge) {
            document = { fan_out: fan, competition };
            failure = fan.status !== "success" || competition.error !== null;
          } else {
            const merge = await autoMerge({
              workspace: invocation.request.workspace,
              fan_out: fan,
              competition,
            });
            document = { fan_out: fan, competition, merge };
            failure =
              fan.status !== "success" ||
              competition.error !== null ||
              merge.error !== null ||
              !merge.clean;
          }
        }
      } finally {
        // Terminal path: once this command's document exists, nothing else
        // can consume the candidate artifact refs, so release them CAS-safe
        // (refs already re-targeted by someone else are left untouched).
        const cleanupErrors = await releaseFanOutArtifactRefs(
          invocation.request.workspace,
          fan,
        );
        if (cleanupErrors.length > 0 && document !== undefined) {
          // Cleanup trouble rides along on the document; it never masks the
          // operation's own result.
          document = { ...document, ref_cleanup_errors: cleanupErrors };
        }
      }
      emit(document);
      return failure ? 1 : 0;
    }

    case "session-create": {
      const session = await createSession(invocation.request);
      emit(session);
      return session.run.status === "success" && session.cleanup_error === null ? 0 : 1;
    }

    case "session-resume": {
      const session = await resumeSession(invocation.request);
      emit(session);
      return session.run.status === "success" && session.cleanup_error === null ? 0 : 1;
    }
  }
}

function isEntrypoint(): boolean {
  if (!process.argv[1]) {
    return false;
  }

  try {
    return realpathSync(process.argv[1]) === fileURLToPath(import.meta.url);
  } catch {
    return false;
  }
}

if (isEntrypoint()) {
  runCli(process.argv.slice(2)).then((exitCode) => {
    process.exitCode = exitCode;
  });
}
