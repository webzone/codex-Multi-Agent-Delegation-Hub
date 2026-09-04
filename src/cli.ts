#!/usr/bin/env node

import { delegate } from "./delegate.js";
import { supportedAgents } from "./adapters/index.js";
import type { DelegateRequest, ExecutionMode } from "./types.js";

const usage = `Usage:
  agent-hub delegate --agent <omp|agy|grok> --mode <direct|isolated> [options] <task>

Options:
  --workspace <path>   Workspace to operate on (default: current directory)
  --allow-dirty        Allow execution when the workspace has local changes
  --max-output-bytes   Limit captured stdout, stderr, and diff output
  --json               Emit the unified JSON result (the default)
  --help               Show this help
`;

export interface CliOptions {
  request: DelegateRequest;
  help: boolean;
}

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

export async function runCli(
  argv: string[],
  output: { stdout: (value: string) => void; stderr: (value: string) => void } = {
    stdout: (value) => process.stdout.write(value),
    stderr: (value) => process.stderr.write(value),
  },
): Promise<number> {
  try {
    const options = parseCliArgs(argv);
    if (options.help) {
      output.stdout(usage);
      return 0;
    }

    const result = await delegate(options.request);
    output.stdout(`${JSON.stringify(result, null, 2)}\n`);
    return result.status === "success" ? 0 : 1;
  } catch (error) {
    output.stderr(`${error instanceof Error ? error.message : String(error)}\n\n${usage}`);
    return 2;
  }
}

if (import.meta.url === `file://${process.argv[1]}`) {
  runCli(process.argv.slice(2)).then((exitCode) => {
    process.exitCode = exitCode;
  });
}
