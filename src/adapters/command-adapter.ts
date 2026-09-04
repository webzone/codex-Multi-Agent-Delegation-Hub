import { AgentHubError } from "../errors.js";
import { runProcess } from "../process.js";
import type { AdapterExecutionResult, AdapterRequest, AgentAdapter } from "../types.js";

export interface CommandAdapterOptions {
  id: string;
  environmentPrefix: string;
  defaultExecutable: string;
  defaultArguments: string[];
}

function parseConfiguredArguments(value: string | undefined, variableName: string): string[] | undefined {
  if (!value) {
    return undefined;
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(value);
  } catch {
    throw new AgentHubError("INVALID_CONFIGURATION", `${variableName} must be a JSON array of strings`);
  }

  if (!Array.isArray(parsed) || parsed.some((item) => typeof item !== "string")) {
    throw new AgentHubError("INVALID_CONFIGURATION", `${variableName} must be a JSON array of strings`);
  }

  return parsed;
}

function buildArguments(template: string[], task: string): string[] {
  const hasTaskPlaceholder = template.some((argument) => argument === "{task}");
  const args = template.map((argument) => (argument === "{task}" ? task : argument));
  return hasTaskPlaceholder ? args : [...args, task];
}

export function createCommandAdapter(
  options: CommandAdapterOptions,
  environment: NodeJS.ProcessEnv = process.env,
): AgentAdapter {
  const executableVariable = `${options.environmentPrefix}_BIN`;
  const argumentsVariable = `${options.environmentPrefix}_ARGS`;
  const executable = environment[executableVariable] || options.defaultExecutable;
  const configuredArguments = parseConfiguredArguments(environment[argumentsVariable], argumentsVariable);
  const argumentTemplate = configuredArguments ?? options.defaultArguments;

  return {
    id: options.id,
    async execute(request: AdapterRequest): Promise<AdapterExecutionResult> {
      const result = await runProcess(
        executable,
        buildArguments(argumentTemplate, request.task),
        {
          cwd: request.cwd,
          env: environment,
          maxOutputBytes: request.maxOutputBytes,
        },
      );

      return {
        exit_code: result.exitCode,
        stdout: result.stdout,
        stderr: result.stderr,
        session_id: null,
        stdout_truncated: result.stdoutTruncated,
        stderr_truncated: result.stderrTruncated,
        error: result.error,
      };
    },
  };
}
