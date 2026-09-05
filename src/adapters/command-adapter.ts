import { AgentHubError } from "../errors.js";
import { runProcess } from "../process.js";
import type { AdapterExecutionResult, AdapterRequest, AgentAdapter } from "../types.js";
import {
  readProviderSessionId,
  type NativeResumeCapableAdapter,
} from "./types.js";

export interface CommandAdapterOptions {
  id: string;
  environmentPrefix: string;
  defaultExecutable: string;
  defaultArguments: string[];
  /**
   * Optional native-resume capability. Built-in adapters pass nothing: we
   * refuse to invent provider resume flags. Only an integrator who has
   * verified the installed command's resume syntax supplies this, and even
   * then the hub consults `verify()` before any resume argv is emitted.
   */
  nativeResume?: {
    verify: () => Promise<boolean>;
    resumeArguments: (providerSessionId: string) => string[];
  };
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

  // Verification is memoized per adapter instance: a probe (if supplied)
  // runs at most once, and any throw counts as unverified.
  let verifyPromise: Promise<boolean> | undefined;
  const resumeSpec = options.nativeResume;
  const verifiedResume = resumeSpec
    ? {
        verify(): Promise<boolean> {
          verifyPromise ??= resumeSpec.verify().then(
            (value) => value === true,
            () => false,
          );
          return verifyPromise;
        },
        resumeArguments: resumeSpec.resumeArguments,
      }
    : undefined;

  const adapter: AgentAdapter & Partial<NativeResumeCapableAdapter> = {
    id: options.id,
    async execute(request: AdapterRequest): Promise<AdapterExecutionResult> {
      const args = buildArguments(argumentTemplate, request.task);
      const providerSessionId = readProviderSessionId(request.metadata);
      if (verifiedResume && providerSessionId && (await verifiedResume.verify())) {
        // Provider session id travels as one dedicated argv element; the
        // spawn is shell:false, so it can never be re-interpreted.
        args.push(...verifiedResume.resumeArguments(providerSessionId));
      }

      const result = await runProcess(executable, args, {
        cwd: request.cwd,
        env: environment,
        maxOutputBytes: request.maxOutputBytes,
      });

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
    // Capability rides along only when an integrator supplied a spec;
    // structural detection happens in asNativeResumeCapableAdapter.
    ...(verifiedResume ? { nativeResumeCapability: verifiedResume } : {}),
  };
  return adapter;
}
