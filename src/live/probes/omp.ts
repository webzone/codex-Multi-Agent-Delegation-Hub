/**
 * Probe + provider factory for the `omp` live provider. Detection runs
 * `<command> --version` only (argv array, `shell:false`) — never
 * `omp --mode rpc`, so no session ever launches during a probe.
 *
 * Command resolution mirrors the agy/hermes probes: explicit option first,
 * then `AGENT_HUB_OMP_BIN`, then `omp`. The seam keeps probe tests hermetic
 * (no real provider binary is ever looked up from PATH) and gives operators
 * one override knob across all four providers.
 *
 * The live RPC v2 dialect this package ships was verified against OMP 18.1.13;
 * other versions are reported honestly as an unverified dialect rather than
 * silently accepted or rejected.
 */
import { runProcess } from "../../process.js";
import type { LiveProbeResult, LiveProviderFactory } from "../types.js";
import { selectPreferredTransport } from "../transports/rpc-base.js";

export const OMP_VERIFIED_VERSION = "18.1.13";
export const OMP_DEFAULT_COMMAND = "omp";
export const OMP_COMMAND_ENV = "AGENT_HUB_OMP_BIN";

const PROBE_OUTPUT_BYTES = 4096;

export interface OmpProbeOptions {
  command?: string;
  environment?: NodeJS.ProcessEnv;
  cwd?: string;
  maxOutputBytes?: number;
}

export function resolveOmpCommand(options: OmpProbeOptions = {}): string {
  const environment = options.environment ?? process.env;
  const override = options.command ?? environment[OMP_COMMAND_ENV];
  return override && override.trim() ? override : OMP_DEFAULT_COMMAND;
}

function parseVersion(output: string): string | null {
  const firstLine = output.trim().split("\n")[0]?.trim() ?? "";
  const prefixed = /^omp\/(\S+)$/.exec(firstLine);
  if (prefixed?.[1]) {
    return prefixed[1];
  }
  return /^\S+$/.test(firstLine) ? firstLine : null;
}

export async function probeOmp(options: OmpProbeOptions = {}): Promise<LiveProbeResult> {
  const command = resolveOmpCommand(options);
  const result = await runProcess(command, ["--version"], {
    cwd: options.cwd ?? process.cwd(),
    env: options.environment ?? process.env,
    maxOutputBytes: options.maxOutputBytes ?? PROBE_OUTPUT_BYTES,
  });

  if (result.error !== null) {
    return { found: false, version: null, detail: `${command} --version failed to run: ${result.error}`.slice(0, 200) };
  }
  if (result.exitCode !== 0) {
    return { found: false, version: null, detail: `${command} --version exited with code ${result.exitCode}`.slice(0, 200) };
  }

  const version = parseVersion(result.stdout);
  if (version === null) {
    return { found: true, version: null, detail: "omp reported an unparsable version; the RPC dialect is unverified" };
  }
  const detail =
    version === OMP_VERIFIED_VERSION
      ? `live RPC dialect verified against omp ${OMP_VERIFIED_VERSION}`
      : `live RPC dialect verified against omp ${OMP_VERIFIED_VERSION}; installed binary reports ${version} (unverified dialect)`;
  return { found: true, version, detail };
}

export const ompProviderFactory: LiveProviderFactory = {
  provider: "omp",
  transports: ["omp-rpc"],
  selectTransport(factories) {
    return selectPreferredTransport(this, factories);
  },
};
