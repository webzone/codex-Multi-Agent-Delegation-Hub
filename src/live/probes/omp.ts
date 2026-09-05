/**
 * Probe + provider factory for the `omp` live provider. Detection runs
 * `omp --version` only (argv array, `shell:false`) — never `omp --mode rpc`,
 * so no session ever launches during a probe.
 *
 * The live RPC dialect this package ships was verified against OMP 18.1.10;
 * other versions are reported honestly as an unverified dialect rather than
 * silently accepted or rejected.
 */
import { runProcess } from "../../process.js";
import type { LiveProbeResult, LiveProviderFactory } from "../types.js";
import { selectPreferredTransport } from "../transports/rpc-base.js";

export const OMP_VERIFIED_VERSION = "18.1.10";

const PROBE_OUTPUT_BYTES = 4096;

function parseVersion(output: string): string | null {
  const firstLine = output.trim().split("\n")[0]?.trim() ?? "";
  const prefixed = /^omp\/(\S+)$/.exec(firstLine);
  if (prefixed?.[1]) {
    return prefixed[1];
  }
  return /^\S+$/.test(firstLine) ? firstLine : null;
}

export async function probeOmp(env: NodeJS.ProcessEnv = process.env): Promise<LiveProbeResult> {
  const result = await runProcess("omp", ["--version"], {
    cwd: process.cwd(),
    env,
    maxOutputBytes: PROBE_OUTPUT_BYTES,
  });

  if (result.error !== null) {
    return { found: false, version: null, detail: `omp --version failed to run: ${result.error}`.slice(0, 200) };
  }
  if (result.exitCode !== 0) {
    return { found: false, version: null, detail: `omp --version exited with code ${result.exitCode}`.slice(0, 200) };
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
