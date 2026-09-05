import type { LiveProbeResult } from "../types.js";
import { runProcess } from "../../process.js";

/**
 * Probe for the installed `hermes` command (Package 3, Hermes 0.20.5 contract).
 *
 * The probe only runs `hermes --version`; it never launches `hermes acp`.
 * The ACP v1 runtime handshake (initialize round-trip, protocolVersion 1
 * negotiation, `loadSession` advertisement) is verified by the transport at
 * session open, because a probe that guesses from argv would violate the
 * "launching nothing is fine, guessing is not" rule in `LiveProbeResult`.
 */

export const HERMES_DEFAULT_COMMAND = "hermes";
export const HERMES_COMMAND_ENV = "AGENT_HUB_HERMES_BIN";

/** Subcommand that starts the ACP v1 stdio server (Hermes 0.20.5 contract). */
export const HERMES_ACP_ARGS = ["acp"] as const;

export interface HermesProbeOptions {
  command?: string;
  environment?: NodeJS.ProcessEnv;
  cwd?: string;
  maxOutputBytes?: number;
}

export function resolveHermesCommand(options: HermesProbeOptions = {}): string {
  const environment = options.environment ?? process.env;
  const override = options.command ?? environment[HERMES_COMMAND_ENV];
  return override && override.trim() ? override : HERMES_DEFAULT_COMMAND;
}

function firstLine(text: string, maxBytes: number): string | null {
  const line = text.split("\n").map((part) => part.trim()).find(Boolean);
  if (!line) {
    return null;
  }
  const bytes = Buffer.from(line, "utf8");
  if (bytes.byteLength <= maxBytes) {
    return line;
  }
  return Buffer.concat([bytes.subarray(0, maxBytes), Buffer.from("…", "utf8")]).toString("utf8");
}

export async function probeHermes(options: HermesProbeOptions = {}): Promise<LiveProbeResult> {
  const maxOutputBytes = options.maxOutputBytes ?? 64 * 1024;
  const command = resolveHermesCommand(options);

  const versionRun = await runProcess(command, ["--version"], {
    cwd: options.cwd ?? process.cwd(),
    env: options.environment ?? process.env,
    maxOutputBytes,
  });

  if (versionRun.error !== null || versionRun.exitCode !== 0) {
    return {
      found: false,
      version: null,
      detail: versionRun.error
        ? `hermes command not runnable: ${firstLine(versionRun.error, 200) ?? "unknown error"}`
        : `hermes --version exited with code ${versionRun.exitCode ?? "null"}`,
    };
  }

  const version = `${versionRun.stdout}\n${versionRun.stderr}`.match(/(\d+\.\d+\.\d+)/)?.[1] ?? null;
  return {
    found: true,
    version,
    detail: firstLine(
      `hermes responds to --version${version ? ` as ${version}` : " without a version string"}; ACP v1 handshake is verified at session open via \`hermes acp\` over stdio`,
      400,
    ),
  };
}
