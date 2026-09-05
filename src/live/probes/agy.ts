import type { LiveProbeResult } from "../types.js";
import { runProcess } from "../../process.js";

/**
 * Probe for the installed `agy` command (Package 3, AGY 1.1.26 contract).
 *
 * The probe launches nothing that runs a turn: only `--version` and `--help`
 * are executed, both with an argument array and `shell: false`. Resume argv
 * verification follows the v2 bar (`NativeResumeCapability.verify()`): true
 * only when the installed binary's own help output advertises the resume
 * flag this transport would emit — never inferred.
 */

export const AGY_DEFAULT_COMMAND = "agy";
export const AGY_COMMAND_ENV = "AGENT_HUB_AGY_BIN";

/** Flags this transport always launches (AGY 1.1.26 stream-json contract). */
export const AGY_STREAM_JSON_ARGS = [
  "--input-format",
  "stream-json",
  "--output-format",
  "stream-json",
] as const;

/** Resume flag required by the AGY 1.1.26 contract; identity-verified at init. */
export const AGY_RESUME_FLAG = "--conversation";

export interface AgyProbeOptions {
  command?: string;
  environment?: NodeJS.ProcessEnv;
  cwd?: string;
  maxOutputBytes?: number;
}

/** `LiveProbeResult` plus the resume-argv verification the transport needs. */
export interface AgyProbeResult extends LiveProbeResult {
  resume_argv_verified: boolean;
}

export function resolveAgyCommand(options: AgyProbeOptions = {}): string {
  const environment = options.environment ?? process.env;
  const override = options.command ?? environment[AGY_COMMAND_ENV];
  return override && override.trim() ? override : AGY_DEFAULT_COMMAND;
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

export async function probeAgy(options: AgyProbeOptions = {}): Promise<AgyProbeResult> {
  const maxOutputBytes = options.maxOutputBytes ?? 64 * 1024;
  const command = resolveAgyCommand(options);
  const runOptions = {
    cwd: options.cwd ?? process.cwd(),
    env: options.environment ?? process.env,
    maxOutputBytes,
  };

  const versionRun = await runProcess(command, ["--version"], runOptions);
  if (versionRun.error !== null || versionRun.exitCode !== 0) {
    return {
      found: false,
      version: null,
      detail: versionRun.error
        ? `agy command not runnable: ${firstLine(versionRun.error, 200) ?? "unknown error"}`
        : `agy --version exited with code ${versionRun.exitCode ?? "null"}`,
      resume_argv_verified: false,
    };
  }

  const helpRun = await runProcess(command, ["--help"], runOptions);
  const helpText = `${helpRun.stdout}\n${helpRun.stderr}`;
  const resumeArgvVerified =
    helpRun.error === null &&
    helpRun.exitCode === 0 &&
    helpText.includes(AGY_RESUME_FLAG) &&
    helpText.includes("--input-format");

  const version = `${versionRun.stdout}\n${versionRun.stderr}`.match(/(\d+\.\d+\.\d+)/)?.[1] ?? null;
  const detail = `agy responds to --version${version ? ` as ${version}` : " without a version string"}; resume argv ${AGY_RESUME_FLAG} ${resumeArgvVerified ? "advertised in --help" : "NOT advertised in --help"}`;

  return {
    found: true,
    version,
    detail: firstLine(detail, 400),
    resume_argv_verified: resumeArgvVerified,
  };
}
