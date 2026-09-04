import { spawn } from "node:child_process";

export interface ProcessResult {
  exitCode: number | null;
  stdout: string;
  stderr: string;
  stdoutTruncated: boolean;
  stderrTruncated: boolean;
  error: string | null;
}

export interface ProcessOptions {
  cwd: string;
  env?: NodeJS.ProcessEnv;
  maxOutputBytes: number;
}

interface OutputCapture {
  append(chunk: Buffer): void;
  value(): string;
  truncated: boolean;
}

function createOutputCapture(maxOutputBytes: number): OutputCapture {
  const chunks: Buffer[] = [];
  let length = 0;
  let isTruncated = false;

  return {
    append(chunk) {
      if (length >= maxOutputBytes) {
        isTruncated = true;
        return;
      }

      const remaining = maxOutputBytes - length;
      if (chunk.byteLength > remaining) {
        chunks.push(chunk.subarray(0, remaining));
        length = maxOutputBytes;
        isTruncated = true;
        return;
      }

      chunks.push(chunk);
      length += chunk.byteLength;
    },
    value() {
      return Buffer.concat(chunks).toString("utf8");
    },
    get truncated() {
      return isTruncated;
    },
  };
}

export function runProcess(
  executable: string,
  args: string[],
  options: ProcessOptions,
): Promise<ProcessResult> {
  return new Promise((resolve) => {
    const child = spawn(executable, args, {
      cwd: options.cwd,
      env: options.env ?? process.env,
      shell: false,
      stdio: ["ignore", "pipe", "pipe"],
    });

    const stdout = createOutputCapture(options.maxOutputBytes);
    const stderr = createOutputCapture(options.maxOutputBytes);

    child.stdout.on("data", (chunk: Buffer) => stdout.append(chunk));
    child.stderr.on("data", (chunk: Buffer) => stderr.append(chunk));
    child.once("error", (error) => {
      resolve({
        exitCode: null,
        stdout: stdout.value(),
        stderr: stderr.value(),
        stdoutTruncated: stdout.truncated,
        stderrTruncated: stderr.truncated,
        error: error.message,
      });
    });
    child.once("close", (exitCode) => {
      resolve({
        exitCode,
        stdout: stdout.value(),
        stderr: stderr.value(),
        stdoutTruncated: stdout.truncated,
        stderrTruncated: stderr.truncated,
        error: null,
      });
    });
  });
}
