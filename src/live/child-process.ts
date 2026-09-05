import { spawn, type ChildProcess } from "node:child_process";

import { deferred } from "../deferred.js";
import { AgentHubError } from "../errors.js";
import type { LiveStopMode, LiveStopReport } from "./types.js";

/**
 * Process-group ownership for live provider processes (v3 live, Package 1).
 *
 * The hub launches every local provider process `detached`, which makes the
 * child its own process-group leader (POSIX: `setpgid` via detached spawn;
 * PGID == child PID). Everything the provider forks shares that group, so a
 * single `kill(-pgid, …)` reaches the whole tree — a provider that spawns
 * helper children can never outlive the hub's shutdown by one level.
 *
 * Ownership facts this module proves and never guesses:
 *   - stdin is a real pipe (`writeStdin` is the only input path);
 *   - stdout is handed over unbuffered-but-bounded-by-consumer as raw chunks
 *     (the transport frames them; the hub never accumulates it here);
 *   - stderr is captured up to `maxStderrBytes` and then *observed-as-
 *     truncated*, never relied on for termination proof;
 *   - exit is *observed* (the `exit`/`close` events), never inferred from a
 *     quiet stdout or a vanished socket.
 *
 * `stop` reports `closed` only with an observed exit, and `orphaned` honestly
 * otherwise. `terminate` mode authorizes bounded SIGKILL escalation of the
 * group; graceful mode stops at SIGTERM and reports survival instead of
 * pretending.
 */

export interface LiveChildSpec {
  command: string;
  args: string[];
  cwd: string;
  env?: NodeJS.ProcessEnv;
  /** Bound on captured stderr bytes; excess is marked, never stored. */
  maxStderrBytes: number;
}

export interface LiveChildExitInfo {
  exit_code: number | null;
  exit_signal: string | null;
  /** False unless the hub requested the shutdown that produced this exit. */
  intentional: boolean;
}

export type StdoutSink = (chunk: Buffer) => void;

export interface LiveChildStopOptions {
  /** How long to wait for a graceful group exit before escalating/giving up. */
  graceMs?: number;
  /** Additional wait after a terminate-mode SIGKILL before declaring orphaned. */
  killWaitMs?: number;
}

export interface LiveChildHandle {
  readonly pid: number;
  /** Process group owned by the hub; equals `pid` for a detached leader. */
  readonly pgid: number;
  /** Command used to launch, for lease/audit display. */
  readonly command: string;
  /** Attach the transport's stdout consumer. Chunk order is stream order. */
  onStdout(sink: StdoutSink): void;
  writeStdin(data: string): Promise<void>;
  closeStdin(): void;
  /** Captured stderr text (bounded); the truncation flag is observable. */
  readonly stderrText: string;
  readonly stderrTruncated: boolean;
  /** Null until an exit has actually been observed. */
  readonly exitInfo: LiveChildExitInfo | null;
  /** Resolves exactly once, when the exit has been observed. */
  exited(): Promise<LiveChildExitInfo>;
  /** Signal the whole group. False once exit was observed (or ESRCH proves gone). */
  signalGroup(signal: NodeJS.Signals): boolean;
  stop(mode: LiveStopMode, options?: LiveChildStopOptions): Promise<LiveStopReport>;
}

const DEFAULT_GRACE_MS = 5_000;
const DEFAULT_KILL_WAIT_MS = 5_000;
const POLL_INTERVAL_MS = 25;

function sleep(ms: number): Promise<void> {
  const { promise, resolve } = deferred<void>();
  setTimeout(resolve, ms);
  return promise;
}

/**
 * Launch a group-owned provider child. Resolves once the spawn itself
 * succeeded (a pid exists); launch-time failures (ENOENT, EACCES) reject with
 * `LIVE_CHILD_SPAWN_FAILED` — the transport turns that into a `launch`-stage
 * LiveError.
 */
export function launchLiveChild(spec: LiveChildSpec): Promise<LiveChildHandle> {
  const { promise, resolve, reject } = deferred<LiveChildHandle>();
  let settled = false;

  const child: ChildProcess = spawn(spec.command, spec.args, {
    cwd: spec.cwd,
    env: spec.env ?? process.env,
    shell: false,
    detached: true, // Owns its process group; the hub signals the whole tree.
    stdio: ["pipe", "pipe", "pipe"],
  });

  child.once("error", (error) => {
    if (!settled) {
      settled = true;
      reject(
        new AgentHubError(
          "LIVE_CHILD_SPAWN_FAILED",
          `provider process "${spec.command}" could not be launched: ${error.message}`,
        ),
      );
      return;
    }
    // Post-launch errors surface through the exit observation, not a second
    // rejection; stdout/stderr pipes may already carry the real story.
    observation.finish({ exit_code: null, exit_signal: null });
  });

  child.once("spawn", () => {
    if (settled || child.pid === undefined) {
      return;
    }
    settled = true;
    resolve(handle);
  });

  const stderrChunks: Buffer[] = [];
  let stderrBytes = 0;
  let stderrTruncated = false;
  child.stderr?.on("data", (chunk: Buffer) => {
    if (stderrBytes >= spec.maxStderrBytes) {
      stderrTruncated = true;
      return;
    }
    const remaining = spec.maxStderrBytes - stderrBytes;
    if (chunk.byteLength > remaining) {
      stderrChunks.push(chunk.subarray(0, remaining));
      stderrBytes = spec.maxStderrBytes;
      stderrTruncated = true;
      return;
    }
    stderrChunks.push(chunk);
    stderrBytes += chunk.byteLength;
  });
  // stderr must always drain, or a chatty provider deadlocks on a full pipe.
  child.stderr?.resume();

  const stdoutSinks: StdoutSink[] = [];
  child.stdout?.on("data", (chunk: Buffer) => {
    for (const sink of stdoutSinks) {
      sink(chunk);
    }
  });

  const exitDeferred = deferred<LiveChildExitInfo>();
  let shutdownRequested = false;
  let exitInfo: LiveChildExitInfo | null = null;
  let stdinBroken = false;

  const observation = {
    finish(raw: { exit_code: number | null; exit_signal: string | null }): void {
      if (exitInfo !== null) {
        return;
      }
      exitInfo = {
        exit_code: raw.exit_code,
        exit_signal: raw.exit_signal,
        intentional: shutdownRequested,
      };
      exitDeferred.resolve(exitInfo);
    },
  };

  child.once("exit", (code, signal) => observation.finish({ exit_code: code, exit_signal: signal }));
  child.once("close", (code, signal) => observation.finish({ exit_code: code, exit_signal: signal }));

  const pid = () => child.pid as number;

  const handle: LiveChildHandle = {
    get pid(): number {
      return pid();
    },
    get pgid(): number {
      // Detached leader: the child's pgid is its own pid. A group leader that
      // has since exec'd/chmod'd nothing changes this — only setpgid by the
      // child itself could, which the hub's own contract forbids providers
      // from relying on; stop() therefore always verifies death, not group.
      return pid();
    },
    command: spec.command,
    onStdout(sink): void {
      stdoutSinks.push(sink);
    },
    writeStdin(data): Promise<void> {
      const stdin = child.stdin;
      if (!stdin || stdin.destroyed || stdinBroken) {
        return Promise.reject(
          new AgentHubError(
            "LIVE_CHILD_STDIN_CLOSED",
            "provider stdin is closed; the command cannot be delivered",
          ),
        );
      }
      const { promise, resolve: done, reject: failed } = deferred<void>();
      stdin.write(data, (error) => {
        if (error) {
          stdinBroken = true;
          failed(
            new AgentHubError(
              "LIVE_CHILD_STDIN_CLOSED",
              `provider stdin write failed: ${error.message}`,
            ),
          );
          return;
        }
        done();
      });
      return promise;
    },
    closeStdin(): void {
      // EOF the provider's input; commands after this are pointless but the
      // exit observation must still run its course.
      stdinBroken = true;
      child.stdin?.end();
    },
    get stderrText(): string {
      return Buffer.concat(stderrChunks, stderrBytes).toString("utf8");
    },
    get stderrTruncated(): boolean {
      return stderrTruncated;
    },
    get exitInfo(): LiveChildExitInfo | null {
      return exitInfo;
    },
    exited(): Promise<LiveChildExitInfo> {
      return exitDeferred.promise;
    },
    signalGroup(signal): boolean {
      if (exitInfo !== null) {
        return false;
      }
      try {
        // Negative pid targets the whole group. ESRCH means the group is
        // already gone; EPERM means unreachable — neither is ever treated as
        // proof of death; only the observed exit is.
        process.kill(-pid(), signal);
        return true;
      } catch {
        return false;
      }
    },
    async stop(mode, options): Promise<LiveStopReport> {
      const graceMs = options?.graceMs ?? DEFAULT_GRACE_MS;
      const killWaitMs = options?.killWaitMs ?? DEFAULT_KILL_WAIT_MS;
      const started = Date.now();

      if (exitInfo === null) {
        shutdownRequested = true;
        handle.signalGroup("SIGTERM");
        const deadline = started + Math.max(0, graceMs);
        while (exitInfo === null && Date.now() < deadline) {
          await sleep(POLL_INTERVAL_MS);
        }
      }

      if (exitInfo === null && mode === "terminate") {
        handle.signalGroup("SIGKILL");
        const deadline = Date.now() + Math.max(0, killWaitMs);
        while (exitInfo === null && Date.now() < deadline) {
          await sleep(POLL_INTERVAL_MS);
        }
      }

      const observed = handle.exitInfo;
      if (observed === null) {
        return {
          status: "orphaned",
          exit_code: null,
          exit_signal: null,
          waited_ms: Date.now() - started,
        };
      }
      return {
        status: "closed",
        exit_code: observed.exit_code,
        exit_signal: observed.exit_signal,
        waited_ms: Date.now() - started,
      };
    },
  };

  return promise;
}
