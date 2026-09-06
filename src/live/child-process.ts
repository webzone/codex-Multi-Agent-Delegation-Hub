import { spawn, type ChildProcess } from "node:child_process";
import type { Readable, Writable } from "node:stream";

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
 * `stop` reports `closed` only when the leader exit has been observed AND
 * (on POSIX) the owned process group is proven gone, and `orphaned` honestly
 * otherwise. Authorization boundary: `terminate` is the ONLY mode allowed to
 * signal a group that outlived its leader, and then only with a bounded
 * TERM→KILL escalation. A graceful stop never escalates after the leader is
 * gone — it proves the group gone or reports `orphaned` while retaining the
 * handle, so a later `terminate` can finish the shutdown with fresh proof.
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
  /** Raw stdout pipe (same stream the `onStdout` sinks read from). */
  readonly stdout: Readable | null;
  /** Raw stdin pipe; transports may also use `writeStdin`/`closeStdin`. */
  readonly stdin: Writable | null;
  /** Attach the transport's stdout consumer. Chunk order is stream order. */
  onStdout(sink: StdoutSink): void;
  /** Attach an additional stderr consumer; capture+drain runs regardless. */
  onStderr(sink: StdoutSink): void;
  writeStdin(data: string): Promise<void>;
  closeStdin(): void;
  /** Captured stderr text (bounded); the truncation flag is observable. */
  readonly stderrText: string;
  readonly stderrTruncated: boolean;
  /** Null until an exit has actually been observed. */
  readonly exitInfo: LiveChildExitInfo | null;
  /** Resolves exactly once, when the exit has been observed. */
  exited(): Promise<LiveChildExitInfo>;
  /**
   * Whether any member of the owned group is still signal-reachable. False
   * only when the OS proves the group is gone (ESRCH). Always false on
   * platforms without POSIX process groups (the leader-exit proof is then
   * the only proof available, and `stop` reports it honestly).
   */
  groupAlive(): boolean;
  /** Signal the whole group. False when the signal cannot be delivered. */
  signalGroup(signal: NodeJS.Signals): boolean;
  /** Resolves true once `groupAlive()` is false; false when it never was. */
  proveGroupGone(timeoutMs: number): Promise<boolean>;
  stop(mode: LiveStopMode, options?: LiveChildStopOptions): Promise<LiveStopReport>;
}

const DEFAULT_GRACE_MS = 5_000;
const DEFAULT_KILL_WAIT_MS = 5_000;
const POLL_INTERVAL_MS = 25;

/**
 * How long a graceful stop observes a surviving owned group after the leader
 * exit. Graceful sends the group nothing at that point (only `terminate`
 * authorizes group signals once the leader is down), so this window only
 * watches helpers that die naturally; survival past it reports `orphaned`.
 */
const GRACEFUL_GROUP_PROVE_MS = 250;

/** POSIX process-group signalling is unavailable on Windows; there `stop`
 * falls back to leader-exit proof alone and says so through `groupAlive`. */
export const SUPPORTS_GROUP_SIGNALS = process.platform !== "win32";

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
  const stderrSinks: StdoutSink[] = [];
  let stderrBytes = 0;
  let stderrTruncated = false;
  child.stderr?.on("data", (chunk: Buffer) => {
    for (const sink of stderrSinks) {
      sink(chunk);
    }
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

  // Permanent ownership of stderr stream failures (Task 1 / C2C review):
  // capture is observational, and a failed capture may never surface as an
  // uncaught 'error' event on the hub process. The exit observation runs its
  // course regardless, so close/recovery stay possible.
  child.stderr?.on("error", () => {
    // Owned and deliberately silent: the bounded capture simply stops
    // growing; `exitInfo`/`close` remain the authority on the process.
  });

  const stdoutSinks: StdoutSink[] = [];
  child.stdout?.on("data", (chunk: Buffer) => {
    for (const sink of stdoutSinks) {
      sink(chunk);
    }
  });

  // Permanent ownership of stdout stream failures: the transport's sinks read
  // the same raw stream, but a failed stdout pipe must not raise an uncaught
  // 'error' on the hub. Losing observation is not losing the session — the
  // exit/close observation (below) still decides the process's fate.
  child.stdout?.on("error", () => {
    // Owned and silent; stdout simply stops delivering chunks.
  });

  const exitDeferred = deferred<LiveChildExitInfo>();
  let shutdownRequested = false;
  let exitInfo: LiveChildExitInfo | null = null;
  let stdinBroken = false;
  // Permanent ownership of stdin failures. A provider that closes or dies
  // mid-session makes the OS pipe answer EPIPE; Node *also* emits that as an
  // 'error' event on the stdin Writable, and an unowned emission is an
  // uncaught exception that kills the hub process. This listener lives for
  // the whole child lifetime (not just around one write): every stdin error,
  // whenever it arrives — during a write, after `closeStdin`, after the
  // provider is gone — is owned here, marks the pipe broken, and any
  // caller-visible failure surfaces as the structured
  // `LIVE_CHILD_STDIN_CLOSED` rejection from `writeStdin`. Close/recovery
  // stay fully possible afterwards.
  child.stdin?.on("error", () => {
    stdinBroken = true;
  });

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
    get stdout(): Readable | null {
      return child.stdout ?? null;
    },
    get stdin(): Writable | null {
      return child.stdin ?? null;
    },
    onStdout(sink): void {
      stdoutSinks.push(sink);
    },
    onStderr(sink): void {
      stderrSinks.push(sink);
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
    groupAlive(): boolean {
      if (!SUPPORTS_GROUP_SIGNALS || child.pid === undefined) {
        return false;
      }
      try {
        process.kill(-pid(), 0);
        return true;
      } catch (error) {
        // Only ESRCH proves the group gone; EPERM proves presence with
        // unreachable ownership, which is never treated as absence.
        return (error as NodeJS.ErrnoException).code !== "ESRCH";
      }
    },
    signalGroup(signal): boolean {
      if (!SUPPORTS_GROUP_SIGNALS) {
        // No group semantics on this platform: the leader alone gets the
        // signal, and `stop` reports leader-exit proof without group proof.
        try {
          child.kill(signal);
          return true;
        } catch {
          return false;
        }
      }
      try {
        // Negative pid targets the whole group — including helpers that
        // outlived the leader. ESRCH means the group is already gone; EPERM
        // means unreachable — neither is ever treated as proof of death;
        // only the observed exit plus a group probe is.
        process.kill(-pid(), signal);
        return true;
      } catch {
        return false;
      }
    },
    async proveGroupGone(timeoutMs): Promise<boolean> {
      const deadline = Date.now() + Math.max(0, timeoutMs);
      for (;;) {
        if (!handle.groupAlive()) {
          return true;
        }
        if (Date.now() >= deadline) {
          return false;
        }
        await sleep(POLL_INTERVAL_MS);
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

      // Leader reaped is not enough: `closed` additionally requires proof the
      // whole owned group is gone. Authorization decides what happens to a
      // group that outlived its leader: `terminate` may run the bounded
      // TERM→KILL ladder against the owned PGID; `graceful` signals the group
      // nothing — it only watches for natural death and reports `orphaned`
      // (retaining this handle) when the group is still there.
      if (SUPPORTS_GROUP_SIGNALS && handle.groupAlive()) {
        if (mode === "terminate") {
          handle.signalGroup("SIGTERM");
          if (!(await handle.proveGroupGone(graceMs))) {
            handle.signalGroup("SIGKILL");
            await handle.proveGroupGone(killWaitMs);
          }
        } else {
          await handle.proveGroupGone(GRACEFUL_GROUP_PROVE_MS);
        }
      }
      if (SUPPORTS_GROUP_SIGNALS && handle.groupAlive()) {
        return {
          status: "orphaned",
          exit_code: observed.exit_code,
          exit_signal: observed.exit_signal,
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
