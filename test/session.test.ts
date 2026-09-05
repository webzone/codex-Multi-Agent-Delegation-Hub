import { spawn } from "node:child_process";
import { access, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { hostname } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { resolveAdapter } from "../src/adapters/index.js";
import type { NativeResumeCapableAdapter } from "../src/adapters/types.js";
import { resolveRepositoryIdentity } from "../src/git.js";
import { acquireRepositoryLock, lockPathFor, type LockOwner } from "../src/locks.js";
import { WORKTREE_ADMIN_LOCK_NAME } from "../src/fanout.js";
import { deferred } from "../src/deferred.js";
import { sessionLockName, sessionPendingPath, sessionStatePath } from "../src/state.js";
import { createSession, resumeSession } from "../src/session.js";
import type {
  AdapterExecutionResult,
  AdapterMetadata,
  AgentAdapter,
  DelegateResult,
} from "../src/types.js";
import { createGitRepository, removeDirectory, runGit } from "./helpers.js";

function okResult(over: Partial<AdapterExecutionResult> = {}): AdapterExecutionResult {
  return {
    exit_code: 0,
    stdout: "",
    stderr: "",
    session_id: null,
    stdout_truncated: false,
    stderr_truncated: false,
    error: null,
    ...over,
  };
}

function expectErrorCode(promise: Promise<unknown>, code: string): Promise<void> {
  return expect(promise).rejects.toMatchObject({ code });
}

async function missing(path: string): Promise<boolean> {
  try {
    await readFile(path);
    return false;
  } catch {
    return true;
  }
}

async function refSha(repo: string, ref: string): Promise<string> {
  return (await runGit(repo, ["rev-parse", ref])).trim();
}

async function blobContent(repo: string, revision: string): Promise<string> {
  return (await runGit(repo, ["show", revision])).trim();
}

async function statusPorcelain(repo: string): Promise<string> {
  return runGit(repo, ["status", "--porcelain=v1", "--untracked-files=all"]);
}

async function deadPid(): Promise<number> {
  const child = spawn(process.execPath, ["-e", "process.exit(0)"]);
  const pid = child.pid as number;
  const { promise, resolve } = deferred<void>();
  child.once("exit", resolve);
  await promise;
  return pid;
}

async function livePid(): Promise<{ pid: number; stop: () => void }> {
  // The child's own timer keeps it alive; the test awaits only child events.
  const child = spawn(process.execPath, ["-e", "setTimeout(() => {}, 60_000)"]);
  const pid = child.pid as number;
  const { promise, resolve } = deferred<void>();
  child.once("spawn", resolve);
  await promise;
  return { pid, stop: () => child.kill("SIGKILL") };
}

async function plantSessionLock(commonDir: string, sessionId: string, pid: number): Promise<string> {
  const lockPath = lockPathFor(commonDir, sessionLockName(sessionId));
  await mkdir(lockPath, { recursive: true });
  const record: LockOwner = {
    token: "planted",
    pid,
    hostname: hostname(),
    started_at: "2026-01-01T00:00:00.000Z",
  };
  await writeFile(join(lockPath, "owner.json"), JSON.stringify(record));
  return lockPath;
}

describe("hub sessions", { timeout: 30_000 }, () => {
  it("creates a session whose artifact, state, and privacy guarantees hold end to end", async () => {
    const repo = await createGitRepository();
    const identity = await resolveRepositoryIdentity(repo);
    const adapter: AgentAdapter = {
      id: "writer",
      async execute(request) {
        await writeFile(join(request.cwd, "a.txt"), "alpha\n");
        return okResult({ stdout: "STDOUT-SECRET-1c9a" });
      },
    };

    const result = await createSession(
      { workspace: repo, agent: "writer", task: "TASK-SECRET-88f2 write a file" },
      { resolveAdapter: () => adapter },
    );

    // v1 wire shape for the run, isolated mode, artifact facts.
    expect(result.run.status).toBe("success");
    expect(result.run.mode).toBe("isolated");
    expect(result.run.execution_workspace).toBe(result.execution_workspace);
    expect(result.run.changed_files).toEqual(["a.txt"]);
    expect(result.artifact.parent).toBe(identity.head);
    expect(result.run.diff).toContain("+alpha");

    // Private ref derived from the generated id only.
    expect(result.session.session_id).toMatch(/^[0-9a-f-]{36}$/);
    expect(result.session.ref).toBe(`refs/agent-hub/sessions/${result.session.session_id}`);
    expect(await refSha(repo, result.session.ref)).toBe(result.artifact.commit);
    expect(await refSha(repo, `${result.artifact.commit}^`)).toBe(identity.head);

    // State is minimal and content-free; the fresh worktree is gone; nothing
    // dirties the caller checkout.
    const stateText = await readFile(
      sessionStatePath(identity.common_dir, result.session.session_id),
      "utf8",
    );
    expect(stateText).not.toContain("TASK-SECRET-88f2");
    expect(stateText).not.toContain("STDOUT-SECRET-1c9a");
    const record = JSON.parse(stateText) as Record<string, unknown>;
    expect(Object.keys(record).sort()).toEqual([
      "agent", "base_commit", "continuation_mode", "created_at", "current_commit",
      "identity", "provider_session_id", "ref", "revision", "schema", "session_id", "updated_at",
    ]);
    expect(await missing(result.execution_workspace)).toBe(true);
    expect(await statusPorcelain(repo)).toBe("");

    // Filesystem continuation guaranteed; native honestly unavailable.
    expect(result.continuation).toEqual({ filesystem: true, native: false, native_status: "no-provider-session" });
    expect(result.session.continuation_mode).toBe("filesystem");
    expect(result.session.revision).toBe(1);
    expect(result.cleanup_error).toBeNull();

    await removeDirectory(repo);
  });


  it("retains the session worktree and reports trouble when its admin lock release fails", async () => {
    const repo = await createGitRepository();
    const identity = await resolveRepositoryIdentity(repo);
    const adapter: AgentAdapter = {
      id: "writer",
      async execute(request) {
        await writeFile(join(request.cwd, "a.txt"), "alpha\n");
        return okResult();
      },
    };
    let attempts = 0;
    let leaked: string | null = null;
    let wedgedRecord: string | null = null;

    // The claim that created the worktree cannot release its lock; a later
    // claim is healthy — it just cannot get in while the record is stuck.
    const firstReleaseFails = async (commonDir: string) => {
      attempts += 1;
      const lock = await acquireRepositoryLock({
        commonDir,
        name: WORKTREE_ADMIN_LOCK_NAME,
      });
      if (attempts > 1) {
        return lock;
      }
      return {
        ...lock,
        release: async () => {
          throw new Error("simulated admin lock release failure");
        },
      };
    };

    try {
      const result = await createSession(
        { workspace: repo, agent: "writer", task: "write a file" },
        { resolveAdapter: () => adapter, acquireAdminLock: firstReleaseFails },
      );

      // The turn is intact and durable: the handle survived the failed
      // release, so the artifact was captured from the worktree and the
      // session advanced exactly one commit.
      expect(result.run.status).toBe("success");
      expect(result.run.changed_files).toEqual(["a.txt"]);
      expect(await refSha(repo, result.session.ref)).toBe(result.artifact.commit);
      expect(result.session.revision).toBe(1);

      wedgedRecord = lockPathFor(identity.common_dir, WORKTREE_ADMIN_LOCK_NAME);
      leaked = result.execution_workspace;

      // Teardown was attempted and reported. The hub will not touch worktree
      // administration while it cannot hold the admin lock, so the worktree is
      // still there — named in the cleanup error, alongside the blocked attempt.
      expect(attempts).toBe(2);
      await expect(access(result.execution_workspace)).resolves.toBeUndefined();
      expect(result.cleanup_error?.code).toBe("LOCK_RELEASE_FAILED");
      expect(result.cleanup_error?.message).toContain("simulated admin lock release failure");
      expect(result.cleanup_error?.message).toContain(result.execution_workspace);
      expect(result.cleanup_error?.message).toContain("Teardown also failed");
    } finally {
      await rm(wedgedRecord ?? lockPathFor(join(repo, ".git"), WORKTREE_ADMIN_LOCK_NAME), {
        recursive: true,
        force: true,
      });
      if (leaked) {
        await removeDirectory(join(leaked, ".."));
      }
      await removeDirectory(repo);
    }
  });

  it("returns the committed create with cleanup_error when only the session lock release fails", async () => {
    const repo = await createGitRepository();
    const adapter: AgentAdapter = {
      id: "writer",
      async execute(request) {
        await writeFile(join(request.cwd, "a.txt"), "alpha\n");
        return okResult();
      },
    };
    // Only the per-session lock's release is broken; worktree administration
    // and the durable transition itself run untouched.
    const brokenSessionRelease: typeof acquireRepositoryLock = async (options) => {
      const lock = await acquireRepositoryLock(options);
      return {
        ...lock,
        release: async () => {
          throw new Error("simulated session lock release failure");
        },
      };
    };

    let sessionId: string | null = null;
    try {
      const result = await createSession(
        { workspace: repo, agent: "writer", task: "write a file" },
        { resolveAdapter: () => adapter, acquireSessionLock: brokenSessionRelease },
      );
      sessionId = result.session.session_id;

      // The transition was durably committed and the completed result — id
      // and revision included — is returned instead of being thrown away.
      expect(result.run.status).toBe("success");
      expect(result.session.revision).toBe(1);
      expect(result.session.session_id).toMatch(/^[0-9a-f-]{36}$/);
      expect(await refSha(repo, result.session.ref)).toBe(result.artifact.commit);

      // The release trouble is reported, not thrown; ordinary teardown still ran.
      expect(result.cleanup_error?.code).toBe("LOCK_RELEASE_FAILED");
      expect(result.cleanup_error?.message).toContain("simulated session lock release failure");
      expect(result.cleanup_error?.message).toContain("session lock record");
      expect(await missing(result.execution_workspace)).toBe(true);
    } finally {
      if (sessionId) {
        await rm(lockPathFor(join(repo, ".git"), sessionLockName(sessionId)), {
          recursive: true,
          force: true,
        });
      }
      await removeDirectory(repo);
    }
  });

  it("returns the committed resume with cleanup_error when only the session lock release fails", async () => {
    const repo = await createGitRepository();
    const adapter: AgentAdapter = {
      id: "writer",
      async execute(request) {
        await writeFile(join(request.cwd, "a.txt"), "beta\n");
        return okResult();
      },
    };
    const created = await createSession(
      { workspace: repo, agent: "writer", task: "first turn" },
      { resolveAdapter: () => adapter },
    );

    const brokenSessionRelease: typeof acquireRepositoryLock = async (options) => {
      const lock = await acquireRepositoryLock(options);
      return {
        ...lock,
        release: async () => {
          throw new Error("simulated resume lock release failure");
        },
      };
    };

    try {
      const resumed = await resumeSession(
        { workspace: repo, session_id: created.session.session_id, task: "second turn" },
        { resolveAdapter: () => adapter, acquireSessionLock: brokenSessionRelease },
      );

      // The advance landed (ref + revision 2); the wedged release only adds
      // evidence to the returned result.
      expect(resumed.run.status).toBe("success");
      expect(resumed.session.session_id).toBe(created.session.session_id);
      expect(resumed.session.revision).toBe(2);
      expect(await refSha(repo, resumed.session.ref)).toBe(resumed.artifact.commit);
      expect(resumed.cleanup_error?.code).toBe("LOCK_RELEASE_FAILED");
      expect(resumed.cleanup_error?.message).toContain("simulated resume lock release failure");
      expect(await missing(resumed.execution_workspace)).toBe(true);
    } finally {
      await rm(lockPathFor(join(repo, ".git"), sessionLockName(created.session.session_id)), {
        recursive: true,
        force: true,
      });
      await removeDirectory(repo);
    }
  });

  it("records an artifact commit even when the run changes nothing", async () => {
    const repo = await createGitRepository();
    const adapter: AgentAdapter = { id: "noop", async execute() { return okResult(); } };

    const result = await createSession(
      { workspace: repo, agent: "noop", task: "look, change nothing" },
      { resolveAdapter: () => adapter },
    );

    expect(result.artifact.commit).not.toBeNull();
    expect(result.run.changed_files).toEqual([]);
    expect(await refSha(repo, result.session.ref)).toBe(result.artifact.commit);
    // Empty artifact: same tree as the base head.
    expect(result.artifact.tree).toBe(
      (await runGit(repo, ["rev-parse", `${result.artifact.parent}^{tree}`])).trim(),
    );

    await removeDirectory(repo);
  });

  it("resumes from a fresh worktree at the artifact commit with prior edits visible, extending the lineage", async () => {
    const repo = await createGitRepository();
    // One adapter, because a session stays pinned to its recorded agent:
    // resume must resolve the same "first" adapter, never a new one.
    let observedInResume: string | null = null;
    const first: AgentAdapter = {
      id: "first",
      async execute(request) {
        if (request.task === "write foo") {
          await writeFile(join(request.cwd, "foo.txt"), "one");
        } else {
          observedInResume = await readFile(join(request.cwd, "foo.txt"), "utf8");
          await writeFile(join(request.cwd, "bar.txt"), `${observedInResume};two`);
        }
        return okResult();
      },
    };
    const deps = { resolveAdapter: (agent: string) => first };

    const created = await createSession({ workspace: repo, agent: "first", task: "write foo" }, deps);
    const resumed = await resumeSession(
      { workspace: repo, session_id: created.session.session_id, task: "read foo, write bar" },
      deps,
    );

    // The resume worktree saw the previous run's committed filesystem edits.
    expect(observedInResume).toBe("one");
    expect(await blobContent(repo, `${resumed.artifact.commit}:bar.txt`)).toBe("one;two");

    // Linear lineage: resume artifact's parent is the create artifact.
    expect(await refSha(repo, `${resumed.artifact.commit}^`)).toBe(created.artifact.commit);
    expect(await refSha(repo, created.session.ref)).toBe(resumed.artifact.commit);

    // Fresh path, never the old one; both torn down after the run.
    expect(resumed.execution_workspace).not.toBe(created.execution_workspace);
    expect(await missing(created.execution_workspace)).toBe(true);
    expect(await missing(resumed.execution_workspace)).toBe(true);

    expect(resumed.session.revision).toBe(2);
    expect(resumed.session.base_commit).toBe(created.session.base_commit);
    expect(resumed.session.created_at).toBe(created.session.created_at);
    expect(resumed.run.execution_workspace).toBe(resumed.execution_workspace);
    expect(await statusPorcelain(repo)).toBe("");

    await removeDirectory(repo);
  });

  it("fails resumes with SESSION_BUSY for live owners and proceeds past dead ones", async () => {
    const repo = await createGitRepository();
    const identity = await resolveRepositoryIdentity(repo);
    const echo: AgentAdapter = { id: "echo", async execute() { return okResult({ stdout: "ok" }); } };
    const deps = { resolveAdapter: () => echo };

    const created = await createSession({ workspace: repo, agent: "echo", task: "one" }, deps);
    const sessionId = created.session.session_id;

    const live = await livePid();
    try {
      const lockPath = await plantSessionLock(identity.common_dir, sessionId, live.pid);
      await expectErrorCode(
        resumeSession({ workspace: repo, session_id: sessionId, task: "two" }, deps),
        "SESSION_BUSY",
      );
      expect(await readFile(join(lockPath, "owner.json"), "utf8")).toContain(String(live.pid));
    } finally {
      live.stop();
    }

    const dead = await deadPid();
    await plantSessionLock(identity.common_dir, sessionId, dead);
    const resumed = await resumeSession({ workspace: repo, session_id: sessionId, task: "two" }, deps);
    expect(resumed.session.revision).toBe(2);

    await removeDirectory(repo);
  });

  it("surfaces explicit inconsistency instead of guessing when state and ref diverge", async () => {
    const repo = await createGitRepository();
    const identity = await resolveRepositoryIdentity(repo);
    const echo: AgentAdapter = { id: "echo", async execute() { return okResult(); } };
    const deps = { resolveAdapter: () => echo };

    const created = await createSession({ workspace: repo, agent: "echo", task: "one" }, deps);
    const statePath = sessionStatePath(identity.common_dir, created.session.session_id);
    const before = await readFile(statePath, "utf8");

    // Move the session ref out from under the state record.
    const head = (await runGit(repo, ["rev-parse", "HEAD"])).trim();
    const tree = (await runGit(repo, ["rev-parse", `${head}^{tree}`])).trim();
    const side = (await runGit(repo, ["commit-tree", tree, "-p", head, "-m", "unrelated"])).trim();
    await runGit(repo, ["update-ref", created.session.ref, side]);

    await expectErrorCode(
      resumeSession({ workspace: repo, session_id: created.session.session_id, task: "two" }, deps),
      "SESSION_STATE_INCONSISTENT",
    );
    expect(await readFile(statePath, "utf8")).toBe(before);
    expect(await missing(sessionPendingPath(identity.common_dir, created.session.session_id))).toBe(true);

    await removeDirectory(repo);
  });

  it("propagates provider session metadata through a verified native-capable adapter", async () => {
    const repo = await createGitRepository();
    const identity = await resolveRepositoryIdentity(repo);
    const seen: Array<AdapterMetadata | null | undefined> = [];
    const adapter: NativeResumeCapableAdapter = {
      id: "native-fake",
      nativeResumeCapability: {
        verify: async () => true,
        resumeArguments: (providerSessionId) => ["--resume", providerSessionId],
      },
      async execute(request) {
        seen.push(request.metadata);
        if (request.metadata?.provider_session_id === "provider-123") {
          return okResult({ session_id: "provider-456" });
        }
        return okResult({ session_id: "provider-123" });
      },
    };
    const deps = { resolveAdapter: () => adapter };

    const created = await createSession({ workspace: repo, agent: "native-fake", task: "one" }, deps);
    // First run: nothing to resume, provider id recorded from the result.
    expect(seen[0]).toBeFalsy();
    expect(created.continuation.native).toBe(false);
    expect(created.continuation.native_status).toBe("no-provider-session");
    expect(created.session.provider_session_id).toBe("provider-123");
    expect(created.session.continuation_mode).toBe("filesystem");

    const resumed = await resumeSession(
      { workspace: repo, session_id: created.session.session_id, task: "two" },
      deps,
    );
    // Resume: the stored provider id travels via the optional metadata path.
    expect(seen[1]).toEqual({ provider_session_id: "provider-123" });
    expect(resumed.continuation).toEqual({ filesystem: true, native: true, native_status: "used" });
    expect(resumed.session.provider_session_id).toBe("provider-456");
    expect(resumed.session.continuation_mode).toBe("native");

    // The state file persists only the provider id itself.
    const stateText = await readFile(
      sessionStatePath(identity.common_dir, created.session.session_id),
      "utf8",
    );
    expect(stateText).toContain("provider-456");

    await removeDirectory(repo);
  });

  it("falls back to filesystem-only continuation for uncapable and unverified adapters", async () => {
    const repo = await createGitRepository();
    const seen: Array<AdapterMetadata | null | undefined> = [];
    const plain: AgentAdapter = {
      id: "plain",
      async execute(request) {
        seen.push(request.metadata);
        return okResult({ session_id: "p-1" });
      },
    };
    const created = await createSession(
      { workspace: repo, agent: "plain", task: "one" },
      { resolveAdapter: () => plain },
    );
    const resumed = await resumeSession(
      { workspace: repo, session_id: created.session.session_id, task: "two" },
      { resolveAdapter: () => plain },
    );
    // A stored provider id exists, but an adapter without capability never
    // sees metadata and never claims native continuation.
    expect(created.session.provider_session_id).toBe("p-1");
    expect(seen[1]).toBeFalsy();
    expect(resumed.continuation).toEqual({ filesystem: true, native: false, native_status: "adapter-incapable" });
    expect(resumed.session.continuation_mode).toBe("filesystem");
    expect(resumed.session.provider_session_id).toBe("p-1");

    const unverifiedSeen: Array<AdapterMetadata | null | undefined> = [];
    const unverified: NativeResumeCapableAdapter = {
      id: "unverified",
      nativeResumeCapability: {
        verify: async () => false, // Installed syntax could not be proven.
        resumeArguments: () => ["--resume", "x"],
      },
      async execute(request) {
        unverifiedSeen.push(request.metadata);
        return request.metadata?.provider_session_id ? okResult() : okResult({ session_id: "u-1" });
      },
    };
    const created2 = await createSession(
      { workspace: repo, agent: "unverified", task: "one" },
      { resolveAdapter: () => unverified },
    );
    const resumed2 = await resumeSession(
      { workspace: repo, session_id: created2.session.session_id, task: "two" },
      { resolveAdapter: () => unverified },
    );
    expect(resumed2.continuation).toEqual({ filesystem: true, native: false, native_status: "not-verified" });
    expect(unverifiedSeen[1]).toBeFalsy();

    await removeDirectory(repo);
  });

  it("keeps built-in command adapters filesystem-only and v1-compatible", async () => {
    const repo = await createGitRepository();
    const environment: NodeJS.ProcessEnv = {
      ...process.env,
      AGENT_HUB_OMP_BIN: process.execPath,
      AGENT_HUB_OMP_ARGS: JSON.stringify(["-e", "process.stdout.write('omp done')", "{task}"]),
    };
    const deps = { resolveAdapter: (agent: string) => resolveAdapter(agent, environment) };

    const created = await createSession({ workspace: repo, agent: "omp", task: "say hi" }, deps);
    expect(created.run.status).toBe("success");
    expect(created.run.stdout).toBe("omp done");
    expect(created.session.provider_session_id).toBeNull();
    expect(created.continuation.native).toBe(false);

    const resumed = await resumeSession(
      { workspace: repo, session_id: created.session.session_id, task: "say hi again" },
      deps,
    );
    // v1 command adapters report no provider session: native stays false.
    expect(resumed.run.stdout).toBe("omp done");
    expect(resumed.continuation).toEqual({ filesystem: true, native: false, native_status: "no-provider-session" });
    expect(resumed.session.continuation_mode).toBe("filesystem");
    expect(await statusPorcelain(repo)).toBe("");

    await removeDirectory(repo);
  });

  it("commits partial edits even when the adapter itself fails", async () => {
    const repo = await createGitRepository();
    const base: AgentAdapter = {
      id: "base",
      async execute(request) {
        await writeFile(join(request.cwd, "keep.txt"), "keep\n");
        return okResult();
      },
    };
    const failing: AgentAdapter = {
      id: "failing",
      async execute(request) {
        await writeFile(join(request.cwd, "partial.txt"), "partial work\n");
        throw new Error("boom");
      },
    };

    const created = await createSession(
      { workspace: repo, agent: "base", task: "one" },
      { resolveAdapter: () => base },
    );
    const resumed = await resumeSession(
      { workspace: repo, session_id: created.session.session_id, task: "two" },
      { resolveAdapter: () => failing },
    );

    const run: DelegateResult = resumed.run;
    expect(run.status).toBe("failure");
    expect(run.error).toEqual({ code: "INTERNAL_ERROR", message: "boom" });
    // Filesystem continuation survives the failed run: the partial edits are
    // committed into the lineage before the error is reported.
    expect(resumed.artifact.changed_files).toContain("partial.txt");
    expect(await blobContent(repo, `${resumed.artifact.commit}:partial.txt`)).toBe("partial work");
    expect(resumed.session.revision).toBe(2);

    await removeDirectory(repo);
  });

  it("guards the caller checkout: dirty creates need allowDirty, and nothing is ever written there", async () => {
    const repo = await createGitRepository();
    await writeFile(join(repo, "caller-dirty.txt"), "caller work\n");
    const adapter: AgentAdapter = {
      id: "writer",
      async execute(request) {
        await writeFile(join(request.cwd, "agent.txt"), "agent\n");
        return okResult();
      },
    };
    const deps = { resolveAdapter: () => adapter };

    await expectErrorCode(
      createSession({ workspace: repo, agent: "writer", task: "one" }, deps),
      "DIRTY_WORKTREE",
    );

    await createSession({ workspace: repo, agent: "writer", task: "one", allowDirty: true }, deps);
    expect(await readFile(join(repo, "caller-dirty.txt"), "utf8")).toBe("caller work\n");
    await expect(readFile(join(repo, "agent.txt"), "utf8")).rejects.toMatchObject({ code: "ENOENT" });
    expect(await statusPorcelain(repo)).toBe("?? caller-dirty.txt\n");

    // A generated-id slot filled with raw text is refused before any path/ref.
    await expectErrorCode(
      createSession(
        { workspace: repo, agent: "writer", task: "one", allowDirty: true },
        { ...deps, newSessionId: () => "task-labeled/../evil" },
      ),
      "SESSION_ID_INVALID",
    );

    await removeDirectory(repo);
  });

  it("refuses raw-text session ids on resume before touching state or refs", async () => {
    const repo = await createGitRepository();
    await expectErrorCode(
      resumeSession({ workspace: repo, session_id: "../../escape", task: "two" }),
      "SESSION_ID_INVALID",
    );
    await removeDirectory(repo);
  });
});
