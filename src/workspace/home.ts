import { randomUUID } from "node:crypto";
import { mkdir, readFile, rename, rm, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { isAbsolute, join, resolve, sep } from "node:path";

import { AgentHubError } from "../errors.js";

/**
 * Durable hub home (P2 WorkspaceLifecycle).
 *
 * `AGENT_HUB_HOME` designates the persistent directory that owns all P2
 * durable metadata: workspace custody records, result records, transaction
 * sidecars, provider process leases, and the per-agent worktrees themselves.
 * It plugs into the same directory-lock layout the repository-local state
 * uses (`<home>/agent-hub/...`), so `src/locks.ts` works unchanged against
 * it — the hub home plays the role a git common dir plays for v3 live state.
 *
 * Resolution order: explicit option > `AGENT_HUB_HOME` env >
 * `~/.local/share/agent-hub`.
 * Nothing under the hub home is deleted by anything but an eligible GC pass.
 */

/** Namespace directory inside the hub home, mirroring `<common-dir>/agent-hub`. */
export const HUB_STATE_SUBDIR = "agent-hub";
export const WORKSPACES_SUBDIR = join(HUB_STATE_SUBDIR, "workspaces");
export const WORKTREES_SUBDIR = join(HUB_STATE_SUBDIR, "worktrees");
export const PROCESSES_SUBDIR = join(HUB_STATE_SUBDIR, "processes");
export const DELETED_SUBDIR = join(HUB_STATE_SUBDIR, "deleted");
export const DEFAULT_HUB_HOME = join(homedir(), ".local", "share", "agent-hub");

const SESSION_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

/** Session ids become path segments; only hub-generated UUIDs qualify. */
export function isWorkspaceSessionId(value: unknown): value is string {
  return typeof value === "string" && SESSION_ID_PATTERN.test(value);
}

export function assertWorkspaceSessionId(sessionId: string): void {
  if (!isWorkspaceSessionId(sessionId)) {
    throw new AgentHubError(
      "WORKSPACE_ID_INVALID",
      `workspace session id "${sessionId}" must be a hub-generated UUID`,
    );
  }
}

/** Explicit argument wins over the environment; the environment wins over the default home. */
export function resolveHubHome(explicit?: string, env: NodeJS.ProcessEnv = process.env): string {
  const raw = explicit ?? env.AGENT_HUB_HOME ?? DEFAULT_HUB_HOME;
  const home = resolve(raw);
  if (!isAbsolute(home)) {
    throw new AgentHubError("HUB_HOME_INVALID", `hub home "${raw}" must resolve to an absolute path`);
  }
  return home;
}

export function workspaceDir(home: string, sessionId: string): string {
  assertWorkspaceSessionId(sessionId);
  return join(home, WORKSPACES_SUBDIR, sessionId);
}

export function workspaceRecordPath(home: string, sessionId: string): string {
  return join(workspaceDir(home, sessionId), "record.json");
}

export function workspacePendingPath(home: string, sessionId: string): string {
  return join(workspaceDir(home, sessionId), "record.pending.json");
}

export function runtimeMirrorPath(home: string, sessionId: string): string {
  return join(workspaceDir(home, sessionId), "runtime.json");
}

export function resultPath(home: string, sessionId: string, seq: number): string {
  assertWorkspaceSessionId(sessionId);
  return join(home, WORKSPACES_SUBDIR, sessionId, "results", `${seq}.json`);
}

export function worktreePath(home: string, sessionId: string): string {
  assertWorkspaceSessionId(sessionId);
  return join(home, WORKTREES_SUBDIR, sessionId);
}

export function leasePath(home: string, sessionId: string): string {
  assertWorkspaceSessionId(sessionId);
  return join(home, PROCESSES_SUBDIR, `${sessionId}.json`);
}

export function tombstonePath(home: string, sessionId: string): string {
  assertWorkspaceSessionId(sessionId);
  return join(home, DELETED_SUBDIR, `${sessionId}.json`);
}

/** True when `child` sits at or below `parent`, without traversing out via `..`. */
export function isInside(parent: string, child: string): boolean {
  const base = resolve(parent);
  const target = resolve(child);
  if (target === base) {
    return true;
  }
  if (!target.startsWith(`${base}${sep}`)) {
    return false;
  }
  const rel = target.slice(base.length + 1);
  return rel !== ".." && !rel.startsWith(`..${sep}`);
}

/** The custody worktree root the hub may own; nothing outside it is removable. */
export function custodialWorktreeRoot(home: string): string {
  return join(home, WORKTREES_SUBDIR);
}

// ---------------------------------------------------------------------------
// Atomic JSON primitives. Absent is `undefined`; present-but-unreadable is
// `null`; nothing is ever guessed from a partial file.
// ---------------------------------------------------------------------------

export async function readJsonFile(path: string): Promise<unknown> {
  try {
    return JSON.parse(await readFile(path, "utf8"));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return undefined;
    }
    return null;
  }
}

export async function writeJsonAtomic(path: string, value: unknown): Promise<void> {
  await mkdir(join(path, ".."), { recursive: true });
  const tempPath = `${path}.${process.pid}.${randomUUID()}.tmp`;
  await writeFile(tempPath, `${JSON.stringify(value, null, 2)}\n`, "utf8");
  await rename(tempPath, path);
}

export async function removeFile(path: string): Promise<void> {
  await rm(path, { force: true });
}

export async function removeTree(path: string): Promise<void> {
  await rm(path, { recursive: true, force: true });
}
