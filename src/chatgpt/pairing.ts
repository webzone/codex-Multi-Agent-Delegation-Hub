import { randomUUID } from "node:crypto";
import { chmod, mkdir, readFile, readdir, realpath, rename, rm, writeFile } from "node:fs/promises";
import { homedir } from "node:os";
import { isAbsolute, join, resolve } from "node:path";

import { AgentHubError } from "../errors.js";
import { resolveRepositoryIdentity } from "../git.js";
import type { PermissionPolicy, ProviderId } from "../kernel/contracts.js";

export const CHATGPT_PAIR_SCHEMA = "agent-hub-chatgpt-pair/v1" as const;
export const CHATGPT_PAIR_PROVIDERS = ["omp", "agy", "pi", "hermes"] as const;
export type ChatGptPairProvider = (typeof CHATGPT_PAIR_PROVIDERS)[number];

export interface ChatGptPair {
  schema: typeof CHATGPT_PAIR_SCHEMA;
  pair_id: string;
  name: string;
  repository: {
    common_dir: string;
    worktree_root: string;
  };
  providers: readonly ChatGptPairProvider[];
  permission_policy: PermissionPolicy;
  created_at: string;
  updated_at: string;
}

export interface ChatGptPairStatus {
  pair_id: string;
  name: string;
  repository: ChatGptPair["repository"];
  status: "ready" | "changed" | "missing";
  detail: string | null;
}

const PAIR_ID_PATTERN = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;

export function resolveChatGptStateHome(
  explicit?: string,
  env: NodeJS.ProcessEnv = process.env,
): string {
  const base = explicit ?? env.AGENT_HUB_CHATGPT_STATE_HOME ??
    join(env.XDG_STATE_HOME ?? join(homedir(), ".local", "state"), "agent-hub", "chatgpt");
  const home = resolve(base);
  if (!isAbsolute(home)) {
    throw new AgentHubError("CHATGPT_STATE_INVALID", `ChatGPT pairing state must be an absolute path: ${base}`);
  }
  return home;
}

function assertPairId(pairId: string): void {
  if (!PAIR_ID_PATTERN.test(pairId)) {
    throw new AgentHubError("CHATGPT_PAIR_INVALID", `invalid Agent Hub pairing id "${pairId}"`);
  }
}

function pairPath(stateHome: string, pairId: string): string {
  assertPairId(pairId);
  return join(resolveChatGptStateHome(stateHome), "pairs", `${pairId}.json`);
}

async function writePairFile(path: string, pair: ChatGptPair): Promise<void> {
  const directory = join(path, "..");
  await mkdir(directory, { recursive: true, mode: 0o700 });
  await chmod(directory, 0o700);
  const temporary = `${path}.${process.pid}.${randomUUID()}.tmp`;
  try {
    await writeFile(temporary, `${JSON.stringify(pair, null, 2)}\n`, { encoding: "utf8", mode: 0o600 });
    await chmod(temporary, 0o600);
    await rename(temporary, path);
    await chmod(path, 0o600);
  } finally {
    await rm(temporary, { force: true });
  }
}

function parsePair(value: unknown, source: string): ChatGptPair {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new AgentHubError("CHATGPT_PAIR_INVALID", `pairing file ${source} is not an object`);
  }
  const record = value as Record<string, unknown>;
  const repository = record.repository;
  const providers = record.providers;
  if (
    record.schema !== CHATGPT_PAIR_SCHEMA ||
    typeof record.pair_id !== "string" ||
    !PAIR_ID_PATTERN.test(record.pair_id) ||
    typeof record.name !== "string" ||
    record.name.trim().length === 0 ||
    typeof repository !== "object" ||
    repository === null ||
    Array.isArray(repository) ||
    typeof (repository as Record<string, unknown>).common_dir !== "string" ||
    typeof (repository as Record<string, unknown>).worktree_root !== "string" ||
    !Array.isArray(providers) ||
    providers.length === 0 ||
    providers.some((provider) => !(CHATGPT_PAIR_PROVIDERS as readonly string[]).includes(String(provider))) ||
    typeof record.permission_policy !== "string" ||
    (record.permission_policy !== "deny" && record.permission_policy !== "interactive") ||
    typeof record.created_at !== "string" ||
    typeof record.updated_at !== "string"
  ) {
    throw new AgentHubError("CHATGPT_PAIR_INVALID", `pairing file ${source} does not match ${CHATGPT_PAIR_SCHEMA}`);
  }
  const repositoryRecord = repository as { common_dir: string; worktree_root: string };
  if (!isAbsolute(repositoryRecord.common_dir) || !isAbsolute(repositoryRecord.worktree_root)) {
    throw new AgentHubError("CHATGPT_PAIR_INVALID", `pairing file ${source} contains a non-absolute repository path`);
  }
  const uniqueProviders = [...new Set(providers as string[])];
  if (uniqueProviders.length !== providers.length) {
    throw new AgentHubError("CHATGPT_PAIR_INVALID", `pairing file ${source} repeats a provider`);
  }
  return {
    schema: CHATGPT_PAIR_SCHEMA,
    pair_id: record.pair_id,
    name: record.name.trim(),
    repository: {
      common_dir: repositoryRecord.common_dir,
      worktree_root: repositoryRecord.worktree_root,
    },
    providers: uniqueProviders as ChatGptPairProvider[],
    permission_policy: record.permission_policy as PermissionPolicy,
    created_at: record.created_at,
    updated_at: record.updated_at,
  };
}

export async function readChatGptPair(
  pairId: string,
  stateHome?: string,
): Promise<ChatGptPair> {
  const path = pairPath(resolveChatGptStateHome(stateHome), pairId);
  let raw: string;
  try {
    raw = await readFile(path, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new AgentHubError("CHATGPT_PAIR_NOT_FOUND", `Agent Hub pairing "${pairId}" does not exist`);
    }
    throw new AgentHubError("CHATGPT_PAIR_UNREADABLE", `cannot read Agent Hub pairing "${pairId}"`);
  }
  try {
    return parsePair(JSON.parse(raw), path);
  } catch (error) {
    if (error instanceof AgentHubError) throw error;
    throw new AgentHubError("CHATGPT_PAIR_INVALID", `cannot parse Agent Hub pairing "${pairId}"`);
  }
}

export async function pairRepository(options: {
  workspace: string;
  name: string;
  stateHome?: string;
  providers?: readonly ChatGptPairProvider[];
  permissionPolicy?: PermissionPolicy;
}): Promise<ChatGptPair> {
  const name = options.name.trim();
  if (name.length === 0 || name.length > 120) {
    throw new AgentHubError("CHATGPT_PAIR_INVALID", "pairing name must contain 1 to 120 characters");
  }
  const identity = await resolveRepositoryIdentity(options.workspace);
  const now = new Date().toISOString();
  const pair: ChatGptPair = {
    schema: CHATGPT_PAIR_SCHEMA,
    pair_id: randomUUID(),
    name,
    repository: {
      common_dir: identity.common_dir,
      worktree_root: identity.worktree_root,
    },
    providers: [...(options.providers ?? CHATGPT_PAIR_PROVIDERS)],
    permission_policy: options.permissionPolicy ?? "deny",
    created_at: now,
    updated_at: now,
  };
  const stateHome = resolveChatGptStateHome(options.stateHome);
  await writePairFile(pairPath(stateHome, pair.pair_id), pair);
  return pair;
}

export async function unpairRepository(pairId: string, stateHome?: string): Promise<void> {
  const path = pairPath(resolveChatGptStateHome(stateHome), pairId);
  try {
    await rm(path);
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new AgentHubError("CHATGPT_PAIR_NOT_FOUND", `Agent Hub pairing "${pairId}" does not exist`);
    }
    throw new AgentHubError("CHATGPT_PAIR_UNPAIR_FAILED", `cannot remove Agent Hub pairing "${pairId}"`);
  }
}

export async function assertChatGptPairRepository(pair: ChatGptPair): Promise<void> {
  let identity;
  try {
    identity = await resolveRepositoryIdentity(pair.repository.worktree_root);
  } catch {
    throw new AgentHubError(
      "CHATGPT_PAIR_REPOSITORY_MISSING",
      `paired repository ${pair.repository.worktree_root} is missing or is no longer a Git worktree`,
    );
  }
  if (
    resolve(identity.common_dir) !== resolve(pair.repository.common_dir) ||
    resolve(identity.worktree_root) !== resolve(pair.repository.worktree_root)
  ) {
    throw new AgentHubError(
      "CHATGPT_PAIR_REPOSITORY_CHANGED",
      "the paired repository identity changed; create a new pairing before using this Project connection",
    );
  }
}

export async function resolvePairedWorkspace(
  pair: ChatGptPair,
  requestedWorkspace: string,
): Promise<string> {
  await assertChatGptPairRepository(pair);
  const canonical = await realpath(pair.repository.worktree_root);
  let requested = resolve(requestedWorkspace);
  try {
    requested = await realpath(requestedWorkspace);
  } catch {
    // The repository check below returns the structured missing/changed error.
  }
  if (requested !== canonical) {
    throw new AgentHubError(
      "CHATGPT_PAIR_WORKSPACE_FORBIDDEN",
      "a ChatGPT Project pairing can access only its paired repository",
    );
  }
  return canonical;
}

export async function inspectChatGptPair(pair: ChatGptPair): Promise<ChatGptPairStatus> {
  try {
    await assertChatGptPairRepository(pair);
    return { pair_id: pair.pair_id, name: pair.name, repository: pair.repository, status: "ready", detail: null };
  } catch (error) {
    const failure = error instanceof AgentHubError ? error : new AgentHubError("CHATGPT_PAIR_INVALID", String(error));
    const status = failure.code === "CHATGPT_PAIR_REPOSITORY_MISSING" ? "missing" : "changed";
    return { pair_id: pair.pair_id, name: pair.name, repository: pair.repository, status, detail: failure.message };
  }
}

export async function chatGptDoctor(stateHome?: string): Promise<{
  state_home: string;
  pairs: Array<ChatGptPairStatus | { pair_id: string; status: "invalid"; detail: string }>;
}> {
  const home = resolveChatGptStateHome(stateHome);
  const directory = join(home, "pairs");
  let names: string[] = [];
  try {
    names = (await readdir(directory)).filter((name) => name.endsWith(".json"));
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  const pairs: Array<ChatGptPairStatus | { pair_id: string; status: "invalid"; detail: string }> = [];
  for (const name of names) {
    const pairId = name.slice(0, -5);
    try {
      pairs.push(await inspectChatGptPair(await readChatGptPair(pairId, home)));
    } catch (error) {
      pairs.push({
        pair_id: pairId,
        status: "invalid",
        detail: error instanceof Error ? error.message : String(error),
      });
    }
  }
  return { state_home: home, pairs };
}

export function isChatGptPairProvider(value: string): value is ProviderId {
  return (CHATGPT_PAIR_PROVIDERS as readonly string[]).includes(value);
}
