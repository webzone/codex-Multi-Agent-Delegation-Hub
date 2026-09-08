import { createHash, randomUUID } from "node:crypto";
import { execFile } from "node:child_process";
import {
  access,
  chmod,
  lstat,
  mkdir,
  readFile,
  rename,
  rm,
  writeFile,
} from "node:fs/promises";
import { constants as fsConstants } from "node:fs";
import { fileURLToPath } from "node:url";
import { homedir } from "node:os";
import { delimiter, dirname, isAbsolute, join, resolve } from "node:path";
import { promisify } from "node:util";

import { AgentHubError } from "../errors.js";
import { PACKAGE_NAME, PACKAGE_VERSION } from "../version.js";

const execFileAsync = promisify(execFile);

export const CODEX_MCP_NAME = "agent_hub";
export const CODEX_MCP_COMMAND = "agent-hub-mcp";
export const CODEX_SKILL_NAME = "agent-hub";
const MANIFEST_SCHEMA = 2;

export interface CodexCommandResult {
  code: number | string;
  stdout: string;
  stderr: string;
}

export interface CodexCommandOptions {
  env?: NodeJS.ProcessEnv;
}

export type CodexCommandRunner = (
  args: string[],
  options?: CodexCommandOptions,
) => Promise<CodexCommandResult>;

export interface CodexIntegrationOptions {
  codexHome?: string;
  stateHome?: string;
  forceSkill?: boolean;
  repairMcp?: boolean;
  runCodex?: CodexCommandRunner;
}

interface CodexManifest {
  schema: 2;
  package_name: string;
  package_version: string;
  codex_home: string;
  skill_path: string;
  skill_sha256: string;
  skill_created: boolean;
  mcp_name: string;
  mcp_command: string;
  mcp_created: boolean;
  mcp_fingerprint: string | null;
}

interface SkillState {
  path: string;
  exists: boolean;
  sha256: string | null;
  managed: boolean;
  owned_by_agent_hub: boolean;
  modified: boolean;
}

interface McpState {
  name: string;
  command: string;
  present: boolean;
  created_by_agent_hub: boolean;
  fingerprint_matches: boolean | null;
  detail: string | null;
}

export interface CodexIntegrationStatus {
  package: { name: string; version: string };
  codex_home: string;
  state_file: string;
  skill: SkillState;
  mcp: McpState;
  executable: string | null;
  manifest_present: boolean;
}

export interface CodexIntegrationResult {
  action: "install" | "uninstall";
  changed: string[];
  status: CodexIntegrationStatus;
}

function defaultCodexHome(): string {
  return resolve(process.env.CODEX_HOME ?? join(homedir(), ".codex"));
}

function defaultStateHome(): string {
  return resolve(process.env.XDG_STATE_HOME ?? join(homedir(), ".local", "state"));
}

function paths(options: CodexIntegrationOptions): {
  codexHome: string;
  skillPath: string;
  manifestPath: string;
} {
  const codexHome = resolve(options.codexHome ?? defaultCodexHome());
  const stateHome = resolve(options.stateHome ?? defaultStateHome());
  return {
    codexHome,
    skillPath: join(codexHome, "skills", CODEX_SKILL_NAME, "SKILL.md"),
    manifestPath: join(stateHome, "agent-hub", "codex", `${sha256(codexHome)}.json`),
  };
}

function skillSourcePath(): string {
  return fileURLToPath(new URL("../../skills/agent-hub/SKILL.md", import.meta.url));
}

function sha256(value: string): string {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

async function writeTextAtomic(path: string, value: string): Promise<void> {
  await mkdir(dirname(path), { recursive: true });
  const temporary = `${path}.${process.pid}.${randomUUID()}.tmp`;
  try {
    await writeFile(temporary, value, { encoding: "utf8", mode: 0o644 });
    await rename(temporary, path);
  } finally {
    await rm(temporary, { force: true });
  }
}

async function readOptional(path: string): Promise<string | null> {
  try {
    return await readFile(path, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") return null;
    throw error;
  }
}

async function readManifest(path: string): Promise<CodexManifest | null> {
  const raw = await readOptional(path);
  if (raw === null) return null;
  try {
    const parsed = JSON.parse(raw) as Partial<CodexManifest>;
    if (
      parsed.schema !== MANIFEST_SCHEMA ||
      parsed.package_name !== PACKAGE_NAME ||
      typeof parsed.package_version !== "string" ||
      typeof parsed.codex_home !== "string" ||
      typeof parsed.skill_path !== "string" ||
      typeof parsed.skill_sha256 !== "string" ||
      typeof parsed.skill_created !== "boolean" ||
      parsed.mcp_name !== CODEX_MCP_NAME ||
      parsed.mcp_command !== CODEX_MCP_COMMAND ||
      typeof parsed.mcp_created !== "boolean" ||
      (parsed.mcp_fingerprint !== null && typeof parsed.mcp_fingerprint !== "string")
    ) {
      throw new Error("manifest fields do not match the supported schema");
    }
    return parsed as CodexManifest;
  } catch (error) {
    throw new AgentHubError(
      "INTEGRATION_STATE_INVALID",
      `cannot read Agent Hub Codex manifest ${path}: ${error instanceof Error ? error.message : String(error)}`,
    );
  }
}

async function defaultRunCodex(
  args: string[],
  options: CodexCommandOptions = {},
): Promise<CodexCommandResult> {
  try {
    const result = await execFileAsync("codex", args, {
      encoding: "utf8",
      maxBuffer: 1024 * 1024,
      env: options.env ?? process.env,
    });
    return { code: 0, stdout: result.stdout, stderr: result.stderr };
  } catch (error) {
    const childError = error as NodeJS.ErrnoException & {
      code?: number | string;
      stdout?: string;
      stderr?: string;
    };
    return {
      code: childError.code ?? 1,
      stdout: childError.stdout ?? "",
      stderr: childError.stderr ?? childError.message ?? String(error),
    };
  }
}

function commandFailure(result: CodexCommandResult, action: string): AgentHubError {
  const detail = result.stderr.trim() || result.stdout.trim() || `exit ${String(result.code)}`;
  if (result.code === "ENOENT") {
    return new AgentHubError(
      "CODEX_NOT_FOUND",
      `cannot ${action}: the codex command is not on PATH`,
    );
  }
  return new AgentHubError("CODEX_COMMAND_FAILED", `cannot ${action}: ${detail}`);
}

function codexRunOptions(codexHome: string): CodexCommandOptions {
  return { env: { ...process.env, CODEX_HOME: codexHome } };
}

function isMissingMcpRegistration(result: CodexCommandResult): boolean {
  const detail = `${result.stdout}\n${result.stderr}`.toLowerCase();
  return (
    detail.includes("no mcp server") ||
    detail.includes("mcp server not found") ||
    detail.includes("server not found") ||
    detail.includes("unknown mcp server")
  );
}

async function mcpLookup(
  runner: CodexCommandRunner,
  runOptions: CodexCommandOptions,
): Promise<{ present: boolean; result: CodexCommandResult }> {
  const result = await runner(["mcp", "get", CODEX_MCP_NAME], runOptions);
  if (result.code === "ENOENT") throw commandFailure(result, "inspect Codex MCP registration");
  if (result.code === 0) return { present: true, result };
  if (isMissingMcpRegistration(result)) return { present: false, result };
  throw commandFailure(result, "inspect Codex MCP registration");
}

async function findExecutable(command: string): Promise<string | null> {
  if (isAbsolute(command)) {
    try {
      await access(command, fsConstants.X_OK);
      return command;
    } catch {
      return null;
    }
  }
  for (const directory of (process.env.PATH ?? "").split(delimiter)) {
    if (directory === "") continue;
    const candidate = join(directory, command);
    try {
      await access(candidate, fsConstants.X_OK);
      return candidate;
    } catch {
      // Keep looking. A PATH entry can be stale or unreadable.
    }
  }
  return null;
}

async function skillState(
  skillPath: string,
  manifest: CodexManifest | null,
): Promise<SkillState> {
  const content = await readOptional(skillPath);
  const currentHash = content === null ? null : sha256(content);
  const matchesManifest = manifest?.skill_path === skillPath && manifest.skill_sha256 === currentHash;
  const owned = matchesManifest && manifest?.skill_created === true;
  return {
    path: skillPath,
    exists: content !== null,
    sha256: currentHash,
    managed: owned,
    owned_by_agent_hub: owned,
    modified: manifest !== null && content !== null && !matchesManifest,
  };
}

async function statusFrom(
  options: CodexIntegrationOptions,
  manifest: CodexManifest | null,
  runner: CodexCommandRunner,
): Promise<CodexIntegrationStatus> {
  const { codexHome, skillPath, manifestPath } = paths(options);
  const runOptions = codexRunOptions(codexHome);
  const skill = await skillState(skillPath, manifest);
  let mcp: McpState;
  try {
    const lookup = await mcpLookup(runner, runOptions);
    const fingerprint = lookup.present ? sha256(lookup.result.stdout) : null;
    mcp = {
      name: CODEX_MCP_NAME,
      command: CODEX_MCP_COMMAND,
      present: lookup.present,
      created_by_agent_hub: manifest?.mcp_created === true,
      fingerprint_matches:
        manifest?.mcp_created === true && fingerprint !== null && manifest.mcp_fingerprint !== null
          ? fingerprint === manifest.mcp_fingerprint
          : manifest?.mcp_created === true && !lookup.present
            ? false
            : null,
      detail: lookup.present ? lookup.result.stdout.trim() || null : lookup.result.stderr.trim() || null,
    };
  } catch (error) {
    if (!(error instanceof AgentHubError) || error.code !== "CODEX_NOT_FOUND") throw error;
    mcp = {
      name: CODEX_MCP_NAME,
      command: CODEX_MCP_COMMAND,
      present: false,
      created_by_agent_hub: manifest?.mcp_created === true,
      fingerprint_matches: null,
      detail: error.message,
    };
  }
  return {
    package: { name: PACKAGE_NAME, version: PACKAGE_VERSION },
    codex_home: codexHome,
    state_file: manifestPath,
    skill,
    mcp,
    executable: await findExecutable(CODEX_MCP_COMMAND),
    manifest_present: manifest !== null,
  };
}

function validateManifestTarget(
  manifest: CodexManifest | null,
  target: { codexHome: string; skillPath: string },
): void {
  if (manifest !== null && manifest.codex_home !== target.codexHome) {
    throw new AgentHubError(
      "INTEGRATION_STATE_INVALID",
      `Agent Hub manifest belongs to Codex home ${manifest.codex_home}, not ${target.codexHome}`,
    );
  }
  if (manifest !== null && manifest.skill_path !== target.skillPath) {
    throw new AgentHubError(
      "INTEGRATION_STATE_INVALID",
      `Agent Hub manifest points to ${manifest.skill_path}, not the requested Codex skill path ${target.skillPath}`,
    );
  }
}

function ensureSkillPath(manifest: CodexManifest | null, skillPath: string, force: boolean): void {
  if (manifest !== null && manifest.skill_path !== skillPath) {
    throw new AgentHubError(
      "INTEGRATION_STATE_INVALID",
      `Agent Hub manifest points to ${manifest.skill_path}, not the requested Codex skill path ${skillPath}`,
    );
  }
  if (manifest === null && !force) return;
}

async function installSkill(
  source: string,
  skillPath: string,
  previousManifest: CodexManifest | null,
  force: boolean,
): Promise<{ previous: string | null; hash: string; changed: boolean; created: boolean }> {
  const content = await readFile(source, "utf8");
  const hash = sha256(content);
  const previous = await readOptional(skillPath);
  ensureSkillPath(previousManifest, skillPath, force);
  if (previous !== null && sha256(previous) !== hash) {
    const managed =
      previousManifest?.skill_path === skillPath &&
      previousManifest.skill_created === true &&
      previousManifest.skill_sha256 === sha256(previous);
    if (!managed && !force) {
      throw new AgentHubError(
        "INTEGRATION_CONFLICT",
        `Codex skill ${skillPath} was changed outside Agent Hub; rerun with --force-skill to replace it`,
      );
    }
  }
  if (previous !== content) {
    await writeTextAtomic(skillPath, content);
  }
  return {
    previous,
    hash,
    changed: previous !== content,
    created: previousManifest?.skill_created ?? previous === null,
  };
}

async function restoreSkill(path: string, previous: string | null): Promise<void> {
  if (previous === null) {
    await rm(path, { force: true });
    return;
  }
  await writeTextAtomic(path, previous);
}

interface FileSnapshot {
  path: string;
  kind: "missing" | "file";
  content?: string;
  mode?: number;
}

async function snapshotFile(path: string): Promise<FileSnapshot> {
  try {
    const stats = await lstat(path);
    if (stats.isSymbolicLink()) {
      throw new AgentHubError(
        "INTEGRATION_STATE_INVALID",
        `Codex config path ${path} is a symlink; refusing an MCP mutation that cannot be rolled back safely`,
      );
    }
    if (!stats.isFile()) {
      throw new AgentHubError(
        "INTEGRATION_STATE_INVALID",
        `Codex config path ${path} is not a regular file or symlink`,
      );
    }
    return {
      path,
      kind: "file",
      content: await readFile(path, "utf8"),
      mode: stats.mode & 0o7777,
    };
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      return { path, kind: "missing" };
    }
    throw error;
  }
}

async function restoreFile(snapshot: FileSnapshot): Promise<void> {
  try {
    const current = await lstat(snapshot.path);
    if (current.isDirectory()) {
      throw new Error(`cannot replace directory at ${snapshot.path} during rollback`);
    }
    await rm(snapshot.path, { force: true });
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== "ENOENT") throw error;
  }
  if (snapshot.kind === "missing") {
    return;
  }
  await mkdir(dirname(snapshot.path), { recursive: true });
  const temporary = `${snapshot.path}.${process.pid}.${randomUUID()}.rollback`;
  try {
    await writeFile(temporary, snapshot.content!, { encoding: "utf8", mode: snapshot.mode });
    await chmod(temporary, snapshot.mode!);
    await rename(temporary, snapshot.path);
  } finally {
    await rm(temporary, { force: true });
  }
}

function codexConfigPath(codexHome: string): string {
  return join(codexHome, "config.toml");
}

async function rollback(
  original: unknown,
  actions: Array<() => Promise<void>>,
): Promise<never> {
  const failures: string[] = [];
  for (const action of actions) {
    try {
      await action();
    } catch (error) {
      failures.push(error instanceof Error ? error.message : String(error));
    }
  }
  if (failures.length > 0) {
    throw new AgentHubError(
      "INTEGRATION_ROLLBACK_FAILED",
      `Agent Hub integration failed: ${original instanceof Error ? original.message : String(original)}; rollback also failed: ${failures.join("; ")}`,
    );
  }
  throw original;
}

async function requireMcpPresent(
  runner: CodexCommandRunner,
  runOptions: CodexCommandOptions,
  action: string,
): Promise<string> {
  const lookup = await mcpLookup(runner, runOptions);
  if (!lookup.present) {
    throw new AgentHubError(
      "CODEX_COMMAND_FAILED",
      `${action} reported success, but Codex did not report the ${CODEX_MCP_NAME} registration`,
    );
  }
  return lookup.result.stdout;
}

async function requireMcpAbsent(
  runner: CodexCommandRunner,
  runOptions: CodexCommandOptions,
  action: string,
): Promise<void> {
  const lookup = await mcpLookup(runner, runOptions);
  if (lookup.present) {
    throw new AgentHubError(
      "CODEX_COMMAND_FAILED",
      `${action} reported success, but Codex still reports the ${CODEX_MCP_NAME} registration`,
    );
  }
}

export async function codexStatus(options: CodexIntegrationOptions = {}): Promise<CodexIntegrationStatus> {
  const resolved = paths(options);
  const { manifestPath } = resolved;
  const manifest = await readManifest(manifestPath);
  validateManifestTarget(manifest, resolved);
  return statusFrom(options, manifest, options.runCodex ?? defaultRunCodex);
}

export async function installCodex(options: CodexIntegrationOptions = {}): Promise<CodexIntegrationResult> {
  const resolved = paths(options);
  const runner = options.runCodex ?? defaultRunCodex;
  const runOptions = codexRunOptions(resolved.codexHome);
  const previousManifest = await readManifest(resolved.manifestPath);
  validateManifestTarget(previousManifest, resolved);
  const lookup = await mcpLookup(runner, runOptions);
  const source = skillSourcePath();
  let installed: Awaited<ReturnType<typeof installSkill>> | null = null;
  let mcpCreated = previousManifest?.mcp_created === true;
  let mcpFingerprint = previousManifest?.mcp_fingerprint ?? null;
  const changed: string[] = [];
  let mcpMutationAttempted = false;
  let mcpSnapshot: FileSnapshot | null = null;
  let committedManifest: CodexManifest;
  try {
    installed = await installSkill(
      source,
      resolved.skillPath,
      previousManifest,
      options.forceSkill === true,
    );
    if (installed.changed) changed.push("skill");
    if (!lookup.present) {
      mcpSnapshot = await snapshotFile(codexConfigPath(resolved.codexHome));
      mcpMutationAttempted = true;
      const added = await runner(["mcp", "add", CODEX_MCP_NAME, "--", CODEX_MCP_COMMAND], runOptions);
      if (added.code !== 0) throw commandFailure(added, "register Agent Hub with Codex");
      mcpCreated = true;
      mcpFingerprint = sha256(await requireMcpPresent(runner, runOptions, "Codex MCP registration"));
      changed.push("mcp");
    } else if (options.repairMcp === true) {
      mcpSnapshot = await snapshotFile(codexConfigPath(resolved.codexHome));
      mcpMutationAttempted = true;
      const removed = await runner(["mcp", "remove", CODEX_MCP_NAME], runOptions);
      if (removed.code !== 0) throw commandFailure(removed, "repair the Agent Hub Codex MCP registration");
      await requireMcpAbsent(runner, runOptions, "Codex MCP removal");
      const added = await runner(["mcp", "add", CODEX_MCP_NAME, "--", CODEX_MCP_COMMAND], runOptions);
      if (added.code !== 0) throw commandFailure(added, "repair the Agent Hub Codex MCP registration");
      mcpCreated = true;
      mcpFingerprint = sha256(await requireMcpPresent(runner, runOptions, "Codex MCP repair"));
      changed.push("mcp");
    }
    if (installed === null) throw new Error("skill installation did not produce state");
    committedManifest = {
      schema: MANIFEST_SCHEMA,
      package_name: PACKAGE_NAME,
      package_version: PACKAGE_VERSION,
      codex_home: resolved.codexHome,
      skill_path: resolved.skillPath,
      skill_sha256: installed.hash,
      skill_created: installed.created,
      mcp_name: CODEX_MCP_NAME,
      mcp_command: CODEX_MCP_COMMAND,
      mcp_created: mcpCreated,
      mcp_fingerprint: mcpFingerprint,
    };
    await writeTextAtomic(resolved.manifestPath, `${JSON.stringify(committedManifest, null, 2)}\n`);
  } catch (error) {
    const actions: Array<() => Promise<void>> = [];
    if (mcpMutationAttempted && mcpSnapshot !== null) actions.push(() => restoreFile(mcpSnapshot!));
    if (installed !== null) actions.push(() => restoreSkill(resolved.skillPath, installed!.previous));
    if (actions.length > 0) await rollback(error, actions);
    throw error;
  }
  return { action: "install", changed, status: await statusFrom(options, committedManifest!, runner) };
}

export async function uninstallCodex(
  options: CodexIntegrationOptions = {},
): Promise<CodexIntegrationResult> {
  const resolved = paths(options);
  const runner = options.runCodex ?? defaultRunCodex;
  const runOptions = codexRunOptions(resolved.codexHome);
  const manifest = await readManifest(resolved.manifestPath);
  validateManifestTarget(manifest, resolved);
  if (manifest === null) {
    return {
      action: "uninstall",
      changed: [],
      status: await statusFrom(options, null, runner),
    };
  }
  const currentSkill = await readOptional(resolved.skillPath);
  if (currentSkill !== null && sha256(currentSkill) !== manifest.skill_sha256) {
    throw new AgentHubError(
      "INTEGRATION_CONFLICT",
      `Codex skill ${resolved.skillPath} was changed outside Agent Hub; it was retained`,
    );
  }
  if (manifest.mcp_created && manifest.mcp_fingerprint === null) {
    throw new AgentHubError(
      "INTEGRATION_STATE_INVALID",
      `Agent Hub manifest does not contain a fingerprint for its owned ${CODEX_MCP_NAME} registration`,
    );
  }
  let mcpMutationAttempted = false;
  let mcpSnapshot: FileSnapshot | null = null;
  let skillRemoved = false;
  try {
    if (manifest.mcp_created) {
      const lookup = await mcpLookup(runner, runOptions);
      if (lookup.present && sha256(lookup.result.stdout) !== manifest.mcp_fingerprint) {
        throw new AgentHubError(
          "INTEGRATION_CONFLICT",
          `Codex MCP registration ${CODEX_MCP_NAME} changed outside Agent Hub; it was retained`,
        );
      }
      if (lookup.present) {
        mcpSnapshot = await snapshotFile(codexConfigPath(resolved.codexHome));
        mcpMutationAttempted = true;
        const removed = await runner(["mcp", "remove", CODEX_MCP_NAME], runOptions);
        if (removed.code !== 0) throw commandFailure(removed, "remove Agent Hub from Codex");
        await requireMcpAbsent(runner, runOptions, "Codex MCP removal");
      }
    }
    if (manifest.skill_created && currentSkill !== null) {
      await rm(resolved.skillPath, { force: true });
      skillRemoved = true;
    }
    await rm(resolved.manifestPath, { force: true });
  } catch (error) {
    const actions: Array<() => Promise<void>> = [];
    if (skillRemoved && currentSkill !== null) actions.push(() => restoreSkill(resolved.skillPath, currentSkill));
    if (mcpMutationAttempted && mcpSnapshot !== null) actions.push(() => restoreFile(mcpSnapshot!));
    if (actions.length > 0) await rollback(error, actions);
    throw error;
  }
  return {
    action: "uninstall",
    changed: [
      ...(skillRemoved ? ["skill"] : []),
      ...(manifest.mcp_created ? ["mcp"] : []),
      "manifest",
    ],
    status: await statusFrom(options, null, runner),
  };
}
