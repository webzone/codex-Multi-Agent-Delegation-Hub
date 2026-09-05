import { execFile } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";

const execFileAsync = promisify(execFile);

export async function runGit(cwd: string, args: string[]): Promise<string> {
  const result = await execFileAsync("git", args, { cwd });
  return result.stdout;
}

export async function createGitRepository(): Promise<string> {
  const repository = await mkdtemp(join(tmpdir(), "agent-hub-test-"));
  await runGit(repository, ["init", "-q"]);
  await runGit(repository, ["config", "user.email", "agent-hub@example.test"]);
  await runGit(repository, ["config", "user.name", "Agent Hub Test"]);
  await writeFile(join(repository, "README.md"), "initial\n");
  await runGit(repository, ["add", "README.md"]);
  await runGit(repository, ["commit", "-qm", "initial"]);
  return repository;
}

export async function removeDirectory(path: string): Promise<void> {
  await rm(path, { recursive: true, force: true });
}

/** Full commit the ref resolves to, or null when it does not resolve. */
export async function resolveRef(repository: string, ref: string): Promise<string | null> {
  try {
    const out = await runGit(repository, ["rev-parse", "--quiet", "--verify", ref]);
    return out.trim() || null;
  } catch {
    return null;
  }
}

/** Names of every still-present private candidate artifact ref. */
export async function candidateRefNames(repository: string): Promise<string[]> {
  const output = await runGit(repository, [
    "for-each-ref",
    "--format=%(refname)",
    "refs/agent-hub/candidates",
  ]);
  return output.split("\n").filter(Boolean);
}
