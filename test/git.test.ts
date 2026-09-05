import { chmod, mkdir, mkdtemp, realpath, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { delimiter, join } from "node:path";

import { describe, expect, it } from "vitest";

import { resolveRepositoryIdentity } from "../src/git.js";
import { removeDirectory, runGit } from "./helpers.js";

/**
 * A fake old Git installed as `git` in a PATH-front directory: it rejects
 * `--path-format=absolute` the way Git predating that flag does, prints
 * `--git-common-dir` output *relative to the queried cwd* (older Git's
 * default for linked worktrees), and delegates everything else verbatim to
 * the real binary. Shebanged with an absolute node so it survives the PATH
 * rewrite and the repo's `"type": "module"`.
 */
async function installOldGitShim(): Promise<string> {
  const shimDir = await mkdtemp(join(tmpdir(), "agent-hub-oldgit-"));
  await writeFile(
    join(shimDir, "git"),
    [
      `#!${process.execPath}`,
      "const { spawnSync } = require('node:child_process');",
      "const fs = require('node:fs');",
      "const path = require('node:path');",
      "const self = fs.realpathSync(process.argv[1]);",
      "const args = process.argv.slice(2);",
      "const real = (process.env.PATH || '').split(path.delimiter)",
      "  .map((dir) => path.join(dir, 'git'))",
      "  .find((candidate) => {",
      "    try { return fs.existsSync(candidate) && fs.realpathSync(candidate) !== self; }",
      "    catch { return false; }",
      "  });",
      "if (!real) { process.stderr.write('shim: no real git on PATH\\n'); process.exit(127); }",
      "if (args.includes('--path-format=absolute')) {",
      "  process.stderr.write('error: unknown option `path-format=absolute\\'\\n');",
      "  process.exit(129);",
      "}",
      "const result = spawnSync(real, args, { cwd: process.cwd(), encoding: 'utf8' });",
      "if (result.error) { process.stderr.write(String(result.error)); process.exit(126); }",
      "if (args[0] === 'rev-parse' && args.includes('--git-common-dir')) {",
      "  process.stdout.write(path.relative(process.cwd(), result.stdout.trim()) + '\\n');",
      "} else {",
      "  process.stdout.write(result.stdout);",
      "}",
      "process.stderr.write(result.stderr);",
      "process.exit(result.status ?? 1);",
      "",
    ].join("\n"),
    { mode: 0o755 },
  );
  await chmod(join(shimDir, "git"), 0o755);
  return shimDir;
}

/** Git canonicalizes symlinks (`/var` → `/private/var`); a missing path is a */
/** plain failed-comparison value, not an ENOENT throw. */
async function canonical(path: string): Promise<string> {
  try {
    return await realpath(path);
  } catch {
    return `<missing: ${path}>`;
  }
}

describe("resolveRepositoryIdentity against an old Git", { timeout: 20_000 }, () => {
  it("resolves the relative --git-common-dir fallback against the queried workspace", async () => {
    // Nested layout + a chdir to a depth that can never coincide: resolving
    // the shim's relative output against process.cwd() instead of the target
    // workspace must land on a path that is not merely different-but-close,
    // it must not exist at all.
    const outer = await mkdtemp(join(tmpdir(), "agent-hub-oldgit-"));
    const repo = join(outer, "a", "b", "repo");
    const linkWorktree = join(outer, "links", "wt");
    const unrelatedCwd = join(outer, "x", "y", "z");
    await mkdir(repo, { recursive: true });
    await runGit(repo, ["init", "-q"]);
    await runGit(repo, ["config", "user.email", "agent-hub@example.test"]);
    await runGit(repo, ["config", "user.name", "Agent Hub Test"]);
    await writeFile(join(repo, "README.md"), "initial\n");
    await runGit(repo, ["add", "README.md"]);
    await runGit(repo, ["commit", "-qm", "initial"]);
    await runGit(repo, ["worktree", "add", "--detach", linkWorktree, "HEAD"]);
    await mkdir(unrelatedCwd, { recursive: true });

    const shimDir = await installOldGitShim();
    const previousPath = process.env.PATH;
    const previousCwd = process.cwd();
    process.env.PATH = `${shimDir}${delimiter}${previousPath}`;
    process.chdir(unrelatedCwd);
    try {
      // From the linked worktree the shim reports `../../a/b/repo/.git`-style
      // output; the hub must resolve it against THAT workspace (and `.git`
      // against the main one), never against the process cwd.
      const linked = await resolveRepositoryIdentity(linkWorktree);
      const main = await resolveRepositoryIdentity(repo);

      const expected = await realpath(join(repo, ".git"));
      expect(await canonical(linked.common_dir)).toBe(expected);
      expect(await canonical(main.common_dir)).toBe(expected);
      expect(linked.head).toBe(main.head);
    } finally {
      process.chdir(previousCwd);
      process.env.PATH = previousPath;
      await removeDirectory(shimDir);
      await removeDirectory(outer);
    }
  });
});
