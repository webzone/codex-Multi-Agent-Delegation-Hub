import { access, chmod, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import {
  ARTIFACT_COMMIT_MESSAGE,
  ARTIFACT_IDENTITY_EMAIL,
  ARTIFACT_IDENTITY_NAME,
  captureCandidateArtifact,
} from "../src/artifacts.js";
import { createWorktreeAtBase, removeWorktree, resolveRepositoryIdentity } from "../src/git.js";
import { createGitRepository, removeDirectory, runGit } from "./helpers.js";

async function commit(repo: string, ...files: string[]): Promise<void> {
  await runGit(repo, ["add", "-A", ...files]);
  await runGit(repo, ["commit", "-qm", "stage"]);
}

describe("candidate artifacts", () => {
  it("captures tracked/untracked/deleted/binary/executable changes without touching the caller", async () => {
    const repo = await createGitRepository();
    await writeFile(join(repo, "doomed.txt"), "doomed\n");
    await writeFile(join(repo, "blob.bin"), Buffer.from([9, 8, 7, 0]));
    await writeFile(join(repo, "exec.sh"), "#!/bin/sh\necho hi\n");
    await commit(repo, "doomed.txt", "blob.bin", "exec.sh");
    const identity = await resolveRepositoryIdentity(repo);

    const worktree = await createWorktreeAtBase(repo, identity.head);
    try {
      await writeFile(join(worktree.path, "README.md"), "updated\n");
      await writeFile(join(worktree.path, "added.txt"), "added\n");
      await rm(join(worktree.path, "doomed.txt"));
      await writeFile(join(worktree.path, "blob.bin"), Buffer.from([0, 1, 2, 255, 0, 42, 7]));
      // Tracked file: mode-only change must surface in the patch.
      await chmod(join(worktree.path, "exec.sh"), 0o755);

      const artifact = await captureCandidateArtifact({
        worktreePath: worktree.path,
        base: identity.head,
        candidateKey: "candidate-alpha",
      });

      expect(artifact.empty).toBe(false);
      expect([...artifact.changed_files].sort()).toEqual(
        ["README.md", "added.txt", "blob.bin", "doomed.txt", "exec.sh"].sort(),
      );
      expect(artifact.diff).toContain("deleted file mode");
      expect(artifact.diff).toContain("new mode 100755");
      expect(artifact.diff).toContain("GIT binary patch");

      // Commit wiring: single parent == captured base, ref points at it.
      expect(artifact.parent).toBe(identity.head);
      expect(artifact.commit).toBeTruthy();
      const format = "%P%x00%an%x00%ae%x00%cn%x00%ce%x00%s";
      const meta = (await runGit(repo, ["log", "-1", `--format=${format}`, artifact.commit as string])).trim();
      const [parent, authorName, authorEmail, committerName, committerEmail, subject] = meta.split("\0");
      expect(parent).toBe(identity.head);
      expect([authorName, authorEmail, committerName, committerEmail]).toEqual([
        ARTIFACT_IDENTITY_NAME,
        ARTIFACT_IDENTITY_EMAIL,
        ARTIFACT_IDENTITY_NAME,
        ARTIFACT_IDENTITY_EMAIL,
      ]);
      // Fixed constant message: no task text, no agent output, repo user config unused.
      expect(subject).toBe(ARTIFACT_COMMIT_MESSAGE);

      expect(artifact.ref).toMatch(/^refs\/agent-hub\/candidates\/[0-9a-f-]+$/);
      const refTarget = (await runGit(repo, ["rev-parse", artifact.ref as string])).trim();
      expect(refTarget).toBe(artifact.commit);

      // Caller checkout and real index are untouched.
      expect(await runGit(repo, ["status", "--porcelain=v1", "--untracked-files=all"])).toBe("");
      expect(await runGit(repo, ["diff", "--cached", "--name-only"])).toBe("");
      expect(await readFile(join(repo, "README.md"), "utf8")).toBe("initial\n");
      // blob.bin is tracked from the base; the caller's copy keeps base bytes.
      expect((await readFile(join(repo, "blob.bin"))).equals(Buffer.from([9, 8, 7, 0]))).toBe(true);
      await expect(access(join(repo, "added.txt"))).rejects.toThrow();
    } finally {
      await removeWorktree(repo, worktree);
      await removeDirectory(repo);
    }
  });

  it("produces no commit and no ref for a no-op candidate", async () => {
    const repo = await createGitRepository();
    const identity = await resolveRepositoryIdentity(repo);
    const worktree = await createWorktreeAtBase(repo, identity.head);

    try {
      const artifact = await captureCandidateArtifact({
        worktreePath: worktree.path,
        base: identity.head,
        candidateKey: "no-op-candidate",
      });

      expect(artifact).toMatchObject({
        parent: identity.head,
        commit: null,
        tree: null,
        ref: null,
        empty: true,
        changed_files: [],
        diff: "",
      });
      expect(await runGit(repo, ["for-each-ref", "refs/agent-hub/candidates"])).toBe("");
    } finally {
      await removeWorktree(repo, worktree);
      await removeDirectory(repo);
    }
  });

  it("never interpolates raw candidate ids into ref names", async () => {
    const repo = await createGitRepository();
    const worktree = await createWorktreeAtBase(repo, (await resolveRepositoryIdentity(repo)).head);

    try {
      await writeFile(join(worktree.path, "note.txt"), "note\n");
      const artifact = await captureCandidateArtifact({
        worktreePath: worktree.path,
        base: (await resolveRepositoryIdentity(repo)).head,
        candidateKey: "../../../../evil*id with spaces",
      });

      expect(artifact.ref).toMatch(/^refs\/agent-hub\/candidates\/[0-9a-f]{16}-[0-9a-f]{12}$/);
      expect(artifact.ref).not.toContain("evil");
      expect(artifact.ref).not.toContain("..");
      expect((await runGit(repo, ["rev-parse", artifact.ref as string])).trim()).toBe(artifact.commit);
    } finally {
      await removeWorktree(repo, worktree);
      await removeDirectory(repo);
    }
  });
});
