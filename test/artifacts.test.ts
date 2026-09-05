import { access, chmod, readFile, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import {
  ARTIFACT_COMMIT_MESSAGE,
  ARTIFACT_IDENTITY_EMAIL,
  ARTIFACT_IDENTITY_NAME,
  captureCandidateArtifact,
  releaseCandidateRef,
  releaseFanOutArtifactRefs,
} from "../src/artifacts.js";
import { createWorktreeAtBase, removeWorktree, resolveRepositoryIdentity } from "../src/git.js";
import type { CandidateArtifact, FanOutResult } from "../src/types.js";
import {
  candidateRefNames,
  createGitRepository,
  removeDirectory,
  resolveRef,
  runGit,
} from "./helpers.js";

async function commit(repo: string, ...files: string[]): Promise<void> {
  await runGit(repo, ["add", "-A", ...files]);
  await runGit(repo, ["commit", "-qm", "stage"]);
}

/** Produces a real artifact commit + private ref exactly as fan-out would. */
async function makeArtifact(
  repository: string,
  fileName: string,
  candidateKey = `key-${fileName}`,
): Promise<CandidateArtifact> {
  const identity = await resolveRepositoryIdentity(repository);
  const worktree = await createWorktreeAtBase(repository, identity.head);
  await writeFile(join(worktree.path, fileName), `${fileName} work\n`);
  const artifact = await captureCandidateArtifact({
    worktreePath: worktree.path,
    base: identity.head,
    candidateKey,
  });
  await removeWorktree(repository, worktree);
  return artifact;
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

describe("candidate artifact ref release", () => {
  it("deletes exactly the ref whose value still CAS-matches", async () => {
    const repo = await createGitRepository();
    try {
      const artifact = await makeArtifact(repo, "cas.txt");
      expect(await resolveRef(repo, artifact.ref!)).toBe(artifact.commit);

      await expect(releaseCandidateRef(repo, artifact.ref!, artifact.commit!)).resolves.toMatchObject({
        status: "released",
        ref: artifact.ref,
        found: null,
        error: null,
      });
      expect(await resolveRef(repo, artifact.ref!)).toBeNull();
      expect(await candidateRefNames(repo)).toEqual([]);
    } finally {
      await removeDirectory(repo);
    }
  });

  it("leaves a concurrently retargeted ref untouched (CAS)", async () => {
    const repo = await createGitRepository();
    try {
      const mine = await makeArtifact(repo, "mine.txt");
      const theirs = await makeArtifact(repo, "theirs.txt");
      // Someone else re-targets our ref between capture and release.
      await runGit(repo, ["update-ref", mine.ref!, theirs.commit!]);

      await expect(releaseCandidateRef(repo, mine.ref!, mine.commit!)).resolves.toMatchObject({
        status: "foreign-retarget",
        found: theirs.commit,
        error: null,
      });
      // The foreign claim stands untouched; it is no longer ours to delete.
      expect(await resolveRef(repo, mine.ref!)).toBe(theirs.commit);
      expect(await resolveRef(repo, theirs.ref!)).toBe(theirs.commit);
    } finally {
      await removeDirectory(repo);
    }
  });

  it("preserves a concurrently retargeted ref even when it no longer names a commit", async () => {
    const repo = await createGitRepository();
    try {
      const artifact = await makeArtifact(repo, "blob-target.txt");
      await writeFile(join(repo, "foreign-object.txt"), "foreign object\n");
      const blob = (await runGit(repo, ["hash-object", "-w", "foreign-object.txt"])).trim();
      await runGit(repo, ["update-ref", artifact.ref!, blob]);

      await expect(releaseCandidateRef(repo, artifact.ref!, artifact.commit!)).resolves.toMatchObject({
        status: "foreign-retarget",
        found: blob,
      });
      expect(await resolveRef(repo, artifact.ref!)).toBe(blob);
    } finally {
      await removeDirectory(repo);
    }
  });

  it("treats an already-deleted ref as released", async () => {
    const repo = await createGitRepository();
    try {
      const artifact = await makeArtifact(repo, "gone.txt");
      await runGit(repo, ["update-ref", "-d", artifact.ref!]);

      await expect(releaseCandidateRef(repo, artifact.ref!, artifact.commit!)).resolves.toMatchObject({
        status: "released",
      });
    } finally {
      await removeDirectory(repo);
    }
  });

  // Unlinking a loose ref needs write permission on its directory; root ignores
  // that, so the blocked-delete probe only means something for a normal user.
  const blockedDelete = typeof process.getuid !== "function" || process.getuid() !== 0;

  it.runIf(blockedDelete)(
    "reports a blocked delete of our own ref as a cleanup failure, ref unchanged",
    async () => {
      const repo = await createGitRepository();
      const refsDir = join(repo, ".git", "refs", "agent-hub", "candidates");
      try {
        const artifact = await makeArtifact(repo, "blocked.txt");
        await chmod(refsDir, 0o555);
        try {
          const release = await releaseCandidateRef(repo, artifact.ref!, artifact.commit!);

          expect(release.status).toBe("cleanup-failed");
          expect(release.ref).toBe(artifact.ref);
          expect(release.found).toBe(artifact.commit);
          expect(release.error?.code).toBe("ARTIFACT_REF_CLEANUP_FAILED");
          expect(release.error?.message).toContain(artifact.ref!);
          // Still there, still ours, and never reported as released.
          expect(await resolveRef(repo, artifact.ref!)).toBe(artifact.commit);

          const errors = await releaseFanOutArtifactRefs(repo, {
            candidates: [{ artifact }],
          } as unknown as FanOutResult);
          expect(errors).toEqual([release.error]);
        } finally {
          await chmod(refsDir, 0o755);
        }

        // The failure was the blocked write, not a misread: now it releases.
        await expect(
          releaseCandidateRef(repo, artifact.ref!, artifact.commit!),
        ).resolves.toMatchObject({ status: "released" });
        expect(await resolveRef(repo, artifact.ref!)).toBeNull();
      } finally {
        await removeDirectory(repo);
      }
    },
  );

  it("refuses to release refs outside the candidate namespace", async () => {
    const repo = await createGitRepository();
    try {
      const branch = (await runGit(repo, ["symbolic-ref", "--short", "HEAD"])).trim();
      const head = (await runGit(repo, ["rev-parse", "HEAD"])).trim();

      await expect(releaseCandidateRef(repo, `refs/heads/${branch}`, head)).rejects.toMatchObject({
        code: "ARTIFACT_REF_INVALID",
      });
      await expect(
        releaseCandidateRef(
          repo,
          "refs/agent-hub/sessions/00000000-0000-4000-8000-000000000000",
          head,
        ),
      ).rejects.toMatchObject({ code: "ARTIFACT_REF_INVALID" });
      // Namespace-shaped but with a non-commit expected value.
      await expect(
        releaseCandidateRef(repo, "refs/agent-hub/candidates/fake-ref", "deadbeef"),
      ).rejects.toMatchObject({ code: "ARTIFACT_COMMIT_INVALID" });

      expect(await resolveRef(repo, `refs/heads/${branch}`)).toBe(head);
    } finally {
      await removeDirectory(repo);
    }
  });

  it("releases every retained ref of a fan-out result, keeping retargeted ones", async () => {
    const repo = await createGitRepository();
    try {
      const first = await makeArtifact(repo, "one.txt");
      const second = await makeArtifact(repo, "two.txt");
      const identity = await resolveRepositoryIdentity(repo);
      const emptyWorktree = await createWorktreeAtBase(repo, identity.head);
      const empty = await captureCandidateArtifact({
        worktreePath: emptyWorktree.path,
        base: identity.head,
        candidateKey: "key-empty",
      });
      await removeWorktree(repo, emptyWorktree);
      expect(empty.ref).toBeNull();
      // Concurrent foreign claim on the first ref.
      await runGit(repo, ["update-ref", first.ref!, second.commit!]);

      const fanOutDoc = {
        candidates: [{ artifact: first }, { artifact: second }, { artifact: empty }],
      } as unknown as FanOutResult;
      await expect(releaseFanOutArtifactRefs(repo, fanOutDoc)).resolves.toEqual([]);

      // Retargeted ref survives with the foreign value; ours is gone.
      expect(await resolveRef(repo, first.ref!)).toBe(second.commit);
      expect(await resolveRef(repo, second.ref!)).toBeNull();
    } finally {
      await removeDirectory(repo);
    }
  });
});
