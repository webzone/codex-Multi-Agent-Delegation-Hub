---
name: delegate
description: Delegate coding tasks from Codex to a local AI coding agent through Agent Hub.
---

# Delegate coding work

Use the repository's `agent-hub` command when a task is better handled by a
separate local coding agent and Codex should remain the orchestrator.

## Choosing a mode

- Use `isolated` for changes, refactors, or anything that should be reviewed
  before it enters the current checkout. The result contains the patch and
  changed files; inspect them before applying any merge or cherry-pick.
- Use `direct` only when the user explicitly wants the selected agent to edit
  the current checkout in place.
- Both modes require a clean Git worktree by default. Use `--allow-dirty` only
  after checking that existing changes are intentional.

## Command

```sh
agent-hub delegate \
  --agent <omp|agy|grok> \
  --mode <direct|isolated> \
  --workspace "$PWD" \
  "<task>"
```

The command returns a serializable JSON object. Check `status`, `exit_code`,
`changed_files`, `diff`, and `error`; a process that exited successfully is not
enough evidence that the implementation is correct. Run the relevant tests and
review the diff in Codex before considering the delegation complete.

Exit codes: `0` success, `1` structured operation failure (JSON on stdout),
`2` parse or usage error.

## Fan-out and competition

When several agents could plausibly solve the same task, or when one task
should be split across agents:

```sh
agent-hub fanout --agent omp --agent agy --task "<shared task>"
agent-hub fanout --agent omp --agent agy --task "<shared task>" \
  --judge <agent>
agent-hub session create --agent omp --task "<first turn>"
agent-hub session resume <session-id> --task "<next turn>"
```

- Fan-out is isolated-only: candidates run in detached worktrees pinned to one
  captured base and are recorded as hook-free artifact commits on private
  refs. Fan-out never touches the primary checkout and never merges.
- A session is an agent continuation lineage. `resume` runs the next turn in a
  fresh isolated worktree at the latest artifact commit.
- A competition is a recommendation. Prefer reviewing the winner's diff
  yourself over automatic adoption; competition output alone is not evidence
  the code works.

## Auto-merge: exact semantics

`--auto-merge` (CLI) / `auto_merge: true` (MCP `compete_candidates`) is opt-in,
defaults to off, and requires a judge: it can only adopt the competition's
internal winner, never an externally supplied commit. Before adoption the
merge revalidates, under the repository merge lock:

- the workspace is still the same repository checkout, on the same named
  branch as when the fan-out started, with `HEAD` still at the captured base;
- the working tree is clean, untracked files included (no override exists);
- the winner's artifact commit still exists on its private ref and descends
  from that base.

If every check passes, the branch is fast-forwarded to the artifact commit
with hooks disabled, and `HEAD` plus a clean tree are re-verified. Otherwise
the result is a refusal — `strategy: "none"`, `clean: false`, a `MERGE_*`
code, and no mutation at all. Auto-merge never stashes, resets, force-updates,
rebases, cherry-picks, applies patches, or resolves conflicts.

Continuation rules:

- `MERGE_BASE_MOVED`, `MERGE_BRANCH_CHANGED`, or `MERGE_IDENTITY_MISMATCH`
  means the world moved under the fan-out. Start a fresh fan-out (or rebase
  the review manually yourself); do not try to force the old artifact in.
- `MERGE_DIRTY_WORKTREE` means someone is working in the checkout now. Resolve
  that first; the hub will not interleave with it.
- A refusal is data, not a crash: handle it like any other review outcome.

The hub still does not merge plain `delegate` or `fanout` output on its own —
adoption only ever happens through an explicit opt-in step you control.
