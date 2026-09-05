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
- Check the document's aggregate `status` (`success` / `partial` / `failure`)
  even when candidates look fine: a partial or all-failed fan-out is an
  operation failure (exit `1` / `isError`), while each candidate keeps its own
  `status` and `error`.
- The terminal command releases the private artifact refs it created before it
  exits, CAS-safe: a ref someone else re-targeted is left alone and that is
  fine, while a ref still pointing at this hub's own commit that it could not
  delete is an error. So read `ref_cleanup_errors`: if it is present the
  command exited `1` for that reason and some
  `refs/agent-hub/candidates/...` refs are still in the repository. Never
  assume they are gone, and never delete one you did not create.
- Review from the `diff` fields in the returned document; do not plan to
  inspect `refs/agent-hub/candidates/...` after a clean command has ended.
- A `LOCK_BUSY` that clears on its own is another Agent Hub operation working.
  A `LOCK_UNRECOVERABLE` is not: it means a lock record whose owner nobody can
  prove is dead — including an ownerless lock directory, which is never
  reclaimed automatically at any age because a paused process looks identical.
  Inspect `<common-dir>/agent-hub/locks/`, confirm no Agent Hub process owns
  it, and only then remove the record. If a candidate, competition, or session
  reports `LOCK_RELEASE_FAILED` with a worktree path, that worktree is still
  there and needs the same kind of manual cleanup.
- A session is an agent continuation lineage. `resume` runs the next turn in a
  fresh isolated worktree at the latest artifact commit. A failed turn still
  commits its artifact and advances the session before the error is reported,
  so a failed session's work is resumable.
- `session create` requires a clean caller checkout unless `--allow-dirty`;
  `session resume` deliberately ignores caller-checkout dirtiness because the
  session ref, not the checkout, is the baseline.
- A competition is a recommendation. Prefer reviewing the winner's diff
  yourself over automatic adoption; competition output alone is not evidence
  the code works.

## Auto-merge: exact semantics

`--auto-merge` (CLI) / `auto_merge: true` (MCP `compete_candidates`) is opt-in,
defaults to off, and requires a judge: it can only adopt the internal winner of
a real `runCompetition()` result, never an externally supplied commit. The
winner is trusted only when the competition reports `status: "selected"` with
no error, names a candidate that appears exactly once among the eligible
entries, shares the fan-out's base, and the fan-out candidate matches the
winner's artifact commit and ref verbatim. Before adoption the merge
revalidates, under the repository merge lock:

- the workspace is still the same repository checkout, on the same named
  branch as when the fan-out started, with `HEAD` still at the captured base;
- the working tree is clean, untracked files included (no override exists);
- the winner's artifact commit still exists on its private ref and descends
  from that base.

If every check passes, the branch is fast-forwarded to the artifact commit
with hooks disabled, and `HEAD`, the branch the checkout is attached to, and a
clean tree are re-verified. Otherwise the result is a refusal — `strategy:
"none"`, `clean: false`, a `MERGE_*` code, and no mutation at all. Auto-merge
never stashes, resets, force-updates, rebases, cherry-picks, applies patches,
or resolves conflicts.

A failure *after* the fast-forward is a different animal and is reported as
one: `strategy: "fast-forward"` with the observed `HEAD` in `applied_commit`
plus `MERGE_POSTCONDITION_FAILED` or `LOCK_RELEASE_FAILED`. There is no
rollback — the checkout already moved. Treat such an outcome as "adopted but
unverified": inspect the working tree and `git status`/`git log` yourself
before doing anything else, and never retry the merge blindly.

The merge lock only serialises Agent Hub against itself. A `git switch`,
`git checkout`, or `git reset` that someone else runs after the pre-check can
still move the checkout: re-verifying the attached branch reports that as an
applied-but-failed outcome rather than a clean adoption, but it narrows the
race, it does not remove it. If the reported branch is not the one you asked
to merge into, look at the checkout before trusting anything else in the
result.

Continuation rules:

- `MERGE_BASE_MOVED`, `MERGE_BRANCH_CHANGED`, or `MERGE_IDENTITY_MISMATCH`
  means the world moved under the fan-out. Start a fresh fan-out (or rebase
  the review manually yourself); do not try to force the old artifact in.
- `MERGE_DIRTY_WORKTREE` means someone is working in the checkout now. Resolve
  that first; the hub will not interleave with it.
- A refusal is data, not a crash: handle it like any other review outcome. But
  a `strategy: "fast-forward"` outcome carrying an error is not a refusal —
  the adoption happened; inspect the checkout as above.

The hub still does not merge plain `delegate` or `fanout` output on its own —
adoption only ever happens through an explicit opt-in step you control.

## Live sessions (v3, additive)

When one agent should work interactively over many turns — steering mid-run,
answering its permission requests, or cancelling a turn — use the live
surface instead of one-shot delegation:

```sh
agent-hub live --agent <omp|agy|pi|hermes> --workspace "$PWD"
agent-hub live --resume <hub-live-id> --workspace "$PWD"
agent-hub live probe --agent <omp|agy|pi|hermes>
```

- The live vocabulary is `omp, agy, pi, hermes` and it is separate: `pi` and
  `hermes` are live-only and stay rejected by `delegate`, `fanout`, and
  `session`; `grok` is legacy-only and is not a live provider.
- `live` is a long-lived NDJSON conversation, not a flag-per-turn command:
  write one Hub command per line to stdin (`prompt` once, then `follow_up`
  or `steer`, `cancel`, `status`, `permission_response`, `close`), read
  `{type:"session"|"event"|"result"|"error"|"close"}` documents on stdout,
  and treat stderr as human diagnostics. Closing stdin ends the session
  gracefully. The MCP tools `live_session_start` / `_resume` / `_command` /
  `_events` / `_close` are the same surface for tool-using clients.
- Trust the capability report, not hope: commands are gated against the
  transport's launch snapshot. An `outcome: "unsupported"` result with a
  `stage: "capability"` error means the hub refused the command and nothing
  reached the provider — do not retry it expecting a different result;
  choose a different flow (e.g. a fresh `follow_up` at the next idle
  boundary when only `steer` is unsupported).
- Consume events by cursor (`live_session_events`) instead of assuming
  delivery: `next_cursor` advances over per-session seqs with no gaps, and
  `dropped: true` says the ring buffer already evicted past your cursor —
  re-read from `earliest_seq` or take `status`.
- A `cancelled` turn result means the hub stopped the provider before the
  turn concluded (close or cancel): partial work may exist; it is not a
  provider crash and not a success. An `orphaned` close means the hub could
  not prove the process died — inspect before starting a sibling session.
- Resume honesty: `--resume` needs the durable live-state store; until that
  package is wired it fails with `LIVE_STATE_UNAVAILABLE`. Likewise a
  provider with no registered transport fails `LIVE_TRANSPORT_UNAVAILABLE`
  — the hub never guesses a command line. `probe` answers what is actually
  installed; a not-found probe exits `1` as honest data.
- Live sessions are process-local and transient by contract: hub live state
  stores only the metadata record (no task text, transcripts, or event
  bodies), and checkpoint pinning belongs to the durable package. Do not
  plan a continuation from hub memory — inspect the workspace itself, as
  with every isolated run, and run the relevant tests before accepting the
  work.
