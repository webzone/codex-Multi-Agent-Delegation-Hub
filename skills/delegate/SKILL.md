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

The v1 hub does not automatically merge isolated work, run agents in parallel,
or persist sessions. Those decisions stay with Codex until the result is
verified.
