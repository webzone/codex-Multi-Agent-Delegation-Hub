# Codex Multi-Agent Delegation Hub

`agent-hub` gives Codex one stable entry point for delegating work to local AI
coding agents. The first version keeps the execution model deliberately small:
OMP, AGY, and Grok are replaceable command adapters, and tasks run either in
the current checkout (`direct`) or in a temporary Git worktree (`isolated`).

## Requirements

- Node.js 20 or newer
- Git
- At least one local agent executable (`omp`, `agy`, or `grok`)

Install and verify the package:

```sh
npm install
npm run typecheck
npm test
npm run build
```

## CLI

The CLI emits the same JSON result used by the MCP server:

```sh
npx agent-hub delegate \
  --agent omp \
  --mode isolated \
  "Implement the requested change and run the relevant tests"
```

Use `--workspace /path/to/repository` to target another checkout. `isolated`
is the safer default for changes because the agent works from the current
`HEAD` in a temporary worktree; the resulting changed-file list and patch are
returned, but nothing is merged automatically. The temporary worktree is
removed on both success and failure.

`direct` runs in the caller's checkout and leaves the agent's edits there. Both
modes reject a dirty repository by default. Pass `--allow-dirty` only when the
existing changes are intentional and should remain in the result.

## MCP server

Start the stdio server after building:

```sh
npx agent-hub-mcp
```

It exposes one tool, `delegate_task`, with `task`, `agent`, `mode`, `workspace`,
`allow_dirty`, and `max_output_bytes` arguments. MCP clients and the CLI call
the same `delegate()` core; transport code does not contain provider or Git
logic.

## Adapter configuration

Each adapter uses argument arrays and never interpolates task text into a
shell command. Override the executable with:

```text
AGENT_HUB_OMP_BIN
AGENT_HUB_AGY_BIN
AGENT_HUB_GROK_BIN
```

Override arguments with a JSON array in the corresponding `*_ARGS` variable.
Use the literal `{task}` item where the task should be inserted; if omitted,
the task is appended:

```sh
export AGENT_HUB_OMP_BIN="$PWD/test-fixtures/fake-agent"
export AGENT_HUB_OMP_ARGS='["--prompt", "{task}"]'
```

Built-in defaults are `omp -p <task>`, `agy -p <task> --output-format
stream-json`, and `grok -p <task>`. Credentials and remote API assumptions are
intentionally outside this hub.

## Delegation guidance

The reusable operator guidance is in [`skills/delegate/SKILL.md`](skills/delegate/SKILL.md).
It explains when to choose direct versus isolated execution and how to inspect
the returned result before accepting an agent's work.

## Safety and v1 scope

- Child processes are started with argument arrays and `shell: false`.
- Dirty worktrees are rejected unless explicitly overridden.
- Isolated execution uses a detached temporary worktree based on `HEAD`.
- Cleanup is attempted in `finally` and never merges or resets the primary checkout.
- Output and diff captures are bounded and report truncation.
- Parallel, competitive, queued, persisted, and automatic-merge workflows are
  intentionally left for a later version.
