# Codex Multi-Agent Delegation Hub

`agent-hub` gives Codex one stable entry point for delegating work to local AI
coding agents. Execution models are deliberately small: OMP, AGY, and Grok are
replaceable command adapters, v1 tasks run either in the current checkout
(`direct`) or in a temporary Git worktree (`isolated`), and v2 adds bounded
fan-out, judged competition, resumable sessions, and strictly opt-in
auto-merge.

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

The CLI emits JSON on stdout. Exit codes: `0` success, `1` structured
operation failure (the JSON on stdout carries `error.code`/`error.message` or
the operation's own error field), `2` parse or usage error (message plus usage
on stderr).

### delegate (v1 — behavior unchanged)

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
existing changes are intentional. In direct mode they remain part of the
result; isolated mode still starts from committed `HEAD`, so caller-local
changes are left untouched and are not carried into the isolated result.

### fanout

```sh
npx agent-hub fanout \
  --agent omp --agent agy --agent grok \
  --task "Fix the flaky test" \
  --concurrency 2
```

Every candidate runs isolated-only — there is no direct fan-out mode and no
flag that would enable one. All candidates pin to one base commit captured
before dispatch and work in their own detached worktrees; each produces a
hook-free artifact commit stored under a private
`refs/agent-hub/candidates/...` ref. The command prints the fan-out document
(`base`, per-candidate results with `candidate_id` and `artifact`); it never
touches the primary checkout.

Repeat `--task` once per `--agent` to pair distinct tasks in order, or pass a
single `--task` to give every agent the same assignment.

### judged competition and auto-merge

Adoption is opt-in and never implicit: `--auto-merge` additionally requires
`--judge`, and it only follows the judge's internal winner selection.

```sh
npx agent-hub fanout --agent omp --agent grok --task "Fix it" \
  --judge omp --auto-merge
```

With `--judge` alone the command prints `{ fan_out, competition }`. With
`--auto-merge` it prints `{ fan_out, competition, merge }` and the merge
outcome is decided by `mergeCandidate` (see Safety below). A merge refusal is
a structured result, not a crash: `strategy: "none"`, `clean: false`, and a
`MERGE_*` code such as `MERGE_BASE_MOVED`, `MERGE_DIRTY_WORKTREE`, or
`MERGE_NOT_DESCENDANT`. Merge status/errors are distinct from the v1
`DelegateStatus` vocabulary and live on the `merge` object only.

### session continuation

Sessions persist one agent's artifact lineage in the repository's Git common
directory. Every resume starts a fresh isolated worktree at the latest
artifact commit, so file-system continuation works even when the provider has
no native conversation-resume capability:

```sh
npx agent-hub session create --agent omp --task "Implement the first step"
npx agent-hub session resume <session-id> --task "Continue and run the tests"
```

The session record contains only repository and provider metadata; task text,
stdout, and diffs are not persisted. Native provider continuation is used only
when an adapter explicitly verifies its command syntax. Competition is a
separate fan-out operation, available through `fanout --judge` or the MCP
`compete_candidates` tool.

## MCP server

Start the stdio server after building:

```sh
npx agent-hub-mcp
```

It exposes `delegate_task` (schema and behavior unchanged from v1) plus the
additive v2 tools `fanout_candidates`, `compete_candidates`, `session_create`,
and `session_resume`. `compete_candidates.auto_merge` defaults to `false`.
Every tool returns structured JSON content with `isError`; a failing operation
comes back as `{ error: { code, message } }` and never escapes as a
transport-level exception. MCP clients and the CLI call the same cores;
transport code does not contain provider or Git logic.

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
It explains when to choose direct versus isolated execution, how to run and
judge a fan-out, and what an auto-merge will and will never do.

## Safety and scope

v1 guarantees (unchanged):

- Child processes are started with argument arrays and `shell: false`.
- Dirty worktrees are rejected unless explicitly overridden.
- Isolated execution uses a detached temporary worktree based on `HEAD`.
- Cleanup is attempted in `finally` and never merges or resets the primary checkout.
- Output and diff captures are bounded and report truncation.

v2 fan-out:

- Fan-out is isolated-only; candidates never execute in the caller checkout.
- The repository identity and base `HEAD` are captured exactly once before
  dispatch; worktree administration is serialized under a repository-local
  lock, with exactly one trailing prune.
- Artifacts are created with Git plumbing through a private temporary index
  and fixed identity; user hooks never run during capture, and the caller's
  index/checkout are untouched. Artifact commits live only on private refs.

v2 auto-merge (opt-in, all conditions enforced again under the
repository-local merge lock immediately before the checkout is touched):

- Same repository identity (common dir + worktree root), same named branch as
  at capture, and current `HEAD` equal to the captured base.
- Clean `git status`, untracked files included. There is no dirty override for
  merges.
- The selected candidate's artifact commit must still exist, resolve from its
  private ref, and descend from the captured base.
- Adoption is a verified fast-forward (`git merge --ff-only`) of that one
  internally selected commit, with hooks disabled. Afterwards, `HEAD` must
  equal the artifact commit and the checkout must still be clean.
- Any failed condition is a serializable refusal (`strategy: "none"`) with no
  mutation: no stash, no reset, no force, no rebase, no cherry-pick, no patch
  apply, no conflict resolution. If the base moved, the correct continuation
  is a fresh fan-out from the new state — never a forced adoption.

Sessions/continuation:

- A session is a private ref plus an atomic state record under the Git common
  directory. Each create/resume advances one linear artifact commit and never
  modifies the primary checkout. Nothing merges on create or resume.

Not in scope for v2: queued workflows, remote providers, or cross-repository
merges.
