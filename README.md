# Codex Multi-Agent Delegation Hub

`agent-hub` gives Codex one stable entry point for delegating work to local AI
coding agents. Execution models are deliberately small: OMP, AGY, and Grok are
replaceable command adapters, v1 tasks run either in the current checkout
(`direct`) or in a temporary Git worktree (`isolated`), and v2 adds bounded
fan-out, judged competition, resumable sessions, and strictly opt-in
auto-merge. v3 adds additive live surfaces: long-lived interactive sessions
(CLI `agent-hub live`, MCP `live_session_*`) for `omp`, `agy`, `pi`, and
`hermes`, with a capability-honesty gate; no v1/v2 behavior changes.

## Requirements

- Node.js 20 or newer (the code base stays on plain ES2022 — no
  `Promise.withResolvers` or other ES2024-only APIs)
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
(`base`, aggregate `status`, per-candidate results with `candidate_id` and
`artifact`); it never touches the primary checkout.

`status` summarizes the operation: `success` when every candidate succeeded,
`partial` when some succeeded and some did not, `failure` when no candidate
succeeded or the fan-out itself errored. A partial or failed fan-out is an
operation failure: the CLI exits `1` and the MCP tools set `isError`, while
each candidate keeps its own `status`/`error`.

The core library hands retained artifact refs to its caller. The terminal
paths — CLI `fanout` and the MCP fan-out tools — own the tail of that
lifecycle: they release every ref they created before returning, CAS-safe, and
they distinguish three outcomes instead of collapsing them into a boolean. The
ref is gone; the ref now names something else, so a foreign claim survives
untouched; or the ref still names this hub's own commit and the delete was
refused (`cleanup-failed` — a locked ref, an unwritable refs directory, a git
failure). Only that third case is an error: it is reported per ref as
`ref_cleanup_errors`, makes the command exit `1` and the tools report
`isError`, and leaves the ref in place for an operator, because the hub never
claims a ref is gone when it could not delete it. Review candidates from the
returned diffs; after a clean command ends, the refs are gone.

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
`--auto-merge` it prints `{ fan_out, competition, merge }`, and adoption runs
through `autoMerge`, which follows only a real `runCompetition()` result:
status `selected` with a null error, a winner appearing exactly once in that
competition's own eligible list, a base identical to the fan-out base, and a
fan-out candidate whose status is `success` and whose recorded artifact
commit and ref match the eligible entry verbatim. Forged or workflow-shaped
competition objects are refused, never followed. A refusal before any
mutation is a structured result, not a crash: `strategy: "none"`,
`clean: false`, and a `MERGE_*` code such as `MERGE_BASE_MOVED`,
`MERGE_DIRTY_WORKTREE`, or `MERGE_NOT_DESCENDANT`.

A failure after the fast-forward already happened never claims there was no
mutation: the outcome reports `strategy: "fast-forward"`, the observed `HEAD`
as `applied_commit`, and `MERGE_POSTCONDITION_FAILED` or
`LOCK_RELEASE_FAILED` — including when something outside Agent Hub switched
the checkout to another branch in the meantime. Adoption is never rolled back —
inspect the checkout.
Merge status/errors are distinct from the v1 `DelegateStatus` vocabulary and
live on the `merge` object only.

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
stdout, and diffs are not persisted. The artifact commit is captured and the
session ref advanced *before* a failing run's error is surfaced: a crashed or
failing turn still persists its filesystem state, and a resume continues from
that artifact. `session create` rejects a dirty caller checkout unless
`--allow-dirty` is passed; `session resume` deliberately ignores caller
checkout dirtiness because the session ref — not the checkout — is the
baseline. Native provider continuation is used only
when an adapter explicitly verifies its command syntax. Competition is a
separate fan-out operation, available through `fanout --judge` or the MCP
`compete_candidates` tool.

### live sessions (v3 — additive)

A live session is a long-lived interactive provider run driven over the hub
wire, not a one-shot delegation:

```sh
npx agent-hub live --agent pi --workspace /path/to/worktree
npx agent-hub live --resume <hub-live-id> --workspace /path/to/worktree
npx agent-hub live probe --agent hermes
```

`live` reads one Hub NDJSON command per line on stdin —
`prompt`, `follow_up`, `steer`, `cancel`, `status`, `permission_response`,
and `close` (`{"action":"prompt","text":"..."}`,
`{"action":"permission_response","request_id":"...","decision":"allow_once|deny"}`,
etc.) — writes normalized NDJSON documents on stdout
(`{type:"session"|"event"|"result"|"error"|"close"}`), and keeps human
diagnostics on stderr. Stdin EOF closes the session gracefully. Exit codes:
`0` clean, `1` structured failure (launch refusal, a failed result, or an
orphaned end), `2` usage error.

Live providers are `omp, agy, pi, hermes` — this vocabulary is separate from
the legacy `omp, agy, grok`: `pi` and `hermes` stay rejected by
`delegate`/`fanout`/`session`, and `grok` is not a live provider. Every
command is gated against the transport's launch capability snapshot: a
command whose claim is `unsupported` comes back `outcome: "unsupported"`
with a `stage: "capability"` error and is never delivered; claims short of
`unsupported` must carry evidence or the transport is refused at launch.
Permission verdicts are exactly `allow_once` and `deny` — any other verdict
is a caller error, never silently converted into a deny. Turn results report
`cancelled` when the hub stopped the provider mid-turn — an intentional exit
never masquerades as `succeeded`.

The `live` command and the MCP tools drive the one authoritative live core
(`src/live/manager.ts`). Production startup wires everything: all four real
transports (OMP RPC, PI RPC, AGY stream-json, Hermes ACP) register through
`registerProductionLiveTransports()`, and `--resume` reads durable live
state through the wired reader in `src/live/state.ts` — an unregistered
provider fails with `LIVE_TRANSPORT_UNAVAILABLE`, and an unknown resume id
fails with `LIVE_SESSION_NOT_FOUND`; nothing ever guesses a command line or
fabricates a continuation. The provider runs in a hub-owned isolated OS-temp
worktree (the primary checkout's HEAD and index are never its working
directory); a lifetime lease, quotas, recovery, and a bounded event ring
belong to the core manager.

## MCP server

Start the stdio server after building:

```sh
npx agent-hub-mcp
```

It exposes `delegate_task` (schema and behavior unchanged from v1) plus the
additive v2 tools `fanout_candidates`, `compete_candidates`, `session_create`,
and `session_resume`. `compete_candidates.auto_merge` defaults to `false`.
The additive v3 live tools — `live_session_start`, `live_session_resume`,
`live_session_command` (the six hub actions), `live_session_events` (poll
with a cursor, honest about ring-buffer eviction), and `live_session_close` —
run on the same core live manager (one per workspace, built by
`createLiveManager`); `live_session_command` reports capability refusals as
`outcome: "unsupported"` with `isError`, never as a delivery.
Every tool returns structured JSON content with `isError`; a failing operation
comes back as `{ error: { code, message } }` and never escapes as a
transport-level exception — and when the failing `compete_candidates` operation
also could not release artifact refs, those `ref_cleanup_errors` entries ride
in the same error document (the CLI's terminal shape), so cleanup evidence is
never lost behind the throw. The fan-out tools report a `partial` or `failure`
aggregate status, and a `ref_cleanup_errors` entry (an artifact ref they could
not release), as `isError`; like the CLI, they release the artifact refs they
created before returning. MCP clients and the CLI call the same cores;
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
- A request is bounded on both axes: at most 16 candidates in total
  (`FANOUT_MAX_CANDIDATES`, enforced by the core, the CLI parse/usage, and
  both MCP tool schemas) and at most 8 in flight at once.
- The repository identity and base `HEAD` are captured exactly once before
  dispatch; worktree administration is serialized under a repository-local
  lock, with exactly one trailing prune.
- Artifacts are created with Git plumbing through a private temporary index
  and fixed identity; user hooks never run during capture, and the caller's
  index/checkout are untouched. Artifact commits live only on private refs.
- The result carries an aggregate `status`; CLI and MCP treat anything but
  `success` as an operation failure. The core retains artifact refs for its
  caller; terminal paths release them CAS-safe at the end and there is no
  ref garbage collection. A ref this hub still owns that it could not delete
  is reported in `ref_cleanup_errors` and fails the command; a ref someone
  else re-targeted is preserved and is not an error.
- Repository locks recover only on proof. A lock whose same-host owner is
  demonstrably dead is reclaimed through an atomic rename arbiter. An ownerless
  lock directory (no `owner.json`) is never reclaimed automatically, at any
  age: a creator that is stopped, throttled, or stuck on a hung filesystem sits
  in exactly that state, and reclaiming on elapsed time would hand one lock to
  two holders. It reports `LOCK_UNRECOVERABLE` and is a manual inspection item
  — automatic recovery would need acquisition to be a single atomic claim that
  records the owner as it creates the lock, which this lock is not.
- When a worktree is created but its admin lock cannot be released, the handle
  is kept, not lost: the artifact is still captured, teardown is still
  attempted, and the leaked worktree path is named in the candidate's `error`,
  the competition's `error`, or the session's `cleanup_error`. While that lock
  record stays on disk the hub will not touch worktree administration, so both
  the worktree and the lock record are the operator's to clear.
- A committed transition survives a wedged release: when the per-session lock
  cannot be released *after* the ref/state transition landed, `session create`
  and `session resume` still return the completed session (id, revision,
  artifact) with the release trouble reported in `cleanup_error`; the lock
  record itself stays for operator recovery.

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
  equal the artifact commit, the checkout must still be attached to the branch
  that was fast-forwarded, and the tree must still be clean.
- Any condition that fails before adoption is a serializable refusal
  (`strategy: "none"`) with no mutation: no stash, no reset, no force, no
  rebase, no cherry-pick, no patch apply, no conflict resolution. If the base
  moved, the correct continuation is a fresh fan-out from the new state —
  never a forced adoption.
- A failure after `git merge --ff-only` ran is reported truthfully as
  `strategy: "fast-forward"` with the observed `HEAD` and
  `MERGE_POSTCONDITION_FAILED` / `LOCK_RELEASE_FAILED`; there is no rollback
  path and no pretense of a no-mutation refusal.
- Residual race, stated plainly: the merge lock serialises Agent Hub against
  itself, not the repository. A concurrent `git switch`, `git checkout`, or
  `git reset` from outside can still move the checkout between the pre-check
  and the post-adoption probe. Verifying the attached branch turns that into a
  reported applied-but-failed outcome instead of a false "adopted and
  verified", but it narrows the window; it does not close it, and nothing is
  ever rolled back.

Sessions/continuation:

- A session is a private ref plus an atomic state record under the Git common
  directory. Each create/resume advances one linear artifact commit and never
  modifies the primary checkout. Nothing merges on create or resume.
- The artifact commit is durable before a run's failure is reported, so a
  failed session turn persists its filesystem state and remains resumable;
  the session ref and state advance together through a sidecar-guarded
  compare-and-set protocol.
- Commit ids in session state are full object names — exactly 40 or exactly 64
  hex characters — and the "this ref must not exist yet" compare-and-set uses an
  all-zero OID at the repository's own hash width, derived from the commit being
  written rather than hard-coded, so sessions work on SHA-1 and SHA-256
  repositories alike.

v3 live surfaces (additive; v1/v2 guarantees above are untouched):

- One durable record per live session, schema `agent-hub-live/v1`, under
  `<common>/agent-hub/live/sessions/` (a namespace v1/v2 never reads):
  identity, commit lineage, `worktree_path`/`worktree_parent`,
  `last_checkpoint_reason`, resume state, status, revision. The record type
  admits no task text, transcripts, or event bodies by construction; events
  live only in a bounded ring per session (cursor polls report evictions).
- Launch integrity: the provider's process identity (pid/pgid/start token)
  is durably recorded on the session's lease the moment the process spawns —
  before any protocol handshake can fail. A launch that rejects afterwards
  may only reclaim resources when shutdown PROVES the whole owned process
  group is dead; otherwise the lease, worktree, and ownership facts are
  retained for recovery or manual review.
- Capability honesty: a launch whose transport claims `native`-class support
  without evidence is refused, and every refused command names
  `stage: "capability"` instead of vanishing. A `native` follow-up is
  delivered to the provider immediately and tracked provider-queued; only a
  `hub-queued` claim waits for the terminal boundary.
- Shutdown honesty: `closed` is reported only with proof the provider group
  is gone (leader reap plus group probe); a still-unproven end is `orphaned`
  (CLI exit `1`, MCP `isError`) and an in-flight turn reports `cancelled`,
  not `succeeded`.

Not in scope for v2: queued workflows, remote providers, or cross-repository
merges.
