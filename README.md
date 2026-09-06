# Codex Multi-Agent Delegation Hub

`agent-hub` gives Codex one stable entry point for delegating work to local AI
coding agents. It installs two commands — `agent-hub` (the CLI) and
`agent-hub-mcp` (an MCP server exposing the same operations as tools) — and
they share one core, so behavior and safety guarantees are identical whichever
entry point you use.

What you can do with it:

- **delegate** — hand one task to one local agent (`omp`, `agy`, or `grok`)
  and get a JSON result back, usually with a reviewable patch.
- **fanout** — run the same task (or one task per agent) on up to 16 agents at
  once, optionally judged, and adoption of a winner stays an explicit opt-in.
- **session** — continue one agent's work across turns; each turn builds on
  the previous turn's committed artifact, even when the provider cannot
  natively resume a conversation.
- **live** — keep a long-lived interactive provider run open (`omp`, `agy`,
  `pi`, `hermes`): prompt over several turns, steer mid-run, cancel a turn,
  check status, and resume after the hub process goes away.

The package is private and never published to a registry: you install it from
this repository, and you run the command you installed.

## Requirements

- Node.js 20 or newer (the code base stays on plain ES2022 — no
  `Promise.withResolvers` or other ES2024-only APIs, and the TypeScript setup
  pins Node 20 types so newer runtime APIs cannot slip in)
- Git
- At least one local agent executable (`omp`, `agy`, or `grok` for
  one-shot work; `omp`, `agy`, `pi`, or `hermes` for live sessions)

## Install (from Git)

This is a private, source-installed package — clone, build, and link it into
your global bin:

```sh
git clone <repo>
cd codex-Multi-Agent-Delegation-Hub
npm ci
npm run build
npm install -g .
agent-hub --help
```

`npm run build` compiles `src/` into `dist/`; the global install links the two
bins (`agent-hub`, `agent-hub-mcp`) from there. If you skip the build, the
installed commands point at files that do not exist yet.

### Why every example uses the installed command

Every example in this README and in `skills/delegate/SKILL.md` uses
`agent-hub …` / `agent-hub-mcp` — the binaries you just installed. Do **not**
rewrite them as `npx agent-hub …`: this package is private and was never
published, and `npx agent-hub` on a machine (or inside a project) where the
local package is not resolvable will silently fetch and run **whatever package
the public registry happens to call `agent-hub`** — an unrelated third-party
program, not this hub. If you do not want a global install, run the built code
from inside this checkout instead:

```sh
node dist/cli.js --help        # equivalent to agent-hub --help
node dist/mcp.js               # equivalent to agent-hub-mcp
```

### Upgrading

Upgrade from inside the clone, in this order:

1. **Close your live sessions first.** End each CLI `agent-hub live` run (send
   `{"action":"close"}` on stdin, or Ctrl-D) and have MCP clients call
   `live_session_close`. A live provider run belongs to the hub process that
   started it; upgrading while sessions are live strands them as leases.
2. **Recover anything that is already orphaned** (see
   [Crash and orphan recovery](#crash-and-orphan-recovery)) before pulling.
   Old leases count against the per-repository quota, so a directory full of
   stale leases can block the new build from starting anything.
3. Pull and reinstall:

   ```sh
   git pull --ff-only
   npm ci
   npm run build
   npm install -g .
   ```

### Uninstalling

```sh
npm uninstall -g codex-multi-agent-delegation-hub
```

That removes the `agent-hub` and `agent-hub-mcp` commands — **only** the
commands. It does not remove the clone, and it does not touch the state the
hub wrote into the Git common directory of every repository you used it on:

- artifact/session/live refs under `refs/agent-hub/...`
- state, locks, live records, and live leases under
  `<git-common-dir>/agent-hub/`
- hub-owned worktrees under your OS temp directory

This version ships **no general garbage-collection or delete command**.
Before any manual cleanup, recover or close active/orphaned live sessions —
a lease is an ownership claim, and a live provider may still be running under
it — and never delete a lock record whose owner you cannot prove is gone (see
the `LOCK_UNRECOVERABLE` rule under Safety).

## Quick start (5 minutes)

Install as above, confirm a provider is actually installed, then try each
surface. The probe reports what it can actually prove about your machine —
which binary answers, with which version — without starting a session.

```sh
agent-hub live probe --agent omp
```

**1. delegate — one task, one agent, a patch you review.**

```sh
agent-hub delegate --agent omp --mode isolated \
  "Fix the flaky test in test/cache.test.ts and run it three times"
```

The agent works in a temporary Git worktree at your `HEAD`; your checkout is
untouched. Read `status`, `changed_files`, and `diff` in the JSON result.
Use `delegate` when one agent should solve one task and you want to review
the patch yourself.

**2. fanout — several agents, one task, compared results.**

```sh
agent-hub fanout --agent omp --agent agy \
  --task "Refactor the parser to drop the regex fallback" --concurrency 2
```

Both candidates run isolated at one pinned base commit and report per-candidate
results plus an aggregate `status`; nothing merges. Use `fanout` when several
agents could plausibly solve the same task, or when you want distinct tasks in
parallel. Add `--judge <agent>` for a ranked recommendation, and only
`--judge` + `--auto-merge` opts in to adoption (see below).

**3. session — the same agent, turn after turn.**

```sh
agent-hub session create --agent omp --task "Implement the first step"
agent-hub session resume <session-id> --task "Continue and run the tests"
```

Each turn runs in a fresh isolated worktree seeded with the previous turn's
committed artifact, so continuation works even if the provider has no native
conversation resume. A failing turn still persists its filesystem state. Use
`session` for multi-turn work that one agent owns.

**4. live — one long interactive run you talk to.**

`live` is a conversation over stdin/stdout, one JSON command per line. A full
exchange with `pi` looks like this (`▸` marks what **you** type on stdin;
everything else is what the hub prints on stdout):

```text
$ agent-hub live --agent pi --workspace ~/repos/app
{"type":"session","session":{"live_session_id":"0f0c9e4c-3d0a-4d4f-9a2e-7b2c0f8f1a3d",
 "session_id":null,"provider":"pi","transport":"pi-rpc","status":"idle",
 "workspace":"/var/folders/…/agent-hub-X0dK2s","base_commit":"2f0c…","current_commit":"2f0c…",
 "capabilities":{"prompt":{"support":"native"},"follow_up":{"support":"native"},…},"warnings":[]}}
▸ {"action":"prompt","text":"Fix the flaky test in test/cache.test.ts"}
{"type":"event","event":{"live_session_id":"0f0c9e4c-…","seq":1,"transport":"pi-rpc",
 "occurred_at":"2026-09-06T10:12:04.221Z","body":{"kind":"tool_start", … }}}
{"type":"event","event":{"…":"…","seq":2,"body":{"kind":"text", … }}}
{"type":"result","result":{"live_session_id":"0f0c9e4c-…","command_id":"…","kind":"prompt",
 "outcome":"succeeded","final_text":{"text":"Tests pass; the fixture was racy.","truncated":false},
 "usage":{…},"checkpoint":{…},"exit_code":null,"exit_signal":null,
 "started_at":"…","finished_at":"…","duration_ms":18342,"error":null},"status":"idle"}
▸ {"action":"follow_up","text":"Now run the full suite and report failures"}
{"type":"result","result":{"…":"…","kind":"follow_up","outcome":"succeeded",…},"status":"idle"}
▸ {"action":"close"}
{"type":"close","close":{"live_session_id":"0f0c9e4c-…","status":"closed",
 "stop":{"status":"closed","exit_code":0,"exit_signal":null,"waited_ms":210},
 "checkpoint_taken":true,"cleanup_errors":[]}}
$ echo $?
0
```

Read the `type` field of every stdout document: `session` (launched — the
first document always), `event` (normalized provider activity), `result` (one
command finished), `error` (a refusal or failure), `close` (terminal shutdown
report). Closing stdin (Ctrl-D) closes the session gracefully too. Use `live`
when one agent should work interactively over many turns — steering,
cancelling, status checks (permission answering is a library-only path; see
[Hermes permissions](#hermes-permissions-deny-vs-interactive)). Exit codes:
`0` clean, `1`
structured failure (launch refusal, failed result, or an `orphaned` end), `2`
usage error.

**5. MCP — the same operations as tools for an MCP client.**

Point your MCP client at the installed server:

```json
{
  "mcpServers": {
    "agent-hub": {
      "command": "agent-hub-mcp"
    }
  }
}
```

It exposes `delegate_task`, `fanout_candidates`, `compete_candidates`,
`session_create`, `session_resume`, and the live tools `live_session_start`,
`live_session_resume`, `live_session_command`, `live_session_events`,
`live_session_close`. Use the MCP server when the orchestrator is an MCP
client rather than a shell. One rule matters most for live sessions: **pin
them to one server process** (see
[Live semantics](#live-session-semantics)).

## Commands (reference)

All CLI commands emit JSON on stdout. Exit codes: `0` success, `1` structured
operation failure (the JSON on stdout carries `error.code`/`error.message` or
the operation's own error field), `2` parse or usage error (message plus usage
on stderr).

### delegate (one-shot)

```sh
agent-hub delegate \
  --agent omp \
  --mode isolated \
  "Implement the requested change and run the relevant tests"
```

Use `--workspace /path/to/repository` to target another checkout. `isolated`
is the safer default for changes because the agent works from the current
`HEAD` in a temporary worktree; the resulting changed-file list and patch are
returned, but nothing is merged automatically. The temporary worktree is
removed on both success and failure.

`direct` runs in the caller's checkout and leaves the agent's edits there.
Both modes reject a dirty repository by default. Pass `--allow-dirty` only
when the existing changes are intentional. In direct mode they remain part of
the result; isolated mode still starts from committed `HEAD`, so caller-local
changes are left untouched and are not carried into the isolated result.

### fanout

```sh
agent-hub fanout \
  --agent omp --agent agy --agent grok \
  --task "Fix the flaky test" \
  --concurrency 2
```

Repeat `--task` once per `--agent` to pair distinct tasks in order, or pass a
single `--task` to give every agent the same assignment.

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

### judged competition and auto-merge

Adoption is opt-in and never implicit: `--auto-merge` additionally requires
`--judge`, and it only follows the judge's internal winner selection.

```sh
agent-hub fanout --agent omp --agent grok --task "Fix it" \
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
agent-hub session create --agent omp --task "Implement the first step"
agent-hub session resume <session-id> --task "Continue and run the tests"
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

### live sessions

A live session is a long-lived interactive provider run driven over the hub
wire, not a one-shot delegation:

```sh
agent-hub live --agent pi --workspace /path/to/repo
agent-hub live --resume <hub-live-id> --workspace /path/to/repo
agent-hub live probe --agent hermes
```

Flags: `--agent` or `--resume` (exactly one), `--workspace` (default the
current directory), `--max-output-bytes` (cap on captured text; default 64
KiB per text blob).

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

### live probe — what the capability check is (and is not)

```sh
agent-hub live probe --agent <omp|agy|pi|hermes>
```

The probe is a **cheap binary/version/capability check, never an
authenticated turn**. It runs only version/help invocations with an argument
array and `shell: false` — `omp --version`, `pi --version`, `hermes --version`,
`agy --version` plus `agy --help` — and never launches an interactive session
(`omp/pi --mode rpc` and `hermes acp` are deliberately not started), never
sends a prompt, and never touches credentials, quotas, or your repository.
It reports what it can prove about the installed binary: found or not, the
version it self-reports, and — for `agy` — whether the installed `--help`
actually advertises the resume flag the transport would emit. A not-found
probe is honest data but exits `1`.

Verified baselines — the exact versions this hub's live dialects and flags
were checked against:

| Provider | Verified baseline |
| --- | --- |
| OMP (`omp`) | 18.1.10 |
| PI (`pi`) | 0.85.0 |
| AGY (`agy`) | 1.1.26 |
| Hermes (`hermes`) | 0.20.5 |

`omp`/`pi` report any other version as an *unverified dialect* rather than
silently accepting or rejecting it. Resume claims are decided by the binaries'
own answers, not by version comparison: AGY's is re-verified from `--help` at
every launch, and Hermes' `loadSession` advertisement can only be observed by
the runtime `initialize` handshake — the probe never starts `hermes acp`.

### Provider capability table

Claims come from each transport's launch snapshot; every non-`unsupported`
claim carries evidence and the hub refuses transports whose claims do not.
`native` = the provider protocol does it; `hub-queued` = the hub holds the
input and delivers it at the next turn boundary; `derived` = answered from
stream evidence, never forwarded as a request; `signal` = an OS signal, not a
protocol message.

| Capability | OMP (omp-rpc) | PI (pi-rpc) | AGY (agy-stream-json) | Hermes (hermes-acp) |
| --- | --- | --- | --- | --- |
| prompt | native | native | native | native |
| follow_up | native | native | hub-queued | hub-queued |
| steer | native (mid-turn only) | native (mid-turn only) | unsupported | unsupported |
| cancel | native (`abort`) | native (`abort`) | signal ¹ | native (`session/cancel`) |
| status | native (`get_state`) | native (`get_state`) | derived | derived |
| permission_response | unsupported | unsupported | unsupported | native ² |
| resume | native | native | native ³ | native ⁴ |
| checkpoint | unsupported | unsupported | unsupported | unsupported |
| usage_reporting | unsupported | native | derived | unsupported |

¹ AGY cancel is SIGINT to the hub-owned process group; on platforms without
reliable process-group signals the claim honestly drops to `unsupported`.
² The ACP round-trip is native, but the CLI and MCP builds run Hermes
**headless**: every permission request is auto-denied. See
[Hermes permissions](#hermes-permissions-deny-vs-interactive).
³ Only while the installed binary's own `--help` advertises `--conversation`
— re-verified against the installed binary at every resume.
⁴ Only when Hermes' own `initialize` response advertises `loadSession`;
verified during the session-open handshake, never assumed at probe time.

A refused command is returned as `outcome: "unsupported"` with a
`stage: "capability"` error and is never delivered — not a silent no-op, not
a retry-with-hope situation. If only `steer` is unsupported, send a fresh
`follow_up` at the next idle boundary instead.

### Limits and quotas

Fixed bounds (not configuration):

| What | Limit | On overflow |
| --- | --- | --- |
| Fan-out candidates per request | 16 total | usage/parse error (CLI exit `2`, schema refusal in MCP) |
| Fan-out candidates in flight | 8 max (`--concurrency 1..8`, default `min(count, 4)`) | `INVALID_CONCURRENCY` |
| Live sessions per hub process | 8 (`LIVE_PROCESS_SESSION_QUOTA`) | `LIVE_QUOTA_EXCEEDED` |
| Live leases per Git common dir | 4 (`LIVE_COMMON_DIR_SESSION_QUOTA`) | `LIVE_QUOTA_EXCEEDED` — recover or release orphaned leases first |
| Follow-up queue, per session | ≤ 32 queued commands, ≤ 1 MiB queued bytes, ≤ 128 KiB per message | `LIVE_QUEUE_FULL` |
| Event ring, per session | 4096 events / 8 MiB (bounded ring) | eviction; `EVENT_CURSOR_EXPIRED` for stale poll cursors |
| Captured text blobs | 64 KiB default per blob (`--max-output-bytes` to override per session) | truncation flagged on the text |

Two of these are per-process by nature: the 8-session quota counts the
sessions **one hub process** owns (each `agent-hub live` CLI run is its own
process; one `agent-hub-mcp` server is one process), while the 4-lease quota
counts everything anchored on **one repository's Git common dir**. Follow-up
queue contents are in-memory only — they are never written to durable state
and are gone after a crash.

### MCP server and client configuration

Start the stdio server (installed globally, or `node dist/mcp.js` inside the
clone):

```sh
agent-hub-mcp
```

It exposes `delegate_task` (schema and behavior unchanged from v1) plus the
additive v2 tools `fanout_candidates`, `compete_candidates`, `session_create`,
and `session_resume`. `compete_candidates.auto_merge` defaults to `false`.
The additive v3 live tools — `live_session_start`, `live_session_resume`,
`live_session_command` (the six hub actions), `live_session_events` (poll
with a cursor, honest about ring-buffer eviction), and `live_session_close` —
run on the same core live manager; `live_session_command` reports capability
refusals as `outcome: "unsupported"` with `isError`, never as a delivery.
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

**Process pinning (live sessions only).** A running live session lives inside
the hub process that launched it — for an MCP client, the specific
`agent-hub-mcp` process behind one stdio connection. Every
`live_session_command`, `live_session_events`, and `live_session_close` call
for that session must reach the same process; a *different* `agent-hub-mcp`
process answers `LIVE_SESSION_NOT_FOUND` ("no live session … in this hub
process") even though the durable record exists, because it has no live
provider in hand. Practical rules for clients:

- Keep all tool calls for one live session on the same server connection —
  do not load-balance or let the client respawn a second server per task.
- If the server process did restart, that is exactly what
  `live_session_resume` is for: it adopts the durable record from any process.
- The 8-session quota is per server process; more than 8 concurrent live
  sessions means several deliberately separate server processes, each with
  its own pinned sessions.
- The 4-lease-per-common-dir quota is shared across **all** hub processes
  (CLI and every MCP server) pointed at the same repository.

## Live session semantics

### Where a live provider runs, and dirty checkouts

The provider never runs in your checkout. `live` (and `live_session_start`)
materializes a hub-owned worktree in the OS temp directory from the
repository's committed `HEAD`, and the `session` document names it in
`workspace`. Your checkout is used only to identify the repository (common
dir, `HEAD`, ref namespace).

That gives live its own dirty-start semantics, different from `delegate` and
`session create`: **live start never rejects a dirty checkout, because your
uncommitted changes are simply not there.** The provider sees committed
`HEAD` — nothing more, nothing less. There is no `--allow-dirty` flag on live
commands because dirtiness is irrelevant, not ignored: your local edits stay
in your checkout, are never carried in, and are never clobbered. Review the
hub worktree (`session` document's `workspace`) as the provider's working
directory.

### Resume: what is durable, what is fresh

`agent-hub live --resume <hub-live-id>` and `live_session_resume` continue an
existing durable record (`agent-hub-live/v1` under
`<git-common-dir>/agent-hub/live/sessions/`, chain head on
`refs/agent-hub/live/...`). The rules:

- Only a **terminal** record (status `closed`, `error`, or `orphaned`) can be
  resumed — `LIVE_SESSION_NOT_RESUMABLE` for a live one, and
  `LIVE_LEASE_EXISTS` while a lease is still held (recover first).
- A **fresh** hub worktree is materialized at the record's `current_commit`;
  the old worktree path is not assumed to still exist.
- The provider relaunches on the **same transport** the record names
  (`LIVE_TRANSPORT_PAIRING_INVALID` for a mismatch) with its recorded opaque
  resume state: OMP switches to the durable `sessionFile` locator and
  re-echoes it; PI feeds back its observed session handle; AGY adds
  `--conversation <id>` only after re-verifying the flag against the
  installed binary's `--help`; Hermes rides `session/load`, and only if the
  agent advertised `loadSession`. A resume whose provider identity does not
  round-trip fails — there is no silent "start fresh instead".
- The existing live ref is CAS-advanced (revision + 1); the namespace is
  never branched per restart, so lineage stays one chain.

### Hermes permissions: deny vs interactive

Hermes is the only live provider whose transport speaks permission
round-trips (`session/request_permission`), and its policy is fixed:

- **Headless (what the CLI and MCP builds run — always, in this version):**
  every permission request is answered `deny`. The hub still emits a
  `permission_request` event and a warn log line, so you can see what was
  refused; Hermes keeps working within what it can do without approval.
- **Interactive (a library-build option, `interactive: true`; not wired into
  the CLI/MCP surfaces):** the transport waits for your `permission_response`
  for at most 60 seconds (configurable down from 60s, never up), then denies.
  `allow_once` selects the agent's own `allow_once` option — `allow_always`
  is never selected; `deny` selects `reject_once` or cancels. A turn
  cancelled while a permission is pending answers `cancelled`.

Across all providers the hub surface accepts exactly `allow_once` and `deny`
verdicts; anything else is rejected at the boundary, never converted.

### Crash and orphan recovery

A live run carries a lifetime **lease**
(`<git-common-dir>/agent-hub/live/leases/<live-session-id>.lease.json`) that
records the hub process and the provider process-group identity, plus the
worktree path. When a hub process dies with sessions in flight — `kill -9`,
a terminal window closed, a reboot — the provider may still be running under
that lease and the durable record still claims a live status. Symptoms you
will actually see:

- `LIVE_QUOTA_EXCEEDED` on a new start: "this Git common dir already has N
  live leases (quota 4); recover or release orphaned leases first".
- `LIVE_LEASE_EXISTS` on a resume: "still holds a lease; run recovery (or
  release the lease) before resuming".

There is **no CLI recovery command in this version**; recovery is a core
manager call, which is a few lines of code run against the repository:

```js
// recover.mjs — place in the clone, run with: node recover.mjs
import { createLiveManager } from "./dist/index.js";

const manager = await createLiveManager("/path/to/your/repository");
const report = await manager.recover();
console.dir(report, { depth: null });
```

`recover()` reconciles every lease with what the OS and the repository
**prove**, never guessing, and classifies each one:

- `kept-live` — a live hub process (this one, or another with matching
  process identity) owns it: untouched.
- `foreign` — the lease names a different host: reported, untouched.
- `recovered` — the provider was orphaned: it is reaped under start-identity
  proof, any surviving worktree is pinned as a `crash_recovery` checkpoint
  first, the record is rewritten to `orphaned`, then the lease and worktree
  are torn down. Order matters and is enforced: reap → checkpoint → rewrite →
  teardown.
- `cleaned` — the launch never completed (no state record) or the record is
  already terminal: the stale lease is released.
- `manual` — nothing provable (corrupt lease record, reused pid, unverifiable
  provider identity, an orphan that survived bounded termination): reported
  with a reason and **deliberately not touched**.

After a `recovered` (or `closed`) outcome the record is terminal and the
lease is gone, so `agent-hub live --resume <id> --workspace /path/to/repo`
(or `live_session_resume`) continues the chain in a fresh worktree. A
`manual` outcome is your cue to inspect before deleting anything: prove the
process group is really dead (`ps -p <pgid>` with the lease's recorded pid),
confirm nothing owns the lease file, and only then remove the lease record —
same discipline as the lock records below. This version ships no general
GC/delete command, so those records are the operator's to clear by hand.

## Adapter configuration

Each adapter uses argument arrays and never interpolates task text into a
shell command. Override the executable with:

```text
AGENT_HUB_OMP_BIN
AGENT_HUB_AGY_BIN
AGENT_HUB_GROK_BIN
AGENT_HUB_HERMES_BIN
```

Override arguments with a JSON array in the corresponding `*_ARGS` variable.
Use the literal `{task}` item where the task should be inserted; if omitted,
the task is appended:

```sh
# Smoke-test the pipeline with the system echo instead of a real agent:
export AGENT_HUB_OMP_BIN=/bin/echo
export AGENT_HUB_OMP_ARGS='["--prompt", "{task}"]'
```

Built-in defaults are `omp -p <task>`, `agy -p <task> --output-format
stream-json`, and `grok -p <task>`. Credentials and remote API assumptions are
intentionally outside this hub. Note that these overrides drive the one-shot
adapters: for live sessions, `agy` and `hermes` honor
`AGENT_HUB_AGY_BIN`/`AGENT_HUB_HERMES_BIN`, while `omp` and `pi` run the
`omp`/`pi` binaries found on `PATH` (live launches use fixed verified argv —
`omp --mode rpc`, `pi --mode rpc`, `agy --input-format stream-json
--output-format stream-json`, `hermes acp` — not `*_ARGS` templates).

## Delegation guidance

The reusable operator guidance is in [`skills/delegate/SKILL.md`](skills/delegate/SKILL.md).
It explains when to choose direct versus isolated execution, how to run and
judge a fan-out, and what an auto-merge will and will never do.

## Safety and scope

How the hub stays safe, grouped by version. These guarantees are enforced by
the core (CLI and MCP share it).

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

Not in scope for this version: queued workflows, remote providers,
cross-repository merges, and any general ref/state garbage collection or
delete command — cleanup is per-command (artifact refs) or operator action
(locks, leases, records), as documented above.
