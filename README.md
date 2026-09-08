# agent-hub

Provider-neutral coding-agent sessions with durable, checkpointed workspaces.

One hub object composes two cores:

- **`InteractionKernel`** — the Git-free interaction core: prompt, follow_up,
  steer, cancel, status, permission, event streaming, resume boundaries.
- **`WorkspaceLifecycle`** — the durable workspace core: hub-owned isolated
  worktrees, a hook-free checkpoint chain pinned on a private Git ref, an
  ownership lease, sidecar/CAS transactional state, safe reconciliation.

Interaction crosses **only** the shipped provider transports, auto-selected
per provider:

| Provider | Transport      | Wire                              |
| -------- | -------------- | --------------------------------- |
| `omp`    | `omp-rpc`      | JSON-RPC — **v2 dialect only**    |
| `pi`     | `pi-rpc`       | JSON-RPC over stdio               |
| `agy`    | `agy-stream-json` | stream-json                    |
| `hermes` | `hermes-acp`   | Agent Client Protocol (ACP)       |

**OMP is RPC v2-only. The hub probes for v2 dialect evidence and refuses the
launch when it is missing; it never falls back to v1, and no code path for v1
remains.** The same honesty gate covers every provider: an unfound binary is
`found: false`, and launching nothing beats guessing.

This is the breaking rewrite: the old `delegate`, `fanout`, `session`,
`live`, competition/judge, and auto-merge surfaces are **gone**, with no
compatibility aliases. What the hub does now is hold a real conversation with
one agent per session, durably, and hand the work back for a human to adopt.

## Requirements

- Node ≥ 20, Git ≥ 2.25 (object hash safety handled; SHA-256 repos supported)
- macOS/Linux (detached process-group ownership + POSIX signal proofs)
- At least one supported provider binary installed for real sessions
  (`omp`, `pi`, `agy`, `hermes`; each honors an `AGENT_HUB_<PROVIDER>_BIN`
  env override for the executable path)

## Install (from Git)

```sh
git clone <repository-url> agent-hub
cd agent-hub
npm ci
npm run build
npm install -g .
```

`npm install -g .` links the `agent-hub` and `agent-hub-mcp` commands built
from `dist/`. Every example below assumes the installed command — the
same binary, the same semantics, no per-repo aliasing.

### Upgrading

```sh
cd agent-hub
git pull
npm ci
npm run build
npm install -g .   # refreshes the same global link
```

The package is `agent-hub` (versioned in `package.json`). The 0.2 line is the
breaking rewrite: nothing from the 0.1 command surface carries over.

### Uninstalling

```sh
npm uninstall -g agent-hub
```

That removes both commands. Repository-local state (`<git-common-dir>/agent-hub/**`,
session refs under `refs/agent-hub/live/…`, hub worktrees under the OS temp
namespace) is **not** touched by uninstalling — reconcile first with
`agent-hub gc` per repository; `handoff` tells you what each session pinned.

## Quick start

```sh
cd /path/to/your/repo          # any Git checkout with at least one commit

# One-shot: start → prompt → wait for the turn → orderly close
agent-hub start --provider omp --task "fix the flaky test in test/foo.test.ts"

# One-shot on another provider, transport auto-selected
agent-hub start --provider pi --task "triage the TODO list in docs/plan.md"

# What is installed and honestly usable right now? Launches nothing.
agent-hub probe
agent-hub probe omp            # v2-only refusal shows up here as found: false

# Review and adopt the result (the hub never merges anything for you)
agent-hub handoff <session-id>
git cherry-pick <base>..<ref>   # exactly what handoff's apply_hint names
```

Every command answers with one JSON document on stdout; human guidance goes
to stderr. Exit codes: `0` success, `1` structured operation failure (the
document carries `error`), `2` usage error.

## Commands

### `agent-hub start`

```
agent-hub start --provider <omp|pi|agy|hermes> [--transport <id>]
                [--task TEXT] [--workspace DIR]
                [--permission-policy deny|interactive] [--max-output-bytes N]
                [--allow-dirty] [--attach]
```

Reserves resources under a repository-wide admin lock (quota check → worktree
prune → fresh detached worktree at the captured HEAD → exclusive lease),
selects the provider's transport through an honest probe, launches the
provider **inside the hub-owned worktree** (never your checkout), and records
the durable pair: the kernel's session record plus the lifecycle state.

Without `--attach` this is one-shot: `--task` is required, the turn settles,
the session closes, and the final document contains `session`, `turn`,
`close`, and `handoff`.

### `agent-hub resume <session-id>`

Continues a **terminal** durable session from this repository: a fresh
hub worktree is materialized at the checkpoint-chain head, the recorded
provider resume handle is replayed and the provider identity must round-trip
(a transport that cannot show post-handshake resume state is refused — a
silent fresh session would lie), and the **same** durable line + ref advance.
Unknown ids fail; a still-leased session must be reconciled (`gc`) first.
`--task` is delivered as a `follow_up` turn.

### `agent-hub status [session-id]`

Without an id: every durable session in this repository. With one: the
lifecycle state, the kernel mirror record, lease facts, and the session ref.

### `agent-hub handoff <session-id>`

The result handoff for a released terminal session: the checkpoint chain
(ref, commits, per-checkpoint reasons), changed files, diff stat, and the
exact human review/adopt command. **The hub never merges, applies, or moves
your branch** — adoption is a `git cherry-pick` you run after review.

### `agent-hub gc [--dry-run]`

Safe reconciliation for this repository:

1. re-prove every ownership lease (foreign hosts, live hub processes, and
   anything unprovable are left completely untouched and reported);
2. reap provably-orphaned provider process groups (bounded
   SIGTERM→SIGKILL, group-death proof required);
3. pin surviving worktrees as a `crash_recovery` checkpoint **before**
   rewriting state to `orphaned`;
4. release leases only after the worktrees they name are proven removed;
5. retry worktree removal for terminal sessions, then `worktree prune`.

`--dry-run` prints every intended action without touching anything. Exit
code `1` means at least one session needs manual review — that is data, not
a crash.

### `agent-hub probe [provider ...]`

Honest probe documents (`found`/`version`/`detail`) for the installed
provider commands. Launches nothing, mutates nothing. `found: false` on omp
means the RPC v2 dialect bar was not met — the hub will refuse omp launches
rather than guess a v1 handshake.

## Real-time interaction: the attach wire

`start --attach` / `resume --attach` keep the process attached and speak
NDJSON: one command per line on stdin, documents on stdout.

```
-> {"action":"prompt","text":"..."}            first task, exactly once
-> {"action":"follow_up","text":"..."}         next turn (queued while running)
-> {"action":"steer","text":"..."}             mid-turn guidance
-> {"action":"cancel","reason":"..."}          abort the in-flight turn
-> {"action":"status"}                         provider's authoritative progress
-> {"action":"permission","request_id":"...","decision":"allow_once|deny","note":"..."}
-> {"action":"close","mode":"graceful|terminate"}
<- {"type":"session",...}                      the launch document
<- {"type":"event",event}                      normalized provider events (gapless seqs)
<- {"type":"result",...}                       a settled command (turn docs pin checkpoints)
<- {"type":"error",error}                      structured refusal/failure (never a hang)
<- {"type":"close",...}                        teardown facts
```

Closing stdin ends the session gracefully. Commands are gated against the
launch's capability snapshot: an `outcome: "unsupported"` result with a
`stage: "capability"` error means the hub refused the command pre-dispatch —
choosing a different provider or flow is the fix, not retrying. A
`checkpoint_error` on a settled turn means the turn ended but the chain
could not be pinned: the session's durable chain is stale and is reported
as such, never silently.

## Semantics worth knowing

**Where work happens.** The provider edits a hub-owned worktree under the OS
temp namespace, detached at a captured commit. Your checkout is only ever the
identity anchor: it is required to be clean unless `--allow-dirty` (the base
is a commit either way; your uncommitted changes never travel).

**Checkpoints are the artifact.** Every terminal boundary the hub observes —
turn end, cancel, error, close — captures the worktree's full working state
as the next commit of a hook-free chain, committed with hub-owned identity
through a sidecar-guarded compare-and-swap on `refs/agent-hub/live/<session-id>`
(the on-disk namespace label is part of the durable storage layout and stays).
Durable records store identity and lineage only: no task text, transcripts,
or event bodies by construction.

**Close only tears down on proof.** If shutdown cannot prove the provider
process group is gone, the session is `orphaned`, not `closed`: the lease,
worktree, and ownership facts stay, and a `terminate`-authorized close or
`gc` finishes the job. A lease is never released unless its worktree removal
ran, under the admin lock, and was proven.

**Permissions.** `deny` (default) answers permission requests headlessly;
`interactive` surfaces them verbatim and answers only with `allow_once` or
`deny` — any other verdict is a caller error, never silently converted.

**Ownership.** One hub process per session. Running sessions live in the
process that launched them (a CLI one-shot, an attached CLI, or the MCP
server). After a hub-process loss: `agent-hub gc` re-proves and reconciles,
`agent-hub resume` adopts.

**Quotas and bounds.** 8 live sessions per hub process, 4 durable leases per
Git common dir, follow-up queues of 32 messages / 1 MiB total / 128 KiB per
message, bounded event ring per session with honest expiry (`EVENT_CURSOR_EXPIRED`
names the oldest replayable cursor). Exceeding a quota is a structured error
with the count that tripped it.

## MCP server

`agent-hub-mcp` speaks MCP over stdio and exposes the same surface as tools:

`hub_start`, `hub_prompt`, `hub_follow_up`, `hub_steer`, `hub_cancel`,
`hub_command_status`, `hub_permission`, `hub_events`, `hub_close`,
`hub_resume`, `hub_status`, `hub_handoff`, `hub_gc`, `hub_probe`.

Client configuration (Claude Desktop / any MCP host):

```json
{
  "mcpServers": {
    "agent-hub": {
      "command": "agent-hub-mcp"
    }
  }
}
```

Every call for a session must name the same `workspace` so it routes back to
the hub process that owns the session (hubs are cached per Git common dir;
the 8-session quota is total for the process, not per repository). Sessions
are in-process state: after the MCP host restarts, `hub_gc` reconciles and
`hub_resume` adopts the durable records.

## Library

```ts
import { AgentHub } from "agent-hub";

const hub = await AgentHub.open("/path/to/repo");
const started = await hub.start({ provider: "omp", permission_policy: "interactive" });
const turn = await hub.prompt(started.session_id, "fix the flaky test");
if (turn.checkpoint === null && turn.checkpoint_error === undefined) {
  // unsupported outcome — refused pre-dispatch, nothing reached the provider
}
const closed = await hub.close(started.session_id);
const handoff = await hub.handoff(started.session_id);
```

`hub.kernel` is the `InteractionKernel` and `hub.lifecycle` the
`WorkspaceLifecycle`; the kernel is also directly constructible for embedders
that bring their own durable/lifecycle layer (seams: `DurableMirror`,
`onProviderSpawn`, `attached()`, and the transport/provider factory
contracts). Every provider answer carries the launch-scoped capability
snapshot — trust the claim and its evidence, not hope.

## Safety and scope

- The hub never merges, fast-forwards, rebases, or applies to your branch.
  Handoff names a ref and a review command; adoption is yours.
- Provider stderr, transcripts, and task text never enter durable records or
  error messages; bounded previews are the only text that crosses.
- Locks: the admin lock serializes worktree/lease administration; the
  per-session lock serializes durable transitions; leases are lifetime
  ownership tokens, released last and only on proof.
- A ref the hub did not create, or that moved under it, is never force-touched.

## Development

```sh
npm ci
npm run typecheck
npm test          # vitest: kernel, lifecycle, transports, hub API, CLI, MCP, docs
npm run hub -- --help
npm run mcp       # stdio MCP server from source
```
