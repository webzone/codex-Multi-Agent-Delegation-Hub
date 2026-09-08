# agent-hub

Provider-neutral coding-agent sessions with durable, real-time interaction and
isolated workspaces.

One hub object composes two cores:

- **`InteractionKernel`** — the Git-free interaction core: prompt, follow_up,
  steer, cancel, status, permission, event streaming, resume boundaries.
- **`WorkspaceLifecycle`** — the durable workspace core: hub-owned isolated
  worktrees, exact result identities, handoff, retention, leases, recovery,
  and safe garbage collection.

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

`npm install -g .` installs the `agent-hub` and `agent-hub-mcp` commands built
from `dist/`.

### Upgrading

```sh
cd agent-hub
git pull --ff-only
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

That removes both commands. Durable state is kept under
`$AGENT_HUB_HOME` (default `~/.local/share/agent-hub`) and is **not** touched
by uninstalling. Inspect it with `agent-hub status`, make the required
handoff decisions, and run `agent-hub gc` after retention expires.

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

# Review the exact result identity printed by the one-shot document
agent-hub status <session-id>
```

Every command answers with one JSON document on stdout; human guidance goes
to stderr. Exit codes: `0` success, `1` structured operation failure (the
document carries `error`), `2` usage error.

## Commands

### `agent-hub start`

```
agent-hub start --provider <omp|pi|agy|hermes>
                [--task TEXT] [--workspace DIR]
                [--permission-policy deny|interactive] [--max-output-bytes N]
                [--attach]
```

Reserves resources under a repository-wide admin lock (quota check → worktree
prune → fresh detached worktree at the captured HEAD → exclusive lease),
selects the provider's transport through an honest probe, launches the
provider **inside the hub-owned worktree** (never your checkout), and records
the durable pair: the kernel's session record plus the lifecycle state.

Without `--attach` this is one-shot: `--task` is required, the turn settles,
the session closes, and the final document contains `session`, `turn`,
`close`, and `next`. `turn.result` is the exact result identity to use for
handoff.

### `agent-hub resume <session-id>`

Continues a **closed** durable session from this repository: its retained
hub worktree is reopened at the recorded head, the recorded
provider resume handle is replayed and the provider identity must round-trip
(a transport that cannot show post-handshake resume state is refused — a
silent fresh session would lie), and the **same** durable line + ref advance.
Unknown ids fail; a still-leased session must be reconciled (`gc`) first.
`--task` is delivered as a `follow_up` turn.

### `agent-hub status [session-id]`

Without an id: every durable session in this repository. With one: the
lifecycle state, the kernel mirror record, lease facts, and the session ref.

### `agent-hub handoff <session-id>`

Handoff is an explicit consumer decision for a closed workspace. It must name
the exact current result:

```sh
agent-hub handoff <session-id> \
  --decision accepted|discarded \
  --result-seq N --commit <40-character-commit> \
  --workspace /path/to/repository
```

The commit and sequence come from `turn.result` or `status`. Handoff starts
the retention clock but does not modify the caller's branch. The hub never
merges or applies the work for you.

When automatic cleanup is enabled (the default), handoff also records the
deadline in a durable GC coordinator and starts a detached worker. The worker
outlives a short-lived CLI command, uses a singleton lease, catches up after a
restart; every later default hub startup also re-arms an unfinished schedule,
and it calls the same safe GC checks described below. It never deletes a
workspace merely because its timer fired. If the worker cannot start, the
durable record remains safe and a later hub startup or `agent-hub gc` catches it
up.

### `agent-hub gc`

GC first re-proves leases and recovers only what can be proven. It deletes a
workspace only after an exact `accepted` or `discarded` handoff, an expired
retention deadline, no live or uncertain lease, no active reference, and
consistent Git state. Unacknowledged, orphaned, uncertain, or actively used
work is retained and reported. Exit code `1` means manual review is needed.

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
<- {"type":"result",...}                       a settled command and result identity
<- {"type":"error",error}                      structured refusal/failure (never a hang)
<- {"type":"close",...}                        teardown facts
```

The reader starts before provider startup finishes, so input written during
the handshake is queued and delivered exactly once. It enforces hard bounds
of 128 queued commands, 1 MiB of queued text, 1 MiB per chunk, and 1 MiB per
line; overflow fails closed. Closing stdin is EOF, not cancellation: accepted
commands settle normally before graceful close. An explicit `close` stops
intake immediately without waiting for an open TTY or FIFO, while already
dispatched commands still produce result documents.

Commands are gated against the launch capability snapshot. An
`outcome: "unsupported"` result with a `stage: "capability"` error means the
hub refused the command before dispatch; nothing reached the provider.

## Semantics worth knowing

**Where work happens.** The provider edits a hub-owned isolated worktree below
`AGENT_HUB_HOME` (default `~/.local/share/agent-hub`). Your checkout is only
the identity anchor and is never used as the provider workspace.

**Results are exact.** Every terminal prompt or follow-up turn publishes a
`result_seq`, commit, tree, and private workspace ref. The result is recorded
before custody can be handed off. Durable records store identities and
lineage only: task text, transcripts, and event bodies are not persisted.

**Close retains work.** Closing captures final state and retains the worktree,
results, ref, and ownership facts. If shutdown cannot prove the provider
process group is gone, the session is `orphaned`, not `closed`; recovery and
GC leave it in place until ownership is proven. Deletion is only possible
through GC after the exact handoff and retention checks pass.

**Permissions.** `deny` (default) answers permission requests headlessly;
`interactive` surfaces them verbatim and answers only with `allow_once` or
`deny` — any other verdict is a caller error, never silently converted.

**Ownership.** One hub process owns a running session. MCP requests for a
session must use the same `workspace`, so the supervisor can reuse the owning
hub. After a hub-process loss, `agent-hub gc` re-proves and reconciles before
`agent-hub resume` adopts a retained session.

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
await hub.close(started.session_id);
if (turn.result !== null) {
  await hub.handoff(started.session_id, {
    decision: "accepted",
    result_seq: turn.result.seq,
    commit: turn.result.commit,
  });
}
```

`hub.kernel` is the `InteractionKernel` and `hub.lifecycle` the
`WorkspaceLifecycle`; the kernel is also directly constructible for embedders
that bring their own durable/lifecycle layer (seams: `DurableMirror`,
`onProviderSpawn`, `attached()`, and the transport/provider factory
contracts). Every provider answer carries the launch-scoped capability
snapshot — trust the claim and its evidence, not hope.

## Safety and scope

- The hub never merges, fast-forwards, rebases, or applies to your branch.
  Handoff names the exact result; review and adoption are yours.
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
