---
name: agent-hub
description: Run provider-neutral coding-agent sessions (omp, pi, agy, hermes) through Agent Hub — durable worktrees, checkpointed handoff, real-time control.
---

# Run agent sessions through Agent Hub

Use the installed `agent-hub` command when a coding task should be carried by
a separate local agent while the orchestrator keeps review and adoption.

## Choosing a provider

`agent-hub probe` first. Providers and transports (auto-selected; never
guessed):

- `omp` — `omp-rpc`, **RPC v2 dialect only**. No v1 fallback exists: if the
  probe lacks v2 evidence, `found: false` and launches are refused. Install a
  v2-capable build; do not work around the refusal.
- `pi` — `pi-rpc` (JSON-RPC over stdio).
- `agy` — `agy-stream-json`.
- `hermes` — `hermes-acp` (ACP). Hermes permission requests honor the launch
  `--permission-policy` (deny is headless; interactive surfaces requests).

## One-shot runs

```sh
agent-hub start --provider <omp|pi|agy|hermes> --workspace "$PWD" \
  --task "<the task>"
```

The agent works in a hub-owned isolated worktree detached at a captured
commit — your checkout is never the workspace. A dirty caller checkout is
refused unless `--allow-dirty`, and even then your uncommitted changes do not
travel (the base is a commit). The answer is one JSON document: `session`,
`turn`, `close`, `handoff`. Check `turn.outcome`, `turn.checkpoint`,
and `close.cleanup_errors` — a process that exited is not evidence the work
is correct. Exit codes: 0 success, 1 structured failure (JSON on stdout),
2 usage error.

## Reviewing and adopting results

```sh
agent-hub handoff <session-id> --workspace "$PWD"
```

The handoff names the private ref (`refs/agent-hub/live/<id>`), the base and
final commits, the per-checkpoint chain with reasons, changed files, and the
adopt command (`git cherry-pick <base>..<ref>`). **Agent Hub never merges or
applies for you** — review the diff, run the relevant tests in your checkout,
then adopt deliberately.

## Real-time control (attach wire)

`start --attach` / `resume --attach` keep the process attached and speak
NDJSON — one command per line on stdin, documents on stdout:

```
-> {"action":"prompt","text":"..."}   first task, exactly once
-> {"action":"follow_up","text":"..."} next turn (queued while a turn runs)
-> {"action":"steer","text":"..."}    mid-turn guidance
-> {"action":"cancel","reason":"..."} abort the in-flight turn
-> {"action":"status"}                authoritative progress
-> {"action":"permission","request_id":"...","decision":"allow_once|deny"}
-> {"action":"close","mode":"graceful|terminate"}
<- {"type":"session"|"event"|"result"|"error"|"close",...}
```

Trust the capability snapshot, not hope: a result with `outcome:
"unsupported"` and `error.stage: "capability"` means the hub refused the
command pre-dispatch and nothing reached the provider. Retrying is useless;
change the flow (e.g. a fresh `follow_up` at the next idle boundary when only
`steer` is unsupported). Permission verdicts are exactly `allow_once`/`deny`;
anything else is a caller error.

A `cancelled` turn means the hub stopped the provider before the turn ended:
partial work may exist — that is neither a crash nor a success. A
`checkpoint_error` on a settled turn means the turn ended but the durable
chain could not be pinned: treat the chain as stale and investigate before
handoff.

## Durability, resume, and crash recovery

A running session belongs to the hub process that launched it. Sessions are
still durable: every terminal boundary pins a full-state checkpoint commit on
the session's private ref, and the durable record (identity + lineage, never
task text) rides a sidecar under the Git common dir.

- `agent-hub resume <id>` continues a terminal session: fresh hub worktree at
  the chain head, the provider's resume handle replayed with identity
  round-trip verification, the same durable line advanced. Refusals are
  honest: unknown id, still-leased (`gc` first), non-terminal state.
- After a hub-process loss, run `agent-hub gc` (add `--dry-run` to preview).
  It re-proves every lease, reaps only provably-orphaned provider groups,
  pins surviving worktrees as `crash_recovery` checkpoints, rewrites state to
  `orphaned`, and releases leases only after proven teardown. Anything it
  cannot prove is reported `manual` and left untouched — a `manual` outcome
  (exit 1) is an instruction to look, not a crash to retry blindly.
- An `orphaned` close means the hub could not prove the provider died. Its
  lease stays: do not start a sibling session into the same lease namespace
  expecting `gc` to guess; reap first.

Quotas: 8 live sessions per hub process, 4 leases per Git common dir, bounded
follow-up queues. A quota error names the count that tripped it — reconcile
with `gc` or close sessions; do not kill locks or leases by hand unless
`gc`/`status` prove nobody owns them.

## MCP clients

The same surface exists as MCP tools (`agent-hub-mcp`): `hub_start`,
`hub_prompt`, `hub_follow_up`, `hub_steer`, `hub_cancel`,
`hub_command_status`, `hub_permission`, `hub_events`, `hub_close`,
`hub_resume`, `hub_status`, `hub_handoff`, `hub_gc`, `hub_probe`. Keep every
call for one session on the same server process and the same `workspace`
(process pinning); after a host restart, `hub_gc` then `hub_resume` adopt.
Consume events by cursor (`hub_events`): seqs are gapless, `next_cursor` is
your resume point, and an `expired` verdict names the oldest replayable
cursor — resynchronize from durable state, never from a guess.
