---
name: agent-hub
description: Run provider-neutral coding-agent sessions (omp, pi, agy, hermes) through Agent Hub with durable isolated worktrees and real-time control.
---

# Run agent sessions through Agent Hub

Use the installed `agent-hub` command when a coding task should be carried by
a separate local agent while the orchestrator keeps review and adoption.

## Install, upgrade, and uninstall

The npm package is the single distribution unit. It contains the CLI, the
stdio MCP server, and this skill. For Codex, install or upgrade all three with:

```sh
npm install -g agent-hub@latest
agent-hub codex install
agent-hub codex status
```

`agent-hub codex install` copies this skill to
`$CODEX_HOME/skills/agent-hub/SKILL.md` (normally `~/.codex/skills/...`) and
keeps a separate ownership record for each Codex home. It registers the
`agent_hub` MCP server only when it is absent. It does not
overwrite an existing MCP registration unless `--repair-mcp` is explicit. If
the installed skill was edited, the installer refuses to replace it unless
`--force-skill` is explicit. Restart Codex after an upgrade if the skill or
MCP tools are not visible yet.

To remove the Codex integration and then the package:

```sh
agent-hub codex uninstall
npm uninstall -g agent-hub
```

Uninstall never deletes `$AGENT_HUB_HOME`, retained worktrees, results, or
other durable session data. A skill or MCP registration changed outside Agent
Hub is retained rather than deleted.

Other MCP-capable AI agents can use the same `agent-hub-mcp` stdio command and
the JSON MCP configuration in the project README. If they support
`SKILL.md`, copy this packaged file from
`<global-npm-root>/agent-hub/skills/agent-hub/SKILL.md` into their skill
directory. MCP supplies the tools; this skill supplies the operating rules.

When the current agent already exposes `hub_*` MCP tools, use those tools
directly. Otherwise fall back to the installed CLI described below.

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

The agent works in a hub-owned isolated worktree below `AGENT_HUB_HOME` (or
`~/.local/share/agent-hub`) — your checkout is never the workspace. The answer
is one JSON document with `session`, `turn`, `close`, and `next`. Check
`turn.outcome`, `turn.result`, and `close.record.status`; a process that exited
is not evidence the work is correct. Exit codes: 0 success, 1 structured
failure (JSON on stdout), 2 usage error.

## Reviewing and adopting results

```sh
agent-hub handoff <session-id> --workspace "$PWD" \
  --decision accepted --result-seq 1 --commit <commit>
```

Use the exact sequence and commit from `turn.result` or `status`. Handoff is
an `accepted` or `discarded` consumer decision, starts the retention clock,
and never changes the caller's branch. Review and adopt the exact Git result
deliberately before retention cleanup.

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
`publish_error` means the turn ended but its exact result identity could not
be recorded: retain and investigate the workspace before handoff.

## Durability, resume, and crash recovery

A running session belongs to the hub process that launched it. Sessions are
still durable: every terminal prompt or follow-up publishes an exact result
sequence, commit, tree, and private ref. Durable records contain identity and
lineage, never task text or transcripts.

- `agent-hub resume <id>` continues a terminal session: fresh hub worktree at
  the chain head, the provider's resume handle replayed with identity
  round-trip verification, the same durable line advanced. Refusals are
  honest: unknown id, still-leased (`gc` first), non-terminal state.
- After a hub-process loss, run `agent-hub gc`. It re-proves leases and
  recovers only what can be proven. It deletes nothing without an exact
  accepted/discarded handoff and an expired retention deadline. Unacknowledged,
  orphaned, uncertain, or actively referenced work is reported and retained.
- Closing a session retains its worktree, result, lease facts, and ref. An
  `orphaned` close means the hub could not prove the provider died; recovery
  keeps the ownership facts until a later proof.

Quotas: 8 live sessions per hub process, 4 leases per Git common dir, bounded
follow-up queues. A quota error names the count that tripped it — reconcile
with `gc` or close sessions; do not kill locks or leases by hand unless
`gc`/`status` prove nobody owns them.

## MCP clients

The same surface exists as MCP tools (`agent-hub-mcp`): `hub_start`,
`hub_prompt`, `hub_follow_up`, `hub_steer`, `hub_cancel`,
`hub_command_status`, `hub_permission`, `hub_events`, `hub_close`,
`hub_resume`, `hub_status`, `hub_handoff`, `hub_gc`, `hub_probe`. Keep every
call for one session on the same server process and the same `workspace`; after
a host restart, `hub_gc` then `hub_resume` adopt.
Consume events by cursor (`hub_events`): seqs are gapless, `next_cursor` is
your resume point, and an `expired` verdict names the oldest replayable
cursor — resynchronize from durable state, never from a guess.
