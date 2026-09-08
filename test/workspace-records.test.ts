import { randomUUID } from "node:crypto";
import { homedir } from "node:os";
import { isAbsolute, resolve } from "node:path";

import { describe, expect, it } from "vitest";

import { AgentHubError } from "../src/errors.js";
import { SESSION_SCHEMA_VERSION } from "../src/kernel/contracts.js";
import { resolveHubHome } from "../src/workspace/home.js";
import {
  parseResultRecord,
  parseRuntimeMirror,
  parseWorkspaceRecord,
  parseWorkspaceTransaction,
  WORKSPACE_RESULT_SCHEMA_VERSION,
  WORKSPACE_RUNTIME_SCHEMA_VERSION,
  WORKSPACE_SCHEMA_VERSION,
  WORKSPACE_TRANSACTION_SCHEMA_VERSION,
  workspaceRefFor,
} from "../src/workspace/records.js";
import { fullCapabilities } from "./kernel-fakes.js";

/**
 * Unit proof for the durable record shapes (P2 WorkspaceLifecycle):
 * hub-home resolution precedence and the rebuild-from-known-keys parsers.
 * Every rejection is a structural lie the hub must never persist or replay.
 */

const SESSION = randomUUID();
const BASE = "a".repeat(40);
const TREE = "b".repeat(40);
const HEAD = "c".repeat(40);
const NEW = "d".repeat(40);
const TREE2 = "e".repeat(40);
const ISO = "2026-09-07T00:00:00.000Z";

type Json = Record<string, unknown>;

function expectInvalid(run: () => unknown, label: string, code = "WORKSPACE_RECORD_INVALID"): AgentHubError {
  let error: unknown;
  try {
    run();
  } catch (caught) {
    error = caught;
  }
  if (error === undefined) {
    throw new Error(`expected "${label}" to be refused, nothing was thrown`);
  }
  expect(error, label).toBeInstanceOf(AgentHubError);
  expect((error as AgentHubError).code, `${label} (code)`).toBe(code);
  return error as AgentHubError;
}

const baseRecord = (over: Json = {}): Json => ({
  schema: WORKSPACE_SCHEMA_VERSION,
  session_id: SESSION,
  agent: "worker",
  provider: null,
  transport: null,
  repository_cwd: "/repo",
  identity: { common_dir: "/repo/.git", worktree_root: "/repo", branch: null, head: HEAD },
  base_commit: BASE,
  worktree_path: `/home/test/agent-hub/worktrees/${SESSION}`,
  ref: workspaceRefFor(SESSION),
  custody: "live",
  runtime_status: null,
  head_commit: BASE,
  head_tree: TREE,
  last_result_seq: 0,
  handoff: null,
  retention_until: null,
  closed_at: null,
  close_evidence: null,
  last_error: null,
  revision: 0,
  created_at: ISO,
  updated_at: ISO,
  ...over,
});

const baseResult = (over: Json = {}): Json => ({
  schema: WORKSPACE_RESULT_SCHEMA_VERSION,
  session_id: SESSION,
  seq: 1,
  command_id: "cmd-1",
  kind: "prompt",
  outcome: "succeeded",
  commit: NEW,
  parent: BASE,
  tree: TREE2,
  ref: workspaceRefFor(SESSION),
  tree_changed: true,
  final_text: { text: "done", truncated: false },
  usage: { input_tokens: 10, output_tokens: null, cached_tokens: 0, cost_usd: 0.5 },
  started_at: ISO,
  finished_at: ISO,
  duration_ms: 1000,
  error: null,
  recorded_at: ISO,
  ...over,
});

const sessionRecordJson = (over: Json = {}): Json => ({
  schema: SESSION_SCHEMA_VERSION,
  session_id: SESSION,
  provider: "fake",
  transport: "fake-rpc",
  capabilities: fullCapabilities(),
  workspace: "/fake/workspace",
  max_text_bytes: 65536,
  resume: null,
  status: "idle",
  revision: 0,
  last_error: null,
  created_at: ISO,
  updated_at: ISO,
  ...over,
});

const intentTx = (over: Json = {}): Json => ({
  schema: WORKSPACE_TRANSACTION_SCHEMA_VERSION,
  session_id: SESSION,
  kind: "result",
  reason: "turn_end",
  seq: 1,
  command_id: "cmd-1",
  ref: workspaceRefFor(SESSION),
  expected_ref: null,
  expected_revision: 0,
  capture_phase: "intent",
  new_commit: null,
  tree: null,
  next_record: null,
  result: null,
  prepared_at: ISO,
  ...over,
});

const capturedTx = (over: Json = {}): Json =>
  intentTx({
    capture_phase: "captured",
    new_commit: NEW,
    tree: TREE2,
    next_record: baseRecord({
      custody: "live",
      revision: 1,
      head_commit: NEW,
      head_tree: TREE2,
      last_result_seq: 1,
    }),
    result: baseResult(),
    ...over,
  });

describe("resolveHubHome", () => {
  it("orders explicit > AGENT_HUB_HOME > homedir and always returns an absolute path", () => {
    expect(resolveHubHome("/explicit", { AGENT_HUB_HOME: "/from-env" })).toBe("/explicit");
    expect(resolveHubHome(undefined, { AGENT_HUB_HOME: "/from-env" })).toBe("/from-env");
    // `env: {}` isolates from the ambient process environment.
    expect(resolveHubHome(undefined, {})).toBe(resolve(homedir(), ".local", "share", "agent-hub"));
    // Relative values are made absolute against the cwd, never left relative.
    const relative = resolveHubHome("relative/home", {});
    expect(relative).toBe(resolve("relative/home"));
    expect(isAbsolute(relative)).toBe(true);
    expect(isAbsolute(resolveHubHome(undefined, { AGENT_HUB_HOME: "relative/env-home" }))).toBe(true);
  });
});

describe("parseWorkspaceRecord", () => {
  it("is stable under reparse, drops unknown keys, and normalizes the ref", () => {
    const once = parseWorkspaceRecord(baseRecord());
    expect(once.ref).toBe(workspaceRefFor(SESSION));
    expect(parseWorkspaceRecord(once)).toEqual(once);
    const polluted = baseRecord({
      evil: { smuggled: true },
      // The ref is derived from the session id, never trusted from disk.
      ref: "refs/agent-hub/workspace/elsewhere",
    });
    expect(parseWorkspaceRecord(polluted)).toEqual(once);
    expect(parseWorkspaceRecord(polluted)).not.toHaveProperty("evil");
  });

  it("refuses every structural lie about the custody record", () => {
    const cases: Array<[string, Json | unknown]> = [
      ["wrong schema", baseRecord({ schema: "agent-hub-workspace/v2" })],
      ["array root", [baseRecord()]],
      ["non-uuid session", baseRecord({ session_id: "not-a-uuid" })],
      ["relative repository_cwd", baseRecord({ repository_cwd: "relative/repo" })],
      ["relative worktree_path", baseRecord({ worktree_path: "worktrees/x" })],
      ["short commit", baseRecord({ base_commit: "abc123" })],
      ["non-hex identity head", baseRecord({ identity: { common_dir: "/r/.git", worktree_root: "/r", branch: null, head: "z".repeat(40) } })],
      ["unknown custody", baseRecord({ custody: "paused" })],
      ["unknown runtime_status", baseRecord({ runtime_status: "sleeping" })],
      ["negative revision", baseRecord({ revision: -1 })],
      ["fractional last_result_seq", baseRecord({ last_result_seq: 1.5 })],
      ["empty agent", baseRecord({ agent: "" })],
      ["missing created_at", baseRecord({ created_at: undefined })],
    ];
    for (const [label, value] of cases) {
      expectInvalid(() => parseWorkspaceRecord(value), label);
    }
  });

  it("requires closed custody and an armed retention deadline for any handoff decision", () => {
    const handoff = {
      decision: "accepted",
      result_seq: 1,
      commit: NEW,
      decided_at: ISO,
      consumer: "consumer",
    };
    expectInvalid(
      () => parseWorkspaceRecord(baseRecord({ custody: "live", handoff, retention_until: ISO })),
      "handoff while custody is live",
    );
    expectInvalid(
      () =>
        parseWorkspaceRecord(
          baseRecord({ custody: "closed", closed_at: ISO, handoff, retention_until: null }),
        ),
      "handoff without a retention deadline",
    );
    expectInvalid(
      () =>
        parseWorkspaceRecord(
          baseRecord({
            custody: "closed",
            closed_at: ISO,
            retention_until: ISO,
            handoff: { ...handoff, decision: "pending" },
          }),
        ),
      "unknown handoff decision",
    );
    const armed = parseWorkspaceRecord(
      baseRecord({ custody: "closed", closed_at: ISO, retention_until: ISO, handoff }),
    );
    expect(armed.handoff).toEqual(handoff);
    // Retention without a decision is allowed — it only ever delays GC.
    expect(
      parseWorkspaceRecord(baseRecord({ retention_until: ISO })).handoff,
    ).toBeNull();
  });
});

describe("parseResultRecord", () => {
  it("round-trips exactly and rebuilds from known keys only", () => {
    const once = parseResultRecord(baseResult());
    expect(parseResultRecord(once)).toEqual(once);
    expect(parseResultRecord({ ...baseResult(), smuggled: "text" })).toEqual(once);
    expect(once.usage).toEqual({ input_tokens: 10, output_tokens: null, cached_tokens: 0, cost_usd: 0.5 });
    const minimal = parseResultRecord(
      baseResult({ final_text: null, usage: null, error: null, tree_changed: false }),
    );
    expect(minimal.final_text).toBeNull();
    expect(minimal.usage).toBeNull();
  });

  it("refuses sequences, enums, commits, and error shapes it cannot honor", () => {
    const cases: Array<[string, Json]> = [
      ["zero seq", baseResult({ seq: 0 })],
      ["unknown kind", baseResult({ kind: "epic" })],
      ["unknown outcome", baseResult({ outcome: "partial" })],
      ["non-hex commit", baseResult({ commit: "nope" })],
      ["string tree_changed", baseResult({ tree_changed: "true" })],
      ["negative duration_ms", baseResult({ duration_ms: -1 })],
      ["non-finite usage counter", baseResult({ usage: { input_tokens: "10", output_tokens: null, cached_tokens: null, cost_usd: null } })],
      [
        "kernel-unknown error stage",
        baseResult({
          error: { code: "E", message: "boom", stage: "cosmic", retryable: false, provider: null },
        }),
      ],
      [
        "error without retryable",
        baseResult({ error: { code: "E", message: "boom", stage: "provider", provider: null } }),
      ],
    ];
    for (const [label, value] of cases) {
      expectInvalid(() => parseResultRecord(value), label);
    }
    const errored = parseResultRecord(
      baseResult({
        outcome: "failed",
        error: { code: "E", message: "boom", stage: "provider", retryable: true, provider: "fake" },
      }),
    );
    expect(errored.error).toEqual({ code: "E", message: "boom", stage: "provider", retryable: true, provider: "fake" });
  });
});

describe("parseRuntimeMirror", () => {
  it("parses a valid mirror, defaults the recovery rewrite flag, and keeps the kernel record intact", () => {
    const mirror = parseRuntimeMirror({
      schema: WORKSPACE_RUNTIME_SCHEMA_VERSION,
      mirrored_at: ISO,
      rewritten_by_recovery: false,
      record: sessionRecordJson(),
    });
    expect(parseRuntimeMirror(mirror)).toEqual(mirror);
    expect(mirror.record.status).toBe("idle");
    expect(mirror.record.capabilities.prompt.support).toBeTypeOf("string");
    const defaulted = parseRuntimeMirror({
      schema: WORKSPACE_RUNTIME_SCHEMA_VERSION,
      mirrored_at: ISO,
      smuggled: 1,
      record: sessionRecordJson(),
    });
    expect(defaulted.rewritten_by_recovery).toBe(false);
    expect(defaulted).not.toHaveProperty("smuggled");
  });

  it("refuses a lying wrapper and propagates the kernel's own verdict on the inner record", () => {
    expectInvalid(
      () => parseRuntimeMirror({ schema: "agent-hub-workspace-runtime/v0", mirrored_at: ISO, record: sessionRecordJson() }),
      "wrong mirror schema",
    );
    expectInvalid(
      () => parseRuntimeMirror({ schema: WORKSPACE_RUNTIME_SCHEMA_VERSION, mirrored_at: "", record: sessionRecordJson() }),
      "empty mirrored_at",
    );
    expectInvalid(
      () =>
        parseRuntimeMirror({
          schema: WORKSPACE_RUNTIME_SCHEMA_VERSION,
          mirrored_at: ISO,
          record: sessionRecordJson({ status: "sleeping" }),
        }),
      "kernel-invalid inner record",
      "SESSION_RECORD_INVALID",
    );
  });
});

describe("parseWorkspaceTransaction", () => {
  it("accepts a coherent intent and a coherent captured transaction, stable under reparse", () => {
    const intent = parseWorkspaceTransaction(intentTx());
    expect(parseWorkspaceTransaction(intent)).toEqual(intent);
    expect(intent.capture_phase).toBe("intent");

    const captured = parseWorkspaceTransaction(capturedTx());
    expect(parseWorkspaceTransaction(captured)).toEqual(captured);
    expect(captured.next_record!.revision).toBe(captured.expected_revision + 1);

    const close = parseWorkspaceTransaction(
      capturedTx({
        kind: "close-capture",
        reason: "close",
        seq: null,
        command_id: null,
        result: null,
        next_record: baseRecord({
          custody: "closed",
          closed_at: ISO,
          revision: 1,
          head_commit: NEW,
          head_tree: TREE2,
        }),
      }),
    );
    expect(close.kind).toBe("close-capture");
    expect(close.result).toBeNull();
    // Unknown keys never survive into the replayable sidecar shape.
    expect(parseWorkspaceTransaction({ ...capturedTx(), junk: [1, 2] })).toEqual(captured);
  });

  it("enforces the seq/kind discipline and keeps intent phase artifact-free", () => {
    const cases: Array<[string, Json]> = [
      ["result transaction without a seq", intentTx({ seq: null })],
      ["close-capture transaction carrying a seq", capturedTx({ kind: "close-capture", reason: "close" })],
      ["intent carrying a captured commit", intentTx({ new_commit: NEW })],
      ["intent carrying a next record", intentTx({ next_record: baseRecord({ revision: 1 }) })],
      ["intent carrying a result", intentTx({ result: baseResult() })],
      ["unknown capture phase", intentTx({ capture_phase: "halfway" })],
      ["garbage expected_ref", intentTx({ expected_ref: "not-a-commit" })],
    ];
    for (const [label, value] of cases) {
      expectInvalid(() => parseWorkspaceTransaction(value), label);
    }
  });

  it("refuses captured transactions whose artifacts disagree with the plan", () => {
    const cases: Array<[string, Json]> = [
      ["captured without a commit", capturedTx({ new_commit: null })],
      ["captured without a tree", capturedTx({ tree: null })],
      ["captured without the next record", capturedTx({ next_record: null })],
      ["captured result without its result record", capturedTx({ result: null })],
      ["close-capture carrying a result", capturedTx({ kind: "close-capture", reason: "close", seq: null, result: baseResult() })],
      ["next record skipping a revision", capturedTx({
        next_record: baseRecord({ revision: 2, head_commit: NEW, head_tree: TREE2, last_result_seq: 1 }),
      })],
      ["next record from another session", capturedTx({
        next_record: baseRecord({ session_id: randomUUID(), revision: 1, head_commit: NEW, head_tree: TREE2 }),
      })],
      ["next record head not the captured commit", capturedTx({
        next_record: baseRecord({ revision: 1, head_commit: "f".repeat(40), head_tree: TREE2, last_result_seq: 1 }),
      })],
      ["result naming another seq", capturedTx({ result: baseResult({ seq: 7 }) })],
      ["result naming another commit", capturedTx({ result: baseResult({ commit: "f".repeat(40) }) })],
    ];
    for (const [label, value] of cases) {
      expectInvalid(() => parseWorkspaceTransaction(value), label);
    }
  });
});
