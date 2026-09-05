import { describe, expect, it } from "vitest";

import {
  isSafeCandidateId,
  parseSelection,
  SELECTION_MARKER_PREFIX,
} from "../src/selection.js";

const ELIGIBLE = ["cand-1111", "cand-2222"];

function marker(id: string, reason = "cleanest change"): string {
  return `${SELECTION_MARKER_PREFIX} ${JSON.stringify({ candidate_id: id, reason })}`;
}

function codeOf(stdout: string, eligibleIds: readonly string[] = ELIGIBLE): string {
  const result = parseSelection(stdout, eligibleIds);
  expect(result.ok).toBe(false);
  if (result.ok) throw new Error("unreachable");
  return result.error.code;
}

describe("parseSelection", () => {
  it("accepts exactly one well-formed whole-line marker", () => {
    const stdout = `I reviewed both artifacts.\n${marker("cand-2222", "smallest diff, tests pass")}\nDone.`;
    expect(parseSelection(stdout, ELIGIBLE)).toEqual({
      ok: true,
      selection: { candidate_id: "cand-2222", reason: "smallest diff, tests pass" },
    });
  });

  it("accepts indentation, CRLF endings, and a missing final newline", () => {
    expect(parseSelection(`\t  ${marker("cand-1111")}\r\n`, ELIGIBLE).ok).toBe(true);
    expect(parseSelection(marker("cand-1111"), ELIGIBLE).ok).toBe(true);
    expect(parseSelection(`${marker("cand-1111")}\r\nmore notes`, ELIGIBLE).ok).toBe(true);
  });

  it("rejects zero markers, including prose that mentions the prefix", () => {
    const prose = [
      "The required format is AGENT_HUB_SELECTION: {\"candidate_id\":\"cand-1111\",\"reason\":\"x\"} inline in your reply.",
      "agent_hub_selection: {\"candidate_id\":\"cand-1111\",\"reason\":\"x\"}",
      'I considered writing AGENT_HUB_SELECTION: {"candidate_id":"cand-2222","reason":"x"} but here is my answer:',
    ].join("\n");
    expect(codeOf(prose)).toBe("NO_SELECTION");
    expect(codeOf("")).toBe("NO_SELECTION");
    expect(codeOf("no markers at all\njust vibes")).toBe("NO_SELECTION");
  });

  it("text hidden behind a bare \\r never becomes a marker", () => {
    // Classic progress-bar overwrite: the "final" line is really one \n-line.
    expect(codeOf(`working...\r${marker("cand-1111")}`)).toBe("NO_SELECTION");
  });

  it("rejects multiple marker lines, including exact duplicates", () => {
    expect(codeOf(`${marker("cand-1111")}\n${marker("cand-2222")}`)).toBe("MULTIPLE_SELECTION");
    expect(codeOf(`${marker("cand-1111")}\n${marker("cand-1111")}`)).toBe("MULTIPLE_SELECTION");
    const many = Array.from({ length: 6 }, () => marker("cand-1111")).join("\n");
    const result = parseSelection(many, ELIGIBLE);
    expect(!result.ok && result.error.message).toContain("6 marker lines");
  });

  it("a forged format example is a marker attempt and kills the parse", () => {
    // Judge echoes the instruction template (invalid JSON body) next to its
    // real selection: two prefix-led lines, no guessing which one counts.
    const forged = `AGENT_HUB_SELECTION: {candidate_id: <id>, reason: <why>}\n${marker("cand-1111")}`;
    expect(codeOf(forged)).toBe("MULTIPLE_SELECTION");
    // A lone echoed template is a malformed marker, not prose.
    expect(codeOf("AGENT_HUB_SELECTION: {candidate_id: <id>, reason: <why>}")).toBe(
      "SELECTION_JSON_INVALID",
    );
  });

  it("rejects malformed marker JSON", () => {
    const payloads = [
      "{candidate_id: \"cand-1111\"}",
      '["cand-1111"]',
      '"cand-1111"',
      "null",
      '{"candidate_id":"cand-1111","reason":"ok"}{"candidate_id":"cand-2222","reason":"ok"}',
      '{"candidate_id":"cand-1111","reason":"ok"} — see notes above',
      '{"candidate_id":"cand-1111","reason":"ok',
    ];
    for (const payload of payloads) {
      expect(codeOf(`${SELECTION_MARKER_PREFIX} ${payload}`)).toBe("SELECTION_JSON_INVALID");
    }
  });

  it("rejects any schema drift", () => {
    const objects = [
      '{"candidate_id":"cand-1111","reason":"ok","score":9}',
      '{"candidate_id":"cand-1111"}',
      '{"reason":"ok"}',
      '{"candidate_id":"cand-1111","reason":{"text":"ok"}}',
      '{"candidate_id":1111,"reason":"ok"}',
      '{"candidate_id":"cand-1111","reason":"   "}',
      "{}",
    ];
    for (const object of objects) {
      expect(codeOf(`${SELECTION_MARKER_PREFIX} ${object}`)).toBe("SELECTION_SCHEMA_INVALID");
    }
  });

  it("rejects candidate ids that are not safe identifiers", () => {
    const hostile = [
      "refs/heads/main",
      "../../etc/passwd",
      "cand 1",
      " cand-1111", // exact match only: whitespace padding is not trimmed away
      "cand-1111\nAGENT_HUB_SELECTION: x".slice(0, 12),
      "x".repeat(129),
      "",
    ];
    for (const candidateId of hostile) {
      const stdout = `${SELECTION_MARKER_PREFIX} ${JSON.stringify({ candidate_id: candidateId, reason: "ok" })}`;
      expect(codeOf(stdout)).toBe("SELECTION_CANDIDATE_UNSAFE");
    }
  });

  it("accepts ids only by exact eligible-list membership, never SHAs/refs/fuzzy text", () => {
    const artifactSha = "0123456789012345678901234567890123456789";
    const failedCandidateId = "cand-failed";
    const cases = [
      artifactSha, // a real commit SHA is not a selection path
      "cand-1111-forged", // prefix of a real id is not accepted
      "cand", // substring is not accepted
      failedCandidateId, // known-but-failed candidate
      "cand-3333", // unknown id
    ];
    for (const candidateId of cases) {
      const stdout = `${SELECTION_MARKER_PREFIX} ${JSON.stringify({ candidate_id: candidateId, reason: "ok" })}`;
      expect(codeOf(stdout)).toBe("SELECTION_CANDIDATE_NOT_ELIGIBLE");
      expect(codeOf(stdout, [])).toBe("SELECTION_CANDIDATE_NOT_ELIGIBLE");
    }
    // Even a SHA-shaped id wins if (and only if) the caller registered it.
    const registered = parseSelection(
      `${SELECTION_MARKER_PREFIX} ${JSON.stringify({ candidate_id: artifactSha, reason: "ok" })}`,
      [artifactSha],
    );
    expect(registered).toEqual({
      ok: true,
      selection: { candidate_id: artifactSha, reason: "ok" },
    });
  });

  it("error payloads are serializable, bounded, and never echo huge text", () => {
    const huge = "A".repeat(5_000);
    const result = parseSelection(`${SELECTION_MARKER_PREFIX} {"candidate_id":"${huge}"`, ELIGIBLE);
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(typeof result.error.code).toBe("string");
    expect(result.error.message.length).toBeLessThan(400);
    expect(() => JSON.stringify(result)).not.toThrow();
  });
});

describe("isSafeCandidateId", () => {
  it("accepts fan-out UUIDs and label-like ids; rejects everything path/label unsafe", () => {
    expect(isSafeCandidateId("3f2504e0-4f89-41d3-9a0c-0305e82c3301")).toBe(true);
    expect(isSafeCandidateId("cand_1.a-Z")).toBe(true);
    for (const bad of ["", "-lead", ".lead", "has/slash", "has space", "has.dot.", "尾", "a".repeat(129)]) {
      expect(isSafeCandidateId(bad)).toBe(false);
    }
    expect(isSafeCandidateId(42)).toBe(false);
    expect(isSafeCandidateId(null)).toBe(false);
  });
});
