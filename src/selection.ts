/**
 * Judge selection parsing (Package 2). Pure by contract: no IO, no throwing,
 * no ambient state — every input maps to a serializable result.
 *
 * All judge text is untrusted. The only accepted signal is exactly one line
 * whose text (after leading whitespace) begins with SELECTION_MARKER_PREFIX,
 * followed by a single-line JSON object with exactly the keys `candidate_id`
 * and `reason`. Consequences, all deliberate:
 *
 * - Any prefix-led line is a marker *attempt*: a hostile or chatty judge that
 *   echoes the format template alongside its real selection produces two
 *   attempts and is rejected, never disambiguated by guessing.
 * - A candidate id is accepted only by exact membership in the eligible list.
 *   SHAs, refs, substrings, fuzzy or trimmed matches are never accepted; a
 *   40-hex commit hash is only valid if a caller literally registered it as
 *   a candidate id.
 * - Zero attempts, multiple attempts, malformed JSON, and schema drift each
 *   get a concrete machine code so callers can branch without parsing text.
 */

/** The only token a judge output line may start with to count as a marker. */
export const SELECTION_MARKER_PREFIX = "AGENT_HUB_SELECTION:";

/**
 * Safe candidate identifier: single path/label-safe segment. Fan-out generates
 * RFC-4122 UUIDs, which match; caller-supplied ids (labels, SHAs, refs with
 * `/`, traversal, whitespace) are rejected at the core boundary.
 */
const SAFE_CANDIDATE_ID_PATTERN = /^(?!.*\.\.)[A-Za-z0-9](?:[A-Za-z0-9._-]{0,126}[A-Za-z0-9_-])?$/;

const MAX_QUOTED_LENGTH = 120;

export function isSafeCandidateId(value: unknown): value is string {
  return typeof value === "string" && SAFE_CANDIDATE_ID_PATTERN.test(value);
}

export type SelectionErrorCode =
  | "NO_SELECTION"
  | "MULTIPLE_SELECTION"
  | "SELECTION_JSON_INVALID"
  | "SELECTION_SCHEMA_INVALID"
  | "SELECTION_CANDIDATE_UNSAFE"
  | "SELECTION_CANDIDATE_NOT_ELIGIBLE";

export interface SelectionError {
  code: SelectionErrorCode;
  message: string;
}

export interface Selection {
  candidate_id: string;
  reason: string;
}

export type SelectionParseResult =
  | { ok: true; selection: Selection }
  | { ok: false; error: SelectionError };

function fail(code: SelectionErrorCode, message: string): SelectionParseResult {
  return { ok: false, error: { code, message } };
}

/** Bounded, JSON-escaped rendering of hostile text for error messages. */
function quote(value: string): string {
  const bounded = value.length > MAX_QUOTED_LENGTH ? `${value.slice(0, MAX_QUOTED_LENGTH)}…` : value;
  return JSON.stringify(bounded);
}

/**
 * Parse judge stdout against the eligible candidate ids.
 *
 * Line splitting treats only `\n` (with optional preceding `\r`) as a line
 * break; a bare `\r` never starts a new line, so text hidden behind an
 * old-style progress-bar overwrite cannot silently become a marker.
 */
export function parseSelection(
  stdout: string,
  eligibleIds: readonly string[],
): SelectionParseResult {
  const lines = stdout.split("\n");
  const attempts: Array<{ line: number; payload: string }> = [];

  for (let index = 0; index < lines.length; index += 1) {
    const raw = lines[index].endsWith("\r") ? lines[index].slice(0, -1) : lines[index];
    const line = raw.trimStart();
    if (line.startsWith(SELECTION_MARKER_PREFIX)) {
      attempts.push({
        line: index + 1,
        payload: line.slice(SELECTION_MARKER_PREFIX.length),
      });
    }
  }

  if (attempts.length === 0) {
    return fail("NO_SELECTION", `No line starts with "${SELECTION_MARKER_PREFIX}"`);
  }
  if (attempts.length > 1) {
    const listed = attempts
      .slice(0, 4)
      .map((attempt) => attempt.line)
      .join(", ");
    const more = attempts.length > 4 ? ` (+${attempts.length - 4} more)` : "";
    return fail(
      "MULTIPLE_SELECTION",
      `Found ${attempts.length} marker lines on lines ${listed}${more}; exactly one is allowed`,
    );
  }

  const attempt = attempts[0];
  let parsed: unknown;
  try {
    parsed = JSON.parse(attempt.payload);
  } catch {
    return fail(
      "SELECTION_JSON_INVALID",
      `Marker on line ${attempt.line} is not valid JSON: ${quote(attempt.payload.trim())}`,
    );
  }
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
    return fail("SELECTION_JSON_INVALID", `Marker on line ${attempt.line} is not a JSON object`);
  }

  const record = parsed as Record<string, unknown>;
  const keys = Object.keys(record);
  const missing = ["candidate_id", "reason"].filter((key) => !keys.includes(key));
  if (missing.length > 0) {
    return fail(
      "SELECTION_SCHEMA_INVALID",
      `Marker on line ${attempt.line} is missing key(s): ${missing.join(", ")}`,
    );
  }
  const extra = keys.filter((key) => key !== "candidate_id" && key !== "reason");
  if (extra.length > 0) {
    return fail(
      "SELECTION_SCHEMA_INVALID",
      `Marker on line ${attempt.line} has unexpected key(s): ${extra.map(quote).join(", ")}`,
    );
  }

  const candidateId = record.candidate_id;
  const reason = record.reason;
  if (typeof candidateId !== "string" || typeof reason !== "string") {
    return fail(
      "SELECTION_SCHEMA_INVALID",
      `Marker on line ${attempt.line}: candidate_id and reason must both be strings`,
    );
  }
  if (reason.trim() === "") {
    return fail(
      "SELECTION_SCHEMA_INVALID",
      `Marker on line ${attempt.line}: reason must be a non-empty string`,
    );
  }
  if (!isSafeCandidateId(candidateId)) {
    return fail(
      "SELECTION_CANDIDATE_UNSAFE",
      `Marker on line ${attempt.line}: candidate_id ${quote(candidateId)} is not a safe identifier`,
    );
  }
  if (!eligibleIds.includes(candidateId)) {
    return fail(
      "SELECTION_CANDIDATE_NOT_ELIGIBLE",
      `Marker on line ${attempt.line}: candidate_id ${quote(candidateId)} is not an eligible candidate (unknown, ineligible, or failed)`,
    );
  }

  return { ok: true, selection: { candidate_id: candidateId, reason } };
}
