import { AgentHubError } from "./errors.js";

/**
 * Shared durable-store primitives for the hub's sidecar/CAS transitions
 * (consumed by `live/state.ts`, the transactional session store).
 */

/** Full object name: exactly one SHA-1 (40) or SHA-256 (64) hex width. */
const COMMIT_PATTERN = /^(?:[0-9a-f]{40}|[0-9a-f]{64})$/;

/**
 * The all-zero OID Git compares against when a ref must not exist yet. Its
 * width is the object-name width of the repository's hash algorithm, derived
 * from the commit being written — that commit's OID came from this very
 * repository — rather than a hard-coded 40: in a SHA-256 repository Git rejects
 * a 40-zero old value outright ("not a valid old SHA1"), so a fixed width
 * breaks every create there.
 */
export function zeroOidFor(oid: string): string {
  if (!COMMIT_PATTERN.test(oid)) {
    throw new AgentHubError(
      "STATE_INCONSISTENT",
      `"${oid}" is not a full commit id, so its zero OID width cannot be derived`,
    );
  }
  return "0".repeat(oid.length);
}
