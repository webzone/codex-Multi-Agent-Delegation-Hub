/**
 * Shared vocabulary for the hub's durable records.
 *
 * The rewrite deleted the v1/v2 one-shot vocabulary (delegate/fanout/
 * competition/session/merge). What remains is exactly what the durable
 * session record needs to name its repository and its errors.
 */

/**
 * Repository state captured exactly once before a launch begins. Every
 * session in the batch pins to `head`; `branch` records the attached branch
 * of the caller checkout at capture time (null when detached).
 */
export interface RepositoryIdentity {
  /** Absolute path of the Git common dir; where Agent Hub repository-local state lives. */
  common_dir: string;
  /** Absolute path of the caller's worktree root. */
  worktree_root: string;
  /** Attached branch short name, or null for a detached HEAD. */
  branch: string | null;
  /** Full commit SHA that sessions branch from. */
  head: string;
}

/**
 * The structured-error shape every hub surface answers with: a stable
 * machine `code` and a hub-generated `message` (provider stderr and
 * transcripts never become error messages).
 */
export interface DelegateError {
  code: string;
  message: string;
}
