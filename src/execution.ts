import { AgentHubError } from "./errors.js";
import { isDirty } from "./git.js";

/**
 * The caller-worktree gate shared by every hub launch (start and resume).
 * The provider never runs in the caller checkout — it runs in a hub-owned
 * detached worktree at a captured commit — so caller dirtiness is only ever
 * a signal of intent, never input. Default: refuse tracked AND untracked
 * local changes; `allowDirty` acknowledges them without carrying them in.
 */
export async function assertCleanUnlessAllowed(
  workspace: string,
  allowDirty?: boolean,
): Promise<void> {
  if (!allowDirty && (await isDirty(workspace))) {
    throw new AgentHubError(
      "DIRTY_WORKTREE",
      "The workspace has uncommitted or untracked changes. Use --allow-dirty only when this is intentional.",
    );
  }
}
