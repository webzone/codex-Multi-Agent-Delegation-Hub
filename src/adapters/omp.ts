import { createCommandAdapter } from "./command-adapter.js";

export function createOmpAdapter(environment: NodeJS.ProcessEnv = process.env) {
  // No native resume capability: omp's installed-command resume syntax has
  // never been verified here, so we refuse to invent flags. Sessions
  // involving omp always continue via the filesystem path (a fresh worktree
  // at the session artifact commit); see src/session.ts.
  return createCommandAdapter(
    {
      id: "omp",
      environmentPrefix: "AGENT_HUB_OMP",
      defaultExecutable: "omp",
      defaultArguments: ["-p", "{task}"],
    },
    environment,
  );
}
