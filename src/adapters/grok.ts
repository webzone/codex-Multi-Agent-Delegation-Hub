import { createCommandAdapter } from "./command-adapter.js";

export function createGrokAdapter(environment: NodeJS.ProcessEnv = process.env) {
  // No native resume capability: grok's installed-command resume syntax has
  // never been verified here, so we refuse to invent flags. Sessions
  // involving grok always continue via the filesystem path (a fresh worktree
  // at the session artifact commit); see src/session.ts.
  return createCommandAdapter(
    {
      id: "grok",
      environmentPrefix: "AGENT_HUB_GROK",
      defaultExecutable: "grok",
      defaultArguments: ["-p", "{task}"],
    },
    environment,
  );
}
