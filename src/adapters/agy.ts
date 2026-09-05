import { createCommandAdapter } from "./command-adapter.js";

export function createAgyAdapter(environment: NodeJS.ProcessEnv = process.env) {
  // No native resume capability: agy's installed-command resume syntax has
  // never been verified here, so we refuse to invent flags. Sessions
  // involving agy always continue via the filesystem path (a fresh worktree
  // at the session artifact commit); see src/session.ts.
  return createCommandAdapter(
    {
      id: "agy",
      environmentPrefix: "AGENT_HUB_AGY",
      defaultExecutable: "agy",
      defaultArguments: ["-p", "{task}", "--output-format", "stream-json"],
    },
    environment,
  );
}
