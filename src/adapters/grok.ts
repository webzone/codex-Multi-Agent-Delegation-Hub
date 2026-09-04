import { createCommandAdapter } from "./command-adapter.js";

export function createGrokAdapter(environment: NodeJS.ProcessEnv = process.env) {
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
