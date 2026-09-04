import { createCommandAdapter } from "./command-adapter.js";

export function createAgyAdapter(environment: NodeJS.ProcessEnv = process.env) {
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
