import { createCommandAdapter } from "./command-adapter.js";

export function createOmpAdapter(environment: NodeJS.ProcessEnv = process.env) {
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
