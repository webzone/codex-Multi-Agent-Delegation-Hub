import { AgentHubError } from "../errors.js";
import type { AgentAdapter } from "../types.js";
import { createAgyAdapter } from "./agy.js";
import { createGrokAdapter } from "./grok.js";
import { createOmpAdapter } from "./omp.js";

export const supportedAgents = ["omp", "agy", "grok"] as const;

export function resolveAdapter(agent: string, environment: NodeJS.ProcessEnv = process.env): AgentAdapter {
  switch (agent) {
    case "omp":
      return createOmpAdapter(environment);
    case "agy":
      return createAgyAdapter(environment);
    case "grok":
      return createGrokAdapter(environment);
    default:
      throw new AgentHubError(
        "UNKNOWN_AGENT",
        `Unsupported agent "${agent}". Choose one of: ${supportedAgents.join(", ")}`,
      );
  }
}

export {
  asNativeResumeCapableAdapter,
  PROVIDER_SESSION_METADATA_KEY,
  readProviderSessionId,
} from "./types.js";
export type {
  AdapterFactory,
  NativeResumeCapableAdapter,
  NativeResumeCapability,
} from "./types.js";
