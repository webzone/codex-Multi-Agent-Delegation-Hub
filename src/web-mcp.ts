#!/usr/bin/env node
import { realpathSync } from "node:fs";
import { fileURLToPath } from "node:url";

import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";

import { AgentHubError, asHubError } from "./errors.js";
import { createPairedHubServer } from "./mcp.js";

/**
 * The web façade is deliberately a separate entrypoint. It never falls back
 * to the unrestricted generic MCP server when pairing arguments are absent or
 * malformed.
 */
export async function startWebMcp(argv: readonly string[] = process.argv.slice(2)): Promise<void> {
  if (argv.length !== 2 || argv[0] !== "--pair" || argv[1]?.length === 0) {
    throw new AgentHubError(
      "CHATGPT_PAIR_INVALID",
      "agent-hub-web-mcp requires exactly: --pair <pair-id>",
    );
  }
  const server = await createPairedHubServer(argv[1]);
  await server.connect(new StdioServerTransport());
}

function isEntrypoint(): boolean {
  const invoked = process.argv[1];
  if (invoked === undefined) return false;
  try {
    return realpathSync(fileURLToPath(import.meta.url)) === realpathSync(invoked);
  } catch {
    return false;
  }
}

if (isEntrypoint()) {
  startWebMcp().catch((error) => {
    console.error(JSON.stringify({ error: asHubError(error) }));
    process.exitCode = 1;
  });
}
