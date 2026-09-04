#!/usr/bin/env node

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { z } from "zod";

import { delegate } from "./delegate.js";
import { supportedAgents } from "./adapters/index.js";

const server = new McpServer({
  name: "codex-multi-agent-delegation-hub",
  version: "0.1.0",
});

server.registerTool(
  "delegate_task",
  {
    description: "Delegate a coding task to a local AI coding agent.",
    inputSchema: {
      task: z.string().min(1),
      agent: z.enum(supportedAgents),
      mode: z.enum(["direct", "isolated"]).default("isolated"),
      workspace: z.string().min(1).default(process.cwd()),
      allow_dirty: z.boolean().default(false),
      max_output_bytes: z.number().int().positive().optional(),
    },
  },
  async ({ task, agent, mode, workspace, allow_dirty, max_output_bytes }) => {
    const result = await delegate({
      task,
      agent,
      mode,
      workspace,
      allowDirty: allow_dirty,
      maxOutputBytes: max_output_bytes,
    });

    return {
      content: [{ type: "text", text: JSON.stringify(result, null, 2) }],
      structuredContent: { ...result } as Record<string, unknown>,
      isError: result.status === "failure",
    };
  },
);

const transport = new StdioServerTransport();
await server.connect(transport);
