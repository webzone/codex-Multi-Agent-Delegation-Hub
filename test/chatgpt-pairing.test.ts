import { mkdtemp, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { InMemoryTransport } from "@modelcontextprotocol/sdk/inMemory.js";
import { describe, expect, it } from "vitest";

import {
  assertChatGptPairRepository,
  inspectChatGptPair,
  pairRepository,
  readChatGptPair,
  resolvePairedWorkspace,
  unpairRepository,
} from "../src/chatgpt/pairing.js";
import { resolveRepositoryIdentity } from "../src/git.js";
import { createPairedHubServer } from "../src/mcp.js";
import { startWebMcp } from "../src/web-mcp.js";
import { createGitRepository, removeDirectory } from "./helpers.js";

describe("ChatGPT Project pairing", () => {
  it("binds one repository without storing task content and rejects workspace escape", async () => {
    const repository = await createGitRepository();
    const other = await createGitRepository();
    const stateHome = await mkdtemp(join(tmpdir(), "agent-hub-chatgpt-state-"));
    try {
      const pair = await pairRepository({ workspace: repository, name: "web project", stateHome });
      const loaded = await readChatGptPair(pair.pair_id, stateHome);
      expect(loaded.repository.worktree_root).toBe((await resolveRepositoryIdentity(repository)).worktree_root);
      expect(JSON.stringify(loaded)).not.toContain("task");
      await expect(resolvePairedWorkspace(loaded, other)).rejects.toMatchObject({
        code: "CHATGPT_PAIR_WORKSPACE_FORBIDDEN",
      });
      await expect(resolvePairedWorkspace(loaded, repository)).resolves.toBe(loaded.repository.worktree_root);
      await expect(assertChatGptPairRepository(loaded)).resolves.toBeUndefined();

      const pairFile = join(stateHome, "pairs", `${pair.pair_id}.json`);
      const pairDirectory = join(stateHome, "pairs");
      expect((await stat(pairFile)).mode & 0o777).toBe(0o600);
      expect((await stat(pairDirectory)).mode & 0o777).toBe(0o700);
      expect((await inspectChatGptPair(loaded)).status).toBe("ready");

      await unpairRepository(pair.pair_id, stateHome);
      await expect(readChatGptPair(pair.pair_id, stateHome)).rejects.toMatchObject({
        code: "CHATGPT_PAIR_NOT_FOUND",
      });
    } finally {
      await removeDirectory(repository);
      await removeDirectory(other);
      await removeDirectory(stateHome);
    }
  });

  it("pins the web façade schema and fails closed after unpair", async () => {
    const repository = await createGitRepository();
    const stateHome = await mkdtemp(join(tmpdir(), "agent-hub-chatgpt-web-"));
    try {
      const pair = await pairRepository({ workspace: repository, name: "web project", stateHome });
      const server = await createPairedHubServer(pair.pair_id, stateHome);
      const client = new Client({ name: "paired-test", version: "0.0.0" });
      const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
      await Promise.all([server.connect(serverTransport), client.connect(clientTransport)]);

      const tools = await client.listTools();
      for (const tool of tools.tools) {
        expect(JSON.stringify(tool.inputSchema)).not.toContain("workspace");
      }

      await unpairRepository(pair.pair_id, stateHome);
      const probe = await client.callTool({ name: "hub_probe", arguments: { provider: "omp" } });
      expect((probe as { isError?: boolean }).isError).toBe(true);
      expect(JSON.stringify(probe)).toContain("CHATGPT_PAIR_NOT_FOUND");
      await client.close();
    } finally {
      await removeDirectory(repository);
      await removeDirectory(stateHome);
    }
  });

  it("requires the web entrypoint to receive exactly one pairing id", async () => {
    await expect(startWebMcp([])).rejects.toMatchObject({ code: "CHATGPT_PAIR_INVALID" });
    await expect(startWebMcp(["--pair", "pair-id", "extra"])).rejects.toMatchObject({
      code: "CHATGPT_PAIR_INVALID",
    });
  });
});
