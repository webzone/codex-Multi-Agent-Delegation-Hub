import { chmod, lstat, mkdir, mkdtemp, readFile, readlink, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import {
  codexStatus,
  installCodex,
  uninstallCodex,
  type CodexCommandResult,
} from "../src/integrations/codex.js";

interface FakeCodex {
  registered: boolean;
  calls: string[][];
  environments: Array<string | undefined>;
  run: (args: string[], options?: { env?: NodeJS.ProcessEnv }) => Promise<CodexCommandResult>;
}

function fakeCodex(initiallyRegistered = false): FakeCodex {
  const fake: FakeCodex = {
    registered: initiallyRegistered,
    calls: [],
    environments: [],
    async run(args, options) {
      fake.calls.push(args);
      fake.environments.push(options?.env?.CODEX_HOME);
      if (args.join(" ") === "mcp get agent_hub") {
        return fake.registered
          ? { code: 0, stdout: "agent_hub\ncommand: agent-hub-mcp\n", stderr: "" }
          : { code: 1, stdout: "", stderr: "server not found\n" };
      }
      if (args.join(" ") === "mcp add agent_hub -- agent-hub-mcp") {
        fake.registered = true;
        return { code: 0, stdout: "added\n", stderr: "" };
      }
      if (args.join(" ") === "mcp remove agent_hub") {
        fake.registered = false;
        return { code: 0, stdout: "removed\n", stderr: "" };
      }
      return { code: 2, stdout: "", stderr: `unexpected fake codex arguments: ${args.join(" ")}` };
    },
  };
  return fake;
}

async function sandbox(): Promise<{ codexHome: string; stateHome: string; cleanup: () => Promise<void> }> {
  const root = await mkdtemp(join(tmpdir(), "agent-hub-codex-integration-"));
  const codexHome = join(root, "codex");
  const stateHome = join(root, "state");
  return { codexHome, stateHome, cleanup: () => rm(root, { recursive: true, force: true }) };
}

describe("Codex integration", () => {
  it("installs the packaged skill and registers MCP once, then upgrades idempotently", async () => {
    const world = await sandbox();
    const codex = fakeCodex();
    try {
      const first = await installCodex({
        codexHome: world.codexHome,
        stateHome: world.stateHome,
        runCodex: codex.run,
      });
      expect(first.action).toBe("install");
      expect(first.changed).toEqual(["skill", "mcp"]);
      expect(codex.calls.filter((args) => args.join(" ") === "mcp add agent_hub -- agent-hub-mcp")).toHaveLength(1);
      expect(await readFile(join(world.codexHome, "skills", "agent-hub", "SKILL.md"), "utf8")).toContain(
        "name: agent-hub",
      );

      const second = await installCodex({
        codexHome: world.codexHome,
        stateHome: world.stateHome,
        runCodex: codex.run,
      });
      expect(second.changed).toEqual([]);
      expect(codex.calls.filter((args) => args.join(" ") === "mcp add agent_hub -- agent-hub-mcp")).toHaveLength(1);
      expect(second.status.skill.managed).toBe(true);
      expect(second.status.mcp.present).toBe(true);
      expect(codex.environments.filter((home) => home === world.codexHome).length).toBeGreaterThan(0);
    } finally {
      await world.cleanup();
    }
  });

  it("refuses to overwrite a user-edited skill unless explicitly forced", async () => {
    const world = await sandbox();
    const codex = fakeCodex();
    try {
      await installCodex({
        codexHome: world.codexHome,
        stateHome: world.stateHome,
        runCodex: codex.run,
      });
      const skillPath = join(world.codexHome, "skills", "agent-hub", "SKILL.md");
      await writeFile(skillPath, "# local customization\n", "utf8");
      await expect(
        installCodex({
          codexHome: world.codexHome,
          stateHome: world.stateHome,
          runCodex: codex.run,
        }),
      ).rejects.toMatchObject({ code: "INTEGRATION_CONFLICT" });
      expect(await readFile(skillPath, "utf8")).toBe("# local customization\n");

      const forced = await installCodex({
        codexHome: world.codexHome,
        stateHome: world.stateHome,
        forceSkill: true,
        runCodex: codex.run,
      });
      expect(forced.status.skill.managed).toBe(true);
    } finally {
      await world.cleanup();
    }
  });

  it("does not remove a pre-existing MCP registration", async () => {
    const world = await sandbox();
    const codex = fakeCodex(true);
    try {
      const installed = await installCodex({
        codexHome: world.codexHome,
        stateHome: world.stateHome,
        runCodex: codex.run,
      });
      expect(installed.status.mcp.created_by_agent_hub).toBe(false);
      await uninstallCodex({
        codexHome: world.codexHome,
        stateHome: world.stateHome,
        runCodex: codex.run,
      });
      expect(codex.registered).toBe(true);
    } finally {
      await world.cleanup();
    }
  });

  it("rolls back the skill when MCP registration fails", async () => {
    const world = await sandbox();
    const codex = fakeCodex();
    try {
      const failingRunner = async (args: string[], options?: { env?: NodeJS.ProcessEnv }) => {
        if (args.join(" ") === "mcp add agent_hub -- agent-hub-mcp") {
          return { code: 2, stdout: "", stderr: "add failed\n" };
        }
        return codex.run(args, options);
      };
      await expect(
        installCodex({
          codexHome: world.codexHome,
          stateHome: world.stateHome,
          runCodex: failingRunner,
        }),
      ).rejects.toMatchObject({ code: "CODEX_COMMAND_FAILED" });
      await expect(readFile(join(world.codexHome, "skills", "agent-hub", "SKILL.md"), "utf8")).rejects.toThrow();
      await expect(readFile(join(world.codexHome, "config.toml"), "utf8")).rejects.toThrow();
    } finally {
      await world.cleanup();
    }
  });

  it("restores the exact MCP config when repair add fails", async () => {
    const world = await sandbox();
    const configPath = join(world.codexHome, "config.toml");
    const originalConfig = "[mcp_servers.agent_hub]\ncommand = \"old-agent-hub\"\n";
    await mkdir(world.codexHome, { recursive: true });
    await writeFile(configPath, originalConfig, "utf8");
    await chmod(configPath, 0o600);
    let registered = true;
    const runner = async (
      args: string[],
      options?: { env?: NodeJS.ProcessEnv },
    ): Promise<CodexCommandResult> => {
      if (args.join(" ") === "mcp get agent_hub") {
        return registered
          ? { code: 0, stdout: "agent_hub\ncommand: old-agent-hub\n", stderr: "" }
          : { code: 1, stdout: "", stderr: "No MCP server named 'agent_hub' found.\n" };
      }
      if (args.join(" ") === "mcp remove agent_hub") {
        registered = false;
        await writeFile(configPath, "", "utf8");
        return { code: 0, stdout: "removed\n", stderr: "" };
      }
      if (args.join(" ") === "mcp add agent_hub -- agent-hub-mcp") {
        return { code: 2, stdout: "", stderr: "repair add failed\n" };
      }
      return { code: 2, stdout: "", stderr: "unexpected fake codex arguments: " + args.join(" ") };
    };
    try {
      await expect(
        installCodex({
          codexHome: world.codexHome,
          stateHome: world.stateHome,
          repairMcp: true,
          runCodex: runner,
        }),
      ).rejects.toMatchObject({ code: "CODEX_COMMAND_FAILED" });
      await expect(readFile(configPath, "utf8")).resolves.toBe(originalConfig);
      expect((await lstat(configPath)).mode & 0o7777).toBe(0o600);
      await expect(readFile(join(world.codexHome, "skills", "agent-hub", "SKILL.md"), "utf8")).rejects.toThrow();
    } finally {
      await world.cleanup();
    }
  });

  it("refuses MCP mutation when the config path is a symlink", async () => {
    const world = await sandbox();
    const configPath = join(world.codexHome, "config.toml");
    const targetPath = join(world.codexHome, "real-config.toml");
    const originalConfig = "[mcp_servers.agent_hub]\ncommand = \"old-agent-hub\"\n";
    await mkdir(world.codexHome, { recursive: true });
    await writeFile(targetPath, originalConfig, "utf8");
    await symlink(targetPath, configPath);
    let registered = true;
    const calls: string[][] = [];
    const runner = async (args: string[]): Promise<CodexCommandResult> => {
      calls.push(args);
      if (args.join(" ") === "mcp get agent_hub") {
        return registered
          ? { code: 0, stdout: "agent_hub\ncommand: old-agent-hub\n", stderr: "" }
          : { code: 1, stdout: "", stderr: "No MCP server named 'agent_hub' found.\n" };
      }
      if (args.join(" ") === "mcp remove agent_hub") {
        registered = false;
        await rm(configPath);
        await writeFile(configPath, "transient\n", "utf8");
        return { code: 0, stdout: "removed\n", stderr: "" };
      }
      if (args.join(" ") === "mcp add agent_hub -- agent-hub-mcp") {
        return { code: 2, stdout: "", stderr: "repair add failed\n" };
      }
      return { code: 2, stdout: "", stderr: "unexpected fake codex arguments: " + args.join(" ") };
    };
    try {
      await expect(
        installCodex({
          codexHome: world.codexHome,
          stateHome: world.stateHome,
          repairMcp: true,
          runCodex: runner,
        }),
      ).rejects.toMatchObject({ code: "INTEGRATION_STATE_INVALID" });
      await expect(readlink(configPath)).resolves.toBe(targetPath);
      await expect(readFile(targetPath, "utf8")).resolves.toBe(originalConfig);
      await expect(readFile(join(world.codexHome, "skills", "agent-hub", "SKILL.md"), "utf8")).rejects.toThrow();
      expect(calls.map((args) => args.join(" "))).toEqual(["mcp get agent_hub"]);
    } finally {
      await world.cleanup();
    }
  });

  it("does not mutate Codex when MCP lookup fails for a reason other than not-found", async () => {
    const world = await sandbox();
    try {
      const runner = async (): Promise<CodexCommandResult> => ({
        code: 1,
        stdout: "",
        stderr: "permission denied reading Codex config\n",
      });
      await expect(
        installCodex({
          codexHome: world.codexHome,
          stateHome: world.stateHome,
          runCodex: runner,
        }),
      ).rejects.toMatchObject({ code: "CODEX_COMMAND_FAILED" });
      await expect(readFile(join(world.codexHome, "skills", "agent-hub", "SKILL.md"), "utf8")).rejects.toThrow();
    } finally {
      await world.cleanup();
    }
  });

  it("does not claim or delete an identical skill that predated installation", async () => {
    const world = await sandbox();
    const codex = fakeCodex();
    const skillPath = join(world.codexHome, "skills", "agent-hub", "SKILL.md");
    try {
      const packaged = await readFile(new URL("../skills/agent-hub/SKILL.md", import.meta.url), "utf8");
      await mkdir(join(world.codexHome, "skills", "agent-hub"), { recursive: true });
      await writeFile(skillPath, packaged, { encoding: "utf8" });
      const installed = await installCodex({
        codexHome: world.codexHome,
        stateHome: world.stateHome,
        runCodex: codex.run,
      });
      expect(installed.status.skill.managed).toBe(false);
      expect(installed.status.skill.owned_by_agent_hub).toBe(false);

      const removed = await uninstallCodex({
        codexHome: world.codexHome,
        stateHome: world.stateHome,
        runCodex: codex.run,
      });
      expect(removed.changed).toEqual(["mcp", "manifest"]);
      await expect(readFile(skillPath, "utf8")).resolves.toBe(packaged);
    } finally {
      await world.cleanup();
    }
  });

  it("removes only the managed skill and MCP entry", async () => {
    const world = await sandbox();
    const codex = fakeCodex();
    try {
      await installCodex({
        codexHome: world.codexHome,
        stateHome: world.stateHome,
        runCodex: codex.run,
      });
      const result = await uninstallCodex({
        codexHome: world.codexHome,
        stateHome: world.stateHome,
        runCodex: codex.run,
      });
      expect(result.changed).toEqual(["skill", "mcp", "manifest"]);
      await expect(readFile(join(world.codexHome, "skills", "agent-hub", "SKILL.md"), "utf8")).rejects.toThrow();
      expect(codex.registered).toBe(false);
      const status = await codexStatus({
        codexHome: world.codexHome,
        stateHome: world.stateHome,
        runCodex: codex.run,
      });
      expect(status.manifest_present).toBe(false);
      expect(status.skill.exists).toBe(false);
    } finally {
      await world.cleanup();
    }
  });
});
