import { readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

import { runCli, type CliIo } from "../src/cli.js";

async function text(path: string): Promise<string> {
  return readFile(new URL(`../${path}`, import.meta.url), "utf8");
}

/** Strip /* … *\/ and // comments so vocabulary scans see code, not prose. */
function stripComments(source: string): string {
  return source
    .replace(/\/\*[\s\S]*?\*\//g, "")
    .replace(/(^|\s)\/\/[^\n]*/g, "$1");
}

async function helpText(): Promise<string> {
  let out = "";
  const io: CliIo = {
    stdin: (async function* empty() {})(),
    stdout: { write: (chunk: string) => (out += chunk) },
    stderr: { write: () => undefined },
  };
  await runCli(["--help"], io, {});
  return out;
}

describe("package surface", () => {
  it("is named agent-hub with agent-hub commands", async () => {
    const pkg = JSON.parse(await text("package.json")) as {
      name: string;
      version: string;
      bin: Record<string, string>;
      scripts: Record<string, string>;
    };
    expect(pkg.name).toBe("agent-hub");
    expect(pkg.bin).toEqual({
      "agent-hub": "./dist/cli.js",
      "agent-hub-mcp": "./dist/mcp.js",
    });
    expect(pkg.scripts.hub).toBe("tsx src/cli.ts");
    expect(pkg.scripts.mcp).toBe("tsx src/mcp.ts");
    expect(pkg.scripts.delegate).toBeUndefined();
  });
});

describe("README", () => {
  it("documents install, upgrade, uninstall, and every command", async () => {
    const readme = await text("README.md");
    for (const required of [
      "npm ci",
      "npm run build",
      "npm install -g .",
      "npm uninstall -g agent-hub",
      "### Upgrading",
      "agent-hub start",
      "agent-hub resume",
      "agent-hub status",
      "agent-hub handoff",
      "agent-hub gc",
      "agent-hub probe",
      "--attach",
      "## Use from Codex",
      "codex mcp add agent_hub -- agent-hub-mcp",
      "hub_steer",
      "hub_events",
      "hub_start",
      "hub_permission",
      "hub_handoff",
      "agent-hub-mcp",
    ]) {
      expect(readme, `README must document: ${required}`).toContain(required);
    }
  });

  it("states the OMP v2-only bar with no v1 fallback", async () => {
    const readme = await text("README.md");
    expect(readme).toContain("v2 dialect only");
    expect(readme).toContain("never falls back to v1");
    const help = await helpText();
    expect(help).toContain("RPC v2 dialect ONLY");
    expect(help).toContain("no v1 fallback");
  });

  it("keeps the deleted vocabulary out of commands and providers", async () => {
    const readme = await text("README.md");
    for (const banned of [
      "agent-hub delegate",
      "agent-hub fanout",
      "agent-hub live",
      "agent-hub session",
      "agent-hub compete",
      "grok",
      "auto-merge the winner",
    ]) {
      expect(readme.toLowerCase(), `README must not document: ${banned}`).not.toContain(banned);
    }
    const help = await helpText();
    for (const banned of ["delegate", "fanout", "compete", "grok", "session create"]) {
      expect(help.toLowerCase()).not.toContain(banned);
    }
    expect(help).toContain("omp");
    expect(help).toContain("hermes");
  });
});

describe("skill", () => {
  it("is the agent-hub skill with the new vocabulary", async () => {
    const skill = await text("skills/agent-hub/SKILL.md");
    expect(skill).toContain("name: agent-hub");
    for (const required of [
      "agent-hub start",
      "agent-hub handoff",
      "agent-hub gc",
      "agent-hub resume",
      "agent-hub probe",
      "RPC v2",
      "hub_start",
    ]) {
      expect(skill, `skill must document: ${required}`).toContain(required);
    }
    for (const banned of ["--agent", "auto-merge", "--judge", "agent-hub delegate", "agent-hub live"]) {
      expect(skill, `skill must not reference: ${banned}`).not.toContain(banned);
    }
  });

  it("has no leftover legacy skill", async () => {
    await expect(text("skills/delegate/SKILL.md")).rejects.toThrow();
  });
});

describe("public library entry", () => {
  it("exports no deleted vocabulary", async () => {
    const code = stripComments(await text("src/index.ts"));
    for (const banned of [
      "delegate",
      "fanOut",
      "fanout",
      "competition",
      "runCompetition",
      "autoMerge",
      "createSession",
      "resumeSession",
      "supportedAgents",
      "grok",
      "LiveSessionManager",
      "runLiveSession",
    ]) {
      expect(code, `src/index.ts must not export: ${banned}`).not.toContain(banned);
    }
    expect(code).toContain("AgentHub");
    expect(code).toContain("InteractionKernel");
    expect(code).toContain("WorkspaceLifecycle");
  });
});
