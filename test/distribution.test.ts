import { access, readFile } from "node:fs/promises";

import { describe, expect, it } from "vitest";

const root = new URL("../", import.meta.url);

async function file(path: string): Promise<string> {
  return readFile(new URL(path, root), "utf8");
}

describe("distribution contract", () => {
  it("publishes the CLI, MCP server, and skill from one package", async () => {
    const packageJson = JSON.parse(await file("package.json")) as {
      version: string;
      files: string[];
      bin: Record<string, string>;
    };
    expect(packageJson.version).toBe("0.3.0");
    expect(packageJson.files).toEqual(expect.arrayContaining(["dist", "skills/agent-hub/SKILL.md"]));
    expect(packageJson.bin).toEqual({
      "agent-hub": "./dist/cli.js",
      "agent-hub-mcp": "./dist/mcp.js",
      "agent-hub-web-mcp": "./dist/web-mcp.js",
    });
    await expect(access(new URL("skills/agent-hub/SKILL.md", root))).resolves.toBeUndefined();
    expect(await file("skills/agent-hub/SKILL.md")).toContain("name: agent-hub");
  });

  it("keeps the lockfile version aligned with package.json", async () => {
    const packageJson = JSON.parse(await file("package.json")) as { version: string };
    const lockfile = JSON.parse(await file("package-lock.json")) as {
      version: string;
      packages: { "": { version: string } };
    };
    expect(lockfile.version).toBe(packageJson.version);
    expect(lockfile.packages[""].version).toBe(packageJson.version);
  });
});
