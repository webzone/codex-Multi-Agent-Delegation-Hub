import { chmod, rm } from "node:fs/promises";
import { spawn } from "node:child_process";

await rm(new URL("../dist/", import.meta.url), { recursive: true, force: true });

const command = process.platform === "win32" ? "tsc.cmd" : "tsc";
const compiler = spawn(command, ["-p", "tsconfig.json"], { stdio: "inherit" });
const exitCode = await new Promise((resolve, reject) => {
  compiler.once("error", reject);
  compiler.once("exit", (code, signal) => resolve(code ?? (signal === null ? 1 : 1)));
});
if (exitCode !== 0) process.exit(exitCode);

for (const entrypoint of ["cli.js", "mcp.js"]) {
  await chmod(new URL(`../dist/${entrypoint}`, import.meta.url), 0o755);
}
