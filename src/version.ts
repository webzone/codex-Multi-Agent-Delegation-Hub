import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

interface PackageMetadata {
  name?: unknown;
  version?: unknown;
}

const packagePath = fileURLToPath(new URL("../package.json", import.meta.url));
const packageMetadata = JSON.parse(readFileSync(packagePath, "utf8")) as PackageMetadata;

if (typeof packageMetadata.name !== "string" || typeof packageMetadata.version !== "string") {
  throw new Error(`invalid package metadata in ${packagePath}`);
}

export const PACKAGE_NAME = packageMetadata.name;
export const PACKAGE_VERSION = packageMetadata.version;
