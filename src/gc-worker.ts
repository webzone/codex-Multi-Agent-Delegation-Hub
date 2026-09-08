#!/usr/bin/env node
import { runGcWorker } from "./workspace/gc-coordinator.js";

const homeFlag = process.argv.indexOf("--home");
const home = homeFlag >= 0 ? process.argv[homeFlag + 1] : process.env.AGENT_HUB_HOME;

if (home === undefined || home.trim() === "") {
  process.exitCode = 2;
} else {
  await runGcWorker(home);
}

