#!/usr/bin/env node
/**
 * Launches the Next.js CLI with `--no-experimental-webstorage`.
 *
 * Node 22.4+ exposes a global `localStorage` on the server. It is not a
 * working Storage unless `--localstorage-file` points somewhere valid, and
 * code that feature-detects storage with `typeof localStorage !== "undefined"`
 * then calls `localStorage.getItem` and throws during SSR — every page render
 * 500s. Disabling the experiment restores the expected "no localStorage on
 * the server" world.
 *
 * The flag doesn't exist before 22.4 and Node refuses to start with an
 * unrecognized execArgv entry, so it's only passed when supported.
 */
const { spawn } = require("child_process");
const path = require("path");

const [major, minor] = process.versions.node.split(".").map(Number);
const supportsFlag = major > 22 || (major === 22 && minor >= 4);

const nextBin = path.join(
  __dirname,
  "..",
  "node_modules",
  "next",
  "dist",
  "bin",
  "next"
);

const child = spawn(
  process.execPath,
  [
    ...(supportsFlag ? ["--no-experimental-webstorage"] : []),
    nextBin,
    ...process.argv.slice(2),
  ],
  { stdio: "inherit" }
);

child.on("exit", (code, signal) => {
  if (signal) process.kill(process.pid, signal);
  else process.exit(code ?? 0);
});
