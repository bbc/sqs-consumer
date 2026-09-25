import { readFile, writeFile } from "node:fs/promises";

import { extractReleaseNotes, synchroniseJsrVersion, validateRelease } from "./lib/release.js";

const readJson = async (path) => {
  return JSON.parse(await readFile(path, "utf8"));
};

const packageJson = await readJson("package.json");
const jsrJson = await readJson("jsr.json");

const [command, argument] = process.argv.slice(2).filter((value) => value !== "--");

if (command === "sync") {
  const synchronisedJsrJson = synchroniseJsrVersion(packageJson, jsrJson);
  await writeFile("jsr.json", `${JSON.stringify(synchronisedJsrJson, null, 2)}\n`);
} else if (command === "validate") {
  const changelog = await readFile("CHANGELOG.md", "utf8");
  const release = validateRelease(packageJson, jsrJson, changelog, argument);
  process.stdout.write(`${JSON.stringify(release)}\n`);
} else if (command === "notes") {
  const changelog = await readFile("CHANGELOG.md", "utf8");
  const notes = extractReleaseNotes(changelog, packageJson.version);
  await writeFile(argument, `${notes}\n`);
} else {
  throw new Error(`Unknown release command: ${command}`);
}
