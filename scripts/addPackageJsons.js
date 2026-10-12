import { readdir, existsSync, writeFile } from "node:fs";
import { join } from "node:path";

const buildDir = "./dist";
/**
 * Adds package.json files to the build directory.
 * @returns {void}
 */
function buildPackageJson() {
  readdir(buildDir, { withFileTypes: true }, (err, entries) => {
    if (err) {
      throw err;
    }
    entries.forEach((entry) => {
      if (!entry.isDirectory() || entry.name === "types") {
        return;
      }

      const packageJsonFile = join(buildDir, entry.name, "/package.json");

      if (!existsSync(packageJsonFile)) {
        const value = entry.name === "esm" ? '{"type": "module"}' : '{"type": "commonjs"}';

        writeFile(packageJsonFile, new Uint8Array(Buffer.from(value)), (writeErr) => {
          if (writeErr) {
            throw writeErr;
          }
        });
      }
    });
  });
}

buildPackageJson();
