import { readdir, existsSync, writeFile } from "node:fs";
import { join } from "node:path";

const buildDir = "./dist";
/**
 * Adds package.json files to the build directory.
 * @returns {void}
 */
function buildPackageJson() {
  readdir(buildDir, (err, dirs) => {
    if (err) {
      throw err;
    }
    dirs.forEach((dir) => {
      if (dir === "types") {
        return;
      }

      const packageJsonFile = join(buildDir, dir, "/package.json");

      if (!existsSync(packageJsonFile)) {
        const value =
          dir === "esm" ? '{"type": "module"}' : '{"type": "commonjs"}';

        writeFile(
          packageJsonFile,
          new Uint8Array(Buffer.from(value)),
          (writeErr) => {
            if (writeErr) {
              throw writeErr;
            }
          },
        );
      }
    });
  });
}

buildPackageJson();
