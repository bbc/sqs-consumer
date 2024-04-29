import { readdir, existsSync, writeFile } from "fs";
import { join } from "path";

const buildDir = "./dist";
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
          (err) => {
            if (err) {
              throw err;
            }
          },
        );
      }
    });
  });
}

buildPackageJson();
