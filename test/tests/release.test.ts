import { describe, expect, it } from "vitest";
import {
  extractReleaseNotes,
  synchroniseJsrVersion,
  validateRelease,
  validateVersion,
} from "../../scripts/lib/release.js";

describe("release metadata", () => {
  it("synchronises the JSR version with the npm version", () => {
    expect(
      synchroniseJsrVersion(
        { version: "16.0.0" },
        { name: "@bbc/sqs-consumer", version: "15.0.3" },
      ),
    ).toEqual({
      name: "@bbc/sqs-consumer",
      version: "16.0.0",
    });
  });

  it("rejects invalid versions", () => {
    expect(() => validateVersion("$(echo unsafe)")).toThrow("Invalid release version");
  });

  it("extracts the matching changelog section", () => {
    const changelog =
      "## 15.0.4\n\n### Patch Changes\n\n- Fix polling\n\n## [15.0.3](https://example.com)\n\n- Previous";

    expect(extractReleaseNotes(changelog, "15.0.4")).toBe("### Patch Changes\n\n- Fix polling");
  });

  it("validates matching versions, release branch, and changelog", () => {
    expect(
      validateRelease(
        { name: "sqs-consumer", version: "15.0.4" },
        { name: "@bbc/sqs-consumer", version: "15.0.4" },
        "## 15.0.4\n\n- Fix polling",
        "release/v15.0.4",
      ),
    ).toEqual({ name: "sqs-consumer", version: "15.0.4", tag: "v15.0.4" });
  });

  it("validates a matching release tag", () => {
    expect(
      validateRelease(
        { name: "sqs-consumer", version: "15.0.4" },
        { name: "@bbc/sqs-consumer", version: "15.0.4" },
        "## 15.0.4\n\n- Fix polling",
        "v15.0.4",
      ),
    ).toEqual({ name: "sqs-consumer", version: "15.0.4", tag: "v15.0.4" });
  });

  it("rejects a release from an unexpected reference", () => {
    expect(() =>
      validateRelease(
        { name: "sqs-consumer", version: "15.0.4" },
        { name: "@bbc/sqs-consumer", version: "15.0.4" },
        "## 15.0.4\n\n- Fix polling",
        "feature/unreviewed",
      ),
    ).toThrow("must be release/v15.0.4 or v15.0.4");
  });

  it("rejects an unexpected registry package", () => {
    expect(() =>
      validateRelease(
        { name: "other-package", version: "15.0.4" },
        { name: "@bbc/sqs-consumer", version: "15.0.4" },
        "## 15.0.4\n\n- Fix polling",
        "release/v15.0.4",
      ),
    ).toThrow("Unexpected npm package name");
  });
});
