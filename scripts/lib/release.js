const SEMVER_PATTERN =
  /^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$/;

export function validateVersion(version) {
  if (!SEMVER_PATTERN.test(version)) {
    throw new Error(`Invalid release version: ${version}`);
  }

  return version;
}

export function synchroniseJsrVersion(packageJson, jsrJson) {
  const version = validateVersion(packageJson.version);

  return {
    ...jsrJson,
    version,
  };
}

export function extractReleaseNotes(changelog, version) {
  validateVersion(version);

  const lines = changelog.split(/\r?\n/);
  const headings = [`## ${version}`, `## [${version}]`];
  const start = lines.findIndex((line) =>
    headings.some(
      (heading) =>
        line === heading || line.startsWith(`${heading}(`) || line.startsWith(`${heading} `),
    ),
  );

  if (start === -1) {
    throw new Error(`CHANGELOG.md has no entry for ${version}`);
  }

  const nextHeading = lines.findIndex((line, index) => index > start && line.startsWith("## "));
  const end = nextHeading === -1 ? lines.length : nextHeading;
  const notes = lines
    .slice(start + 1, end)
    .join("\n")
    .trim();

  if (!notes) {
    throw new Error(`CHANGELOG.md entry for ${version} is empty`);
  }

  return notes;
}

export function validateRelease(packageJson, jsrJson, changelog, reference) {
  const version = validateVersion(packageJson.version);

  if (packageJson.name !== "sqs-consumer") {
    throw new Error(`Unexpected npm package name: ${packageJson.name}`);
  }

  if (jsrJson.name !== "@bbc/sqs-consumer") {
    throw new Error(`Unexpected JSR package name: ${jsrJson.name}`);
  }

  if (jsrJson.version !== version) {
    throw new Error(`JSR version ${jsrJson.version} does not match npm version ${version}`);
  }

  const expectedReferences = [`release/v${version}`, `v${version}`];
  if (!expectedReferences.includes(reference)) {
    throw new Error(`Release reference ${reference} must be ${expectedReferences.join(" or ")}`);
  }

  extractReleaseNotes(changelog, version);

  return {
    name: packageJson.name,
    version,
    tag: `v${version}`,
  };
}
