import { defineConfig } from "oxlint";

export default defineConfig({
  ignorePatterns: ["CHANGELOG.md", "dist", ".github", "test/reports"],
});