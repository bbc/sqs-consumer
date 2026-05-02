import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: ["test/tests/**/*.test.ts"],
    pool: "threads",
    coverage: {
      provider: "v8",
      include: ["src/**/*.ts"],
    },
  },
});
