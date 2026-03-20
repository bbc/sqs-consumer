import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    environment: "node",
    include: ["test/tests/**/*.test.ts"],
    dangerouslyIgnoreUnhandledErrors: true,
    coverage: {
      provider: "v8",
      include: ["src/**/*.ts"],
    },
  },
});
