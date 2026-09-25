import { describe, expect, it } from "vitest";

import { StandardError, TimeoutError } from "../../src/errors.js";

describe("errors", () => {
  describe("TimeoutError", () => {
    it("uses the default message when none is provided", () => {
      const error = new TimeoutError();

      expect(error.message).toBe("Operation timed out.");
    });

    it("uses the provided message", () => {
      const error = new TimeoutError("Custom timeout.");

      expect(error.message).toBe("Custom timeout.");
    });
  });

  describe("StandardError", () => {
    it("uses the default message when none is provided", () => {
      const error = new StandardError();

      expect(error.message).toBe("An unexpected error occurred:");
    });

    it("uses the provided message", () => {
      const error = new StandardError("Custom failure.");

      expect(error.message).toBe("Custom failure.");
    });
  });
});
