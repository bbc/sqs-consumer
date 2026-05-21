import { assert } from "chai";
import { describe, it } from "vitest";

import { StandardError, TimeoutError } from "../../src/errors.js";

describe("errors", () => {
  describe("TimeoutError", () => {
    it("uses the default message when none is provided", () => {
      const error = new TimeoutError();

      assert.equal(error.message, "Operation timed out.");
    });

    it("uses the provided message", () => {
      const error = new TimeoutError("Custom timeout.");

      assert.equal(error.message, "Custom timeout.");
    });
  });

  describe("StandardError", () => {
    it("uses the default message when none is provided", () => {
      const error = new StandardError();

      assert.equal(error.message, "An unexpected error occurred:");
    });

    it("uses the provided message", () => {
      const error = new StandardError("Custom failure.");

      assert.equal(error.message, "Custom failure.");
    });
  });
});
