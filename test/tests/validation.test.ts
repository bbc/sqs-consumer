import { assert } from "chai";
import { describe, it } from "vitest";

import { isAwsQueueUrl } from "../../src/validation.js";

describe("validation", () => {
  describe("isAwsQueueUrl", () => {
    it("returns true for AWS queue URLs", () => {
      assert.isTrue(isAwsQueueUrl("https://sqs.eu-west-1.amazonaws.com/123456789012/queue"));
      assert.isTrue(isAwsQueueUrl("https://sqs.eu-west-1.amazonaws.com.cn/123456789012/queue"));
      assert.isTrue(isAwsQueueUrl("https://sqs.eu-west-1.api.aws/123456789012/queue"));
    });

    it("returns false for non-HTTPS AWS queue URLs", () => {
      assert.isFalse(isAwsQueueUrl("http://sqs.eu-west-1.amazonaws.com/123456789012/queue"));
    });

    it("returns false for non-AWS queue URLs", () => {
      assert.isFalse(isAwsQueueUrl("http://localhost:9324/000000000000/sqs-consumer-data"));
      assert.isFalse(isAwsQueueUrl("https://example.com/000000000000/sqs-consumer-data"));
    });

    it("returns false for invalid queue URLs", () => {
      assert.isFalse(isAwsQueueUrl("not-a-url"));
    });
  });
});
