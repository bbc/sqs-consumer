import { ReceiveMessageCommandOutput } from "@aws-sdk/client-sqs";

import { ConsumerOptions } from "./types.js";

const requiredOptions = [
  "queueUrl",
  // only one of handleMessage / handleMessagesBatch is required
  "handleMessage|handleMessageBatch",
];

function validateOption(
  option: string,
  value: any,
  allOptions: { [key: string]: any },
  strict?: boolean,
): void {
  switch (option) {
    case "batchSize":
      if (value > 10 || value < 1) {
        throw new Error("batchSize must be between 1 and 10.");
      }
      break;
    case "heartbeatInterval":
      if (
        !allOptions.visibilityTimeout ||
        value >= allOptions.visibilityTimeout
      ) {
        throw new Error(
          "heartbeatInterval must be less than visibilityTimeout.",
        );
      }
      break;
    case "visibilityTimeout":
      if (
        allOptions.heartbeatInterval &&
        value <= allOptions.heartbeatInterval
      ) {
        throw new Error(
          "heartbeatInterval must be less than visibilityTimeout.",
        );
      }
      break;
    case "waitTimeSeconds":
      if (value < 1 || value > 20) {
        throw new Error("waitTimeSeconds must be between 0 and 20.");
      }
      break;
    case "pollingWaitTimeMs":
      if (value < 0) {
        throw new Error("pollingWaitTimeMs must be greater than 0.");
      }
      break;
    default:
      if (strict) {
        throw new Error(`The update ${option} cannot be updated`);
      }
      break;
  }
}

/**
 * Ensure that the required options have been set.
 * @param options The options that have been set by the application.
 */
function assertOptions(options: ConsumerOptions): void {
  requiredOptions.forEach((option) => {
    const possibilities = option.split("|");
    if (!possibilities.find((p) => options[p])) {
      throw new Error(
        `Missing SQS consumer option [ ${possibilities.join(" or ")} ].`,
      );
    }
  });

  if (options.batchSize) {
    validateOption("batchSize", options.batchSize, options);
  }
  if (options.heartbeatInterval) {
    validateOption("heartbeatInterval", options.heartbeatInterval, options);
  }
}

/**
 * Determine if the response from SQS has messages in it.
 * @param response The response from SQS.
 */
function hasMessages(response: ReceiveMessageCommandOutput): boolean {
  return response.Messages && response.Messages.length > 0;
}

export { hasMessages, assertOptions, validateOption };
