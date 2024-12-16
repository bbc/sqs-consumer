import { Consumer } from "../../../../dist/esm/consumer.js";

import { QUEUE_URL, sqs } from "../sqs.js";

export const consumer = Consumer.create({
  queueUrl: QUEUE_URL,
  sqs,
  pollingWaitTimeMs: 100,
  async handleMessage(message) {
    return message;
  },
});
