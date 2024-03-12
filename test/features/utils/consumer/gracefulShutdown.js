import { Consumer } from "../../../../dist/consumer.js";

import { QUEUE_URL, sqs } from "../sqs.js";

export const consumer = Consumer.create({
  queueUrl: QUEUE_URL,
  sqs,
  pollingWaitTimeMs: 1000,
  pollingCompleteWaitTimeMs: 5000,
  batchSize: 10,
  handleMessage: async (message) => {
    await new Promise((resolve) => setTimeout(resolve, 1500));
    return message;
  },
});
