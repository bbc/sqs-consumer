import { Producer } from "sqs-producer";

import { QUEUE_URL, sqsConfig, sqs } from "./sqs.js";

export const producer = Producer.create({
  queueUrl: QUEUE_URL,
  region: sqsConfig.region,
  sqs,
});
