import { SQSClient } from "@aws-sdk/client-sqs";

export const sqsConfig = {
  region: "eu-west-1",
  endpoint: "http://localhost:4566",
  credentials: {
    accessKeyId: "key",
    secretAccessKey: "secret",
  },
};

export const sqs = new SQSClient(sqsConfig);

export const QUEUE_URL =
  process.env.SQS_QUEUE_URL ||
  "http://localhost:4566/000000000000/sqs-consumer-data";
