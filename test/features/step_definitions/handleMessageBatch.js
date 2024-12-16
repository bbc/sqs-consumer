import { Given, Then, After } from "@cucumber/cucumber";
import { strictEqual } from "node:assert";
import { PurgeQueueCommand } from "@aws-sdk/client-sqs";
import { pEvent } from "p-event";

import { consumer } from "../utils/consumer/handleMessageBatch.js";
import { producer } from "../utils/producer.js";
import { sqs, QUEUE_URL } from "../utils/sqs.js";

Given("a message batch is sent to the SQS queue", async () => {
  const params = {
    QueueUrl: QUEUE_URL,
  };
  const command = new PurgeQueueCommand(params);
  const response = await sqs.send(command);

  strictEqual(response.$metadata.httpStatusCode, 200);

  await producer.send(["msg1", "msg2", "msg3", "msg4"]);

  const size = await producer.queueSize();

  strictEqual(size, 4);
});

Then("the message batch should be consumed without error", async () => {
  consumer.start();

  strictEqual(consumer.status.isRunning, true);

  await pEvent(consumer, "response_processed");

  consumer.stop();
  strictEqual(consumer.status.isRunning, false);

  const size = await producer.queueSize();
  strictEqual(size, 0);
});

Given("message batches are sent to the SQS queue", async () => {
  const params = {
    QueueUrl: QUEUE_URL,
  };
  const command = new PurgeQueueCommand(params);
  const response = await sqs.send(command);

  strictEqual(response.$metadata.httpStatusCode, 200);

  await producer.send(["msg1", "msg2", "msg3", "msg4", "msg5", "msg6"]);

  const size = await producer.queueSize();

  strictEqual(size, 6);
});

Then(
  "the message batches should be consumed without error",
  { timeout: 2 * 5000 },
  async () => {
    consumer.start();

    strictEqual(consumer.status.isRunning, true);

    await pEvent(consumer, "message_received");

    const size = await producer.queueSize();
    strictEqual(size, 1);

    await pEvent(consumer, "message_received");

    const size2 = await producer.queueSize();
    strictEqual(size2, 0);

    consumer.stop();
    strictEqual(consumer.status.isRunning, false);
  },
);

After(() => consumer.stop());
