import { Given, Then, After } from "@cucumber/cucumber";
import { strictEqual } from "node:assert";
import { PurgeQueueCommand } from "@aws-sdk/client-sqs";
import { pEvent } from "p-event";

import { consumer } from "../utils/consumer/handleMessage.js";
import { producer } from "../utils/producer.js";
import { sqs, QUEUE_URL } from "../utils/sqs.js";

Given("a message is sent to the SQS queue", async () => {
  const params = {
    QueueUrl: QUEUE_URL,
  };
  const command = new PurgeQueueCommand(params);
  const response = await sqs.send(command);

  strictEqual(response.$metadata.httpStatusCode, 200);

  await producer.send(["msg1"]);

  const size = await producer.queueSize();

  strictEqual(size, 1);
});

Then("the message should be consumed without error", async () => {
  consumer.start();

  strictEqual(consumer.status.isRunning, true);

  await pEvent(consumer, "response_processed");

  consumer.stop();
  strictEqual(consumer.status.isRunning, false);

  const size = await producer.queueSize();
  strictEqual(size, 0);
});

Given("messages are sent to the SQS queue", async () => {
  const params = {
    QueueUrl: QUEUE_URL,
  };
  const command = new PurgeQueueCommand(params);
  const response = await sqs.send(command);

  strictEqual(response.$metadata.httpStatusCode, 200);

  await producer.send(["msg2", "msg3", "msg4"]);

  const size = await producer.queueSize();

  strictEqual(size, 3);
});

Then(
  "the messages should be consumed without error",
  { timeout: 2 * 5000 },
  async () => {
    consumer.start();

    strictEqual(consumer.status.isRunning, true);

    await pEvent(consumer, "message_received");
    const size = await producer.queueSize();

    strictEqual(size, 2);

    await pEvent(consumer, "message_received");

    const size2 = await producer.queueSize();

    strictEqual(size2, 1);

    await pEvent(consumer, "message_received");

    const size3 = await producer.queueSize();

    strictEqual(size3, 0);

    consumer.stop();

    strictEqual(consumer.status.isRunning, false);
  },
);

After(() => consumer.stop());
