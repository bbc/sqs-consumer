import { Given, Then, After } from "@cucumber/cucumber";
import { strictEqual } from "node:assert";
import { PurgeQueueCommand } from "@aws-sdk/client-sqs";
import { pEvent } from "p-event";

import { consumer } from "../utils/consumer/gracefulShutdown.js";
import { producer } from "../utils/producer.js";
import { sqs, QUEUE_URL } from "../utils/sqs.js";

Given("Several messages are sent to the SQS queue", async () => {
  const params = {
    QueueUrl: QUEUE_URL,
  };
  const command = new PurgeQueueCommand(params);
  const response = await sqs.send(command);

  strictEqual(response.$metadata.httpStatusCode, 200);

  const size = await producer.queueSize();
  strictEqual(size, 0);

  await producer.send(["msg1", "msg2", "msg3"]);

  const size2 = await producer.queueSize();

  strictEqual(size2, 3);
});

Then("the application is stopped while messages are in flight", async () => {
  consumer.start();

  consumer.stop();

  strictEqual(consumer.status.isRunning, false);
});

Then(
  "the in-flight messages should be processed before stopped is emitted",
  async () => {
    let numProcessed = 0;
    consumer.on("message_processed", () => {
      numProcessed++;
    });

    await pEvent(consumer, "stopped");

    strictEqual(numProcessed, 3);

    const size = await producer.queueSize();
    strictEqual(size, 0);
  },
);

After(() => consumer.stop());
