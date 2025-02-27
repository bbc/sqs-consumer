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

  await new Promise(resolve => setTimeout(resolve, 1000));

  const size = await producer.queueSize();
  
  if (size > 0) {
    await sqs.send(command);
    await new Promise(resolve => setTimeout(resolve, 1000));
    const sizeAfterSecondPurge = await producer.queueSize();
    strictEqual(sizeAfterSecondPurge, 0, "Queue should be empty after second purge");
  } else {
    strictEqual(size, 0, "Queue should be empty after purge");
  }

  await producer.send(["msg1", "msg2", "msg3"]);

  const size2 = await producer.queueSize();
  strictEqual(size2, 3, "Queue should have exactly 3 messages");
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

    strictEqual(numProcessed, 3, "Should process exactly 3 messages");

    const size = await producer.queueSize();
    strictEqual(size, 0, "Queue should be empty after processing");
  },
);

After(async () => {
  consumer.stop();
  
  await sqs.send(new PurgeQueueCommand({ QueueUrl: QUEUE_URL }));
});
