/* eslint-disable no-undefined -- This is a test file */

import { Given, When, Then, After } from "@cucumber/cucumber";
import { strictEqual, ok, deepStrictEqual } from "node:assert";
import { PurgeQueueCommand } from "@aws-sdk/client-sqs";
import { pEvent } from "p-event";

import { Consumer } from "../../../dist/esm/consumer.js";
import { producer } from "../utils/producer.js";
import { sqs, QUEUE_URL } from "../utils/sqs.js";

let consumer;
let errorEvent;
let messageParam;
let metadataParam;
let receivedEvents = {};

Given("a consumer that will encounter an error without a message", () => {
  const mockSqs = {
    send() {
      throw new Error("Receive error");
    },
  };

  consumer = new Consumer({
    queueUrl: QUEUE_URL,
    sqs: mockSqs,
    handleMessage: async (message) => message,
  });

  consumer.on("error", (err, message, metadata) => {
    errorEvent = err;
    messageParam = message;
    metadataParam = metadata;
  });
});

Given("a consumer that will encounter an error with a message", () => {
  const mockSqs = {
    send(command) {
      if (command.constructor.name === "DeleteMessageCommand") {
        const error = new Error("Delete error");
        error.name = "SQSError";
        throw error;
      }
      return sqs.send(command);
    },
  };

  consumer = new Consumer({
    queueUrl: QUEUE_URL,
    sqs: mockSqs,
    handleMessage: async (message) => message,
  });

  consumer.on("error", (err, message, metadata) => {
    errorEvent = err;
    messageParam = message;
    metadataParam = metadata;
  });
});

Given("a consumer that processes messages normally", () => {
  consumer = new Consumer({
    queueUrl: QUEUE_URL,
    sqs,
    handleMessage: async (message) => message,
    pollingWaitTimeMs: 100,
    waitTimeSeconds: 1,
  });

  [
    "message_received",
    "message_processed",
    "empty",
    "started",
    "stopped",
  ].forEach((eventName) => {
    consumer.on(eventName, (...args) => {
      const metadata = args.at(-1);
      receivedEvents[eventName] = {
        args,
        metadata,
      };
    });
  });
});

Given("a test message is sent to the SQS queue for events test", async () => {
  await producer.send("test-message-for-events");
});

Given("an empty SQS queue", async () => {
  await sqs.send(new PurgeQueueCommand({ QueueUrl: QUEUE_URL }));
});

When("the consumer starts", async () => {
  consumer.start();
  strictEqual(consumer.status.isRunning, true);

  try {
    if (!errorEvent) {
      await Promise.race([
        pEvent(consumer, "error", { timeout: 3000 }),
        new Promise((resolve) => setTimeout(resolve, 3000)),
      ]);
    } else {
      await new Promise((resolve) => setTimeout(resolve, 5000));
    }
  } catch {
    // Timeout is expected in some cases
  }

  consumer.stop();
  strictEqual(consumer.status.isRunning, false);
});

When("the consumer polls an empty queue", async () => {
  consumer.start();
  strictEqual(consumer.status.isRunning, true);

  try {
    await pEvent(consumer, "empty", { timeout: 10000 });
  } catch {
    // If timeout occurs, we'll check if the event was registered anyway
  }

  consumer.stop();
  strictEqual(consumer.status.isRunning, false);
});

When("the consumer starts and stops", async () => {
  try {
    consumer.start();
    strictEqual(consumer.status.isRunning, true);

    await Promise.race([
      pEvent(consumer, "started", { timeout: 2000 }),
      new Promise((resolve) => setTimeout(resolve, 2000)),
    ]);

    await new Promise((resolve) => setTimeout(resolve, 1000));

    consumer.stop();

    await Promise.race([
      pEvent(consumer, "stopped", { timeout: 2000 }),
      new Promise((resolve) => setTimeout(resolve, 2000)),
    ]);

    strictEqual(consumer.status.isRunning, false);
  } catch {
    consumer.stop();
  }
});

Then(
  "an error event should be emitted with undefined as the second parameter",
  () => {
    ok(errorEvent, "Error event should be defined");
    strictEqual(
      messageParam,
      undefined,
      "Message parameter should be undefined",
    );
  },
);

Then(
  "an error event should be emitted with the message as the second parameter",
  () => {
    ok(errorEvent, "Error event should be defined");
    ok(messageParam, "Message parameter should be defined");
    ok(messageParam.MessageId, "Message should have an ID");
    ok(messageParam.Body, "Message should have a body");
  },
);

Then("the event should include metadata with queueUrl", () => {
  deepStrictEqual(
    metadataParam,
    { queueUrl: QUEUE_URL },
    "Metadata should contain the queue URL",
  );
});

Then("message_received event should be emitted with metadata", () => {
  ok(
    receivedEvents.message_received,
    "message_received event should be emitted",
  );
  ok(
    receivedEvents.message_received.args.length > 0,
    "message_received event should have arguments",
  );
  deepStrictEqual(
    receivedEvents.message_received.metadata,
    { queueUrl: QUEUE_URL },
    "message_received metadata should contain the queue URL",
  );
});

Then("message_processed event should be emitted with metadata", () => {
  ok(
    receivedEvents.message_processed,
    "message_processed event should be emitted",
  );
  ok(
    receivedEvents.message_processed.args.length > 0,
    "message_processed event should have arguments",
  );
  deepStrictEqual(
    receivedEvents.message_processed.metadata,
    { queueUrl: QUEUE_URL },
    "message_processed metadata should contain the queue URL",
  );
});

Then("empty event should be emitted with metadata", () => {
  ok(receivedEvents.empty, "empty event should be emitted");
  deepStrictEqual(
    receivedEvents.empty.metadata,
    { queueUrl: QUEUE_URL },
    "empty event metadata should contain the queue URL",
  );
});

Then("started event should be emitted with metadata", () => {
  ok(receivedEvents.started, "started event should be emitted");
  deepStrictEqual(
    receivedEvents.started.metadata,
    { queueUrl: QUEUE_URL },
    "started event metadata should contain the queue URL",
  );
});

Then("stopped event should be emitted with metadata", () => {
  ok(receivedEvents.stopped, "stopped event should be emitted");
  deepStrictEqual(
    receivedEvents.stopped.metadata,
    { queueUrl: QUEUE_URL },
    "stopped event metadata should contain the queue URL",
  );
});

After(() => {
  if (consumer && consumer.status.isRunning) {
    consumer.stop();
  }

  errorEvent = undefined;
  messageParam = undefined;
  metadataParam = undefined;
  receivedEvents = {};
});

/* eslint-enable no-undefined -- This is a test file */
