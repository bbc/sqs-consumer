const { Given, Then } = require('@cucumber/cucumber');
const assert = require('assert');
const { PurgeQueueCommand } = require('@aws-sdk/client-sqs');
const pEvent = require('p-event');

const { consumer } = require('../utils/consumer/handleMessageBatch');
const { producer } = require('../utils/producer');
const { sqs, QUEUE_URL } = require('../utils/sqs');

let messageIdsTestOne;
let messageIdsTestTwo;

Given('a message batch is sent to the SQS queue', async () => {
  const params = {
    QueueUrl: QUEUE_URL
  };
  const command = new PurgeQueueCommand(params);
  const response = await sqs.send(command);

  assert.strictEqual(response['$metadata'].httpStatusCode, 200);

  const msg = await producer.send(['msg1', 'msg2', 'msg3', 'msg4']);

  messageIdsTestOne = msg.map((m) => m.MessageId);

  const size = await producer.queueSize();

  assert.strictEqual(size, 4);
});

Then('the message batch should be consumed without error', async () => {
  consumer.start();

  const isRunning = consumer.isRunning;

  assert.strictEqual(isRunning, true);

  await pEvent(consumer, 'message_received');
  const size = await producer.queueSize();

  assert.strictEqual(size, 1);
  assert.strictEqual(consumer.messagesInQueue.length, 4);
  assert.strictEqual(consumer.messagesInQueue[0], messageIdsTestOne[0]);
  assert.strictEqual(consumer.messagesInQueue[1], messageIdsTestOne[1]);
  assert.strictEqual(consumer.messagesInQueue[2], messageIdsTestOne[2]);
  assert.strictEqual(consumer.messagesInQueue[3], messageIdsTestOne[3]);

  await pEvent(consumer, 'response_processed');

  consumer.stop();
  assert.strictEqual(consumer.isRunning, false);

  const sizeEnd = await producer.queueSize();
  assert.strictEqual(sizeEnd, 0);
  assert.strictEqual(consumer.messagesInQueue.length, 0);
});

Given('message batches are sent to the SQS queue', async () => {
  const params = {
    QueueUrl: QUEUE_URL
  };
  const command = new PurgeQueueCommand(params);
  const response = await sqs.send(command);

  assert.strictEqual(response['$metadata'].httpStatusCode, 200);

  const msg = await producer.send(['msg1', 'msg2', 'msg3', 'msg4', 'msg5', 'msg6']);

  messageIdsTestTwo = msg.map((m) => m.MessageId);

  const size = await producer.queueSize();

  assert.strictEqual(size, 6);
});

Then(
  'the message batches should be consumed without error',
  { timeout: 2 * 5000 },
  async () => {
    consumer.start();

    const isRunning = consumer.isRunning;

    assert.strictEqual(isRunning, true);

    await pEvent(consumer, 'message_received');

    const size = await producer.queueSize();
    assert.strictEqual(size, 1);
    assert.strictEqual(consumer.messagesInQueue.length, 4);
    assert.strictEqual(consumer.messagesInQueue[0], messageIdsTestTwo[0]);
    assert.strictEqual(consumer.messagesInQueue[1], messageIdsTestTwo[1]);
    assert.strictEqual(consumer.messagesInQueue[2], messageIdsTestTwo[2]);
    assert.strictEqual(consumer.messagesInQueue[3], messageIdsTestTwo[3]);
    assert.strictEqual(consumer.messagesInQueue[4], messageIdsTestTwo[4]);

    await pEvent(consumer, 'message_received');

    const size2 = await producer.queueSize();
    assert.strictEqual(size2, 0);
    assert.strictEqual(consumer.messagesInQueue.length, 1);
    assert.strictEqual(consumer.messagesInQueue[5], messageIdsTestTwo[5]);

    consumer.stop();
    assert.strictEqual(consumer.isRunning, false);
  }
);
