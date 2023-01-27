const { Given, Then } = require('@cucumber/cucumber');
const assert = require('assert');
const { PurgeQueueCommand } = require('@aws-sdk/client-sqs');
const pEvent = require('p-event');

const { consumer } = require('../utils/consumer/handleMessage');
const { producer } = require('../utils/producer');
const { sqs, QUEUE_URL } = require('../utils/sqs');

let messageId;
let messageIds;

Given('a message is sent to the SQS queue', async () => {
  const params = {
    QueueUrl: QUEUE_URL
  };
  const command = new PurgeQueueCommand(params);
  const response = await sqs.send(command);

  assert.strictEqual(response['$metadata'].httpStatusCode, 200);

  const msg = await producer.send(['msg1']);

  messageId = msg[0].MessageId;

  const size = await producer.queueSize();

  assert.strictEqual(size, 1);
});

Then('the message should be consumed without error', async () => {
  consumer.start();

  const isRunning = consumer.isRunning;

  assert.strictEqual(isRunning, true);

  await pEvent(consumer, 'message_received');

  assert.strictEqual(consumer.messagesInQueue.length, 1);
  assert.strictEqual(consumer.messagesInQueue[0], messageId);

  await pEvent(consumer, 'response_processed');

  assert.strictEqual(consumer.messagesInQueue.length, 0);

  consumer.stop();
  assert.strictEqual(consumer.isRunning, false);

  const size = await producer.queueSize();
  assert.strictEqual(size, 0);
});

Given('messages are sent to the SQS queue', async () => {
  const params = {
    QueueUrl: QUEUE_URL
  };
  const command = new PurgeQueueCommand(params);
  const response = await sqs.send(command);

  assert.strictEqual(response['$metadata'].httpStatusCode, 200);

  const msg = await producer.send(['msg2', 'msg3', 'msg4']);

  messageIds = msg.map((m) => m.MessageId);

  const size = await producer.queueSize();

  assert.strictEqual(size, 3);
});

Then(
  'the messages should be consumed without error',
  { timeout: 2 * 5000 },
  async () => {
    consumer.start();

    assert.strictEqual(consumer.isRunning, true);

    await pEvent(consumer, 'message_received');
    const size = await producer.queueSize();

    assert.strictEqual(size, 2);
    assert.strictEqual(consumer.messagesInQueue.length, 2);
    assert.strictEqual(consumer.messagesInQueue[0], messageIds[1]);
    assert.strictEqual(consumer.messagesInQueue[1], messageIds[2]);

    await pEvent(consumer, 'message_received');

    const size2 = await producer.queueSize();

    assert.strictEqual(size2, 1);
    assert.strictEqual(consumer.messagesInQueue.length, 1);
    assert.strictEqual(consumer.messagesInQueue[0], messageIds[2]);

    await pEvent(consumer, 'message_received');

    const size3 = await producer.queueSize();
    assert.strictEqual(consumer.messagesInQueue.length, 0);

    assert.strictEqual(size3, 0);

    consumer.stop();

    assert.strictEqual(consumer.isRunning, false);
  }
);
