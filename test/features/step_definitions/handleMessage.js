const { Given, Then } = require('@cucumber/cucumber');
const assert = require('assert');
const { PurgeQueueCommand } = require('@aws-sdk/client-sqs');
const pEvent = require('p-event');

const { consumer } = require('../utils/consumer/handleMessage');
const { producer } = require('../utils/producer');
const { sqs, QUEUE_URL } = require('../utils/sqs');

Given('a message is sent to the SQS queue', async () => {
  const params = {
    QueueUrl: QUEUE_URL
  };
  const command = new PurgeQueueCommand(params);
  const response = await sqs.send(command);

  assert.strictEqual(response['$metadata'].httpStatusCode, 200);

  await producer.send(['msg1']);

  const size = await producer.queueSize();

  assert.strictEqual(size, 1);
});

Then('the message should be consumed without error', async () => {
  consumer.start();

  const isRunning = consumer.isRunning;

  assert.strictEqual(isRunning, true);

  await pEvent(consumer, 'message_processed');

  const size = await producer.queueSize();

  assert.strictEqual(size, 0);

  consumer.stop();

  assert.strictEqual(consumer.isRunning, false);
});

Given('messages are sent to the SQS queue', async () => {
  const params = {
    QueueUrl: QUEUE_URL
  };
  const command = new PurgeQueueCommand(params);
  const response = await sqs.send(command);

  assert.strictEqual(response['$metadata'].httpStatusCode, 200);

  await producer.send(['msg2', 'msg3', 'msg4']);

  const size = await producer.queueSize();

  assert.strictEqual(size, 3);
});

Then('the messages should be consumed without error', async () => {
  consumer.start();

  assert.strictEqual(consumer.isRunning, true);

  await pEvent(consumer, 'message_processed');
  const size = await producer.queueSize();

  assert.strictEqual(size, 2);

  await pEvent(consumer, 'message_processed');

  const size2 = await producer.queueSize();

  assert.strictEqual(size2, 1);

  await pEvent(consumer, 'message_processed');

  const size3 = await producer.queueSize();

  assert.strictEqual(size3, 0);

  consumer.stop();

  assert.strictEqual(consumer.isRunning, false);
});
