const { Given, Then } = require('@cucumber/cucumber');
const assert = require('assert');
const { PurgeQueueCommand } = require('@aws-sdk/client-sqs');
const pEvent = require('p-event');

const { consumer } = require('../utils/consumer/handleMessageBatch');
const { producer } = require('../utils/producer');
const { sqs, QUEUE_URL } = require('../utils/sqs');

Given('a message batch is sent to the SQS queue', async () => {
  const params = {
    QueueUrl: QUEUE_URL
  };
  const command = new PurgeQueueCommand(params);
  const response = await sqs.send(command);

  assert.strictEqual(response['$metadata'].httpStatusCode, 200);

  await producer.send(['msg1', 'msg2', 'msg3', 'msg4']);

  const size = await producer.queueSize();

  assert.strictEqual(size, 4);
});

Then('the message batch should be consumed without error', async () => {
  consumer.start();

  const isRunning = consumer.isRunning;

  assert.strictEqual(isRunning, true);

  await pEvent(consumer, 'response_processed');

  consumer.stop();
  assert.strictEqual(consumer.isRunning, false);

  const size = await producer.queueSize();
  assert.strictEqual(size, 0);
});

Given('message batches are sent to the SQS queue', async () => {
  const params = {
    QueueUrl: QUEUE_URL
  };
  const command = new PurgeQueueCommand(params);
  const response = await sqs.send(command);

  assert.strictEqual(response['$metadata'].httpStatusCode, 200);

  await producer.send(['msg1', 'msg2', 'msg3', 'msg4', 'msg5', 'msg6']);

  const size = await producer.queueSize();

  assert.strictEqual(size, 6);
});

Then('the message batches should be consumed without error', {timeout: 2 * 5000}, async () => {
  consumer.start();

  const isRunning = consumer.isRunning;

  assert.strictEqual(isRunning, true);

  await pEvent(consumer, 'message_received');

  const size = await producer.queueSize();
  assert.strictEqual(size, 1);

  await pEvent(consumer, 'message_received');

  const size2 = await producer.queueSize();
  assert.strictEqual(size2, 0);

  consumer.stop();
  assert.strictEqual(consumer.isRunning, false);
});