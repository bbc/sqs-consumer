const { Given, When, Then } = require('@cucumber/cucumber');
const assert = require('assert');
const { PurgeQueueCommand } = require('@aws-sdk/client-sqs');

const { consumer } = require('../utils/consumer/handleMessage');
const { producer } = require('../utils/producer');
const { sqs, QUEUE_URL } = require('../utils/sqs');
const { delay } = require('../utils/delay');

const POLLING_TIMEOUT = 100;

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

When('SQS Consumer is started', () => {
  consumer.start();

  const isRunning = consumer.isRunning;

  assert.strictEqual(isRunning, true);
});

Then('the message should be consumed without error', async () => {
  const size = await producer.queueSize();

  await delay(POLLING_TIMEOUT);

  assert.strictEqual(size, 0);
});

Then('SQS Consumer should stop', () => {
  consumer.stop();

  const isRunning = consumer.isRunning;

  assert.strictEqual(isRunning, false);
});
