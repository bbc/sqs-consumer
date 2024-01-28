const { Given, Then, After } = require('@cucumber/cucumber');
const assert = require('assert');
const { PurgeQueueCommand } = require('@aws-sdk/client-sqs');
const pEvent = require('p-event');

const { consumer } = require('../utils/consumer/gracefulShutdown');
const { producer } = require('../utils/producer');
const { sqs, QUEUE_URL } = require('../utils/sqs');

Given('Several messages are sent to the SQS queue', async () => {
  const params = {
    QueueUrl: QUEUE_URL
  };
  const command = new PurgeQueueCommand(params);
  const response = await sqs.send(command);

  assert.strictEqual(response['$metadata'].httpStatusCode, 200);

  const size = await producer.queueSize();
  assert.strictEqual(size, 0);

  await producer.send(['msg1', 'msg2', 'msg3']);

  const size2 = await producer.queueSize();

  assert.strictEqual(size2, 3);
});

Then('the application is stopped while messages are in flight', async () => {
  consumer.start();

  consumer.stop();

  assert.strictEqual(consumer.isRunning, false);
});

Then(
  'the in-flight messages should be processed before stopped is emitted',
  async () => {
    let numProcessed = 0;
    consumer.on('message_processed', () => {
      numProcessed++;
    });

    await pEvent(consumer, 'stopped');

    assert.strictEqual(numProcessed, 3);

    const size = await producer.queueSize();
    assert.strictEqual(size, 0);
  }
);

After(() => {
  return consumer.stop();
});
