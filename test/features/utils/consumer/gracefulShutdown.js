const { Consumer } = require('../../../../dist/consumer');

const { QUEUE_URL, sqs } = require('../sqs');

const consumer = Consumer.create({
  queueUrl: QUEUE_URL,
  sqs,
  pollingWaitTimeMs: 1000,
  batchSize: 10,
  handleMessage: async (message) => {
    await new Promise((resolve) => setTimeout(resolve, 100));
    return message;
  }
});

exports.consumer = consumer;
