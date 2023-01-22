const { Consumer } = require('../../../../dist/consumer');

const { QUEUE_URL, sqs } = require('../sqs');

const consumer = Consumer.create({
  queueUrl: QUEUE_URL,
  sqs,
  batchSize: 5,
  handleMessageBatch: async (messages) => {
    return messages;
  }
});

exports.consumer = consumer;
