const { Consumer } = require('../../../../dist/consumer');

const { QUEUE_URL, sqs } = require('../sqs');

const consumer = Consumer.create({
  queueUrl: QUEUE_URL,
  sqs,
  handleMessage: async (message) => {
    return message;
  }
});

exports.consumer = consumer;
