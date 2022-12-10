const { Producer } = require('sqs-producer');

const { QUEUE_URL, sqsConfig, sqs } = require('./sqs');

const producer = Producer.create({
  queueUrl: QUEUE_URL,
  region: sqsConfig.region,
  sqs
});

exports.producer = producer;

export {};
