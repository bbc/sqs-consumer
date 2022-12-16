const { Consumer } = require('sqs-consumer');

const { QUEUE_URL, sqs } = require('./sqs');

const consumer = Consumer.create({
  queueUrl: QUEUE_URL,
  sqs,
  handleMessage: async (message) => {
    // eslint-disable-next-line no-console
    console.log('HANDLING MESSAGE:');
    // eslint-disable-next-line no-console
    console.log(message);
  }
});

consumer.on('error', (err) => {
  // eslint-disable-next-line no-console
  console.log('RECEIVED SQS ERROR:');
  // eslint-disable-next-line no-console
  console.error(err.message);
});

consumer.on('processing_error', (err) => {
  // eslint-disable-next-line no-console
  console.log('RECEIVED SQS PROCESSING ERROR:');
  // eslint-disable-next-line no-console
  console.error(err.message);
});

consumer.on('timeout_error', (err) => {
  // eslint-disable-next-line no-console
  console.log('RECEIVED SQS TIMEOUT ERROR:');
  // eslint-disable-next-line no-console
  console.error(err.message);
});

consumer.on('timeout_error', (err) => {
  // eslint-disable-next-line no-console
  console.log('RECEIVED SQS TIMEOUT ERROR:');
  // eslint-disable-next-line no-console
  console.error(err.message);
});

consumer.on('message_received', (message) => {
  // eslint-disable-next-line no-console
  console.log('RECEIVED SQS MESSAGE:');
  // eslint-disable-next-line no-console
  console.error(message);
});

consumer.on('message_processed', (message) => {
  // eslint-disable-next-line no-console
  console.log('RECEIVED SQS MESSAGE PROCESSED:');
  // eslint-disable-next-line no-console
  console.error(message);
});

consumer.on('response_processed', () => {
  // eslint-disable-next-line no-console
  console.log('RECEIVED SQS RESPONSE PROCESSED:');
});

consumer.on('stopped', () => {
  // eslint-disable-next-line no-console
  console.log('RECEIVED SQS STOPPED:');
});

consumer.on('empty', () => {
  // eslint-disable-next-line no-console
  console.log('RECEIVED SQS EMPTY:');
});

exports.consumer = consumer;

export {};
