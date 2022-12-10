const express = require('express');

const { consumer } = require('./utils/consumer');
const { producer } = require('./utils/producer');

const expressApp = express();

expressApp.get('/', (_, res) => {
  res.send(
    'Hello! Send me a request to one of my other endpoints to test SQS Consumer!'
  );
});

expressApp.get('/queue-size', async (_, res) => {
  // get the current size of the queue
  const size = await producer.queueSize();

  res.send({
    message: 'Queue size retrieved!',
    data: size
  });
});

expressApp.post('/sample', async (_, res) => {
  // send messages to the queue
  const messages = await producer.send(['msg1', 'msg2']);

  res.send({
    message: 'Sample messages sent successfully!',
    data: messages
  });
});

expressApp.post('/sample-with-id', async (_, res) => {
  // send a message to the queue with a specific ID (by default the body is used as the ID)
  const messages = await producer.send([
    {
      id: 'id1',
      body: 'Hello world'
    }
  ]);

  res.send({
    message: 'Sample messages sent successfully!',
    data: messages
  });
});

expressApp.post('/sample-with-attributes', async (_, res) => {
  // send a message to the queue with
  // - messageAttributes
  const messages = await producer.send([
    {
      id: 'id1',
      body: 'Hello world with two string attributes: attr1 and attr2',
      messageAttributes: {
        attr1: { DataType: 'String', StringValue: 'stringValue' },
        attr2: { DataType: 'String', StringValue: 'stringValue2' }
      }
    }
  ]);

  res.send({
    message: 'Sample messages sent successfully!',
    data: messages
  });
});

expressApp.post('/sample-with-delay', async (_, res) => {
  // send a message to the queue with
  // - delaySeconds (must be an number contained within 0 and 900)
  const messages = await producer.send([
    {
      id: 'id1',
      body: 'Hello world delayed by 5 seconds',
      delaySeconds: 5
    }
  ]);

  res.send({
    message: 'Sample messages sent successfully!',
    data: messages
  });
});

expressApp.post('/sample-with-fido', async (_, res) => {
  // send a message to a FIFO queue
  //
  // note that AWS FIFO queues require two additional params:
  // - groupId (string)
  // - deduplicationId (string)
  //
  // deduplicationId can be excluded if content-based deduplication is enabled
  //
  // http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/FIFO-queue-recommendations.html
  const messages = await producer.send({
    id: 'testId',
    body: 'Hello world from our FIFO queue!',
    groupId: 'group1234',
    deduplicationId: 'abcdef123456' // typically a hash of the message body
  });

  res.send({
    message: 'Sample messages sent successfully!',
    data: messages
  });
});

expressApp.listen(3026, () => {
  // eslint-disable-next-line no-console
  console.log('STARTING SQS CONSUMER');
  consumer.start();

  // eslint-disable-next-line no-console
  console.log('EXPRESS APP LISTENING ON: http://localhost:3026');
});
