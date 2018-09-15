'use strict';

const Consumer = require('..');
const assert = require('assert');
const sandbox = require('sinon').sandbox.create();
const awsResolve = (value) =>
  sandbox
    .stub()
    .returns({promise: sandbox.stub().resolves(value)});
const awsReject = (value) =>
  sandbox
    .stub()
    .returns({promise: sandbox.stub().rejects(value)});

describe('Consumer', () => {
  let consumer;
  let handleMessage;
  let sqs;
  const response = {
    Messages: [{
      ReceiptHandle: 'receipt-handle',
      MessageId: '123',
      Body: 'body'
    }]
  };

  beforeEach(() => {
    handleMessage = sandbox.stub().resolves(null);
    sqs = sandbox.mock();
    sqs.receiveMessage = awsResolve(response);
    sqs.deleteMessage = awsResolve();
    sqs.changeMessageVisibility = awsResolve();

    consumer = new Consumer({
      queueUrl: 'some-queue-url',
      region: 'some-region',
      handleMessage,
      sqs,
      authenticationErrorTimeout: 20
    });
  });

  afterEach(() => {
    sandbox.restore();
  });

  it('requires a queueUrl to be set', () => {
    assert.throws(() => {
      new Consumer({
        region: 'some-region',
        handleMessage
      });
    });
  });

  it('requires a handleMessage function to be set', () => {
    assert.throws(() => {
      new Consumer({
        region: 'some-region',
        queueUrl: 'some-queue-url'
      });
    });
  });

  it('requires the batchSize option to be no greater than 10', () => {
    assert.throws(() => {
      new Consumer({
        region: 'some-region',
        queueUrl: 'some-queue-url',
        handleMessage,
        batchSize: 11
      });
    });
  });

  it('requires the batchSize option to be greater than 0', () => {
    assert.throws(() => {
      new Consumer({
        region: 'some-region',
        queueUrl: 'some-queue-url',
        handleMessage,
        batchSize: -1
      });
    });
  });

  describe('.create', () => {
    it('creates a new instance of a Consumer object', () => {
      const consumer = Consumer.create({
        region: 'some-region',
        queueUrl: 'some-queue-url',
        batchSize: 1,
        visibilityTimeout: 10,
        waitTimeSeconds: 10,
        handleMessage
      });

      assert(consumer instanceof Consumer);
    });
  });

  describe('.start', () => {
    it('fires an error event when an error occurs receiving a message', (done) => {
      const receiveErr = new Error('Receive error');

      sqs.receiveMessage = awsReject(receiveErr);

      consumer.on('error', (err) => {
        assert.ok(err);
        assert.equal(err.message, 'SQS receive message failed: Receive error');
        consumer.stop();
        done();
      });

      consumer.start();
    });

    it('handles unexpected exceptions thrown by the handler function', (done) => {
      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        region: 'some-region',
        handleMessage: () => {
          throw new Error('unexpected parsing error');
        },
        sqs,
        authenticationErrorTimeout: 20
      });

      consumer.on('processing_error', (err) => {
        assert.ok(err);
        assert.equal(err.message, 'Unexpected message handler failure: unexpected parsing error');
        consumer.stop();
        done();
      });

      consumer.start();
    });

    it('fires an error event when an error occurs deleting a message', (done) => {
      const deleteErr = new Error('Delete error');

      handleMessage.resolves(null);
      sqs.deleteMessage = awsReject(deleteErr);

      consumer.on('error', (err) => {
        assert.ok(err);
        assert.equal(err.message, 'SQS delete message failed: Delete error');
        consumer.stop();
        done();
      });

      consumer.start();
    });

    it('fires a `processing_error` event when a non-`SQSError` error occurs processing a message', (done) => {
      const processingErr = new Error('Processing error');

      handleMessage.rejects(processingErr);

      consumer.on('processing_error', (err, message) => {
        assert.equal(err.message,'Unexpected message handler failure: Processing error');
        assert.equal(message.MessageId, '123');
        consumer.stop();
        done();
      });

      consumer.start();
    });

    it('fires an `error` event when an `SQSError` occurs processing a message', (done) => {
      const sqsError = new Error('Processing error');
      sqsError.name = 'SQSError';

      handleMessage.resolves(sqsError);
      sqs.deleteMessage = awsReject(sqsError);

      consumer.on('error', (err, message) => {
        assert.equal(err.message, 'SQS delete message failed: Processing error');
        assert.equal(message.MessageId, '123');
        consumer.stop();
        done();
      });

      consumer.start();
    });

    it('waits before repolling when a credentials error occurs', (done) => {
      const credentialsErr = {
        code: 'CredentialsError',
        message: 'Missing credentials in config'
      };
      const timings = [];
      const listener = sandbox.stub().callsFake(() => timings.push(new Date()));

      sqs.receiveMessage = awsReject(credentialsErr);

      listener.onThirdCall().callsFake(() => {
        consumer.stop();
        sandbox.assert.calledThrice(sqs.receiveMessage);
        assert((timings[1] - timings[0]) > 10);
        done();
      });

      consumer.on('error', listener);
      consumer.start();
    });

    it('waits before repolling when a 403 error occurs', (done) => {
      const invalidSignatureErr = {
        statusCode: 403,
        message: 'The security token included in the request is invalid'
      };
      const timings = [];
      const listener = sandbox.stub().callsFake(() => timings.push(new Date()));

      sqs.receiveMessage = awsReject(invalidSignatureErr);

      listener.onThirdCall().callsFake(() => {
        consumer.stop();
        sandbox.assert.calledThrice(sqs.receiveMessage);
        assert((timings[1] - timings[0]) > 10);
        done();
      });

      consumer.on('error', listener);
      consumer.start();
    });

    it('fires a message_received event when a message is received', (done) => {
      consumer.on('message_received', (message) => {
        assert.equal(message, response.Messages[0]);
        consumer.stop();
        done();
      });

      consumer.start();
    });

    it('fires a message_processed event when a message is successfully deleted', (done) => {
      handleMessage.resolves();

      consumer.on('message_processed', (message) => {
        assert.equal(message, response.Messages[0]);
        consumer.stop();
        done();
      });

      consumer.start();
    });

    it('calls the handleMessage function when a message is received', (done) => {
      consumer.start();

      consumer.on('message_processed', () => {
        sandbox.assert.calledWith(handleMessage, response.Messages[0]);
        consumer.stop();
        done();
      });
    });

    it('deletes the message when the handleMessage callback is called', (done) => {
      handleMessage.resolves();

      consumer.start();

      consumer.on('message_processed', () => {
        sandbox.assert.calledWith(sqs.deleteMessage, {
          QueueUrl: 'some-queue-url',
          ReceiptHandle: 'receipt-handle'
        });
        consumer.stop();
        done();
      });
    });

    it('doesn\'t delete the message when a processing error is reported', () => {
      handleMessage.rejects(new Error('Processing error'));

      consumer.on('processing_error', () => {
        // ignore the error
      });

      consumer.start();

      sandbox.assert.notCalled(sqs.deleteMessage);
      consumer.stop();
    });

    it('consumes another message once one is processed', (done) => {
      handleMessage.resolves();
      handleMessage.onSecondCall().callsFake(() => {
        consumer.stop();
        done();
      });

      consumer.start();
    });

    it('doesn\'t consume more messages when called multiple times', () => {
      sqs.receiveMessage = awsResolve(new Promise((res) => setTimeout(res, 100)));
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.stop();

      sandbox.assert.calledOnce(sqs.receiveMessage);
    });

    it('consumes multiple messages when the batchSize is greater than 1', (done) => {
      sqs.receiveMessage = awsResolve({
        Messages: [
          {
            ReceiptHandle: 'receipt-handle-1',
            MessageId: '1',
            Body: 'body-1'
          },
          {
            ReceiptHandle: 'receipt-handle-2',
            MessageId: '2',
            Body: 'body-2'
          },
          {
            ReceiptHandle: 'receipt-handle-3',
            MessageId: '3',
            Body: 'body-3'
          }
        ]
      });

      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        messageAttributeNames: ['attribute-1', 'attribute-2'],
        region: 'some-region',
        handleMessage,
        batchSize: 3,
        sqs
      });

      consumer.start();
      handleMessage.onThirdCall().callsFake(() => {
        sandbox.assert.calledWith(sqs.receiveMessage, {
          QueueUrl: 'some-queue-url',
          AttributeNames: [],
          MessageAttributeNames: ['attribute-1', 'attribute-2'],
          MaxNumberOfMessages: 3,
          WaitTimeSeconds: 20,
          VisibilityTimeout: undefined
        });
        sandbox.assert.callCount(handleMessage, 3);
        consumer.stop();
        done();
      });
    });

    it('consumes messages with message attribute \'ApproximateReceiveCount\'', (done) => {

      const messageWithAttr = {
        ReceiptHandle: 'receipt-handle-1',
        MessageId: '1',
        Body: 'body-1',
        Attributes: {
          ApproximateReceiveCount: 1
        }
      };

      sqs.receiveMessage = awsResolve({
        Messages: [messageWithAttr]
      });

      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        attributeNames: ['ApproximateReceiveCount'],
        region: 'some-region',
        handleMessage,
        sqs
      });

      consumer.on('message_received', (message) => {
        sandbox.assert.calledWith(sqs.receiveMessage, {
          QueueUrl: 'some-queue-url',
          AttributeNames: ['ApproximateReceiveCount'],
          MessageAttributeNames: [],
          MaxNumberOfMessages: 1,
          WaitTimeSeconds: 20,
          VisibilityTimeout: undefined
        });
        assert.equal(message, messageWithAttr);
        consumer.stop();
        done();
      });

      consumer.start();
    });

    it('fires an emptyQueue event when all messages have been consumed', (done) => {
      sqs.receiveMessage = awsResolve({});

      consumer.on('empty', () => {
        consumer.stop();
        done();
      });

      consumer.start();
    });

    it('terminate message visibility timeout on processing error', (done) => {
      handleMessage.rejects(new Error('Processing error'));

      consumer.terminateVisibilityTimeout = true;
      consumer.on('processing_error', () => {
        consumer.stop();
        setImmediate(() => {
          sandbox.assert.calledWith(sqs.changeMessageVisibility, {
            QueueUrl: 'some-queue-url',
            ReceiptHandle: 'receipt-handle',
            VisibilityTimeout: 0
          });
          done();
        });
      });

      consumer.start();
    });

    it('does not terminate visibility timeout when `terminateVisibilityTimeout` option is false', (done) => {
      handleMessage.rejects(new Error('Processing error'));

      consumer.terminateVisibilityTimeout = false;
      consumer.on('processing_error', () => {
        consumer.stop();
        setImmediate(() => {
          sandbox.assert.notCalled(sqs.changeMessageVisibility);
          done();
        });
      });

      consumer.start();
    });

    it('fires error event when failed to terminate visibility timeout on processing error', (done) => {
      handleMessage.rejects(new Error('Processing error'));

      const sqsError = new Error('Processing error');
      sqsError.name = 'SQSError';
      sqs.changeMessageVisibility = awsReject(sqsError);

      consumer.terminateVisibilityTimeout = true;
      consumer.on('error', () => {
        consumer.stop();
        setImmediate(() => {
          sandbox.assert.calledWith(sqs.changeMessageVisibility, {
            QueueUrl: 'some-queue-url',
            ReceiptHandle: 'receipt-handle',
            VisibilityTimeout: 0
          });
          done();
        });
      });

      consumer.start();
    });

    it('fires response_processed event for each batch', (done) => {
      sqs.receiveMessage = awsResolve({
        Messages: [
          {
            ReceiptHandle: 'receipt-handle-1',
            MessageId: '1',
            Body: 'body-1'
          },
          {
            ReceiptHandle: 'receipt-handle-2',
            MessageId: '2',
            Body: 'body-2'
          }
        ]
      });
      handleMessage.resolves(null);

      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        messageAttributeNames: ['attribute-1', 'attribute-2'],
        region: 'some-region',
        handleMessage,
        batchSize: 2,
        sqs
      });

      consumer.on('response_processed', () => {
        consumer.stop();
        sandbox.assert.callCount(handleMessage, 2);
        done();
      });
      consumer.start();

    });
  });

  describe('.stop', () => {
    it('stops the consumer polling for messages', (done) => {
      consumer.start();
      consumer.stop();

      consumer.on('stopped', () => {
        sandbox.assert.calledOnce(handleMessage);
        done();
      });
    });

    it('fires a stopped event when last poll occurs after stopping', (done) => {
      consumer.on('stopped', done);

      consumer.start();
      consumer.stop();
    });

    it('fires a stopped event only once when stopped multiple times', (done) => {
      const handleStop = sandbox.stub().returns();

      consumer.on('stopped', handleStop);

      consumer.start();
      consumer.stop();
      consumer.stop();
      consumer.stop();

      setTimeout(() => {
        sandbox.assert.calledOnce(handleStop);
        done();
      }, 10);
    });

    it('fires a stopped event a second time if started and stopped twice', (done) => {
      const handleStop = sandbox.stub().returns().onSecondCall().callsFake(() => {
        sandbox.assert.calledTwice(handleStop);
        done();
      });

      consumer.on('stopped', handleStop);

      consumer.start();
      consumer.stop();
      consumer.start();
      consumer.stop();
    });
  });
});
