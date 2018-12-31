'use strict';

const Consumer = require('..');
const assert = require('assert');
const sandbox = require('sinon').sandbox.create();

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
    handleMessage = sandbox.stub().yieldsAsync(null);
    sqs = sandbox.mock();
    sqs.receiveMessage = sandbox.stub().yieldsAsync(null, response);
    sqs.receiveMessage.onSecondCall().returns();
    sqs.deleteMessage = sandbox.stub().yieldsAsync(null);
    sqs._deleteMessage = sandbox.stub().yieldsAsync(null);
    sqs.changeMessageVisibility = sandbox.stub().yieldsAsync(null);

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

      sqs.receiveMessage.yields(receiveErr);

      consumer.on('error', (err) => {
        assert.ok(err);
        assert.equal(err.message, 'SQS receive message failed: Receive error');
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
        done();
      });

      consumer.start();
    });

    it('fires an error event when an error occurs deleting a message', (done) => {
      const deleteErr = new Error('Delete error');

      handleMessage.yields(null);
      sqs.deleteMessage.yields(deleteErr);

      consumer.on('error', (err) => {
        assert.ok(err);
        assert.equal(err.message, 'SQS delete message failed: Delete error');
        done();
      });

      consumer.start();
    });

    it('fires a `processing_error` event when a non-`SQSError` error occurs processing a message', (done) => {
      const processingErr = new Error('Processing error');

      handleMessage.yields(processingErr);

      consumer.on('processing_error', (err, message) => {
        assert.equal(err, processingErr);
        assert.equal(message.MessageId, '123');
        done();
      });

      consumer.start();
    });

    it('fires an `error` event when an `SQSError` occurs processing a message', (done) => {
      const sqsError = new Error('Processing error');
      sqsError.name = 'SQSError';

      handleMessage.yields(sqsError);

      consumer.on('error', (err, message) => {
        assert.equal(err, sqsError);
        assert.equal(message.MessageId, '123');
        done();
      });

      consumer.start();
    });

    it('waits before repolling when a credentials error occurs', (done) => {
      const credentialsErr = {
        code: 'CredentialsError',
        message: 'Missing credentials in config'
      };

      sqs.receiveMessage.yields(credentialsErr);

      consumer.on('error', () => {
        setTimeout(() => {
          sandbox.assert.calledOnce(sqs.receiveMessage);
        }, 10);
        setTimeout(() => {
          sandbox.assert.calledTwice(sqs.receiveMessage);
          done();
        }, 30);
      });

      consumer.start();
    });

    it('waits before repolling when a 403 error occurs', (done) => {
      const invalidSignatureErr = {
        statusCode: 403,
        message: 'The security token included in the request is invalid'
      };

      sqs.receiveMessage.yields(invalidSignatureErr);

      consumer.on('error', () => {
        setTimeout(() => {
          sandbox.assert.calledOnce(sqs.receiveMessage);
        }, 10);
        setTimeout(() => {
          sandbox.assert.calledTwice(sqs.receiveMessage);
          done();
        }, 30);
      });

      consumer.start();
    });

    it('fires a message_received event when a message is received', (done) => {
      consumer.on('message_received', (message) => {
        assert.equal(message, response.Messages[0]);
        done();
      });

      consumer.start();
    });

    it('fires a message_processed event when a message is successfully deleted', (done) => {
      handleMessage.yields(null);

      consumer.on('message_processed', (message) => {
        assert.equal(message, response.Messages[0]);
        done();
      });

      consumer.start();
    });

    it('calls the handleMessage function when a message is received', (done) => {
      consumer.start();

      consumer.on('message_processed', () => {
        sandbox.assert.calledWith(handleMessage, response.Messages[0]);
        done();
      });
    });

    it('deletes the message when the handleMessage callback is called', (done) => {
      handleMessage.yields(null);

      consumer.start();

      consumer.on('message_processed', () => {
        sandbox.assert.calledWith(sqs.deleteMessage, {
          QueueUrl: 'some-queue-url',
          ReceiptHandle: 'receipt-handle'
        });
        done();
      });
    });

    it('doesn\'t delete the message when a processing error is reported', () => {
      handleMessage.yields(new Error('Processing error'));

      consumer.on('processing_error', () => {
        // ignore the error
      });

      consumer.start();

      sandbox.assert.notCalled(sqs.deleteMessage);
    });

    it('consumes another message once one is processed', (done) => {
      sqs.receiveMessage.onSecondCall().yields(null, response);
      sqs.receiveMessage.onThirdCall().returns();

      consumer.start();
      setTimeout(() => {
        sandbox.assert.calledTwice(handleMessage);
        done();
      }, 10);
    });

    it('doesn\'t consume more messages when called multiple times', () => {
      sqs.receiveMessage = sandbox.stub().returns();
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.start();

      sandbox.assert.calledOnce(sqs.receiveMessage);
    });

    it('consumes multiple messages when the batchSize is greater than 1', (done) => {
      sqs.receiveMessage.yieldsAsync(null, {
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

      setTimeout(() => {
        sandbox.assert.calledWith(sqs.receiveMessage, {
          QueueUrl: 'some-queue-url',
          AttributeNames: [],
          MessageAttributeNames: ['attribute-1', 'attribute-2'],
          MaxNumberOfMessages: 3,
          WaitTimeSeconds: 20,
          VisibilityTimeout: undefined
        });
        sandbox.assert.callCount(handleMessage, 3);
        done();
      }, 10);
    });

    it('consumes messages with message attibute \'ApproximateReceiveCount\'', (done) => {

      const messageWithAttr = {
        ReceiptHandle: 'receipt-handle-1',
        MessageId: '1',
        Body: 'body-1',
        Attributes: {
          ApproximateReceiveCount: 1
        }
      };

      sqs.receiveMessage.yieldsAsync(null, {
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
        done();
      });

      consumer.start();
    });

    it('fires a emptyQueue event when all messages have been consumed', (done) => {
      sqs.receiveMessage.yieldsAsync(null, {});

      consumer.on('empty', () => {
        done();
      });

      consumer.start();
    });

    it('terminate message visibility timeout on processing error', (done) => {
      handleMessage.yields(new Error('Processing error'));

      consumer.terminateVisibilityTimeout = true;
      consumer.on('processing_error', () => {
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
      handleMessage.yields(new Error('Processing error'));

      consumer.terminateVisibilityTimeout = false;
      consumer.on('processing_error', () => {
        setImmediate(() => {
          sandbox.assert.notCalled(sqs.changeMessageVisibility);
          done();
        });
      });

      consumer.start();
    });

    it('fires error event when failed to terminate visibility timeout on processing error', (done) => {
      handleMessage.yields(new Error('Processing error'));

      const sqsError = new Error('Processing error');
      sqsError.name = 'SQSError';
      sqs.changeMessageVisibility.yields(sqsError);

      consumer.terminateVisibilityTimeout = true;
      consumer.on('error', () => {
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
      sqs.receiveMessage.yieldsAsync(null, {
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
      handleMessage.yields(null);

      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        messageAttributeNames: ['attribute-1', 'attribute-2'],
        region: 'some-region',
        handleMessage,
        batchSize: 2,
        sqs
      });

      consumer.on('response_processed', done);
      consumer.start();

    });
  });

  describe('.stop', () => {
    beforeEach(() => {
      sqs.receiveMessage.onSecondCall().yieldsAsync(null, response);
      sqs.receiveMessage.onThirdCall().returns();
    });

    it('stops the consumer polling for messages', (done) => {
      consumer.start();
      consumer.stop();

      setTimeout(() => {
        sandbox.assert.calledOnce(handleMessage);
        done();
      }, 10);
    });

    it('fires a stopped event when last poll occurs after stopping', (done) => {
      const handleStop = sandbox.stub().returns();

      consumer.on('stopped', handleStop);

      consumer.start();
      consumer.stop();

      setTimeout(() => {
        sandbox.assert.calledOnce(handleStop);
        done();
      }, 10);
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
      const handleStop = sandbox.stub().returns();

      consumer.on('stopped', handleStop);

      consumer.start();
      consumer.stop();
      consumer.start();
      consumer.stop();

      setTimeout(() => {
        sandbox.assert.calledTwice(handleStop);
        done();
      }, 10);
    });
  });
});
