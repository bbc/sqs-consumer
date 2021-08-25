import { assert } from 'chai';
import pEvent from 'p-event';

import * as sinon from 'sinon';
import { Consumer } from '../src/index';

const sandbox = sinon.createSandbox();

const AUTHENTICATION_ERROR_TIMEOUT = 20;
const POLLING_TIMEOUT = 100;

function stubResolve(value?: any): any {
  return sandbox
    .stub()
    .returns({ promise: sandbox.stub().resolves(value) });
}

function stubReject(value?: any): any {
  return sandbox
    .stub()
    .returns({ promise: sandbox.stub().rejects(value) });
}

class MockSQSError extends Error {
  code: string;
  statusCode: number;
  region: string;
  hostname: string;
  time: Date;
  retryable: boolean;

  constructor(message: string) {
    super(message);
    this.message = message;
  }
}

// tslint:disable:no-unused-expression
describe('Consumer', () => {
  let consumer;
  let clock;
  let handleMessage;
  let handleMessageBatch;
  let sqs;
  const response = {
    Messages: [{
      ReceiptHandle: 'receipt-handle',
      MessageId: '123',
      Body: 'body'
    }]
  };

  beforeEach(() => {
    clock = sinon.useFakeTimers();
    handleMessage = sandbox.stub().resolves(null);
    handleMessageBatch = sandbox.stub().resolves(null);
    sqs = sandbox.mock();
    sqs.receiveMessage = stubResolve(response);
    sqs.deleteMessage = stubResolve();
    sqs.deleteMessageBatch = stubResolve();
    sqs.changeMessageVisibility = stubResolve();
    sqs.changeMessageVisibilityBatch = stubResolve();

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
      Consumer.create({
        region: 'some-region',
        handleMessage
      });
    });
  });

  it('requires a handleMessage or handleMessagesBatch function to be set', () => {
    assert.throws(() => {
      new Consumer({
        handleMessage: undefined,
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

  it('requires visibilityTimeout to be set with heartbeatInterval', () => {
    assert.throws(() => {
      new Consumer({
        region: 'some-region',
        queueUrl: 'some-queue-url',
        handleMessage,
        heartbeatInterval: 30
      });
    });
  });

  it('requires heartbeatInterval to be less than visibilityTimeout', () => {
    assert.throws(() => {
      new Consumer({
        region: 'some-region',
        queueUrl: 'some-queue-url',
        handleMessage,
        heartbeatInterval: 30,
        visibilityTimeout: 30
      });
    });
  });

  describe('.create', () => {
    it('creates a new instance of a Consumer object', () => {
      const instance = Consumer.create({
        region: 'some-region',
        queueUrl: 'some-queue-url',
        batchSize: 1,
        visibilityTimeout: 10,
        waitTimeSeconds: 10,
        handleMessage
      });

      assert.instanceOf(instance, Consumer);
    });
  });

  describe('.start', () => {
    it('fires an error event when an error occurs receiving a message', async () => {
      const receiveErr = new Error('Receive error');

      sqs.receiveMessage = stubReject(receiveErr);

      consumer.start();

      const err: any = await pEvent(consumer, 'error');

      consumer.stop();
      assert.ok(err);
      assert.equal(err.message, 'SQS receive message failed: Receive error');
    });

    it('retains sqs error information', async () => {
      const receiveErr = new MockSQSError('Receive error');
      receiveErr.code = 'short code';
      receiveErr.retryable = false;
      receiveErr.statusCode = 403;
      receiveErr.time = new Date();
      receiveErr.hostname = 'hostname';
      receiveErr.region = 'eu-west-1';

      sqs.receiveMessage = stubReject(receiveErr);

      consumer.start();
      const err: any = await pEvent(consumer, 'error');
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, 'SQS receive message failed: Receive error');
      assert.equal(err.code, receiveErr.code);
      assert.equal(err.retryable, receiveErr.retryable);
      assert.equal(err.statusCode, receiveErr.statusCode);
      assert.equal(err.time, receiveErr.time);
      assert.equal(err.hostname, receiveErr.hostname);
      assert.equal(err.region, receiveErr.region);
    });

    it('fires a timeout event if handler function takes too long', async () => {
      const handleMessageTimeout = 500;
      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        region: 'some-region',
        handleMessage: () => new Promise((resolve) => setTimeout(resolve, 1000)),
        handleMessageTimeout,
        sqs,
        authenticationErrorTimeout: 20
      });

      consumer.start();
      const [err]: any = await Promise.all([pEvent(consumer, 'timeout_error'), clock.tickAsync(handleMessageTimeout)]);
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, `Message handler timed out after ${handleMessageTimeout}ms: Operation timed out.`);
    });

    it('handles unexpected exceptions thrown by the handler function', async () => {
      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        region: 'some-region',
        handleMessage: () => {
          throw new Error('unexpected parsing error');
        },
        sqs,
        authenticationErrorTimeout: 20
      });

      consumer.start();
      const err: any = await pEvent(consumer, 'processing_error');
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, 'Unexpected message handler failure: unexpected parsing error');
    });

    it('fires an error event when an error occurs deleting a message', async () => {
      const deleteErr = new Error('Delete error');

      handleMessage.resolves(null);
      sqs.deleteMessage = stubReject(deleteErr);

      consumer.start();
      const err: any = await pEvent(consumer, 'error');
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, 'SQS delete message failed: Delete error');
    });

    it('fires a `processing_error` event when a non-`SQSError` error occurs processing a message', async () => {
      const processingErr = new Error('Processing error');

      handleMessage.rejects(processingErr);

      consumer.start();
      const [err, message] = await pEvent(consumer, 'processing_error', { multiArgs: true });
      consumer.stop();

      assert.equal(err.message, 'Unexpected message handler failure: Processing error');
      assert.equal(message.MessageId, '123');
    });

    it('fires an `error` event when an `SQSError` occurs processing a message', async () => {
      const sqsError = new Error('Processing error');
      sqsError.name = 'SQSError';

      handleMessage.resolves(sqsError);
      sqs.deleteMessage = stubReject(sqsError);

      consumer.start();
      const [err, message] = await pEvent(consumer, 'error', { multiArgs: true });
      consumer.stop();

      assert.equal(err.message, 'SQS delete message failed: Processing error');
      assert.equal(message.MessageId, '123');
    });

    it('waits before repolling when a credentials error occurs', async () => {
      const credentialsErr = {
        code: 'CredentialsError',
        message: 'Missing credentials in config'
      };
      sqs.receiveMessage = stubReject(credentialsErr);
      const errorListener = sandbox.stub();
      consumer.on('error', errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledTwice(sqs.receiveMessage);
    });

    it('waits before repolling when a 403 error occurs', async () => {
      const invalidSignatureErr = {
        statusCode: 403,
        message: 'The security token included in the request is invalid'
      };
      sqs.receiveMessage = stubReject(invalidSignatureErr);
      const errorListener = sandbox.stub();
      consumer.on('error', errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledTwice(sqs.receiveMessage);
    });

    it('waits before repolling when a UnknownEndpoint error occurs', async () => {
      const unknownEndpointErr = {
        code: 'UnknownEndpoint',
        message: 'Inaccessible host: `sqs.eu-west-1.amazonaws.com`. This service may not be available in the `eu-west-1` region.'
      };
      sqs.receiveMessage = stubReject(unknownEndpointErr);
      const errorListener = sandbox.stub();
      consumer.on('error', errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledTwice(sqs.receiveMessage);
    });

    it('waits before repolling when a polling timeout is set', async () => {
      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        region: 'some-region',
        handleMessage,
        sqs,
        authenticationErrorTimeout: 20,
        pollingWaitTimeMs: POLLING_TIMEOUT
      });

      consumer.start();
      await clock.tickAsync(POLLING_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(sqs.receiveMessage);
    });

    it('fires a message_received event when a message is received', async () => {
      consumer.start();
      const message = await pEvent(consumer, 'message_received');
      consumer.stop();

      assert.equal(message, response.Messages[0]);
    });

    it('fires a message_processed event when a message is successfully deleted', async () => {
      handleMessage.resolves();

      consumer.start();
      const message = await pEvent(consumer, 'message_received');
      consumer.stop();

      assert.equal(message, response.Messages[0]);
    });

    it('calls the handleMessage function when a message is received', async () => {
      consumer.start();
      await pEvent(consumer, 'message_processed');
      consumer.stop();

      sandbox.assert.calledWith(handleMessage, response.Messages[0]);
    });

    it('deletes the message when the handleMessage function is called', async () => {
      handleMessage.resolves();

      consumer.start();
      await pEvent(consumer, 'message_processed');
      consumer.stop();

      sandbox.assert.calledWith(sqs.deleteMessage, {
        QueueUrl: 'some-queue-url',
        ReceiptHandle: 'receipt-handle'
      });
    });

    it('doesn\'t delete the message when a processing error is reported', async () => {
      handleMessage.rejects(new Error('Processing error'));

      consumer.start();
      await pEvent(consumer, 'processing_error');
      consumer.stop();

      sandbox.assert.notCalled(sqs.deleteMessage);
    });

    it('consumes another message once one is processed', async () => {
      handleMessage.resolves();

      consumer.start();
      await clock.runToLastAsync();
      consumer.stop();

      sandbox.assert.calledTwice(handleMessage);
    });

    it('doesn\'t consume more messages when called multiple times', () => {
      sqs.receiveMessage = stubResolve(new Promise((res) => setTimeout(res, 100)));
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.stop();

      sandbox.assert.calledOnce(sqs.receiveMessage);
    });

    it('consumes multiple messages when the batchSize is greater than 1', async () => {
      sqs.receiveMessage = stubResolve({
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

      return new Promise((resolve) => {
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
          resolve();
        });
      });
    });

    it('consumes messages with message attribute \'ApproximateReceiveCount\'', async () => {
      const messageWithAttr = {
        ReceiptHandle: 'receipt-handle-1',
        MessageId: '1',
        Body: 'body-1',
        Attributes: {
          ApproximateReceiveCount: 1
        }
      };

      sqs.receiveMessage = stubResolve({
        Messages: [messageWithAttr]
      });

      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        attributeNames: ['ApproximateReceiveCount'],
        region: 'some-region',
        handleMessage,
        sqs
      });

      consumer.start();
      const message = await pEvent(consumer, 'message_received');
      consumer.stop();

      sandbox.assert.calledWith(sqs.receiveMessage, {
        QueueUrl: 'some-queue-url',
        AttributeNames: ['ApproximateReceiveCount'],
        MessageAttributeNames: [],
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 20,
        VisibilityTimeout: undefined
      });

      assert.equal(message, messageWithAttr);
    });

    it('fires an emptyQueue event when all messages have been consumed', async () => {
      sqs.receiveMessage = stubResolve({});

      consumer.start();
      await pEvent(consumer, 'empty');
      consumer.stop();
    });

    it('terminate message visibility timeout on processing error', async () => {
      handleMessage.rejects(new Error('Processing error'));

      consumer.terminateVisibilityTimeout = true;

      consumer.start();
      await pEvent(consumer, 'processing_error');
      consumer.stop();

      sandbox.assert.calledWith(sqs.changeMessageVisibility, {
        QueueUrl: 'some-queue-url',
        ReceiptHandle: 'receipt-handle',
        VisibilityTimeout: 0
      });
    });

    it('does not terminate visibility timeout when `terminateVisibilityTimeout` option is false', async () => {
      handleMessage.rejects(new Error('Processing error'));
      consumer.terminateVisibilityTimeout = false;

      consumer.start();
      await pEvent(consumer, 'processing_error');
      consumer.stop();

      sandbox.assert.notCalled(sqs.changeMessageVisibility);
    });

    it('fires error event when failed to terminate visibility timeout on processing error', async () => {
      handleMessage.rejects(new Error('Processing error'));

      const sqsError = new Error('Processing error');
      sqsError.name = 'SQSError';
      sqs.changeMessageVisibility = stubReject(sqsError);
      consumer.terminateVisibilityTimeout = true;

      consumer.start();
      await pEvent(consumer, 'error');
      consumer.stop();

      sandbox.assert.calledWith(sqs.changeMessageVisibility, {
        QueueUrl: 'some-queue-url',
        ReceiptHandle: 'receipt-handle',
        VisibilityTimeout: 0
      });
    });

    it('fires response_processed event for each batch', async () => {
      sqs.receiveMessage = stubResolve({
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

      consumer.start();
      await pEvent(consumer, 'response_processed');
      consumer.stop();

      sandbox.assert.callCount(handleMessage, 2);
    });

    it('calls the handleMessagesBatch function when a batch of messages is received', async () => {
      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        messageAttributeNames: ['attribute-1', 'attribute-2'],
        region: 'some-region',
        handleMessageBatch,
        batchSize: 2,
        sqs
      });

      consumer.start();
      await pEvent(consumer, 'response_processed');
      consumer.stop();

      sandbox.assert.callCount(handleMessageBatch, 1);
    });

    it('prefers handleMessagesBatch over handleMessage when both are set', async () => {
      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        messageAttributeNames: ['attribute-1', 'attribute-2'],
        region: 'some-region',
        handleMessageBatch,
        handleMessage,
        batchSize: 2,
        sqs
      });

      consumer.start();
      await pEvent(consumer, 'response_processed');
      consumer.stop();

      sandbox.assert.callCount(handleMessageBatch, 1);
      sandbox.assert.callCount(handleMessage, 0);

    });

    it('uses the correct visibility timeout for long running handler functions', async () => {
      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        region: 'some-region',
        handleMessage: () => new Promise((resolve) => setTimeout(resolve, 75000)),
        sqs,
        visibilityTimeout: 40,
        heartbeatInterval: 30
      });
      const clearIntervalSpy = sinon.spy(global, 'clearInterval');

      consumer.start();
      await Promise.all([pEvent(consumer, 'response_processed'), clock.tickAsync(75000)]);
      consumer.stop();

      sandbox.assert.calledWith(sqs.changeMessageVisibility, {
        QueueUrl: 'some-queue-url',
        ReceiptHandle: 'receipt-handle',
        VisibilityTimeout: 40
      });
      sandbox.assert.calledWith(sqs.changeMessageVisibility, {
        QueueUrl: 'some-queue-url',
        ReceiptHandle: 'receipt-handle',
        VisibilityTimeout: 40
      });
      sandbox.assert.calledOnce(clearIntervalSpy);
    });

    it('passes in the correct visibility timeout for long running batch handler functions', async () => {
      sqs.receiveMessage = stubResolve({
        Messages: [
          { MessageId: '1', ReceiptHandle: 'receipt-handle-1', Body: 'body-1' },
          { MessageId: '2', ReceiptHandle: 'receipt-handle-2', Body: 'body-2' },
          { MessageId: '3', ReceiptHandle: 'receipt-handle-3', Body: 'body-3' }
        ]
      });
      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        region: 'some-region',
        handleMessageBatch: () => new Promise((resolve) => setTimeout(resolve, 75000)),
        batchSize: 3,
        sqs,
        visibilityTimeout: 40,
        heartbeatInterval: 30
      });
      const clearIntervalSpy = sinon.spy(global, 'clearInterval');

      consumer.start();
      await Promise.all([pEvent(consumer, 'response_processed'), clock.tickAsync(75000)]);
      consumer.stop();

      sandbox.assert.calledWith(sqs.changeMessageVisibilityBatch, {
        QueueUrl: 'some-queue-url',
        Entries: [
          { Id: '1', ReceiptHandle: 'receipt-handle-1', VisibilityTimeout: 40 },
          { Id: '2', ReceiptHandle: 'receipt-handle-2', VisibilityTimeout: 40 },
          { Id: '3', ReceiptHandle: 'receipt-handle-3', VisibilityTimeout: 40 }
        ]
      });
      sandbox.assert.calledOnce(clearIntervalSpy);
    });

    it('can process more messages than the batch limit', async () => {
      sqs.receiveMessage = stubResolve({
        Messages: [
          { MessageId: '1', ReceiptHandle: 'receipt-handle-1', Body: 'body-1' },
          { MessageId: '2', ReceiptHandle: 'receipt-handle-2', Body: 'body-2' },
          { MessageId: '3', ReceiptHandle: 'receipt-handle-3', Body: 'body-3' }
        ]
      });
      handleMessage = sinon.stub().callsFake(() => new Promise((resolve) => setTimeout(resolve, 100))),
      consumer = new Consumer({
        queueUrl: 'some-queue-url',
        region: 'some-region',
        handleMessage,
        batchSize: 3,
        concurrency: 6,
        sqs,
      });

      consumer.start();
      await clock.tickAsync(100);
      consumer.stop();

      sandbox.assert.callCount(handleMessage, 6);
    });
  });

  describe('.stop', () => {
    it('stops the consumer polling for messages', async () => {
      consumer.start();
      consumer.stop();

      await Promise.all([pEvent(consumer, 'stopped'), clock.runAllAsync()]);

      sandbox.assert.calledOnce(handleMessage);
    });

    it('fires a stopped event when last poll occurs after stopping', async () => {
      consumer.start();
      consumer.stop();
      await Promise.all([pEvent(consumer, 'stopped'), clock.runAllAsync()]);
    });

    it('fires a stopped event only once when stopped multiple times', async () => {
      const handleStop = sandbox.stub().returns(null);

      consumer.on('stopped', handleStop);

      consumer.start();
      consumer.stop();
      consumer.stop();
      consumer.stop();
      await clock.runAllAsync();

      sandbox.assert.calledOnce(handleStop);
    });

    it('fires a stopped event a second time if started and stopped twice', async () => {
      const handleStop = sandbox.stub().returns(null);

      consumer.on('stopped', handleStop);

      consumer.start();
      consumer.stop();
      await clock.runAllAsync();
      consumer.start();
      consumer.stop();
      await clock.runAllAsync();

      sandbox.assert.calledTwice(handleStop);
    });
  });

  describe('isRunning', async () => {
    it('returns true if the consumer is polling', () => {
      consumer.start();
      assert.isTrue(consumer.isRunning);
      consumer.stop();
    });

    it('returns false if the consumer is not polling', () => {
      consumer.start();
      consumer.stop();
      assert.isFalse(consumer.isRunning);
    });
  });
});
