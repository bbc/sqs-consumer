import {
  ChangeMessageVisibilityBatchCommand,
  ChangeMessageVisibilityCommand,
  DeleteMessageBatchCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SQSClient
} from '@aws-sdk/client-sqs';
import { assert } from 'chai';
import * as pEvent from 'p-event';

import * as sinon from 'sinon';
import { Consumer } from '../src/index';
import { SdkError } from '@aws-sdk/smithy-client';

const sandbox = sinon.createSandbox();

const AUTHENTICATION_ERROR_TIMEOUT = 20; // 20???
const POLLING_TIMEOUT = 1; // 100???

const QUEUE_URL = 'some-queue-url';
const REGION = 'some-region';

const mockReceiveMessage = sinon.match.instanceOf(ReceiveMessageCommand);
const mockDeleteMessage = sinon.match.instanceOf(DeleteMessageCommand);
const mockDeleteMessageBatch = sinon.match.instanceOf(DeleteMessageBatchCommand);
const mockChangeMessageVisibility = sinon.match.instanceOf(ChangeMessageVisibilityCommand);
const mockChangeMessageVisibilityBatch = sinon.match.instanceOf(ChangeMessageVisibilityBatchCommand);

class MockSQSError extends Error implements SdkError {
  name: string;
  $metadata: {
    httpStatusCode: number;
  };
  $service: string;
  $retryable: {
    throttling: boolean;
  };
  $fault: 'client' | 'server'
  time: Date;

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
    sqs = sinon.createStubInstance(SQSClient);
    sqs.send = sinon.stub();

    sqs.send.withArgs(mockReceiveMessage).resolves(response);
    sqs.send.withArgs(mockDeleteMessage).resolves();
    sqs.send.withArgs(mockDeleteMessageBatch).resolves();
    sqs.send.withArgs(mockChangeMessageVisibility).resolves();
    sqs.send.withArgs(mockChangeMessageVisibilityBatch).resolves();

    consumer = new Consumer({
      queueUrl: QUEUE_URL,
      region: REGION,
      handleMessage,
      sqs,
      authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT
    });
  });

  afterEach(() => {
    sandbox.restore();
  });

  it('requires a queueUrl to be set', () => {
    assert.throws(() => {
      Consumer.create({
        region: REGION,
        handleMessage
      });
    });
  });

  it('requires a handleMessage or handleMessagesBatch function to be set', () => {
    assert.throws(() => {
      new Consumer({
        handleMessage: undefined,
        region: REGION,
        queueUrl: QUEUE_URL
      });
    });
  });

  it('requires the batchSize option to be no greater than 10', () => {
    assert.throws(() => {
      new Consumer({
        region: REGION,
        queueUrl: QUEUE_URL,
        handleMessage,
        batchSize: 11
      });
    });
  });

  it('requires the batchSize option to be greater than 0', () => {
    assert.throws(() => {
      new Consumer({
        region: REGION,
        queueUrl: QUEUE_URL,
        handleMessage,
        batchSize: -1
      });
    });
  });

  it('requires visibilityTimeout to be set with heartbeatInterval', () => {
    assert.throws(() => {
      new Consumer({
        region: REGION,
        queueUrl: QUEUE_URL,
        handleMessage,
        heartbeatInterval: 30
      });
    });
  });

  it('requires heartbeatInterval to be less than visibilityTimeout', () => {
    assert.throws(() => {
      new Consumer({
        region: REGION,
        queueUrl: QUEUE_URL,
        handleMessage,
        heartbeatInterval: 30,
        visibilityTimeout: 30
      });
    });
  });

  describe('.create', () => {
    it('creates a new instance of a Consumer object', () => {
      const instance = Consumer.create({
        region: REGION,
        queueUrl: QUEUE_URL,
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

      sqs.send.withArgs(mockReceiveMessage).rejects(receiveErr);

      consumer.start();

      const err: any = await pEvent(consumer, 'error');

      consumer.stop();
      assert.ok(err);
      assert.equal(err.message, 'SQS receive message failed: Receive error');
    });

    it('retains sqs error information', async () => {
      const receiveErr = new MockSQSError('Receive error');
      receiveErr.name = 'short code';
      receiveErr.$retryable = {
        throttling: false
      };
      receiveErr.$metadata = {
        httpStatusCode: 403
      };
      receiveErr.time = new Date();
      receiveErr.$service = 'service';

      sqs.send.withArgs(mockReceiveMessage).rejects(receiveErr);

      consumer.start();
      const err: any = await pEvent(consumer, 'error');
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, 'SQS receive message failed: Receive error');
      assert.equal(err.code, receiveErr.name);
      assert.equal(err.retryable, receiveErr.$retryable.throttling);
      assert.equal(err.statusCode, receiveErr.$metadata.httpStatusCode);
      assert.equal(err.time.toString(), receiveErr.time.toString());
      assert.equal(err.service, receiveErr.$service);
      assert.equal(err.fault, receiveErr.$fault);
    });

    it('fires a timeout event if handler function takes too long', async () => {
      const handleMessageTimeout = 500;
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
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
        queueUrl: QUEUE_URL,
        region: REGION,
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
      sqs.send.withArgs(mockDeleteMessage).rejects(deleteErr);

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
      sqs.send.withArgs(mockDeleteMessage).rejects(sqsError);

      consumer.start();
      const [err, message] = await pEvent(consumer, 'error', { multiArgs: true });
      consumer.stop();

      assert.equal(err.message, 'SQS delete message failed: Processing error');
      assert.equal(message.MessageId, '123');
    });

    it('waits before repolling when a credentials error occurs', async () => {
      const credentialsErr = {
        name: 'CredentialsError',
        message: 'Missing credentials in config'
      };
      sqs.send.withArgs(mockReceiveMessage).rejects(credentialsErr);
      const errorListener = sandbox.stub();
      consumer.on('error', errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockReceiveMessage);
    });

    it('waits before repolling when a 403 error occurs', async () => {
      const invalidSignatureErr = {
        $metadata: {
          httpStatusCode: 403
        },
        message: 'The security token included in the request is invalid'
      };
      sqs.send.withArgs(mockReceiveMessage).rejects(invalidSignatureErr);
      const errorListener = sandbox.stub();
      consumer.on('error', errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockReceiveMessage);
    });

    it('waits before repolling when a UnknownEndpoint error occurs', async () => {
      const unknownEndpointErr = {
        name: 'UnknownEndpoint',
        message: 'Inaccessible host: `sqs.eu-west-1.amazonaws.com`. This service may not be available in the `eu-west-1` region.'
      };
      sqs.send.withArgs(mockReceiveMessage).rejects(unknownEndpointErr);
      const errorListener = sandbox.stub();
      consumer.on('error', errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockReceiveMessage);
    });

    it('waits before repolling when a polling timeout is set', async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage,
        sqs,
        authenticationErrorTimeout: 20,
        pollingWaitTimeMs: 100
      });

      consumer.start();
      await clock.tickAsync(POLLING_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(sqs.send);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockDeleteMessage);
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

      sandbox.assert.calledWith(sqs.send.secondCall, mockDeleteMessage);
      sandbox.assert.match(sqs.send.secondCall.args[0].input, sinon.match({
        QueueUrl: QUEUE_URL,
        ReceiptHandle: 'receipt-handle'
      }));
    });

    it('doesn\'t delete the message when a processing error is reported', async () => {
      handleMessage.rejects(new Error('Processing error'));

      consumer.start();
      await pEvent(consumer, 'processing_error');
      consumer.stop();

      sandbox.assert.neverCalledWithMatch(sqs.send, mockDeleteMessage);
    });

    it('consumes another message once one is processed', async () => {
      handleMessage.resolves();

      consumer.start();
      await clock.runToLastAsync();
      consumer.stop();

      sandbox.assert.calledTwice(handleMessage);
    });

    it('doesn\'t consume more messages when called multiple times', () => {
      sqs.send.withArgs(mockReceiveMessage).resolves(new Promise((res) => setTimeout(res, 100)));
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.stop();

      sqs.send.calledOnceWith(mockReceiveMessage);
    });

    it('consumes multiple messages when the batchSize is greater than 1', async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({
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
        queueUrl: QUEUE_URL,
        messageAttributeNames: ['attribute-1', 'attribute-2'],
        region: REGION,
        handleMessage,
        batchSize: 3,
        sqs
      });

      consumer.start();
      await pEvent(consumer, 'message_received');
      consumer.stop();

      sandbox.assert.callCount(handleMessage, 3);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.match(sqs.send.firstCall.args[0].input, sinon.match({
        QueueUrl: QUEUE_URL,
        AttributeNames: [],
        MessageAttributeNames: ['attribute-1', 'attribute-2'],
        MaxNumberOfMessages: 3,
        WaitTimeSeconds: 20,
        VisibilityTimeout: undefined
      }));
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

      sqs.send.withArgs(mockReceiveMessage).resolves({
        Messages: [messageWithAttr]
      });

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        attributeNames: ['ApproximateReceiveCount'],
        region: REGION,
        handleMessage,
        sqs
      });

      consumer.start();
      const message = await pEvent(consumer, 'message_received');
      consumer.stop();

      sandbox.assert.calledWith(sqs.send, mockReceiveMessage);
      sandbox.assert.match(sqs.send.firstCall.args[0].input, sinon.match({
        QueueUrl: QUEUE_URL,
        AttributeNames: ['ApproximateReceiveCount'],
        MessageAttributeNames: [],
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 20,
        VisibilityTimeout: undefined
      }));

      assert.equal(message, messageWithAttr);
    });

    it('fires an emptyQueue event when all messages have been consumed', async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({});

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

      sandbox.assert.calledWith(sqs.send.secondCall, mockChangeMessageVisibility);
      sandbox.assert.match(sqs.send.secondCall.args[0].input, sinon.match({
        QueueUrl: QUEUE_URL,
        ReceiptHandle: 'receipt-handle',
        VisibilityTimeout: 0
      }));
    });

    it('does not terminate visibility timeout when `terminateVisibilityTimeout` option is false', async () => {
      handleMessage.rejects(new Error('Processing error'));
      consumer.terminateVisibilityTimeout = false;

      consumer.start();
      await pEvent(consumer, 'processing_error');
      consumer.stop();

      sqs.send.neverCalledWith(mockChangeMessageVisibility);
    });

    it('fires error event when failed to terminate visibility timeout on processing error', async () => {
      handleMessage.rejects(new Error('Processing error'));

      const sqsError = new Error('Processing error');
      sqsError.name = 'SQSError';
      sqs.send.withArgs(mockChangeMessageVisibility).rejects(sqsError);
      consumer.terminateVisibilityTimeout = true;

      consumer.start();
      await pEvent(consumer, 'error');
      consumer.stop();

      sandbox.assert.calledWith(sqs.send.secondCall, mockChangeMessageVisibility);
      sandbox.assert.match(sqs.send.secondCall.args[0].input, sinon.match({
        QueueUrl: QUEUE_URL,
        ReceiptHandle: 'receipt-handle',
        VisibilityTimeout: 0
      }));
    });

    it('fires response_processed event for each batch', async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({
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
        queueUrl: QUEUE_URL,
        messageAttributeNames: ['attribute-1', 'attribute-2'],
        region: REGION,
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
        queueUrl: QUEUE_URL,
        messageAttributeNames: ['attribute-1', 'attribute-2'],
        region: REGION,
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
        queueUrl: QUEUE_URL,
        messageAttributeNames: ['attribute-1', 'attribute-2'],
        region: REGION,
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
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: () => new Promise((resolve) => setTimeout(resolve, 75000)),
        sqs,
        visibilityTimeout: 40,
        heartbeatInterval: 30
      });
      const clearIntervalSpy = sinon.spy(global, 'clearInterval');

      consumer.start();
      await Promise.all([pEvent(consumer, 'response_processed'), clock.tickAsync(75000)]);
      consumer.stop();

      sandbox.assert.calledWith(sqs.send.secondCall, mockChangeMessageVisibility);
      sandbox.assert.match(sqs.send.secondCall.args[0].input, sinon.match({
        QueueUrl: QUEUE_URL,
        ReceiptHandle: 'receipt-handle',
        VisibilityTimeout: 40
      }));
      sandbox.assert.calledWith(sqs.send.thirdCall, mockChangeMessageVisibility);
      sandbox.assert.match(sqs.send.thirdCall.args[0].input, sinon.match({
        QueueUrl: QUEUE_URL,
        ReceiptHandle: 'receipt-handle',
        VisibilityTimeout: 40
      }));
      sandbox.assert.calledOnce(clearIntervalSpy);
    });
    it('extends visibility timeout for long running batch handler functions', async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({
        Messages: [
          { MessageId: '1', ReceiptHandle: 'receipt-handle-1', Body: 'body-1' },
          { MessageId: '2', ReceiptHandle: 'receipt-handle-2', Body: 'body-2' },
          { MessageId: '3', ReceiptHandle: 'receipt-handle-3', Body: 'body-3' }
        ]
      });
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
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

      sandbox.assert.calledWith(sqs.send.secondCall, mockChangeMessageVisibilityBatch);
      sandbox.assert.match(sqs.send.secondCall.args[0].input, sinon.match({
        QueueUrl: QUEUE_URL,
        Entries: sinon.match.array.deepEquals([
          { Id: '1', ReceiptHandle: 'receipt-handle-1', VisibilityTimeout: 40 },
          { Id: '2', ReceiptHandle: 'receipt-handle-2', VisibilityTimeout: 40 },
          { Id: '3', ReceiptHandle: 'receipt-handle-3', VisibilityTimeout: 40 }
        ])
      }));
      sandbox.assert.calledWith(sqs.send.thirdCall, mockChangeMessageVisibilityBatch);
      sandbox.assert.match(sqs.send.thirdCall.args[0].input, sinon.match({
        QueueUrl: QUEUE_URL,
        Entries: [
          { Id: '1', ReceiptHandle: 'receipt-handle-1', VisibilityTimeout: 40 },
          { Id: '2', ReceiptHandle: 'receipt-handle-2', VisibilityTimeout: 40 },
          { Id: '3', ReceiptHandle: 'receipt-handle-3', VisibilityTimeout: 40 }
        ]
      }));
      sandbox.assert.calledOnce(clearIntervalSpy);
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
