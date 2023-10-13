import {
  ChangeMessageVisibilityBatchCommand,
  ChangeMessageVisibilityCommand,
  DeleteMessageBatchCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SQSClient
} from '@aws-sdk/client-sqs';
import { assert } from 'chai';
import * as sinon from 'sinon';
import * as pEvent from 'p-event';

import { AWSError } from '../../src/types';
import { Consumer } from '../../src/consumer';
import { logger } from '../../src/logger';

const sandbox = sinon.createSandbox();

const AUTHENTICATION_ERROR_TIMEOUT = 20;
const POLLING_TIMEOUT = 100;
const QUEUE_URL = 'some-queue-url';
const REGION = 'some-region';

const mockReceiveMessage = sinon.match.instanceOf(ReceiveMessageCommand);
const mockDeleteMessage = sinon.match.instanceOf(DeleteMessageCommand);
const mockDeleteMessageBatch = sinon.match.instanceOf(
  DeleteMessageBatchCommand
);
const mockChangeMessageVisibility = sinon.match.instanceOf(
  ChangeMessageVisibilityCommand
);
const mockChangeMessageVisibilityBatch = sinon.match.instanceOf(
  ChangeMessageVisibilityBatchCommand
);

class MockSQSError extends Error implements AWSError {
  name: string;
  $metadata: {
    httpStatusCode: number;
  };
  $service: string;
  $retryable: {
    throttling: boolean;
  };
  $fault: 'client' | 'server';
  time: Date;

  constructor(message: string) {
    super(message);
    this.message = message;
  }
}

describe('Consumer', () => {
  let consumer;
  let clock;
  let handleMessage;
  let handleMessageBatch;
  let sqs;
  const response = {
    Messages: [
      {
        ReceiptHandle: 'receipt-handle',
        MessageId: '123',
        Body: 'body'
      }
    ]
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
    clock.restore();
    sandbox.restore();
  });

  describe('options validation', () => {
    it('requires a handleMessage or handleMessagesBatch function to be set', () => {
      assert.throws(() => {
        new Consumer({
          handleMessage: undefined,
          region: REGION,
          queueUrl: QUEUE_URL
        });
      }, `Missing SQS consumer option [ handleMessage or handleMessageBatch ].`);
    });

    it('requires the batchSize option to be no greater than 10', () => {
      assert.throws(() => {
        new Consumer({
          region: REGION,
          queueUrl: QUEUE_URL,
          handleMessage,
          batchSize: 11
        });
      }, 'batchSize must be between 1 and 10.');
    });

    it('requires the batchSize option to be greater than 0', () => {
      assert.throws(() => {
        new Consumer({
          region: REGION,
          queueUrl: QUEUE_URL,
          handleMessage,
          batchSize: -1
        });
      }, 'batchSize must be between 1 and 10.');
    });

    it('requires visibilityTimeout to be set with heartbeatInterval', () => {
      assert.throws(() => {
        new Consumer({
          region: REGION,
          queueUrl: QUEUE_URL,
          handleMessage,
          heartbeatInterval: 30
        });
      }, 'heartbeatInterval must be less than visibilityTimeout.');
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
      }, 'heartbeatInterval must be less than visibilityTimeout.');
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
    it('uses the correct abort signal', async () => {
      sqs.send
        .withArgs(mockReceiveMessage)
        .resolves(new Promise((res) => setTimeout(res, 100)));

      // Starts and abort is false
      consumer.start();
      assert.isFalse(sqs.send.lastCall.lastArg.abortSignal.aborted);

      // normal stop without an abort and abort is false
      consumer.stop();
      assert.isFalse(sqs.send.lastCall.lastArg.abortSignal.aborted);

      // Starts and abort is false
      consumer.start();
      assert.isFalse(sqs.send.lastCall.lastArg.abortSignal.aborted);

      // Stop with abort and abort is true
      consumer.stop({ abort: true });
      assert.isTrue(sqs.send.lastCall.lastArg.abortSignal.aborted);

      // Starts and abort is false
      consumer.start();
      assert.isFalse(sqs.send.lastCall.lastArg.abortSignal.aborted);
    });

    it('fires an event when the consumer is started', async () => {
      const handleStart = sandbox.stub().returns(null);

      consumer.on('started', handleStart);

      consumer.start();
      consumer.stop();

      sandbox.assert.calledOnce(handleStart);
    });

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
        handleMessage: () =>
          new Promise((resolve) => setTimeout(resolve, 1000)),
        handleMessageTimeout,
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT
      });

      consumer.start();
      const [err]: any = await Promise.all([
        pEvent(consumer, 'timeout_error'),
        clock.tickAsync(handleMessageTimeout)
      ]);
      consumer.stop();

      assert.ok(err);
      assert.equal(
        err.message,
        `Message handler timed out after ${handleMessageTimeout}ms: Operation timed out.`
      );
    });

    it('handles unexpected exceptions thrown by the handler function', async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: () => {
          throw new Error('unexpected parsing error');
        },
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT
      });

      consumer.start();
      const err: any = await pEvent(consumer, 'processing_error');
      consumer.stop();

      assert.ok(err);
      assert.equal(
        err.message,
        'Unexpected message handler failure: unexpected parsing error'
      );
    });

    it('handles non-standard exceptions thrown by the handler function', async () => {
      class CustomError {
        private _message: string;

        constructor(message) {
          this._message = message;
        }

        get message() {
          return this._message;
        }
      }

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: () => {
          throw new CustomError('unexpected parsing error');
        },
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT
      });

      consumer.start();
      const err: any = await pEvent(consumer, 'processing_error');
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, 'unexpected parsing error');
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
      const [err, message] = await pEvent<
        string | symbol,
        { [key: string]: string }[]
      >(consumer, 'processing_error', {
        multiArgs: true
      });
      consumer.stop();

      assert.equal(
        err instanceof Error ? err.message : '',
        'Unexpected message handler failure: Processing error'
      );
      assert.equal(message.MessageId, '123');
    });

    it('fires an `error` event when an `SQSError` occurs processing a message', async () => {
      const sqsError = new Error('Processing error');
      sqsError.name = 'SQSError';

      handleMessage.resolves();
      sqs.send.withArgs(mockDeleteMessage).rejects(sqsError);

      consumer.start();
      const [err, message] = await pEvent<
        string | symbol,
        { [key: string]: string }[]
      >(consumer, 'error', {
        multiArgs: true
      });
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
        message:
          'Inaccessible host: `sqs.eu-west-1.amazonaws.com`. This service may not be available in the `eu-west-1` region.'
      };
      sqs.send.withArgs(mockReceiveMessage).rejects(unknownEndpointErr);
      const errorListener = sandbox.stub();
      consumer.on('error', errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledTwice(sqs.send);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockReceiveMessage);
    });

    it('waits before repolling when a NonExistentQueue error occurs', async () => {
      const nonExistentQueueErr = {
        name: 'AWS.SimpleQueueService.NonExistentQueue',
        message: 'The specified queue does not exist for this wsdl version.'
      };
      sqs.send.withArgs(mockReceiveMessage).rejects(nonExistentQueueErr);
      const errorListener = sandbox.stub();
      consumer.on('error', errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledTwice(sqs.send);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockReceiveMessage);
    });

    it('waits before repolling when a polling timeout is set', async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage,
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
        pollingWaitTimeMs: POLLING_TIMEOUT
      });

      consumer.start();
      await clock.tickAsync(POLLING_TIMEOUT);
      consumer.stop();

      sandbox.assert.callCount(sqs.send, 4);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockDeleteMessage);
      sandbox.assert.calledWithMatch(sqs.send.thirdCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.getCall(3), mockDeleteMessage);
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
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          ReceiptHandle: 'receipt-handle'
        })
      );
    });

    it('does not delete the message if shouldDeleteMessages is false', async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage,
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
        shouldDeleteMessages: false
      });

      handleMessage.resolves();

      consumer.start();
      await pEvent(consumer, 'message_processed');
      consumer.stop();

      sandbox.assert.neverCalledWithMatch(sqs.send, mockDeleteMessage);
    });

    it("doesn't delete the message when a processing error is reported", async () => {
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

    it("doesn't consume more messages when called multiple times", () => {
      sqs.send
        .withArgs(mockReceiveMessage)
        .resolves(new Promise((res) => setTimeout(res, 100)));
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.stop();

      sqs.send.calledOnceWith(mockReceiveMessage);
    });

    it("doesn't consume more messages when called multiple times after stopped", () => {
      sqs.send
        .withArgs(mockReceiveMessage)
        .resolves(new Promise((res) => setTimeout(res, 100)));
      consumer.start();
      consumer.stop();

      consumer.start();
      consumer.start();
      consumer.start();
      consumer.start();

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
      sandbox.assert.match(
        sqs.send.firstCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          AttributeNames: [],
          MessageAttributeNames: ['attribute-1', 'attribute-2'],
          MaxNumberOfMessages: 3,
          WaitTimeSeconds: AUTHENTICATION_ERROR_TIMEOUT,
          VisibilityTimeout: undefined
        })
      );
    });

    it("consumes messages with message attribute 'ApproximateReceiveCount'", async () => {
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
      sandbox.assert.match(
        sqs.send.firstCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          AttributeNames: ['ApproximateReceiveCount'],
          MessageAttributeNames: [],
          MaxNumberOfMessages: 1,
          WaitTimeSeconds: AUTHENTICATION_ERROR_TIMEOUT,
          VisibilityTimeout: undefined
        })
      );

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

      sandbox.assert.calledWith(
        sqs.send.secondCall,
        mockChangeMessageVisibility
      );
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          ReceiptHandle: 'receipt-handle',
          VisibilityTimeout: 0
        })
      );
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

      sandbox.assert.calledWith(
        sqs.send.secondCall,
        mockChangeMessageVisibility
      );
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          ReceiptHandle: 'receipt-handle',
          VisibilityTimeout: 0
        })
      );
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

    it('handles unexpected exceptions thrown by the handler batch function', async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        messageAttributeNames: ['attribute-1', 'attribute-2'],
        region: REGION,
        handleMessageBatch: () => {
          throw new Error('unexpected parsing error');
        },
        batchSize: 2,
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT
      });

      consumer.start();
      const err: any = await pEvent(consumer, 'error');
      consumer.stop();

      assert.ok(err);
      assert.equal(
        err.message,
        'Unexpected message handler failure: unexpected parsing error'
      );
    });

    it('handles non-standard exceptions thrown by the handler batch function', async () => {
      class CustomError {
        private _message: string;

        constructor(message) {
          this._message = message;
        }

        get message() {
          return this._message;
        }
      }

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        messageAttributeNames: ['attribute-1', 'attribute-2'],
        region: REGION,
        handleMessageBatch: () => {
          throw new CustomError('unexpected parsing error');
        },
        batchSize: 2,
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT
      });

      consumer.start();
      const err: any = await pEvent(consumer, 'error');
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, 'unexpected parsing error');
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

    it('ack the message if handleMessage returns void', async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: async () => {},
        sqs
      });

      consumer.start();
      await pEvent(consumer, 'message_processed');
      consumer.stop();

      sandbox.assert.callCount(sqs.send, 2);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockDeleteMessage);
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          ReceiptHandle: 'receipt-handle'
        })
      );
    });

    it('ack the message if handleMessage returns a message with the same ID', async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: async () => {
          return {
            MessageId: '123'
          };
        },
        sqs
      });

      consumer.start();
      await pEvent(consumer, 'message_processed');
      consumer.stop();

      sandbox.assert.callCount(sqs.send, 2);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockDeleteMessage);
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          ReceiptHandle: 'receipt-handle'
        })
      );
    });

    it('does not ack the message if handleMessage returns an empty object', async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: async () => {
          return {};
        },
        sqs
      });

      consumer.start();
      await pEvent(consumer, 'response_processed');
      consumer.stop();

      sandbox.assert.callCount(sqs.send, 1);
      sandbox.assert.neverCalledWithMatch(sqs.send, mockDeleteMessage);
    });

    it('does not ack the message if handleMessage returns a different ID', async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: async () => {
          return {
            MessageId: '143'
          };
        },
        sqs
      });

      consumer.start();
      await pEvent(consumer, 'response_processed');
      consumer.stop();

      sandbox.assert.callCount(sqs.send, 1);
      sandbox.assert.neverCalledWithMatch(sqs.send, mockDeleteMessage);
    });

    it('does not call deleteMessageBatch if handleMessagesBatch returns an empty array', async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: async () => [],
        batchSize: 2,
        sqs
      });

      consumer.start();
      await pEvent(consumer, 'response_processed');
      consumer.stop();

      sandbox.assert.callCount(sqs.send, 1);
      sandbox.assert.neverCalledWithMatch(sqs.send, mockDeleteMessageBatch);
    });

    it('ack all messages if handleMessageBatch returns void', async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: async () => {},
        batchSize: 2,
        sqs
      });

      consumer.start();
      await pEvent(consumer, 'response_processed');
      consumer.stop();

      sandbox.assert.callCount(sqs.send, 2);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(
        sqs.send.secondCall,
        mockDeleteMessageBatch
      );
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          Entries: [{ Id: '123', ReceiptHandle: 'receipt-handle' }]
        })
      );
    });

    it('ack only returned messages if handleMessagesBatch returns an array', async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: async () => [
          { MessageId: '123', ReceiptHandle: 'receipt-handle' }
        ],
        batchSize: 2,
        sqs
      });

      consumer.start();
      await pEvent(consumer, 'response_processed');
      consumer.stop();

      sandbox.assert.callCount(sqs.send, 2);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(
        sqs.send.secondCall,
        mockDeleteMessageBatch
      );
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          Entries: [{ Id: '123', ReceiptHandle: 'receipt-handle' }]
        })
      );
    });

    it('uses the correct visibility timeout for long running handler functions', async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: () =>
          new Promise((resolve) => setTimeout(resolve, 75000)),
        sqs,
        visibilityTimeout: 40,
        heartbeatInterval: 30
      });
      const clearIntervalSpy = sinon.spy(global, 'clearInterval');

      consumer.start();
      await Promise.all([
        pEvent(consumer, 'response_processed'),
        clock.tickAsync(75000)
      ]);
      consumer.stop();

      sandbox.assert.calledWith(
        sqs.send.secondCall,
        mockChangeMessageVisibility
      );
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          ReceiptHandle: 'receipt-handle',
          VisibilityTimeout: 40
        })
      );
      sandbox.assert.calledWith(
        sqs.send.thirdCall,
        mockChangeMessageVisibility
      );
      sandbox.assert.match(
        sqs.send.thirdCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          ReceiptHandle: 'receipt-handle',
          VisibilityTimeout: 40
        })
      );
      sandbox.assert.calledTwice(clearIntervalSpy);
    });

    it('passes in the correct visibility timeout for long running batch handler functions', async () => {
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
        handleMessageBatch: () =>
          new Promise((resolve) => setTimeout(resolve, 75000)),
        batchSize: 3,
        sqs,
        visibilityTimeout: 40,
        heartbeatInterval: 30
      });
      const clearIntervalSpy = sinon.spy(global, 'clearInterval');

      consumer.start();
      await Promise.all([
        pEvent(consumer, 'response_processed'),
        clock.tickAsync(75000)
      ]);
      consumer.stop();

      sandbox.assert.calledWith(
        sqs.send.secondCall,
        mockChangeMessageVisibilityBatch
      );
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          Entries: sinon.match.array.deepEquals([
            {
              Id: '1',
              ReceiptHandle: 'receipt-handle-1',
              VisibilityTimeout: 40
            },
            {
              Id: '2',
              ReceiptHandle: 'receipt-handle-2',
              VisibilityTimeout: 40
            },
            {
              Id: '3',
              ReceiptHandle: 'receipt-handle-3',
              VisibilityTimeout: 40
            }
          ])
        })
      );
      sandbox.assert.calledWith(
        sqs.send.thirdCall,
        mockChangeMessageVisibilityBatch
      );
      sandbox.assert.match(
        sqs.send.thirdCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          Entries: [
            {
              Id: '1',
              ReceiptHandle: 'receipt-handle-1',
              VisibilityTimeout: 40
            },
            {
              Id: '2',
              ReceiptHandle: 'receipt-handle-2',
              VisibilityTimeout: 40
            },
            {
              Id: '3',
              ReceiptHandle: 'receipt-handle-3',
              VisibilityTimeout: 40
            }
          ]
        })
      );
      sandbox.assert.calledTwice(clearIntervalSpy);
    });

    it('emit error when changing visibility timeout fails', async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({
        Messages: [
          { MessageId: '1', ReceiptHandle: 'receipt-handle-1', Body: 'body-1' }
        ]
      });
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: () =>
          new Promise((resolve) => setTimeout(resolve, 75000)),
        sqs,
        visibilityTimeout: 40,
        heartbeatInterval: 30
      });

      const receiveErr = new MockSQSError('failed');
      sqs.send.withArgs(mockChangeMessageVisibility).rejects(receiveErr);

      consumer.start();
      const [err]: any[] = await Promise.all([
        pEvent(consumer, 'error'),
        clock.tickAsync(75000)
      ]);
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, 'Error changing visibility timeout: failed');
    });

    it('emit error when changing visibility timeout fails for batch handler functions', async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({
        Messages: [
          { MessageId: '1', ReceiptHandle: 'receipt-handle-1', Body: 'body-1' },
          { MessageId: '2', ReceiptHandle: 'receipt-handle-2', Body: 'body-2' }
        ]
      });
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: () =>
          new Promise((resolve) => setTimeout(resolve, 75000)),
        sqs,
        batchSize: 2,
        visibilityTimeout: 40,
        heartbeatInterval: 30
      });

      const receiveErr = new MockSQSError('failed');
      sqs.send.withArgs(mockChangeMessageVisibilityBatch).rejects(receiveErr);

      consumer.start();
      const [err]: any[] = await Promise.all([
        pEvent(consumer, 'error'),
        clock.tickAsync(75000)
      ]);
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, 'Error changing visibility timeout: failed');
    });
  });

  describe('event listeners', () => {
    it('fires the event multiple times', async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({});

      const handleEmpty = sandbox.stub().returns(null);

      consumer.on('empty', handleEmpty);

      consumer.start();

      await clock.tickAsync(0);

      consumer.stop();

      await clock.runAllAsync();

      sandbox.assert.calledTwice(handleEmpty);
    });

    it('fires the events only once', async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({});

      const handleEmpty = sandbox.stub().returns(null);

      consumer.once('empty', handleEmpty);

      consumer.start();

      await clock.tickAsync(0);

      consumer.stop();

      await clock.runAllAsync();

      sandbox.assert.calledOnce(handleEmpty);
    });
  });

  describe('.stop', () => {
    it('stops the consumer polling for messages', async () => {
      const handleStop = sandbox.stub().returns(null);

      consumer.on('stopped', handleStop);

      consumer.start();
      consumer.stop();

      await clock.runAllAsync();

      sandbox.assert.calledOnce(handleStop);
      sandbox.assert.calledOnce(handleMessage);
    });

    it('clears the polling timeout when stopped', async () => {
      sinon.spy(clock, 'clearTimeout');

      consumer.start();
      await clock.tickAsync(0);
      consumer.stop();

      await clock.runAllAsync();

      sinon.assert.calledTwice(clock.clearTimeout);
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

    it('aborts requests when the abort param is true', async () => {
      const handleStop = sandbox.stub().returns(null);
      const handleAbort = sandbox.stub().returns(null);

      consumer.on('stopped', handleStop);
      consumer.on('aborted', handleAbort);

      consumer.start();
      consumer.stop({ abort: true });

      await clock.runAllAsync();

      assert.isTrue(consumer.abortController.signal.aborted);
      sandbox.assert.calledOnce(handleMessage);
      sandbox.assert.calledOnce(handleAbort);
      sandbox.assert.calledOnce(handleStop);
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

  describe('updateOption', async () => {
    it('updates the visibilityTimeout option and emits an event', () => {
      const optionUpdatedListener = sandbox.stub();
      consumer.on('option_updated', optionUpdatedListener);

      consumer.updateOption('visibilityTimeout', 45);

      assert.equal(consumer.visibilityTimeout, 45);

      sandbox.assert.calledWithMatch(
        optionUpdatedListener,
        'visibilityTimeout',
        45
      );
    });

    it('does not update the visibilityTimeout if the value is less than the heartbeatInterval', () => {
      consumer = new Consumer({
        region: REGION,
        queueUrl: QUEUE_URL,
        handleMessage,
        heartbeatInterval: 30,
        visibilityTimeout: 60
      });

      const optionUpdatedListener = sandbox.stub();
      consumer.on('option_updated', optionUpdatedListener);

      assert.throws(() => {
        consumer.updateOption('visibilityTimeout', 30);
      }, 'heartbeatInterval must be less than visibilityTimeout.');

      assert.equal(consumer.visibilityTimeout, 60);

      sandbox.assert.notCalled(optionUpdatedListener);
    });

    it('updates the batchSize option and emits an event', () => {
      const optionUpdatedListener = sandbox.stub();
      consumer.on('option_updated', optionUpdatedListener);

      consumer.updateOption('batchSize', 4);

      assert.equal(consumer.batchSize, 4);

      sandbox.assert.calledWithMatch(optionUpdatedListener, 'batchSize', 4);
    });

    it('does not update the batchSize if the value is more than 10', () => {
      const optionUpdatedListener = sandbox.stub();
      consumer.on('option_updated', optionUpdatedListener);

      assert.throws(() => {
        consumer.updateOption('batchSize', 13);
      }, 'batchSize must be between 1 and 10.');

      assert.equal(consumer.batchSize, 1);

      sandbox.assert.notCalled(optionUpdatedListener);
    });

    it('does not update the batchSize if the value is less than 1', () => {
      const optionUpdatedListener = sandbox.stub();
      consumer.on('option_updated', optionUpdatedListener);

      assert.throws(() => {
        consumer.updateOption('batchSize', 0);
      }, 'batchSize must be between 1 and 10.');

      assert.equal(consumer.batchSize, 1);

      sandbox.assert.notCalled(optionUpdatedListener);
    });

    it('updates the waitTimeSeconds option and emits an event', () => {
      const optionUpdatedListener = sandbox.stub();
      consumer.on('option_updated', optionUpdatedListener);

      consumer.updateOption('waitTimeSeconds', 18);

      assert.equal(consumer.waitTimeSeconds, 18);

      sandbox.assert.calledWithMatch(
        optionUpdatedListener,
        'waitTimeSeconds',
        18
      );
    });

    it('does not update the batchSize if the value is less than 0', () => {
      const optionUpdatedListener = sandbox.stub();
      consumer.on('option_updated', optionUpdatedListener);

      assert.throws(() => {
        consumer.updateOption('waitTimeSeconds', -1);
      }, 'waitTimeSeconds must be between 0 and 20.');

      assert.equal(consumer.waitTimeSeconds, 20);

      sandbox.assert.notCalled(optionUpdatedListener);
    });

    it('does not update the batchSize if the value is more than 20', () => {
      const optionUpdatedListener = sandbox.stub();
      consumer.on('option_updated', optionUpdatedListener);

      assert.throws(() => {
        consumer.updateOption('waitTimeSeconds', 27);
      }, 'waitTimeSeconds must be between 0 and 20.');

      assert.equal(consumer.waitTimeSeconds, 20);

      sandbox.assert.notCalled(optionUpdatedListener);
    });

    it('throws an error for an unknown option', () => {
      consumer = new Consumer({
        region: REGION,
        queueUrl: QUEUE_URL,
        handleMessage,
        visibilityTimeout: 60
      });

      assert.throws(() => {
        consumer.updateOption('unknown', 'value');
      }, `The update unknown cannot be updated`);
    });
  });

  describe('logger', () => {
    it('logs a debug event when an event is emitted', async () => {
      const loggerDebug = sandbox.stub(logger, 'debug');

      consumer.start();
      consumer.stop();

      sandbox.assert.callCount(loggerDebug, 5);
      sandbox.assert.calledWithMatch(loggerDebug, 'starting');
      sandbox.assert.calledWithMatch(loggerDebug, 'started');
      sandbox.assert.calledWithMatch(loggerDebug, 'polling');
      sandbox.assert.calledWithMatch(loggerDebug, 'stopping');
      sandbox.assert.calledWithMatch(loggerDebug, 'stopped');
    });

    it('logs a debug event while the handler is processing, for every second', async () => {
      const loggerDebug = sandbox.stub(logger, 'debug');
      const clearIntervalSpy = sinon.spy(global, 'clearInterval');

      sqs.send.withArgs(mockReceiveMessage).resolves({
        Messages: [
          { MessageId: '1', ReceiptHandle: 'receipt-handle-1', Body: 'body-1' }
        ]
      });
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: () =>
          new Promise((resolve) => setTimeout(resolve, 4000)),
        sqs
      });

      consumer.start();
      await Promise.all([clock.tickAsync(5000)]);
      sandbox.assert.calledOnce(clearIntervalSpy);
      consumer.stop();

      sandbox.assert.callCount(loggerDebug, 15);
      sandbox.assert.calledWith(loggerDebug, 'handler_processing', {
        detail: 'The handler is still processing the message(s)...'
      });
    });
  });
});
