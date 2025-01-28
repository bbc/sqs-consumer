import {
  ChangeMessageVisibilityBatchCommand,
  ChangeMessageVisibilityCommand,
  DeleteMessageBatchCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SQSClient,
  QueueAttributeName,
  Message,
} from "@aws-sdk/client-sqs";
import { assert } from "chai";
import * as sinon from "sinon";
import { pEvent } from "p-event";

import { AWSError } from "../../src/types.js";
import { Consumer } from "../../src/consumer.js";
import { logger } from "../../src/logger.js";

const sandbox = sinon.createSandbox();

const AUTHENTICATION_ERROR_TIMEOUT = 20;
const POLLING_TIMEOUT = 100;
const QUEUE_URL = "https://sqs.some-region.amazonaws.com/123456789012/queue-name";
const REGION = "some-region";

const mockReceiveMessage = sinon.match.instanceOf(ReceiveMessageCommand);
const mockDeleteMessage = sinon.match.instanceOf(DeleteMessageCommand);
const mockDeleteMessageBatch = sinon.match.instanceOf(
  DeleteMessageBatchCommand,
);
const mockChangeMessageVisibility = sinon.match.instanceOf(
  ChangeMessageVisibilityCommand,
);
const mockChangeMessageVisibilityBatch = sinon.match.instanceOf(
  ChangeMessageVisibilityBatchCommand,
);

class MockSQSError extends Error implements AWSError {
  name: string;
  $metadata: AWSError["$metadata"];
  $service: string;
  $retryable: AWSError["$retryable"];
  $fault: AWSError["$fault"];
  $response?:
    | {
        statusCode?: number | undefined;
        headers: Record<string, string>;
        body?: any;
      }
    | undefined;
  time: Date;

  constructor(message: string) {
    super(message);
    this.message = message;
  }
}

describe("Consumer", () => {
  let consumer;
  let clock;
  let handleMessage;
  let handleMessageBatch;
  let sqs;
  const response = {
    Messages: [
      {
        ReceiptHandle: "receipt-handle",
        MessageId: "123",
        Body: "body",
      },
    ],
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
      authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
    });
  });

  afterEach(() => {
    clock.restore();
    sandbox.restore();
  });

  describe("options validation", () => {
    it("requires a handleMessage or handleMessagesBatch function to be set", () => {
      assert.throws(() => {
        new Consumer({
          handleMessage: undefined,
          region: REGION,
          queueUrl: QUEUE_URL,
        });
      }, `Missing SQS consumer option [ handleMessage or handleMessageBatch ].`);
    });

    it("requires the batchSize option to be no greater than 10", () => {
      assert.throws(() => {
        new Consumer({
          region: REGION,
          queueUrl: QUEUE_URL,
          handleMessage,
          batchSize: 11,
        });
      }, "batchSize must be between 1 and 10.");
    });

    it("requires the batchSize option to be greater than 0", () => {
      assert.throws(() => {
        new Consumer({
          region: REGION,
          queueUrl: QUEUE_URL,
          handleMessage,
          batchSize: -1,
        });
      }, "batchSize must be between 1 and 10.");
    });

    it("requires visibilityTimeout to be set with heartbeatInterval", () => {
      assert.throws(() => {
        new Consumer({
          region: REGION,
          queueUrl: QUEUE_URL,
          handleMessage,
          heartbeatInterval: 30,
        });
      }, "heartbeatInterval must be less than visibilityTimeout.");
    });

    it("requires heartbeatInterval to be less than visibilityTimeout", () => {
      assert.throws(() => {
        new Consumer({
          region: REGION,
          queueUrl: QUEUE_URL,
          handleMessage,
          heartbeatInterval: 30,
          visibilityTimeout: 30,
        });
      }, "heartbeatInterval must be less than visibilityTimeout.");
    });

    it("requires concurrency to be a positive integer", () => {
      assert.throws(() => {
        new Consumer({
          region: REGION,
          queueUrl: QUEUE_URL,
          handleMessage,
          concurrency: 0,
        });
      }, "concurrency must be a positive integer.");

      assert.throws(() => {
        new Consumer({
          region: REGION,
          queueUrl: QUEUE_URL,
          handleMessage,
          concurrency: -1,
        });
      }, "concurrency must be a positive integer.");

      assert.throws(() => {
        new Consumer({
          region: REGION,
          queueUrl: QUEUE_URL,
          handleMessage,
          concurrency: 1.5,
        });
      }, "concurrency must be a positive integer.");
    });

    it("requires concurrency to be greater than or equal to batchSize", () => {
      assert.throws(() => {
        new Consumer({
          region: REGION,
          queueUrl: QUEUE_URL,
          handleMessage,
          batchSize: 5,
          concurrency: 3,
        });
      }, "concurrency must be greater than or equal to batchSize.");
    });

    it("allows concurrency to be equal to batchSize", () => {
      assert.doesNotThrow(() => {
        new Consumer({
          region: REGION,
          queueUrl: QUEUE_URL,
          handleMessage,
          batchSize: 5,
          concurrency: 5,
        });
      });
    });

    it("allows concurrency to be greater than batchSize", () => {
      assert.doesNotThrow(() => {
        new Consumer({
          region: REGION,
          queueUrl: QUEUE_URL,
          handleMessage,
          batchSize: 5,
          concurrency: 10,
        });
      });
    });
  });

  describe(".create", () => {
    it("creates a new instance of a Consumer object", () => {
      const instance = Consumer.create({
        region: REGION,
        queueUrl: QUEUE_URL,
        batchSize: 1,
        visibilityTimeout: 10,
        waitTimeSeconds: 10,
        handleMessage,
      });

      assert.instanceOf(instance, Consumer);
    });
  });

  describe(".start", () => {
    it("uses the correct abort signal", async () => {
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

    it("fires an event when the consumer is started", async () => {
      const handleStart = sandbox.stub().returns(null);

      consumer.on("started", handleStart);

      consumer.start();
      consumer.stop();

      sandbox.assert.calledOnce(handleStart);
    });

    it("fires an error event when an error occurs receiving a message", async () => {
      const receiveErr = new Error("Receive error");

      sqs.send.withArgs(mockReceiveMessage).rejects(receiveErr);

      consumer.start();

      const err: any = await pEvent(consumer, "error");

      consumer.stop();
      assert.ok(err);
      assert.equal(err.message, "SQS receive message failed: Receive error");
    });

    it("retains sqs error information", async () => {
      const receiveErr = new MockSQSError("Receive error");
      receiveErr.name = "short code";
      receiveErr.$retryable = {
        throttling: false,
      };
      receiveErr.$metadata = {
        httpStatusCode: 403,
      };
      receiveErr.time = new Date();
      receiveErr.$service = "service";

      sqs.send.withArgs(mockReceiveMessage).rejects(receiveErr);

      consumer.start();
      const err: any = await pEvent(consumer, "error");
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, "SQS receive message failed: Receive error");
      assert.equal(err.code, receiveErr.name);
      assert.equal(err.retryable, receiveErr.$retryable.throttling);
      assert.equal(err.statusCode, receiveErr.$metadata.httpStatusCode);
      assert.equal(err.time.toString(), receiveErr.time.toString());
      assert.equal(err.service, receiveErr.$service);
      assert.equal(err.fault, receiveErr.$fault);
      assert.isUndefined(err.response);
      assert.isUndefined(err.metadata);
    });

    it('includes the response and metadata in the error when "extendedAWSErrors" is true', async () => {
      const receiveErr = new MockSQSError("Receive error");
      receiveErr.name = "short code";
      receiveErr.$retryable = {
        throttling: false,
      };
      receiveErr.$metadata = {
        httpStatusCode: 403,
      };
      receiveErr.time = new Date();
      receiveErr.$service = "service";
      receiveErr.$response = {
        statusCode: 200,
        headers: {},
        body: "body",
      };

      sqs.send.withArgs(mockReceiveMessage).rejects(receiveErr);

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage,
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
        extendedAWSErrors: true,
      });

      consumer.start();
      const err: any = await pEvent(consumer, "error");
      consumer.stop();

      assert.ok(err);
      assert.equal(err.response, receiveErr.$response);
      assert.equal(err.metadata, receiveErr.$metadata);
    });

    it("does not include the response and metadata in the error when extendedAWSErrors is false", async () => {
      const receiveErr = new MockSQSError("Receive error");
      receiveErr.name = "short code";
      receiveErr.$retryable = {
        throttling: false,
      };
      receiveErr.$metadata = {
        httpStatusCode: 403,
      };
      receiveErr.time = new Date();
      receiveErr.$service = "service";
      receiveErr.$response = {
        statusCode: 200,
        headers: {},
        body: "body",
      };

      sqs.send.withArgs(mockReceiveMessage).rejects(receiveErr);

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage,
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
        extendedAWSErrors: false,
      });

      consumer.start();
      const err: any = await pEvent(consumer, "error");
      consumer.stop();

      assert.ok(err);
      assert.isUndefined(err.response);
      assert.isUndefined(err.metadata);
    });

    it("fires a timeout event if handler function takes too long", async () => {
      const handleMessageTimeout = 500;
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: () =>
          new Promise((resolve) => setTimeout(resolve, 1000)),
        handleMessageTimeout,
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
      });

      consumer.start();
      const [err]: any = await Promise.all([
        pEvent(consumer, "timeout_error"),
        clock.tickAsync(handleMessageTimeout),
      ]);
      consumer.stop();

      assert.ok(err);
      assert.equal(
        err.message,
        `Message handler timed out after ${handleMessageTimeout}ms: Operation timed out.`,
      );
    });

    it("handles unexpected exceptions thrown by the handler function", async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: () => {
          throw new Error("unexpected parsing error");
        },
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
      });

      consumer.start();
      const err: any = await pEvent(consumer, "processing_error");
      consumer.stop();

      assert.ok(err);
      assert.equal(
        err.message,
        "Unexpected message handler failure: unexpected parsing error",
      );
    });

    it("handles non-standard objects thrown by the handler function", async () => {
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
          throw new CustomError("unexpected parsing error");
        },
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
      });

      consumer.start();
      const err: any = await pEvent(consumer, "processing_error");
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, "unexpected parsing error");
    });

    it("handles non-standard exceptions thrown by the handler function", async () => {
      const customError = new Error();
      Object.defineProperty(customError, "message", {
        get: () => "unexpected parsing error",
      });

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: () => {
          throw customError;
        },
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
      });

      consumer.start();
      const err: any = await pEvent(consumer, "processing_error");
      consumer.stop();

      assert.ok(err);
      assert.equal(
        err.message,
        "Unexpected message handler failure: unexpected parsing error",
      );
    });

    it("fires an error event when an error occurs deleting a message", async () => {
      const deleteErr = new Error("Delete error");

      handleMessage.resolves(null);
      sqs.send.withArgs(mockDeleteMessage).rejects(deleteErr);

      consumer.start();
      const err: any = await pEvent(consumer, "error");
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, "SQS delete message failed: Delete error");
    });

    it("fires a `processing_error` event when a non-`SQSError` error occurs processing a message", async () => {
      const processingErr = new Error("Processing error");

      handleMessage.rejects(processingErr);

      consumer.start();
      const [err, message] = await pEvent<
        string | symbol,
        { [key: string]: string }[]
      >(consumer, "processing_error", {
        multiArgs: true,
      });
      consumer.stop();

      assert.equal(
        err instanceof Error ? err.message : "",
        "Unexpected message handler failure: Processing error",
      );
      assert.equal(message.MessageId, "123");
      assert.deepEqual((err as any).messageIds, ["123"]);
    });

    it("fires an `error` event when an `SQSError` occurs processing a message", async () => {
      const sqsError = new Error("Processing error");
      sqsError.name = "SQSError";

      handleMessage.resolves();
      sqs.send.withArgs(mockDeleteMessage).rejects(sqsError);

      consumer.start();
      const [err, message] = await pEvent<
        string | symbol,
        { [key: string]: string }[]
      >(consumer, "error", {
        multiArgs: true,
      });
      consumer.stop();

      assert.equal(err.message, "SQS delete message failed: Processing error");
      assert.equal(message.MessageId, "123");
    });

    it("waits before repolling when a credentials error occurs", async () => {
      const loggerDebug = sandbox.stub(logger, "debug");

      const credentialsErr = {
        name: "CredentialsError",
        message: "Missing credentials in config",
      };
      sqs.send.withArgs(mockReceiveMessage).rejects(credentialsErr);
      const errorListener = sandbox.stub();
      consumer.on("error", errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockReceiveMessage);

      sandbox.assert.calledWith(loggerDebug, "authentication_error", {
        code: "CredentialsError",
        detail: "There was an authentication error. Pausing before retrying.",
      });
    });

    it("waits before repolling when a 403 error occurs", async () => {
      const loggerDebug = sandbox.stub(logger, "debug");

      const invalidSignatureErr = {
        $metadata: {
          httpStatusCode: 403,
        },
        message: "The security token included in the request is invalid",
      };
      sqs.send.withArgs(mockReceiveMessage).rejects(invalidSignatureErr);
      const errorListener = sandbox.stub();
      consumer.on("error", errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockReceiveMessage);

      sandbox.assert.calledWith(loggerDebug, "authentication_error", {
        code: "Unknown",
        detail: "There was an authentication error. Pausing before retrying.",
      });
    });

    it("waits before repolling when a UnknownEndpoint error occurs", async () => {
      const loggerDebug = sandbox.stub(logger, "debug");

      const unknownEndpointErr = {
        name: "UnknownEndpoint",
        message:
          "Inaccessible host: `sqs.eu-west-1.amazonaws.com`. This service may not be available in the `eu-west-1` region.",
      };
      sqs.send.withArgs(mockReceiveMessage).rejects(unknownEndpointErr);
      const errorListener = sandbox.stub();
      consumer.on("error", errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledTwice(sqs.send);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockReceiveMessage);

      sandbox.assert.calledWith(loggerDebug, "authentication_error", {
        code: "UnknownEndpoint",
        detail: "There was an authentication error. Pausing before retrying.",
      });
    });

    it("waits before repolling when a NonExistentQueue error occurs", async () => {
      const loggerDebug = sandbox.stub(logger, "debug");

      const nonExistentQueueErr = {
        name: "AWS.SimpleQueueService.NonExistentQueue",
        message: "The specified queue does not exist for this wsdl version.",
      };
      sqs.send.withArgs(mockReceiveMessage).rejects(nonExistentQueueErr);
      const errorListener = sandbox.stub();
      consumer.on("error", errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledTwice(sqs.send);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockReceiveMessage);

      sandbox.assert.calledWith(loggerDebug, "authentication_error", {
        code: "AWS.SimpleQueueService.NonExistentQueue",
        detail: "There was an authentication error. Pausing before retrying.",
      });
    });

    it("waits before repolling when a CredentialsProviderError error occurs", async () => {
      const loggerDebug = sandbox.stub(logger, "debug");

      const credentialsProviderErr = {
        name: "CredentialsProviderError",
        message: "Could not load credentials from any providers.",
      };
      sqs.send.withArgs(mockReceiveMessage).rejects(credentialsProviderErr);
      const errorListener = sandbox.stub();
      consumer.on("error", errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledTwice(sqs.send);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockReceiveMessage);

      sandbox.assert.calledWith(loggerDebug, "authentication_error", {
        code: "CredentialsProviderError",
        detail: "There was an authentication error. Pausing before retrying.",
      });
    });

    it("waits before repolling when a InvalidAddress error occurs", async () => {
      const loggerDebug = sandbox.stub(logger, "debug");

      const credentialsProviderErr = {
        name: "InvalidAddress",
        message: "The address some-queue-url is not valid for this endpoint.",
      };
      sqs.send.withArgs(mockReceiveMessage).rejects(credentialsProviderErr);
      const errorListener = sandbox.stub();
      consumer.on("error", errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledTwice(sqs.send);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockReceiveMessage);

      sandbox.assert.calledWith(loggerDebug, "authentication_error", {
        code: "InvalidAddress",
        detail: "There was an authentication error. Pausing before retrying.",
      });
    });

    it("waits before repolling when a InvalidSecurity error occurs", async () => {
      const loggerDebug = sandbox.stub(logger, "debug");

      const credentialsProviderErr = {
        name: "InvalidSecurity",
        message: "The queue is not is not HTTPS and SigV4.",
      };
      sqs.send.withArgs(mockReceiveMessage).rejects(credentialsProviderErr);
      const errorListener = sandbox.stub();
      consumer.on("error", errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledTwice(sqs.send);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockReceiveMessage);

      sandbox.assert.calledWith(loggerDebug, "authentication_error", {
        code: "InvalidSecurity",
        detail: "There was an authentication error. Pausing before retrying.",
      });
    });

    it("waits before repolling when a QueueDoesNotExist error occurs", async () => {
      const loggerDebug = sandbox.stub(logger, "debug");

      const credentialsProviderErr = {
        name: "QueueDoesNotExist",
        message: "The queue does not exist.",
      };
      sqs.send.withArgs(mockReceiveMessage).rejects(credentialsProviderErr);
      const errorListener = sandbox.stub();
      consumer.on("error", errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledTwice(sqs.send);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockReceiveMessage);

      sandbox.assert.calledWith(loggerDebug, "authentication_error", {
        code: "QueueDoesNotExist",
        detail: "There was an authentication error. Pausing before retrying.",
      });
    });

    it("waits before repolling when a RequestThrottled error occurs", async () => {
      const loggerDebug = sandbox.stub(logger, "debug");

      const credentialsProviderErr = {
        name: "RequestThrottled",
        message: "Requests have been throttled.",
      };
      sqs.send.withArgs(mockReceiveMessage).rejects(credentialsProviderErr);
      const errorListener = sandbox.stub();
      consumer.on("error", errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledTwice(sqs.send);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockReceiveMessage);

      sandbox.assert.calledWith(loggerDebug, "authentication_error", {
        code: "RequestThrottled",
        detail: "There was an authentication error. Pausing before retrying.",
      });
    });

    it("waits before repolling when a RequestThrottled error occurs", async () => {
      const loggerDebug = sandbox.stub(logger, "debug");

      const credentialsProviderErr = {
        name: "OverLimit",
        message: "An over limit error.",
      };
      sqs.send.withArgs(mockReceiveMessage).rejects(credentialsProviderErr);
      const errorListener = sandbox.stub();
      consumer.on("error", errorListener);

      consumer.start();
      await clock.tickAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      sandbox.assert.calledTwice(errorListener);
      sandbox.assert.calledTwice(sqs.send);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockReceiveMessage);

      sandbox.assert.calledWith(loggerDebug, "authentication_error", {
        code: "OverLimit",
        detail: "There was an authentication error. Pausing before retrying.",
      });
    });

    it("waits before repolling when a polling timeout is set", async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage,
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
        pollingWaitTimeMs: POLLING_TIMEOUT,
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

    it("fires a message_received event when a message is received", async () => {
      consumer.start();
      const message = await pEvent(consumer, "message_received");
      consumer.stop();

      assert.equal(message, response.Messages[0]);
    });

    it("fires a message_processed event when a message is successfully deleted", async () => {
      handleMessage.resolves();

      consumer.start();
      const message = await pEvent(consumer, "message_received");
      consumer.stop();

      assert.equal(message, response.Messages[0]);
    });

    it("calls the handleMessage function when a message is received", async () => {
      consumer.start();
      await pEvent(consumer, "message_processed");
      consumer.stop();

      sandbox.assert.calledWith(handleMessage, response.Messages[0]);
    });

    it("calls the preReceiveMessageCallback and postReceiveMessageCallback function before receiving a message", async () => {
      const preReceiveMessageCallbackStub = sandbox.stub().resolves(null);
      const postReceiveMessageCallbackStub = sandbox.stub().resolves(null);

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage,
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
        preReceiveMessageCallback: preReceiveMessageCallbackStub,
        postReceiveMessageCallback: postReceiveMessageCallbackStub,
      });

      consumer.start();
      await pEvent(consumer, "message_processed");
      consumer.stop();

      sandbox.assert.calledOnce(preReceiveMessageCallbackStub);
      sandbox.assert.calledOnce(postReceiveMessageCallbackStub);
    });

    it("deletes the message when the handleMessage function is called", async () => {
      handleMessage.resolves();

      consumer.start();
      await pEvent(consumer, "message_processed");
      consumer.stop();

      sandbox.assert.calledWith(sqs.send.secondCall, mockDeleteMessage);
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          ReceiptHandle: "receipt-handle",
        }),
      );
    });

    it("does not delete the message if shouldDeleteMessages is false", async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage,
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
        shouldDeleteMessages: false,
      });

      handleMessage.resolves();

      consumer.start();
      await pEvent(consumer, "message_processed");
      consumer.stop();

      sandbox.assert.neverCalledWithMatch(sqs.send, mockDeleteMessage);
    });

    it("doesn't delete the message when a processing error is reported", async () => {
      handleMessage.rejects(new Error("Processing error"));

      consumer.start();
      await pEvent(consumer, "processing_error");
      consumer.stop();

      sandbox.assert.neverCalledWithMatch(sqs.send, mockDeleteMessage);
    });

    it("consumes another message once one is processed", async () => {
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

    it("consumes multiple messages when the batchSize is greater than 1", async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({
        Messages: [
          {
            ReceiptHandle: "receipt-handle-1",
            MessageId: "1",
            Body: "body-1",
          },
          {
            ReceiptHandle: "receipt-handle-2",
            MessageId: "2",
            Body: "body-2",
          },
          {
            ReceiptHandle: "receipt-handle-3",
            MessageId: "3",
            Body: "body-3",
          },
        ],
      });

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        messageAttributeNames: ["attribute-1", "attribute-2"],
        messageSystemAttributeNames: ["All"],
        region: REGION,
        handleMessage,
        batchSize: 3,
        sqs,
      });

      consumer.start();
      await pEvent(consumer, "message_received");
      consumer.stop();

      sandbox.assert.callCount(handleMessage, 3);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.match(
        sqs.send.firstCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          AttributeNames: [],
          MessageAttributeNames: ["attribute-1", "attribute-2"],
          MessageSystemAttributeNames: ["All"],
          MaxNumberOfMessages: 3,
          WaitTimeSeconds: AUTHENTICATION_ERROR_TIMEOUT,
          VisibilityTimeout: undefined,
        }),
      );
    });

    it("consumes messages with message attribute 'ApproximateReceiveCount'", async () => {
      const messageWithAttr = {
        ReceiptHandle: "receipt-handle-1",
        MessageId: "1",
        Body: "body-1",
        Attributes: {
          ApproximateReceiveCount: 1,
        },
      };

      sqs.send.withArgs(mockReceiveMessage).resolves({
        Messages: [messageWithAttr],
      });

      const attributeNames: QueueAttributeName[] = [
        "ApproximateReceiveCount" as QueueAttributeName,
      ];

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        attributeNames,
        region: REGION,
        handleMessage,
        sqs,
      });

      consumer.start();
      const message = await pEvent(consumer, "message_received");
      consumer.stop();

      sandbox.assert.calledWith(sqs.send, mockReceiveMessage);
      sandbox.assert.match(
        sqs.send.firstCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          AttributeNames: ["ApproximateReceiveCount"],
          MessageAttributeNames: [],
          MessageSystemAttributeNames: [],
          MaxNumberOfMessages: 1,
          WaitTimeSeconds: AUTHENTICATION_ERROR_TIMEOUT,
          VisibilityTimeout: undefined,
        }),
      );

      assert.equal(message, messageWithAttr);
    });

    it("fires an emptyQueue event when all messages have been consumed", async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({});

      consumer.start();
      await pEvent(consumer, "empty");
      consumer.stop();
    });

    it("terminates message visibility timeout on processing error", async () => {
      handleMessage.rejects(new Error("Processing error"));

      consumer.terminateVisibilityTimeout = true;

      consumer.start();
      await pEvent(consumer, "processing_error");
      consumer.stop();

      sandbox.assert.calledWith(
        sqs.send.secondCall,
        mockChangeMessageVisibility,
      );
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          ReceiptHandle: "receipt-handle",
          VisibilityTimeout: 0,
        }),
      );
    });

    it("terminates message visibility timeout with a function to calculate timeout on processing error", async () => {
      const messageWithAttr = {
        ReceiptHandle: "receipt-handle",
        MessageId: "1",
        Body: "body-2",
        Attributes: {
          ApproximateReceiveCount: 2,
        },
      };
      sqs.send.withArgs(mockReceiveMessage).resolves({
        Messages: [messageWithAttr],
      });

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        messageSystemAttributeNames: ["ApproximateReceiveCount"],
        region: REGION,
        handleMessage,
        sqs,
        terminateVisibilityTimeout: (messages: Message[]) => {
          const receiveCount =
            Number.parseInt(
              messages[0].Attributes?.ApproximateReceiveCount || "1",
            ) || 1;
          return receiveCount * 10;
        },
      });

      handleMessage.rejects(new Error("Processing error"));

      consumer.start();
      await pEvent(consumer, "processing_error");
      consumer.stop();

      sandbox.assert.calledWith(
        sqs.send.secondCall,
        mockChangeMessageVisibility,
      );
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          ReceiptHandle: "receipt-handle",
          VisibilityTimeout: 20,
        }),
      );
    });

    it("changes message visibility timeout on processing error", async () => {
      handleMessage.rejects(new Error("Processing error"));

      consumer.terminateVisibilityTimeout = 10;

      consumer.start();
      await pEvent(consumer, "processing_error");
      consumer.stop();

      sandbox.assert.calledWith(
        sqs.send.secondCall,
        mockChangeMessageVisibility,
      );
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          ReceiptHandle: "receipt-handle",
          VisibilityTimeout: 10,
        }),
      );
    });

    it("does not terminate visibility timeout when `terminateVisibilityTimeout` option is false", async () => {
      handleMessage.rejects(new Error("Processing error"));
      consumer.terminateVisibilityTimeout = false;

      consumer.start();
      await pEvent(consumer, "processing_error");
      consumer.stop();

      sqs.send.neverCalledWith(mockChangeMessageVisibility);
    });

    it("fires error event when failed to terminate visibility timeout on processing error", async () => {
      handleMessage.rejects(new Error("Processing error"));

      const sqsError = new Error("Processing error");
      sqsError.name = "SQSError";
      sqs.send.withArgs(mockChangeMessageVisibility).rejects(sqsError);
      consumer.terminateVisibilityTimeout = true;

      consumer.start();
      await pEvent(consumer, "error");
      consumer.stop();

      sandbox.assert.calledWith(
        sqs.send.secondCall,
        mockChangeMessageVisibility,
      );
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          ReceiptHandle: "receipt-handle",
          VisibilityTimeout: 0,
        }),
      );
    });

    it("fires response_processed event for each batch", async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({
        Messages: [
          {
            ReceiptHandle: "receipt-handle-1",
            MessageId: "1",
            Body: "body-1",
          },
          {
            ReceiptHandle: "receipt-handle-2",
            MessageId: "2",
            Body: "body-2",
          },
        ],
      });
      handleMessage.resolves(null);

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        messageAttributeNames: ["attribute-1", "attribute-2"],
        region: REGION,
        handleMessage,
        batchSize: 2,
        sqs,
      });

      consumer.start();
      await pEvent(consumer, "response_processed");
      consumer.stop();

      sandbox.assert.callCount(handleMessage, 2);
    });

    it("calls the handleMessagesBatch function when a batch of messages is received", async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        messageAttributeNames: ["attribute-1", "attribute-2"],
        region: REGION,
        handleMessageBatch,
        batchSize: 2,
        sqs,
      });

      consumer.start();
      await pEvent(consumer, "response_processed");
      consumer.stop();

      sandbox.assert.callCount(handleMessageBatch, 1);
    });

    it("handles unexpected exceptions thrown by the handler batch function", async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        messageAttributeNames: ["attribute-1", "attribute-2"],
        region: REGION,
        handleMessageBatch: () => {
          throw new Error("unexpected parsing error");
        },
        batchSize: 2,
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
      });

      consumer.start();
      const err: any = await pEvent(consumer, "error");
      consumer.stop();

      assert.ok(err);
      assert.equal(
        err.message,
        "Unexpected message handler failure: unexpected parsing error",
      );
    });

    it("handles non-standard objects thrown by the handler batch function", async () => {
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
        messageAttributeNames: ["attribute-1", "attribute-2"],
        region: REGION,
        handleMessageBatch: () => {
          throw new CustomError("unexpected parsing error");
        },
        batchSize: 2,
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
      });

      consumer.start();
      const err: any = await pEvent(consumer, "error");
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, "unexpected parsing error");
    });

    it("handles non-standard exceptions thrown by the handler batch function", async () => {
      const customError = new Error();
      Object.defineProperty(customError, "message", {
        get: () => "unexpected parsing error",
      });

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        messageAttributeNames: ["attribute-1", "attribute-2"],
        region: REGION,
        handleMessageBatch: () => {
          throw customError;
        },
        batchSize: 2,
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
      });

      consumer.start();
      const err: any = await pEvent(consumer, "error");
      consumer.stop();

      assert.ok(err);
      assert.equal(
        err.message,
        "Unexpected message handler failure: unexpected parsing error",
      );
    });

    it("prefers handleMessagesBatch over handleMessage when both are set", async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        messageAttributeNames: ["attribute-1", "attribute-2"],
        region: REGION,
        handleMessageBatch,
        handleMessage,
        batchSize: 2,
        sqs,
      });

      consumer.start();
      await pEvent(consumer, "response_processed");
      consumer.stop();

      sandbox.assert.callCount(handleMessageBatch, 1);
      sandbox.assert.callCount(handleMessage, 0);
    });

    it("ack the message if handleMessage returns void", async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: async () => {},
        sqs,
      });

      consumer.start();
      await pEvent(consumer, "message_processed");
      consumer.stop();

      sandbox.assert.callCount(sqs.send, 2);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockDeleteMessage);
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          ReceiptHandle: "receipt-handle",
        }),
      );
    });

    it("ack the message if handleMessage returns a message with the same ID", async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: async () => {
          return {
            MessageId: "123",
          };
        },
        sqs,
      });

      consumer.start();
      await pEvent(consumer, "message_processed");
      consumer.stop();

      sandbox.assert.callCount(sqs.send, 2);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockDeleteMessage);
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          ReceiptHandle: "receipt-handle",
        }),
      );
    });

    it("does not ack the message if handleMessage returns an empty object", async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: async () => {
          return {};
        },
        sqs,
      });

      consumer.start();
      await pEvent(consumer, "response_processed");
      consumer.stop();

      sandbox.assert.callCount(sqs.send, 1);
      sandbox.assert.neverCalledWithMatch(sqs.send, mockDeleteMessage);
    });

    it("does not ack the message if handleMessage returns a different ID", async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: async () => {
          return {
            MessageId: "143",
          };
        },
        sqs,
      });

      consumer.start();
      await pEvent(consumer, "response_processed");
      consumer.stop();

      sandbox.assert.callCount(sqs.send, 1);
      sandbox.assert.neverCalledWithMatch(sqs.send, mockDeleteMessage);
    });

    it("deletes the message if alwaysAcknowledge is `true` and handleMessage returns an empty object", async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: async () => {
          return {};
        },
        sqs,
        alwaysAcknowledge: true,
      });

      consumer.start();
      await pEvent(consumer, "response_processed");
      consumer.stop();

      sandbox.assert.callCount(sqs.send, 2);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(sqs.send.secondCall, mockDeleteMessage);
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          ReceiptHandle: "receipt-handle",
        }),
      );
    });

    it("does not call deleteMessageBatch if handleMessagesBatch returns an empty array", async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: async () => [],
        batchSize: 2,
        sqs,
      });

      consumer.start();
      await pEvent(consumer, "response_processed");
      consumer.stop();

      sandbox.assert.callCount(sqs.send, 1);
      sandbox.assert.neverCalledWithMatch(sqs.send, mockDeleteMessageBatch);
    });

    it("calls deleteMessageBatch if alwaysAcknowledge is `true` and handleMessagesBatch returns an empty array", async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: async () => [],
        batchSize: 2,
        sqs,
        alwaysAcknowledge: true,
      });

      consumer.start();
      await pEvent(consumer, "response_processed");
      consumer.stop();

      sandbox.assert.callCount(sqs.send, 2);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(
        sqs.send.secondCall,
        mockDeleteMessageBatch,
      );
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          Entries: [{ Id: "123", ReceiptHandle: "receipt-handle" }],
        }),
      );
    });

    it("ack all messages if handleMessageBatch returns void", async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: async () => {},
        batchSize: 2,
        sqs,
      });

      consumer.start();
      await pEvent(consumer, "response_processed");
      consumer.stop();

      sandbox.assert.callCount(sqs.send, 2);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(
        sqs.send.secondCall,
        mockDeleteMessageBatch,
      );
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          Entries: [{ Id: "123", ReceiptHandle: "receipt-handle" }],
        }),
      );
    });

    it("ack only returned messages if handleMessagesBatch returns an array", async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: async () => [
          { MessageId: "123", ReceiptHandle: "receipt-handle" },
        ],
        batchSize: 2,
        sqs,
      });

      consumer.start();
      await pEvent(consumer, "response_processed");
      consumer.stop();

      sandbox.assert.callCount(sqs.send, 2);
      sandbox.assert.calledWithMatch(sqs.send.firstCall, mockReceiveMessage);
      sandbox.assert.calledWithMatch(
        sqs.send.secondCall,
        mockDeleteMessageBatch,
      );
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          Entries: [{ Id: "123", ReceiptHandle: "receipt-handle" }],
        }),
      );
    });

    it("uses the correct visibility timeout for long running handler functions", async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: () =>
          new Promise((resolve) => setTimeout(resolve, 75000)),
        sqs,
        visibilityTimeout: 40,
        heartbeatInterval: 30,
      });
      const clearIntervalSpy = sinon.spy(global, "clearInterval");

      consumer.start();
      await Promise.all([
        pEvent(consumer, "response_processed"),
        clock.tickAsync(75000),
      ]);
      consumer.stop();

      sandbox.assert.calledWith(
        sqs.send.secondCall,
        mockChangeMessageVisibility,
      );
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          ReceiptHandle: "receipt-handle",
          VisibilityTimeout: 40,
        }),
      );
      sandbox.assert.calledWith(
        sqs.send.thirdCall,
        mockChangeMessageVisibility,
      );
      sandbox.assert.match(
        sqs.send.thirdCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          ReceiptHandle: "receipt-handle",
          VisibilityTimeout: 40,
        }),
      );
      sandbox.assert.calledOnce(clearIntervalSpy);
    });

    it("passes in the correct visibility timeout for long running batch handler functions", async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({
        Messages: [
          { MessageId: "1", ReceiptHandle: "receipt-handle-1", Body: "body-1" },
          { MessageId: "2", ReceiptHandle: "receipt-handle-2", Body: "body-2" },
          { MessageId: "3", ReceiptHandle: "receipt-handle-3", Body: "body-3" },
        ],
      });
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: () =>
          new Promise((resolve) => setTimeout(resolve, 75000)),
        batchSize: 3,
        sqs,
        visibilityTimeout: 40,
        heartbeatInterval: 30,
      });
      const clearIntervalSpy = sinon.spy(global, "clearInterval");

      consumer.start();
      await Promise.all([
        pEvent(consumer, "response_processed"),
        clock.tickAsync(75000),
      ]);
      consumer.stop();

      sandbox.assert.calledWith(
        sqs.send.secondCall,
        mockChangeMessageVisibilityBatch,
      );
      sandbox.assert.match(
        sqs.send.secondCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          Entries: sinon.match.array.deepEquals([
            {
              Id: "1",
              ReceiptHandle: "receipt-handle-1",
              VisibilityTimeout: 40,
            },
            {
              Id: "2",
              ReceiptHandle: "receipt-handle-2",
              VisibilityTimeout: 40,
            },
            {
              Id: "3",
              ReceiptHandle: "receipt-handle-3",
              VisibilityTimeout: 40,
            },
          ]),
        }),
      );
      sandbox.assert.calledWith(
        sqs.send.thirdCall,
        mockChangeMessageVisibilityBatch,
      );
      sandbox.assert.match(
        sqs.send.thirdCall.args[0].input,
        sinon.match({
          QueueUrl: QUEUE_URL,
          Entries: [
            {
              Id: "1",
              ReceiptHandle: "receipt-handle-1",
              VisibilityTimeout: 40,
            },
            {
              Id: "2",
              ReceiptHandle: "receipt-handle-2",
              VisibilityTimeout: 40,
            },
            {
              Id: "3",
              ReceiptHandle: "receipt-handle-3",
              VisibilityTimeout: 40,
            },
          ],
        }),
      );
      sandbox.assert.calledOnce(clearIntervalSpy);
    });

    it("emit error when changing visibility timeout fails", async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({
        Messages: [
          { MessageId: "1", ReceiptHandle: "receipt-handle-1", Body: "body-1" },
        ],
      });
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: () =>
          new Promise((resolve) => setTimeout(resolve, 75000)),
        sqs,
        visibilityTimeout: 40,
        heartbeatInterval: 30,
      });

      const receiveErr = new MockSQSError("failed");
      sqs.send.withArgs(mockChangeMessageVisibility).rejects(receiveErr);

      consumer.start();
      const [err]: any[] = await Promise.all([
        pEvent(consumer, "error"),
        clock.tickAsync(75000),
      ]);
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, "Error changing visibility timeout: failed");
      assert.equal(err.queueUrl, QUEUE_URL);
      assert.deepEqual(err.messageIds, ["1"]);
    });

    it("emit error when changing visibility timeout fails for batch handler functions", async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({
        Messages: [
          { MessageId: "1", ReceiptHandle: "receipt-handle-1", Body: "body-1" },
          { MessageId: "2", ReceiptHandle: "receipt-handle-2", Body: "body-2" },
        ],
      });
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: () =>
          new Promise((resolve) => setTimeout(resolve, 75000)),
        sqs,
        batchSize: 2,
        visibilityTimeout: 40,
        heartbeatInterval: 30,
      });

      const receiveErr = new MockSQSError("failed");
      sqs.send.withArgs(mockChangeMessageVisibilityBatch).rejects(receiveErr);

      consumer.start();
      const [err]: any[] = await Promise.all([
        pEvent(consumer, "error"),
        clock.tickAsync(75000),
      ]);
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, "Error changing visibility timeout: failed");
      assert.equal(err.queueUrl, QUEUE_URL);
      assert.deepEqual(err.messageIds, ["1", "2"]);
    });

    it("includes messageIds in timeout errors", async () => {
      const handleMessageTimeout = 500;
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: () =>
          new Promise((resolve) => setTimeout(resolve, 1000)),
        handleMessageTimeout,
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
      });

      consumer.start();
      const [err]: any = await Promise.all([
        pEvent(consumer, "timeout_error"),
        clock.tickAsync(handleMessageTimeout),
      ]);
      consumer.stop();

      assert.ok(err);
      assert.equal(
        err.message,
        `Message handler timed out after ${handleMessageTimeout}ms: Operation timed out.`,
      );
      assert.deepEqual(err.messageIds, ["123"]);
    });

    it("includes messageIds in batch processing errors", async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({
        Messages: [
          { MessageId: "1", ReceiptHandle: "receipt-handle-1", Body: "body-1" },
          { MessageId: "2", ReceiptHandle: "receipt-handle-2", Body: "body-2" },
        ],
      });

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: () => {
          throw new Error("Batch processing error");
        },
        batchSize: 2,
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
      });

      consumer.start();
      const [err]: any = await Promise.all([
        pEvent(consumer, "error"),
        clock.tickAsync(100),
      ]);
      consumer.stop();

      assert.ok(err);
      assert.equal(
        err.message,
        "Unexpected message handler failure: Batch processing error",
      );
      assert.deepEqual(err.messageIds, ["1", "2"]);
    });

    it("includes queueUrl and messageIds in SQS errors when deleting message", async () => {
      const deleteErr = new Error("Delete error");
      deleteErr.name = "SQSError";

      handleMessage.resolves(null);
      sqs.send.withArgs(mockDeleteMessage).rejects(deleteErr);

      consumer.start();
      const [err]: any = await Promise.all([
        pEvent(consumer, "error"),
        clock.tickAsync(100),
      ]);
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, "SQS delete message failed: Delete error");
      assert.equal(err.queueUrl, QUEUE_URL);
      assert.deepEqual(err.messageIds, ["123"]);
    });

    it("includes queueUrl and messageIds in SQS errors when changing visibility timeout", async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({
        Messages: [
          { MessageId: "1", ReceiptHandle: "receipt-handle-1", Body: "body-1" },
        ],
      });
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: () =>
          new Promise((resolve) => setTimeout(resolve, 75000)),
        sqs,
        visibilityTimeout: 40,
        heartbeatInterval: 30,
      });

      const receiveErr = new MockSQSError("failed");
      sqs.send.withArgs(mockChangeMessageVisibility).rejects(receiveErr);

      consumer.start();
      const [err]: any = await Promise.all([
        pEvent(consumer, "error"),
        clock.tickAsync(75000),
      ]);
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, "Error changing visibility timeout: failed");
      assert.equal(err.queueUrl, QUEUE_URL);
      assert.deepEqual(err.messageIds, ["1"]);
    });

    it("includes queueUrl and messageIds in batch SQS errors", async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({
        Messages: [
          { MessageId: "1", ReceiptHandle: "receipt-handle-1", Body: "body-1" },
          { MessageId: "2", ReceiptHandle: "receipt-handle-2", Body: "body-2" },
        ],
      });

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: () =>
          new Promise((resolve) => setTimeout(resolve, 75000)),
        sqs,
        batchSize: 2,
        visibilityTimeout: 40,
        heartbeatInterval: 30,
      });

      const receiveErr = new MockSQSError("failed");
      sqs.send.withArgs(mockChangeMessageVisibilityBatch).rejects(receiveErr);

      consumer.start();
      const [err]: any = await Promise.all([
        pEvent(consumer, "error"),
        clock.tickAsync(75000),
      ]);
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, "Error changing visibility timeout: failed");
      assert.equal(err.queueUrl, QUEUE_URL);
      assert.deepEqual(err.messageIds, ["1", "2"]);
    });

    describe("concurrency", () => {
      it("processes messages respecting the concurrency limit", async () => {
        const message1 = { MessageId: "1", ReceiptHandle: "1", Body: "1" };
        const message2 = { MessageId: "2", ReceiptHandle: "2", Body: "2" };
        const message3 = { MessageId: "3", ReceiptHandle: "3", Body: "3" };

        handleMessage.callsFake(() => new Promise(resolve => setTimeout(resolve, 2000)));
        sqs.send.withArgs(mockReceiveMessage).resolves({ Messages: [message1, message2, message3] });

        consumer = new Consumer({
          queueUrl: QUEUE_URL,
          region: REGION,
          handleMessage,
          sqs,
          concurrency: 2,
          batchSize: 1
        });

        consumer.start();
        await clock.tickAsync(0);

        // First two messages should be in flight
        assert.equal(handleMessage.callCount, 2);
        assert.deepEqual(handleMessage.firstCall.args[0], message1);
        assert.deepEqual(handleMessage.secondCall.args[0], message2);

        // Complete first two messages
        await clock.tickAsync(2000);

        // Third message should now be processing
        assert.equal(handleMessage.callCount, 3);
        assert.deepEqual(handleMessage.thirdCall.args[0], message3);

        consumer.stop();
      });

      it("bypasses concurrency limits in batch mode", async () => {
        const messages = [
          { MessageId: "1", ReceiptHandle: "1", Body: "1" },
          { MessageId: "2", ReceiptHandle: "2", Body: "2" },
          { MessageId: "3", ReceiptHandle: "3", Body: "3" }
        ];

        handleMessageBatch.callsFake(() => new Promise(resolve => setTimeout(resolve, 2000)));
        sqs.send.withArgs(mockReceiveMessage).resolves({ Messages: messages });

        consumer = new Consumer({
          queueUrl: QUEUE_URL,
          region: REGION,
          handleMessageBatch,
          sqs,
          batchSize: 3
        });

        consumer.start();
        await clock.tickAsync(0);

        // All messages should be processed in one batch
        assert.equal(handleMessageBatch.callCount, 1);
        assert.deepEqual(handleMessageBatch.firstCall.args[0], messages);

        await clock.tickAsync(2000);
        consumer.stop();
      });

      it("handles dynamic concurrency updates", async () => {
        const messages = [
          { MessageId: "1", ReceiptHandle: "1", Body: "1" },
          { MessageId: "2", ReceiptHandle: "2", Body: "2" },
          { MessageId: "3", ReceiptHandle: "3", Body: "3" }
        ];

        handleMessage.callsFake(() => new Promise(resolve => setTimeout(resolve, 2000)));
        sqs.send.withArgs(mockReceiveMessage).resolves({ Messages: messages });

        consumer = new Consumer({
          queueUrl: QUEUE_URL,
          region: REGION,
          handleMessage,
          sqs,
          concurrency: 1,
          batchSize: 1,
          pollingWaitTimeMs: 0
        });

        consumer.start();
        await clock.tickAsync(0);

        // First message starts processing
        assert.equal(handleMessage.callCount, 1);
        assert.deepEqual(handleMessage.firstCall.args[0], messages[0]);

        // Update concurrency to 3
        consumer.updateOption("concurrency", 3);
        
        // Need to wait for the next polling cycle
        await clock.tickAsync(POLLING_TIMEOUT);

        // Second and third messages should now be processing
        assert.equal(handleMessage.callCount, 3);
        assert.deepEqual(handleMessage.secondCall.args[0], messages[1]);
        assert.deepEqual(handleMessage.thirdCall.args[0], messages[2]);

        await clock.tickAsync(2000);
        consumer.stop();
      });

      it("completes in-flight messages when stopping", async () => {
        const messages = [
          { MessageId: "1", ReceiptHandle: "1", Body: "1" },
          { MessageId: "2", ReceiptHandle: "2", Body: "2" },
          { MessageId: "3", ReceiptHandle: "3", Body: "3" }
        ];

        let resolveMessage1: (() => void) | undefined;
        
        handleMessage.callsFake((message) => {
          return new Promise<void>(resolve => {
            if (message.MessageId === "1") {
              resolveMessage1 = resolve;
            } else {
              resolve();
            }
          });
        });

        sqs.send.withArgs(mockReceiveMessage).resolves({ Messages: messages });

        consumer = new Consumer({
          queueUrl: QUEUE_URL,
          region: REGION,
          handleMessage,
          sqs,
          concurrency: 2,
          batchSize: 1
        });

        consumer.start();
        await clock.tickAsync(0);

        // First two messages should be in flight
        assert.equal(handleMessage.callCount, 2);
        assert.deepEqual(handleMessage.firstCall.args[0], messages[0]);
        assert.deepEqual(handleMessage.secondCall.args[0], messages[1]);

        consumer.stop();

        // Complete first message
        resolveMessage1?.();
        await clock.tickAsync(0);

        // No more messages should be processed after stopping
        assert.equal(handleMessage.callCount, 2);
      });

      it("reports concurrency slot usage", async () => {
        const messages = [
          { MessageId: "1", ReceiptHandle: "1", Body: "1" },
          { MessageId: "2", ReceiptHandle: "2", Body: "2" },
          { MessageId: "3", ReceiptHandle: "3", Body: "3" }
        ];

        handleMessage.callsFake(() => Promise.resolve());
        sqs.send.withArgs(mockReceiveMessage).resolves({ Messages: messages });

        const concurrencyUpdateHandler = sandbox.stub();
        
        consumer = new Consumer({
          queueUrl: QUEUE_URL,
          region: REGION,
          handleMessage,
          sqs,
          concurrency: 2,
          batchSize: 1
        });

        consumer.on("concurrency_limit_reached", concurrencyUpdateHandler);
        consumer.start();
        await clock.tickAsync(0);

        // First two messages should be in flight, using all concurrency slots
        assert.equal(handleMessage.callCount, 2);
        assert.equal(concurrencyUpdateHandler.callCount, 1);
        assert.deepEqual(concurrencyUpdateHandler.firstCall.args[0], {
          limit: 2,
          waiting: 1
        });

        consumer.stop();
      });

      it("reports concurrency changes when updating limit", async () => {
        const messages = [
          { MessageId: "1", ReceiptHandle: "1", Body: "1" },
          { MessageId: "2", ReceiptHandle: "2", Body: "2" }
        ];

        handleMessage.callsFake(() => new Promise(resolve => setTimeout(resolve, 2000)));
        sqs.send.withArgs(mockReceiveMessage).resolves({ Messages: messages });

        const concurrencyUpdateHandler = sandbox.stub();
        
        consumer = new Consumer({
          queueUrl: QUEUE_URL,
          region: REGION,
          handleMessage,
          sqs,
          concurrency: 1,
          batchSize: 1
        });

        consumer.on("concurrency_limit_reached", concurrencyUpdateHandler);
        consumer.start();
        await clock.tickAsync(0);

        // First message uses the only slot
        assert.equal(handleMessage.callCount, 1);
        assert.equal(concurrencyUpdateHandler.callCount, 1);
        assert.deepEqual(concurrencyUpdateHandler.firstCall.args[0], {
          limit: 1,
          waiting: 1
        });

        // Update concurrency limit
        consumer.updateOption("concurrency", 2);
        await clock.tickAsync(POLLING_TIMEOUT);

        // Second message should now be processing
        assert.equal(handleMessage.callCount, 2);

        consumer.stop();
      });
    });
  });

  describe("event listeners", () => {
    it("fires the event multiple times", async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({});

      const handleEmpty = sandbox.stub().returns(null);

      consumer.on("empty", handleEmpty);

      consumer.start();

      await clock.tickAsync(0);

      consumer.stop();

      await clock.runAllAsync();

      sandbox.assert.calledTwice(handleEmpty);
    });

    it("fires the events only once", async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({});

      const handleEmpty = sandbox.stub().returns(null);

      consumer.once("empty", handleEmpty);

      consumer.start();

      await clock.tickAsync(0);

      consumer.stop();

      await clock.runAllAsync();

      sandbox.assert.calledOnce(handleEmpty);
    });
  });

  describe(".stop", () => {
    it("stops the consumer polling for messages", async () => {
      const handleStop = sandbox.stub().returns(null);

      consumer.on("stopped", handleStop);

      consumer.start();
      consumer.stop();

      await clock.runAllAsync();

      sandbox.assert.calledOnce(handleStop);
      sandbox.assert.calledOnce(handleMessage);
    });

    it("clears the polling timeout when stopped", async () => {
      sinon.spy(clock, "clearTimeout");

      consumer.start();
      await clock.tickAsync(0);
      consumer.stop();

      await clock.runAllAsync();

      sinon.assert.calledTwice(clock.clearTimeout);
    });

    it("fires a stopped event only once when stopped multiple times", async () => {
      const handleStop = sandbox.stub().returns(null);

      consumer.on("stopped", handleStop);

      consumer.start();
      consumer.stop();
      consumer.stop();
      consumer.stop();
      await clock.runAllAsync();

      sandbox.assert.calledOnce(handleStop);
    });

    it("fires a stopped event a second time if started and stopped twice", async () => {
      const handleStop = sandbox.stub().returns(null);

      consumer.on("stopped", handleStop);

      consumer.start();
      consumer.stop();
      consumer.start();
      consumer.stop();
      await clock.runAllAsync();

      sandbox.assert.calledTwice(handleStop);
    });

    it("aborts requests when the abort param is true", async () => {
      const handleStop = sandbox.stub().returns(null);
      const handleAbort = sandbox.stub().returns(null);

      consumer.on("stopped", handleStop);
      consumer.on("aborted", handleAbort);

      consumer.start();
      consumer.stop({ abort: true });

      await clock.runAllAsync();

      assert.isTrue(consumer.abortController.signal.aborted);
      sandbox.assert.calledOnce(handleMessage);
      sandbox.assert.calledOnce(handleAbort);
      sandbox.assert.calledOnce(handleStop);
    });

    it("waits for in-flight messages before emitting stopped (within timeout)", async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({
        Messages: [
          { MessageId: "1", ReceiptHandle: "receipt-handle-1", Body: "body-1" },
        ],
      });
      const handleStop = sandbox.stub().returns(null);
      const handleResponseProcessed = sandbox.stub().returns(null);
      const waitingForPollingComplete = sandbox.stub().returns(null);
      const waitingForPollingCompleteTimeoutExceeded = sandbox
        .stub()
        .returns(null);

      // A slow message handler
      handleMessage = sandbox
        .stub()
        .resolves(new Promise((resolve) => setTimeout(resolve, 5000)));

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage,
        sqs,
        pollingCompleteWaitTimeMs: 5000,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
      });

      consumer.on("stopped", handleStop);
      consumer.on("response_processed", handleResponseProcessed);
      consumer.on("waiting_for_polling_to_complete", waitingForPollingComplete);
      consumer.on(
        "waiting_for_polling_to_complete_timeout_exceeded",
        waitingForPollingCompleteTimeoutExceeded,
      );

      consumer.start();
      await Promise.all([clock.tickAsync(1)]);
      consumer.stop();

      await clock.runAllAsync();

      sandbox.assert.calledOnce(handleStop);
      sandbox.assert.calledOnce(handleResponseProcessed);
      sandbox.assert.calledOnce(handleMessage);
      assert(waitingForPollingComplete.callCount === 5);
      assert(waitingForPollingCompleteTimeoutExceeded.callCount === 0);

      assert.ok(handleMessage.calledBefore(handleStop));

      // handleResponseProcessed is called after handleMessage, indicating
      // messages were allowed to complete before 'stopped' was emitted
      assert.ok(handleResponseProcessed.calledBefore(handleStop));
    });

    it("waits for in-flight messages before emitting stopped (timeout reached)", async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({
        Messages: [
          { MessageId: "1", ReceiptHandle: "receipt-handle-1", Body: "body-1" },
        ],
      });
      const handleStop = sandbox.stub().returns(null);
      const handleResponseProcessed = sandbox.stub().returns(null);
      const waitingForPollingComplete = sandbox.stub().returns(null);
      const waitingForPollingCompleteTimeoutExceeded = sandbox
        .stub()
        .returns(null);

      // A slow message handler
      handleMessage = sandbox
        .stub()
        .resolves(new Promise((resolve) => setTimeout(resolve, 5000)));

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage,
        sqs,
        pollingCompleteWaitTimeMs: 500,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
      });

      consumer.on("stopped", handleStop);
      consumer.on("response_processed", handleResponseProcessed);
      consumer.on("waiting_for_polling_to_complete", waitingForPollingComplete);
      consumer.on(
        "waiting_for_polling_to_complete_timeout_exceeded",
        waitingForPollingCompleteTimeoutExceeded,
      );

      consumer.start();
      await Promise.all([clock.tickAsync(1)]);
      consumer.stop();

      await clock.runAllAsync();

      sandbox.assert.calledOnce(handleStop);
      sandbox.assert.calledOnce(handleResponseProcessed);
      sandbox.assert.calledOnce(handleMessage);
      sandbox.assert.calledOnce(waitingForPollingComplete);
      sandbox.assert.calledOnce(waitingForPollingCompleteTimeoutExceeded);
      assert(handleMessage.calledBefore(handleStop));

      // Stop was called before the message could be processed, because we reached timeout.
      assert(handleStop.calledBefore(handleResponseProcessed));
    });
  });

  describe("status", async () => {
    it("returns the defaults before the consumer is started", () => {
      assert.isFalse(consumer.status.isRunning);
      assert.isFalse(consumer.status.isPolling);
    });

    it("returns true for `isRunning` if the consumer has not been stopped", () => {
      consumer.start();
      assert.isTrue(consumer.status.isRunning);
      consumer.stop();
    });

    it("returns false for `isRunning` if the consumer has been stopped", () => {
      consumer.start();
      consumer.stop();
      assert.isFalse(consumer.status.isRunning);
    });

    it("returns true for `isPolling` if the consumer is polling for messages", async () => {
      sqs.send.withArgs(mockReceiveMessage).resolves({
        Messages: [
          { MessageId: "1", ReceiptHandle: "receipt-handle-1", Body: "body-1" },
        ],
      });
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: () => new Promise((resolve) => setTimeout(resolve, 20)),
        sqs,
      });

      consumer.start();
      await Promise.all([clock.tickAsync(1)]);
      assert.isTrue(consumer.status.isPolling);
      consumer.stop();
      assert.isTrue(consumer.status.isPolling);
      await Promise.all([clock.tickAsync(21)]);
      assert.isFalse(consumer.status.isPolling);
    });
  });

  describe("updateOption", async () => {
    it("updates the visibilityTimeout option and emits an event", () => {
      const optionUpdatedListener = sandbox.stub();
      consumer.on("option_updated", optionUpdatedListener);

      consumer.updateOption("visibilityTimeout", 45);

      assert.equal(consumer.visibilityTimeout, 45);

      sandbox.assert.calledWithMatch(
        optionUpdatedListener,
        "visibilityTimeout",
        45,
      );
    });

    it("does not update the visibilityTimeout if the value is less than the heartbeatInterval", () => {
      consumer = new Consumer({
        region: REGION,
        queueUrl: QUEUE_URL,
        handleMessage,
        heartbeatInterval: 30,
        visibilityTimeout: 60,
      });

      const optionUpdatedListener = sandbox.stub();
      consumer.on("option_updated", optionUpdatedListener);

      assert.throws(() => {
        consumer.updateOption("visibilityTimeout", 30);
      }, "heartbeatInterval must be less than visibilityTimeout.");

      assert.equal(consumer.visibilityTimeout, 60);

      sandbox.assert.notCalled(optionUpdatedListener);
    });

    it("updates the batchSize option and emits an event", () => {
      const optionUpdatedListener = sandbox.stub();
      consumer.on("option_updated", optionUpdatedListener);

      consumer.updateOption("batchSize", 4);

      assert.equal(consumer.batchSize, 4);

      sandbox.assert.calledWithMatch(optionUpdatedListener, "batchSize", 4);
    });

    it("does not update the batchSize if the value is more than 10", () => {
      const optionUpdatedListener = sandbox.stub();
      consumer.on("option_updated", optionUpdatedListener);

      assert.throws(() => {
        consumer.updateOption("batchSize", 13);
      }, "batchSize must be between 1 and 10.");

      assert.equal(consumer.batchSize, 1);

      sandbox.assert.notCalled(optionUpdatedListener);
    });

    it("does not update the batchSize if the value is less than 1", () => {
      const optionUpdatedListener = sandbox.stub();
      consumer.on("option_updated", optionUpdatedListener);

      assert.throws(() => {
        consumer.updateOption("batchSize", 0);
      }, "batchSize must be between 1 and 10.");

      assert.equal(consumer.batchSize, 1);

      sandbox.assert.notCalled(optionUpdatedListener);
    });

    it("updates the waitTimeSeconds option and emits an event", () => {
      const optionUpdatedListener = sandbox.stub();
      consumer.on("option_updated", optionUpdatedListener);

      consumer.updateOption("waitTimeSeconds", 18);

      assert.equal(consumer.waitTimeSeconds, 18);

      sandbox.assert.calledWithMatch(
        optionUpdatedListener,
        "waitTimeSeconds",
        18,
      );
    });

    it("does not update the batchSize if the value is less than 0", () => {
      const optionUpdatedListener = sandbox.stub();
      consumer.on("option_updated", optionUpdatedListener);

      assert.throws(() => {
        consumer.updateOption("waitTimeSeconds", -1);
      }, "waitTimeSeconds must be between 0 and 20.");

      assert.equal(consumer.waitTimeSeconds, 20);

      sandbox.assert.notCalled(optionUpdatedListener);
    });

    it("does not update the batchSize if the value is more than 20", () => {
      const optionUpdatedListener = sandbox.stub();
      consumer.on("option_updated", optionUpdatedListener);

      assert.throws(() => {
        consumer.updateOption("waitTimeSeconds", 27);
      }, "waitTimeSeconds must be between 0 and 20.");

      assert.equal(consumer.waitTimeSeconds, 20);

      sandbox.assert.notCalled(optionUpdatedListener);
    });

    it("updates the pollingWaitTimeMs option and emits an event", () => {
      const optionUpdatedListener = sandbox.stub();
      consumer.on("option_updated", optionUpdatedListener);

      consumer.updateOption("pollingWaitTimeMs", 1000);

      assert.equal(consumer.pollingWaitTimeMs, 1000);

      sandbox.assert.calledWithMatch(
        optionUpdatedListener,
        "pollingWaitTimeMs",
        1000,
      );
    });

    it("does not update the pollingWaitTimeMs if the value is less than 0", () => {
      const optionUpdatedListener = sandbox.stub();
      consumer.on("option_updated", optionUpdatedListener);

      assert.throws(() => {
        consumer.updateOption("pollingWaitTimeMs", -1);
      }, "pollingWaitTimeMs must be greater than 0.");

      assert.equal(consumer.pollingWaitTimeMs, 0);

      sandbox.assert.notCalled(optionUpdatedListener);
    });

    it("throws an error for an unknown option", () => {
      consumer = new Consumer({
        region: REGION,
        queueUrl: QUEUE_URL,
        handleMessage,
        visibilityTimeout: 60,
      });

      assert.throws(() => {
        consumer.updateOption("unknown", "value");
      }, `The update unknown cannot be updated`);
    });
  });

  describe("events", () => {
    it("logs a debug event when an event is emitted", async () => {
      const loggerDebug = sandbox.stub(logger, "debug");

      consumer.start();
      consumer.stop();

      sandbox.assert.callCount(loggerDebug, 5);
      // Logged directly
      sandbox.assert.calledWithMatch(loggerDebug, "starting");
      // Sent from the emitter
      sandbox.assert.calledWithMatch(loggerDebug, "started", {
        queueUrl: QUEUE_URL,
      });
      // Logged directly
      sandbox.assert.calledWithMatch(loggerDebug, "polling");
      // Logged directly
      sandbox.assert.calledWithMatch(loggerDebug, "stopping");
      // Sent from the emitter
      sandbox.assert.calledWithMatch(loggerDebug, "stopped", {
        queueUrl: QUEUE_URL,
      });
    });

    it("includes queueUrl in emitted events", async () => {
      const startedListener = sandbox.stub();
      const messageReceivedListener = sandbox.stub();
      const messageProcessedListener = sandbox.stub();
      const emptyListener = sandbox.stub();
      const stoppedListener = sandbox.stub();
      const errorListener = sandbox.stub();
      const processingErrorListener = sandbox.stub();

      consumer.on("started", startedListener);
      consumer.on("message_received", messageReceivedListener);
      consumer.on("message_processed", messageProcessedListener);
      consumer.on("empty", emptyListener);
      consumer.on("stopped", stoppedListener);
      consumer.on("error", errorListener);
      consumer.on("processing_error", processingErrorListener);

      consumer.start();
      await pEvent(consumer, "message_processed");
      consumer.stop();

      handleMessage.rejects(new Error("Processing error"));
      consumer.start();
      await pEvent(consumer, "processing_error");
      consumer.stop();

      sandbox.assert.calledWith(startedListener, { queueUrl: QUEUE_URL });
      sandbox.assert.calledWith(messageReceivedListener, response.Messages[0], {
        queueUrl: QUEUE_URL,
      });
      sandbox.assert.calledWith(
        messageProcessedListener,
        response.Messages[0],
        { queueUrl: QUEUE_URL },
      );
      sandbox.assert.calledWith(stoppedListener, { queueUrl: QUEUE_URL });
      sandbox.assert.calledWith(
        processingErrorListener,
        sinon.match.instanceOf(Error),
        response.Messages[0],
        { queueUrl: QUEUE_URL },
      );
    });
  });
});
