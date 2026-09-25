import {
  ChangeMessageVisibilityBatchCommand,
  ChangeMessageVisibilityCommand,
  DeleteMessageBatchCommand,
  DeleteMessageCommand,
  ReceiveMessageCommand,
  SQSClient,
} from "@aws-sdk/client-sqs";
import type { QueueAttributeName, Message } from "@aws-sdk/client-sqs";
import { strict as assert } from "node:assert";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { pEvent } from "p-event";

import type { AWSError } from "../../src/types.js";
import { Consumer } from "../../src/consumer.js";
import { logger } from "../../src/logger.js";

const AUTHENTICATION_ERROR_TIMEOUT = 20;
const POLLING_TIMEOUT = 100;
const QUEUE_URL = "some-queue-url";
const REGION = "some-region";

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
  let handleMessage;
  let handleMessageBatch;
  let sqs;
  let receiveMessageMock;
  let deleteMessageMock;
  let deleteMessageBatchMock;
  let changeMessageVisibilityMock;
  let changeMessageVisibilityBatchMock;
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
    vi.useFakeTimers();
    handleMessage = vi.fn().mockResolvedValue(response.Messages[0]);
    handleMessageBatch = vi.fn().mockResolvedValue([]);
    receiveMessageMock = vi.fn().mockResolvedValue(response);
    deleteMessageMock = vi.fn().mockResolvedValue(undefined);
    deleteMessageBatchMock = vi.fn().mockResolvedValue(undefined);
    changeMessageVisibilityMock = vi.fn().mockResolvedValue(undefined);
    changeMessageVisibilityBatchMock = vi.fn().mockResolvedValue(undefined);

    sqs = new SQSClient({ region: REGION });
    vi.spyOn(sqs, "send").mockImplementation((command) => {
      if (command instanceof ReceiveMessageCommand) {
        return receiveMessageMock(command);
      }
      if (command instanceof DeleteMessageCommand) {
        return deleteMessageMock(command);
      }
      if (command instanceof DeleteMessageBatchCommand) {
        return deleteMessageBatchMock(command);
      }
      if (command instanceof ChangeMessageVisibilityCommand) {
        return changeMessageVisibilityMock(command);
      }
      if (command instanceof ChangeMessageVisibilityBatchCommand) {
        return changeMessageVisibilityBatchMock(command);
      }

      throw new Error(`Unexpected SQS command: ${command.constructor.name}`);
    });

    consumer = new Consumer({
      queueUrl: QUEUE_URL,
      region: REGION,
      handleMessage,
      sqs,
      authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
    });
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  describe("options validation", () => {
    it("requires a handleMessage or handleMessagesBatch function to be set", () => {
      expect(() => {
        new Consumer({
          handleMessage: undefined,
          region: REGION,
          queueUrl: QUEUE_URL,
        });
      }).toThrow(`Missing SQS consumer option [ handleMessage or handleMessageBatch ].`);
    });

    it("requires the batchSize option to be no greater than 10", () => {
      expect(() => {
        new Consumer({
          region: REGION,
          queueUrl: QUEUE_URL,
          handleMessage,
          batchSize: 11,
        });
      }).toThrow("batchSize must be between 1 and 10.");
    });

    it("requires the batchSize option to be greater than 0", () => {
      expect(() => {
        new Consumer({
          region: REGION,
          queueUrl: QUEUE_URL,
          handleMessage,
          batchSize: -1,
        });
      }).toThrow("batchSize must be between 1 and 10.");
    });

    it("requires visibilityTimeout to be set with heartbeatInterval", () => {
      expect(() => {
        new Consumer({
          region: REGION,
          queueUrl: QUEUE_URL,
          handleMessage,
          heartbeatInterval: 30,
        });
      }).toThrow("heartbeatInterval must be less than visibilityTimeout.");
    });

    it("requires heartbeatInterval to be less than visibilityTimeout", () => {
      expect(() => {
        new Consumer({
          region: REGION,
          queueUrl: QUEUE_URL,
          handleMessage,
          heartbeatInterval: 30,
          visibilityTimeout: 30,
        });
      }).toThrow("heartbeatInterval must be less than visibilityTimeout.");
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

      expect(instance).toBeInstanceOf(Consumer);
    });
  });

  describe(".start", () => {
    it("uses the correct abort signal", async () => {
      receiveMessageMock.mockResolvedValue(new Promise((res) => setTimeout(res, 100)));

      // Starts and abort is false
      consumer.start();
      expect(vi.mocked(sqs.send).mock.lastCall?.[1]?.abortSignal?.aborted).toBe(false);

      // normal stop without an abort and abort is false
      consumer.stop();
      expect(vi.mocked(sqs.send).mock.lastCall?.[1]?.abortSignal?.aborted).toBe(false);

      // Starts and abort is false
      consumer.start();
      expect(vi.mocked(sqs.send).mock.lastCall?.[1]?.abortSignal?.aborted).toBe(false);

      // Stop with abort and abort is true
      consumer.stop({ abort: true });
      expect(vi.mocked(sqs.send).mock.lastCall?.[1]?.abortSignal?.aborted).toBe(true);

      // Starts and abort is false
      consumer.start();
      expect(vi.mocked(sqs.send).mock.lastCall?.[1]?.abortSignal?.aborted).toBe(false);
    });

    it("fires an event when the consumer is started", async () => {
      const handleStart = vi.fn().mockReturnValue(null);

      consumer.on("started", handleStart);

      consumer.start();
      consumer.stop();

      expect(handleStart).toHaveBeenCalledOnce();
    });

    it("fires an error event when an error occurs receiving a message", async () => {
      const receiveErr = new Error("Receive error");

      receiveMessageMock.mockRejectedValue(receiveErr);

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

      receiveMessageMock.mockRejectedValue(receiveErr);

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
      expect(err.response).toBeUndefined();
      expect(err.metadata).toBeUndefined();
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

      receiveMessageMock.mockRejectedValue(receiveErr);

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

      receiveMessageMock.mockRejectedValue(receiveErr);

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
      expect(err.response).toBeUndefined();
      expect(err.metadata).toBeUndefined();
    });

    it("fires a timeout event if handler function takes too long", async () => {
      const handleMessageTimeout = 500;
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: () => new Promise((resolve) => setTimeout(() => resolve(undefined), 1000)),
        handleMessageTimeout,
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
      });

      consumer.start();
      const [err]: any = await Promise.all([
        pEvent(consumer, "timeout_error"),
        vi.advanceTimersByTimeAsync(handleMessageTimeout),
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
      assert.equal(err.message, "Unexpected message handler failure: unexpected parsing error");
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
      assert.equal(err.message, "Unexpected message handler failure: unexpected parsing error");
    });

    it("fires an error event when an error occurs deleting a message", async () => {
      const deleteErr = new Error("Delete error");

      handleMessage.mockResolvedValue(response.Messages[0]);
      deleteMessageMock.mockRejectedValue(deleteErr);

      consumer.start();
      const err: any = await pEvent(consumer, "error");
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, "SQS delete message failed: Delete error");
    });

    it("fires a `processing_error` event when a non-`SQSError` error occurs processing a message", async () => {
      const processingErr = new Error("Processing error");

      handleMessage.mockRejectedValue(processingErr);

      consumer.start();
      const [err, message] = await pEvent<string | symbol, { [key: string]: string }[]>(
        consumer,
        "processing_error",
        {
          multiArgs: true,
        },
      );
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

      handleMessage.mockResolvedValue(response.Messages[0]);
      deleteMessageMock.mockRejectedValue(sqsError);

      consumer.start();
      const [err, message] = await pEvent<string | symbol, { [key: string]: string }[]>(
        consumer,
        "error",
        {
          multiArgs: true,
        },
      );
      consumer.stop();

      assert.equal(err.message, "SQS delete message failed: Processing error");
      assert.equal(message.MessageId, "123");
    });

    it("waits before repolling when a credentials error occurs", async () => {
      const loggerDebug = vi.spyOn(logger, "debug").mockImplementation(() => undefined);

      const credentialsErr = {
        name: "CredentialsError",
        message: "Missing credentials in config",
      };
      receiveMessageMock.mockRejectedValue(credentialsErr);
      const errorListener = vi.fn();
      consumer.on("error", errorListener);

      consumer.start();
      await vi.advanceTimersByTimeAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      expect(errorListener).toHaveBeenCalledTimes(2);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(ReceiveMessageCommand);

      expect(loggerDebug).toHaveBeenCalledWith("authentication_error", {
        code: "CredentialsError",
        detail: "There was an authentication error. Pausing before retrying.",
      });
    });

    it("waits before repolling when a 403 error occurs", async () => {
      const loggerDebug = vi.spyOn(logger, "debug").mockImplementation(() => undefined);

      const invalidSignatureErr = {
        $metadata: {
          httpStatusCode: 403,
        },
        message: "The security token included in the request is invalid",
      };
      receiveMessageMock.mockRejectedValue(invalidSignatureErr);
      const errorListener = vi.fn();
      consumer.on("error", errorListener);

      consumer.start();
      await vi.advanceTimersByTimeAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      expect(errorListener).toHaveBeenCalledTimes(2);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(ReceiveMessageCommand);

      expect(loggerDebug).toHaveBeenCalledWith("authentication_error", {
        code: "Unknown",
        detail: "There was an authentication error. Pausing before retrying.",
      });
    });

    it("waits before repolling when a UnknownEndpoint error occurs", async () => {
      const loggerDebug = vi.spyOn(logger, "debug").mockImplementation(() => undefined);

      const unknownEndpointErr = {
        name: "UnknownEndpoint",
        message:
          "Inaccessible host: `sqs.eu-west-1.amazonaws.com`. This service may not be available in the `eu-west-1` region.",
      };
      receiveMessageMock.mockRejectedValue(unknownEndpointErr);
      const errorListener = vi.fn();
      consumer.on("error", errorListener);

      consumer.start();
      await vi.advanceTimersByTimeAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      expect(errorListener).toHaveBeenCalledTimes(2);
      expect(sqs.send).toHaveBeenCalledTimes(2);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(ReceiveMessageCommand);

      expect(loggerDebug).toHaveBeenCalledWith("authentication_error", {
        code: "UnknownEndpoint",
        detail: "There was an authentication error. Pausing before retrying.",
      });
    });

    it("waits before repolling when a NonExistentQueue error occurs", async () => {
      const loggerDebug = vi.spyOn(logger, "debug").mockImplementation(() => undefined);

      const nonExistentQueueErr = {
        name: "AWS.SimpleQueueService.NonExistentQueue",
        message: "The specified queue does not exist for this wsdl version.",
      };
      receiveMessageMock.mockRejectedValue(nonExistentQueueErr);
      const errorListener = vi.fn();
      consumer.on("error", errorListener);

      consumer.start();
      await vi.advanceTimersByTimeAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      expect(errorListener).toHaveBeenCalledTimes(2);
      expect(sqs.send).toHaveBeenCalledTimes(2);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(ReceiveMessageCommand);

      expect(loggerDebug).toHaveBeenCalledWith("authentication_error", {
        code: "AWS.SimpleQueueService.NonExistentQueue",
        detail: "There was an authentication error. Pausing before retrying.",
      });
    });

    it("waits before repolling when a CredentialsProviderError error occurs", async () => {
      const loggerDebug = vi.spyOn(logger, "debug").mockImplementation(() => undefined);

      const credentialsProviderErr = {
        name: "CredentialsProviderError",
        message: "Could not load credentials from any providers.",
      };
      receiveMessageMock.mockRejectedValue(credentialsProviderErr);
      const errorListener = vi.fn();
      consumer.on("error", errorListener);

      consumer.start();
      await vi.advanceTimersByTimeAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      expect(errorListener).toHaveBeenCalledTimes(2);
      expect(sqs.send).toHaveBeenCalledTimes(2);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(ReceiveMessageCommand);

      expect(loggerDebug).toHaveBeenCalledWith("authentication_error", {
        code: "CredentialsProviderError",
        detail: "There was an authentication error. Pausing before retrying.",
      });
    });

    it("waits before repolling when a InvalidAddress error occurs", async () => {
      const loggerDebug = vi.spyOn(logger, "debug").mockImplementation(() => undefined);

      const credentialsProviderErr = {
        name: "InvalidAddress",
        message: "The address some-queue-url is not valid for this endpoint.",
      };
      receiveMessageMock.mockRejectedValue(credentialsProviderErr);
      const errorListener = vi.fn();
      consumer.on("error", errorListener);

      consumer.start();
      await vi.advanceTimersByTimeAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      expect(errorListener).toHaveBeenCalledTimes(2);
      expect(sqs.send).toHaveBeenCalledTimes(2);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(ReceiveMessageCommand);

      expect(loggerDebug).toHaveBeenCalledWith("authentication_error", {
        code: "InvalidAddress",
        detail: "There was an authentication error. Pausing before retrying.",
      });
    });

    it("waits before repolling when a InvalidSecurity error occurs", async () => {
      const loggerDebug = vi.spyOn(logger, "debug").mockImplementation(() => undefined);

      const credentialsProviderErr = {
        name: "InvalidSecurity",
        message: "The queue is not is not HTTPS and SigV4.",
      };
      receiveMessageMock.mockRejectedValue(credentialsProviderErr);
      const errorListener = vi.fn();
      consumer.on("error", errorListener);

      consumer.start();
      await vi.advanceTimersByTimeAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      expect(errorListener).toHaveBeenCalledTimes(2);
      expect(sqs.send).toHaveBeenCalledTimes(2);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(ReceiveMessageCommand);

      expect(loggerDebug).toHaveBeenCalledWith("authentication_error", {
        code: "InvalidSecurity",
        detail: "There was an authentication error. Pausing before retrying.",
      });
    });

    it("waits before repolling when a QueueDoesNotExist error occurs", async () => {
      const loggerDebug = vi.spyOn(logger, "debug").mockImplementation(() => undefined);

      const credentialsProviderErr = {
        name: "QueueDoesNotExist",
        message: "The queue does not exist.",
      };
      receiveMessageMock.mockRejectedValue(credentialsProviderErr);
      const errorListener = vi.fn();
      consumer.on("error", errorListener);

      consumer.start();
      await vi.advanceTimersByTimeAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      expect(errorListener).toHaveBeenCalledTimes(2);
      expect(sqs.send).toHaveBeenCalledTimes(2);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(ReceiveMessageCommand);

      expect(loggerDebug).toHaveBeenCalledWith("authentication_error", {
        code: "QueueDoesNotExist",
        detail: "There was an authentication error. Pausing before retrying.",
      });
    });

    it("waits before repolling when a RequestThrottled error occurs", async () => {
      const loggerDebug = vi.spyOn(logger, "debug").mockImplementation(() => undefined);

      const credentialsProviderErr = {
        name: "RequestThrottled",
        message: "Requests have been throttled.",
      };
      receiveMessageMock.mockRejectedValue(credentialsProviderErr);
      const errorListener = vi.fn();
      consumer.on("error", errorListener);

      consumer.start();
      await vi.advanceTimersByTimeAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      expect(errorListener).toHaveBeenCalledTimes(2);
      expect(sqs.send).toHaveBeenCalledTimes(2);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(ReceiveMessageCommand);

      expect(loggerDebug).toHaveBeenCalledWith("authentication_error", {
        code: "RequestThrottled",
        detail: "There was an authentication error. Pausing before retrying.",
      });
    });

    it("waits before repolling when a RequestThrottled error occurs", async () => {
      const loggerDebug = vi.spyOn(logger, "debug").mockImplementation(() => undefined);

      const credentialsProviderErr = {
        name: "OverLimit",
        message: "An over limit error.",
      };
      receiveMessageMock.mockRejectedValue(credentialsProviderErr);
      const errorListener = vi.fn();
      consumer.on("error", errorListener);

      consumer.start();
      await vi.advanceTimersByTimeAsync(AUTHENTICATION_ERROR_TIMEOUT);
      consumer.stop();

      expect(errorListener).toHaveBeenCalledTimes(2);
      expect(sqs.send).toHaveBeenCalledTimes(2);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(ReceiveMessageCommand);

      expect(loggerDebug).toHaveBeenCalledWith("authentication_error", {
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
      await vi.advanceTimersByTimeAsync(POLLING_TIMEOUT);
      consumer.stop();

      expect(sqs.send).toHaveBeenCalledTimes(4);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(DeleteMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[2]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[3]?.[0]).toBeInstanceOf(DeleteMessageCommand);
    });

    it("fires a message_received event when a message is received", async () => {
      consumer.start();
      const message = await pEvent(consumer, "message_received");
      consumer.stop();

      assert.equal(message, response.Messages[0]);
    });

    it("fires a message_processed event when a message is successfully deleted", async () => {
      handleMessage.mockResolvedValue(response.Messages[0]);

      consumer.start();
      const message = await pEvent(consumer, "message_received");
      consumer.stop();

      assert.equal(message, response.Messages[0]);
    });

    it("calls the handleMessage function when a message is received", async () => {
      consumer.start();
      await pEvent(consumer, "message_processed");
      consumer.stop();

      expect(handleMessage).toHaveBeenCalledWith(response.Messages[0]);
    });

    it("calls the preReceiveMessageCallback and postReceiveMessageCallback function before receiving a message", async () => {
      const preReceiveMessageCallbackStub = vi.fn().mockResolvedValue(null);
      const postReceiveMessageCallbackStub = vi.fn().mockResolvedValue(null);

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

      expect(preReceiveMessageCallbackStub).toHaveBeenCalledOnce();
      expect(postReceiveMessageCallbackStub).toHaveBeenCalledOnce();
    });

    it("deletes the message when the handleMessage function is called", async () => {
      handleMessage.mockResolvedValue(response.Messages[0]);

      consumer.start();
      await pEvent(consumer, "message_processed");
      consumer.stop();

      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(DeleteMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0].input).toMatchObject({
        QueueUrl: QUEUE_URL,
        ReceiptHandle: "receipt-handle",
      });
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

      handleMessage.mockResolvedValue(response.Messages[0]);

      consumer.start();
      await pEvent(consumer, "message_processed");
      consumer.stop();

      expect(
        vi.mocked(sqs.send).mock.calls.some(([command]) => command instanceof DeleteMessageCommand),
      ).toBe(false);
    });

    it("doesn't delete the message when a processing error is reported", async () => {
      handleMessage.mockRejectedValue(new Error("Processing error"));

      consumer.start();
      await pEvent(consumer, "processing_error");
      consumer.stop();

      expect(
        vi.mocked(sqs.send).mock.calls.some(([command]) => command instanceof DeleteMessageCommand),
      ).toBe(false);
    });

    it("consumes another message once one is processed", async () => {
      handleMessage.mockResolvedValue(response.Messages[0]);

      consumer.start();
      await vi.runOnlyPendingTimersAsync();
      consumer.stop();

      expect(handleMessage).toHaveBeenCalledTimes(2);
    });

    it("doesn't consume more messages when called multiple times", () => {
      receiveMessageMock.mockResolvedValue(new Promise((res) => setTimeout(res, 100)));
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.start();
      consumer.stop();

      expect(sqs.send).toHaveBeenCalledOnce();
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
    });

    it("doesn't consume more messages when called multiple times after stopped", () => {
      receiveMessageMock.mockResolvedValue(new Promise((res) => setTimeout(res, 100)));
      consumer.start();
      consumer.stop();

      consumer.start();
      consumer.start();
      consumer.start();
      consumer.start();

      expect(sqs.send).toHaveBeenCalledTimes(2);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
    });

    it("consumes multiple messages when the batchSize is greater than 1", async () => {
      receiveMessageMock.mockResolvedValue({
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

      expect(handleMessage).toHaveBeenCalledTimes(3);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0].input).toMatchObject({
        QueueUrl: QUEUE_URL,
        AttributeNames: [],
        MessageAttributeNames: ["attribute-1", "attribute-2"],
        MessageSystemAttributeNames: ["All"],
        MaxNumberOfMessages: 3,
        WaitTimeSeconds: AUTHENTICATION_ERROR_TIMEOUT,
        VisibilityTimeout: undefined,
      });
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

      receiveMessageMock.mockResolvedValue({
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

      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0].input).toMatchObject({
        QueueUrl: QUEUE_URL,
        AttributeNames: ["ApproximateReceiveCount"],
        MessageAttributeNames: [],
        MessageSystemAttributeNames: [],
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: AUTHENTICATION_ERROR_TIMEOUT,
        VisibilityTimeout: undefined,
      });

      assert.equal(message, messageWithAttr);
    });

    it("fires an emptyQueue event when all messages have been consumed", async () => {
      receiveMessageMock.mockResolvedValue({});

      consumer.start();
      await pEvent(consumer, "empty");
      consumer.stop();
    });

    it("terminates message visibility timeout on processing error", async () => {
      handleMessage.mockRejectedValue(new Error("Processing error"));

      consumer.terminateVisibilityTimeout = true;

      consumer.start();
      await pEvent(consumer, "processing_error");
      consumer.stop();

      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(ChangeMessageVisibilityCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0].input).toMatchObject({
        QueueUrl: QUEUE_URL,
        ReceiptHandle: "receipt-handle",
        VisibilityTimeout: 0,
      });
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
      receiveMessageMock.mockResolvedValue({
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
            Number.parseInt(messages[0].Attributes?.ApproximateReceiveCount || "1") || 1;
          return receiveCount * 10;
        },
      });

      handleMessage.mockRejectedValue(new Error("Processing error"));

      consumer.start();
      await pEvent(consumer, "processing_error");
      consumer.stop();

      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(ChangeMessageVisibilityCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0].input).toMatchObject({
        QueueUrl: QUEUE_URL,
        ReceiptHandle: "receipt-handle",
        VisibilityTimeout: 20,
      });
    });

    it("changes message visibility timeout on processing error", async () => {
      handleMessage.mockRejectedValue(new Error("Processing error"));

      consumer.terminateVisibilityTimeout = 10;

      consumer.start();
      await pEvent(consumer, "processing_error");
      consumer.stop();

      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(ChangeMessageVisibilityCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0].input).toMatchObject({
        QueueUrl: QUEUE_URL,
        ReceiptHandle: "receipt-handle",
        VisibilityTimeout: 10,
      });
    });

    it("does not terminate visibility timeout when `terminateVisibilityTimeout` option is false", async () => {
      handleMessage.mockRejectedValue(new Error("Processing error"));
      consumer.terminateVisibilityTimeout = false;

      consumer.start();
      await pEvent(consumer, "processing_error");
      consumer.stop();

      expect(
        vi
          .mocked(sqs.send)
          .mock.calls.some(([command]) => command instanceof ChangeMessageVisibilityCommand),
      ).toBe(false);
    });

    it("fires error event when failed to terminate visibility timeout on processing error", async () => {
      handleMessage.mockRejectedValue(new Error("Processing error"));

      const sqsError = new Error("Processing error");
      sqsError.name = "SQSError";
      changeMessageVisibilityMock.mockRejectedValue(sqsError);
      consumer.terminateVisibilityTimeout = true;

      consumer.start();
      await pEvent(consumer, "error");
      consumer.stop();

      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(ChangeMessageVisibilityCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0].input).toMatchObject({
        QueueUrl: QUEUE_URL,
        ReceiptHandle: "receipt-handle",
        VisibilityTimeout: 0,
      });
    });

    it("fires response_processed event for each batch", async () => {
      receiveMessageMock.mockResolvedValue({
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
      handleMessage.mockResolvedValue(response.Messages[0]);

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

      expect(handleMessage).toHaveBeenCalledTimes(2);
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

      expect(handleMessageBatch).toHaveBeenCalledTimes(1);
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
        "Unexpected message batch handler failure: unexpected parsing error",
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
        "Unexpected message batch handler failure: unexpected parsing error",
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

      expect(handleMessageBatch).toHaveBeenCalledTimes(1);
      expect(handleMessage).toHaveBeenCalledTimes(0);
    });

    it("does not ack the message if handleMessage returns void", async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        // @ts-expect-error - we want to test expected behaviour
        handleMessage: async () => {},
        sqs,
      });

      consumer.start();
      await pEvent(consumer, "response_processed");
      consumer.stop();

      expect(sqs.send).toHaveBeenCalledTimes(1);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(
        vi.mocked(sqs.send).mock.calls.some(([command]) => command instanceof DeleteMessageCommand),
      ).toBe(false);
    });

    it("logs deprecation warning when handleMessage returns null", async () => {
      const consoleWarnStub = vi.spyOn(console, "warn").mockImplementation(() => undefined);

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: async () => null,
        sqs,
      });

      consumer.start();
      await pEvent(consumer, "response_processed");
      consumer.stop();

      expect(consoleWarnStub).toHaveBeenCalledOnce();
      expect(consoleWarnStub).toHaveBeenCalledWith(
        "[DEPRECATION] Future versions will throw on void/null returns. Enable `strictReturn` now to prepare.",
      );
    });

    it("does not log deprecation warning when handleMessage returns undefined", async () => {
      const consoleWarnStub = vi.spyOn(console, "warn").mockImplementation(() => undefined);
      const undefinedHandler = vi.fn().mockResolvedValue(undefined);

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: undefinedHandler,
        sqs,
      });

      consumer.start();
      await pEvent(consumer, "response_processed");
      consumer.stop();

      expect(consoleWarnStub).not.toHaveBeenCalled();
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

      expect(sqs.send).toHaveBeenCalledTimes(2);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(DeleteMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0].input).toMatchObject({
        QueueUrl: QUEUE_URL,
        ReceiptHandle: "receipt-handle",
      });
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

      expect(sqs.send).toHaveBeenCalledTimes(1);
      expect(
        vi.mocked(sqs.send).mock.calls.some(([command]) => command instanceof DeleteMessageCommand),
      ).toBe(false);
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

      expect(sqs.send).toHaveBeenCalledTimes(1);
      expect(
        vi.mocked(sqs.send).mock.calls.some(([command]) => command instanceof DeleteMessageCommand),
      ).toBe(false);
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

      expect(sqs.send).toHaveBeenCalledTimes(2);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(DeleteMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0].input).toMatchObject({
        QueueUrl: QUEUE_URL,
        ReceiptHandle: "receipt-handle",
      });
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

      expect(sqs.send).toHaveBeenCalledTimes(1);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(
        vi
          .mocked(sqs.send)
          .mock.calls.some(([command]) => command instanceof DeleteMessageBatchCommand),
      ).toBe(false);
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

      expect(sqs.send).toHaveBeenCalledTimes(2);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(DeleteMessageBatchCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0].input).toMatchObject({
        QueueUrl: QUEUE_URL,
        Entries: [{ Id: "123", ReceiptHandle: "receipt-handle" }],
      });
    });

    it("does not ack messages if handleMessageBatch returns void", async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        // @ts-expect-error - we want to test expected behaviour
        handleMessageBatch: async () => {},
        batchSize: 2,
        sqs,
      });

      consumer.start();
      await pEvent(consumer, "response_processed");
      consumer.stop();

      expect(sqs.send).toHaveBeenCalledTimes(1);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(
        vi
          .mocked(sqs.send)
          .mock.calls.some(([command]) => command instanceof DeleteMessageBatchCommand),
      ).toBe(false);
    });

    it("ack only returned messages if handleMessagesBatch returns an array", async () => {
      deleteMessageBatchMock.mockResolvedValue({ Successful: [{ Id: "123" }] });

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: async () => [{ MessageId: "123", ReceiptHandle: "receipt-handle" }],
        batchSize: 2,
        sqs,
      });

      consumer.start();
      await pEvent(consumer, "response_processed");
      consumer.stop();

      expect(sqs.send).toHaveBeenCalledTimes(2);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(DeleteMessageBatchCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0].input).toMatchObject({
        QueueUrl: QUEUE_URL,
        Entries: [{ Id: "123", ReceiptHandle: "receipt-handle" }],
      });
    });

    it("emits message_processed only for successful DeleteMessageBatch entries", async () => {
      const messages = [
        { MessageId: "1", ReceiptHandle: "receipt-handle-1", Body: "body-1" },
        { MessageId: "2", ReceiptHandle: "receipt-handle-2", Body: "body-2" },
      ];
      receiveMessageMock.mockResolvedValue({ Messages: messages });
      deleteMessageBatchMock.mockResolvedValue({
        Successful: [{ Id: "1" }],
        Failed: [
          {
            Id: "2",
            SenderFault: false,
            Code: "InternalError",
            Message: "simulated partial delete failure",
          },
        ],
      });

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: async () => messages,
        batchSize: 2,
        sqs,
      });

      const messageProcessedListener = vi.fn();
      const errorListener = vi.fn();
      consumer.on("message_processed", messageProcessedListener);
      consumer.on("error", errorListener);

      const responseProcessed = new Promise<void>((resolve) => {
        consumer.once("response_processed", () => resolve());
      });
      consumer.start();
      await responseProcessed;
      consumer.stop();

      expect(messageProcessedListener).toHaveBeenCalledOnce();
      assert.equal(vi.mocked(messageProcessedListener).mock.calls[0][0].MessageId, "1");
      expect(errorListener).toHaveBeenCalledOnce();
      assert.equal(
        vi.mocked(errorListener).mock.calls[0][0].message,
        "Batch operation failed for entries with Ids: 2",
      );
      assert.deepEqual(vi.mocked(errorListener).mock.calls[0][1], messages);
    });

    it("does not emit message_processed when DeleteMessageBatch has no successful entries", async () => {
      const messages = [
        { MessageId: "1", ReceiptHandle: "receipt-handle-1", Body: "body-1" },
        { MessageId: "2", ReceiptHandle: "receipt-handle-2", Body: "body-2" },
      ];
      receiveMessageMock.mockResolvedValue({ Messages: messages });
      deleteMessageBatchMock.mockResolvedValue({
        Failed: [
          {
            Id: "2",
            SenderFault: false,
            Code: "InternalError",
            Message: "simulated partial delete failure",
          },
        ],
      });

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: async () => messages,
        batchSize: 2,
        sqs,
      });

      const messageProcessedListener = vi.fn();
      const errorListener = vi.fn();
      consumer.on("message_processed", messageProcessedListener);
      consumer.on("error", errorListener);

      const responseProcessed = new Promise<void>((resolve) => {
        consumer.once("response_processed", () => resolve());
      });
      consumer.start();
      await responseProcessed;
      consumer.stop();

      expect(messageProcessedListener).not.toHaveBeenCalled();
      expect(errorListener).toHaveBeenCalledOnce();
      assert.equal(
        vi.mocked(errorListener).mock.calls[0][0].message,
        "Batch operation failed for entries with Ids: 2",
      );
      assert.deepEqual(vi.mocked(errorListener).mock.calls[0][1], messages);
    });

    it("logs deprecation warning when handleMessageBatch returns null", async () => {
      const consoleWarnStub = vi.spyOn(console, "warn").mockImplementation(() => undefined);

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: async () => null,
        batchSize: 2,
        sqs,
      });

      consumer.start();
      await pEvent(consumer, "response_processed");
      consumer.stop();

      expect(consoleWarnStub).toHaveBeenCalledOnce();
      expect(consoleWarnStub).toHaveBeenCalledWith(
        "[DEPRECATION] Future versions will throw on void/null returns. Enable `strictReturn` now to prepare.",
      );
    });

    it("does not ack messages if handleMessageBatch returns undefined", async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: async () => undefined,
        batchSize: 2,
        sqs,
      });

      consumer.start();
      await pEvent(consumer, "response_processed");
      consumer.stop();

      expect(sqs.send).toHaveBeenCalledTimes(1);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(
        vi
          .mocked(sqs.send)
          .mock.calls.some(([command]) => command instanceof DeleteMessageBatchCommand),
      ).toBe(false);
    });

    it("does not log deprecation warning when handleMessageBatch returns undefined", async () => {
      const consoleLogStub = vi.spyOn(console, "warn").mockImplementation(() => undefined);

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: async () => undefined,
        batchSize: 2,
        sqs,
      });

      consumer.start();
      await pEvent(consumer, "response_processed");
      consumer.stop();

      expect(consoleLogStub).not.toHaveBeenCalled();
    });

    it("does not ack messages if handleMessageBatch returns []", async () => {
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

      expect(sqs.send).toHaveBeenCalledTimes(1);
      expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
      expect(
        vi
          .mocked(sqs.send)
          .mock.calls.some(([command]) => command instanceof DeleteMessageBatchCommand),
      ).toBe(false);
    });

    describe("strictReturn flag", () => {
      it("throws error when strictReturn is enabled and handleMessage returns null", async () => {
        consumer = new Consumer({
          queueUrl: QUEUE_URL,
          region: REGION,
          handleMessage: async () => null,
          sqs,
          strictReturn: true,
        });

        consumer.start();
        const err: any = await pEvent(consumer, "processing_error");
        consumer.stop();

        assert.ok(err);
        assert.equal(
          err.message,
          "Unexpected message handler failure: strictReturn is enabled: handleMessage must return a Message object or an object with the same MessageId. Returning null is not allowed.",
        );
      });

      it("throws error when strictReturn is enabled and handleMessageBatch returns null", async () => {
        consumer = new Consumer({
          queueUrl: QUEUE_URL,
          region: REGION,
          handleMessageBatch: async () => null,
          batchSize: 2,
          sqs,
          strictReturn: true,
        });

        consumer.start();
        const err: any = await pEvent(consumer, "error");
        consumer.stop();

        assert.ok(err);
        assert.equal(
          err.message,
          "Unexpected message batch handler failure: strictReturn is enabled: handleMessageBatch must return an array of Message objects. Returning null is not allowed.",
        );
      });

      it("works normally when strictReturn is disabled and handleMessage returns null", async () => {
        consumer = new Consumer({
          queueUrl: QUEUE_URL,
          region: REGION,
          handleMessage: async () => null,
          sqs,
          strictReturn: false,
        });

        consumer.start();
        await pEvent(consumer, "response_processed");
        consumer.stop();

        expect(sqs.send).toHaveBeenCalledTimes(1);
        expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
        expect(
          vi
            .mocked(sqs.send)
            .mock.calls.some(([command]) => command instanceof DeleteMessageCommand),
        ).toBe(false);
      });

      it("works normally when strictReturn is disabled and handleMessageBatch returns null", async () => {
        consumer = new Consumer({
          queueUrl: QUEUE_URL,
          region: REGION,
          handleMessageBatch: async () => null,
          batchSize: 2,
          sqs,
          strictReturn: false,
        });

        consumer.start();
        await pEvent(consumer, "response_processed");
        consumer.stop();

        expect(sqs.send).toHaveBeenCalledTimes(1);
        expect(vi.mocked(sqs.send).mock.calls[0]?.[0]).toBeInstanceOf(ReceiveMessageCommand);
        expect(
          vi
            .mocked(sqs.send)
            .mock.calls.some(([command]) => command instanceof DeleteMessageBatchCommand),
        ).toBe(false);
      });
    });

    it("uses the correct visibility timeout for long running handler functions", async () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: () => new Promise((resolve) => setTimeout(() => resolve(undefined), 75000)),
        sqs,
        visibilityTimeout: 40,
        heartbeatInterval: 30,
      });
      const clearIntervalSpy = vi.spyOn(globalThis, "clearInterval");

      consumer.start();
      await Promise.all([
        pEvent(consumer, "response_processed"),
        vi.advanceTimersByTimeAsync(75000),
      ]);
      consumer.stop();

      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(ChangeMessageVisibilityCommand);
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0].input).toMatchObject({
        QueueUrl: QUEUE_URL,
        ReceiptHandle: "receipt-handle",
        VisibilityTimeout: 40,
      });
      expect(vi.mocked(sqs.send).mock.calls[2]?.[0]).toBeInstanceOf(ChangeMessageVisibilityCommand);
      expect(vi.mocked(sqs.send).mock.calls[2]?.[0].input).toMatchObject({
        QueueUrl: QUEUE_URL,
        ReceiptHandle: "receipt-handle",
        VisibilityTimeout: 40,
      });
      expect(clearIntervalSpy).toHaveBeenCalledOnce();
    });

    it("passes in the correct visibility timeout for long running batch handler functions", async () => {
      receiveMessageMock.mockResolvedValue({
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
          new Promise((resolve) => setTimeout(() => resolve(undefined), 75000)),
        batchSize: 3,
        sqs,
        visibilityTimeout: 40,
        heartbeatInterval: 30,
      });
      const clearIntervalSpy = vi.spyOn(globalThis, "clearInterval");

      consumer.start();
      await Promise.all([
        pEvent(consumer, "response_processed"),
        vi.advanceTimersByTimeAsync(75000),
      ]);
      consumer.stop();

      expect(vi.mocked(sqs.send).mock.calls[1]?.[0]).toBeInstanceOf(
        ChangeMessageVisibilityBatchCommand,
      );
      expect(vi.mocked(sqs.send).mock.calls[1]?.[0].input).toMatchObject({
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
      });
      expect(vi.mocked(sqs.send).mock.calls[2]?.[0]).toBeInstanceOf(
        ChangeMessageVisibilityBatchCommand,
      );
      expect(vi.mocked(sqs.send).mock.calls[2]?.[0].input).toMatchObject({
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
      });
      expect(clearIntervalSpy).toHaveBeenCalledOnce();
    });

    it("emit error when changing visibility timeout fails", async () => {
      receiveMessageMock.mockResolvedValue({
        Messages: [{ MessageId: "1", ReceiptHandle: "receipt-handle-1", Body: "body-1" }],
      });
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: () => new Promise((resolve) => setTimeout(() => resolve(undefined), 75000)),
        sqs,
        visibilityTimeout: 40,
        heartbeatInterval: 30,
      });

      const receiveErr = new MockSQSError("failed");
      changeMessageVisibilityMock.mockRejectedValue(receiveErr);
      const errorListener = vi.fn();
      consumer.on("error", errorListener);

      consumer.start();
      await vi.advanceTimersByTimeAsync(75000);
      consumer.stop();

      const err = vi.mocked(errorListener).mock.calls[0][0];
      assert.ok(err);
      assert.equal(err.message, "Error changing visibility timeout: failed");
      assert.equal(err.queueUrl, QUEUE_URL);
      assert.deepEqual(err.messageIds, ["1"]);
    });

    it("emit error when changing visibility timeout fails for batch handler functions", async () => {
      receiveMessageMock.mockResolvedValue({
        Messages: [
          { MessageId: "1", ReceiptHandle: "receipt-handle-1", Body: "body-1" },
          { MessageId: "2", ReceiptHandle: "receipt-handle-2", Body: "body-2" },
        ],
      });
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: () =>
          new Promise((resolve) => setTimeout(() => resolve(undefined), 75000)),
        sqs,
        batchSize: 2,
        visibilityTimeout: 40,
        heartbeatInterval: 30,
      });

      const receiveErr = new MockSQSError("failed");
      changeMessageVisibilityBatchMock.mockRejectedValue(receiveErr);
      const errorListener = vi.fn();
      consumer.on("error", errorListener);

      consumer.start();
      await vi.advanceTimersByTimeAsync(75000);
      consumer.stop();

      const err = vi.mocked(errorListener).mock.calls[0][0];
      assert.ok(err);
      assert.equal(err.message, "Error changing visibility timeout: failed");
      assert.equal(err.queueUrl, QUEUE_URL);
      assert.deepEqual(err.messageIds, ["1", "2"]);
    });

    it("emits error when ChangeMessageVisibilityBatch returns failed entries", async () => {
      receiveMessageMock.mockResolvedValue({
        Messages: [
          { MessageId: "1", ReceiptHandle: "receipt-handle-1", Body: "body-1" },
          { MessageId: "2", ReceiptHandle: "receipt-handle-2", Body: "body-2" },
        ],
      });
      changeMessageVisibilityBatchMock.mockResolvedValue({
        Successful: [{ Id: "1" }],
        Failed: [
          {
            Id: "2",
            SenderFault: false,
            Code: "InternalError",
            Message: "simulated partial visibility failure",
          },
        ],
      });

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: () =>
          new Promise((resolve) => setTimeout(() => resolve(undefined), 75000)),
        sqs,
        batchSize: 2,
        visibilityTimeout: 40,
        heartbeatInterval: 30,
      });

      const errorListener = vi.fn();
      consumer.on("error", errorListener);

      consumer.start();
      await vi.advanceTimersByTimeAsync(30000);
      consumer.stop();

      expect(errorListener).toHaveBeenCalledOnce();
      const err = vi.mocked(errorListener).mock.calls[0][0];
      assert.equal(err.message, "Batch operation failed for entries with Ids: 2");
      assert.deepEqual(vi.mocked(errorListener).mock.calls[0][1], [
        { MessageId: "1", ReceiptHandle: "receipt-handle-1", Body: "body-1" },
        { MessageId: "2", ReceiptHandle: "receipt-handle-2", Body: "body-2" },
      ]);
    });

    it("includes messageIds in timeout errors", async () => {
      const handleMessageTimeout = 500;
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: () => new Promise((resolve) => setTimeout(() => resolve(undefined), 1000)),
        handleMessageTimeout,
        sqs,
        authenticationErrorTimeout: AUTHENTICATION_ERROR_TIMEOUT,
      });

      consumer.start();
      const [err]: any = await Promise.all([
        pEvent(consumer, "timeout_error"),
        vi.advanceTimersByTimeAsync(handleMessageTimeout),
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
      receiveMessageMock.mockResolvedValue({
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
        vi.advanceTimersByTimeAsync(100),
      ]);
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, "Unexpected message batch handler failure: Batch processing error");
      assert.deepEqual(err.messageIds, ["1", "2"]);
    });

    it("includes queueUrl and messageIds in SQS errors when deleting message", async () => {
      const deleteErr = new Error("Delete error");
      deleteErr.name = "SQSError";

      handleMessage.mockResolvedValue(response.Messages[0]);
      deleteMessageMock.mockRejectedValue(deleteErr);

      consumer.start();
      const [err]: any = await Promise.all([
        pEvent(consumer, "error"),
        vi.advanceTimersByTimeAsync(100),
      ]);
      consumer.stop();

      assert.ok(err);
      assert.equal(err.message, "SQS delete message failed: Delete error");
      assert.equal(err.queueUrl, QUEUE_URL);
      assert.deepEqual(err.messageIds, ["123"]);
    });

    it("includes queueUrl and messageIds in SQS errors when changing visibility timeout", async () => {
      receiveMessageMock.mockResolvedValue({
        Messages: [{ MessageId: "1", ReceiptHandle: "receipt-handle-1", Body: "body-1" }],
      });
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: () => new Promise((resolve) => setTimeout(() => resolve(undefined), 75000)),
        sqs,
        visibilityTimeout: 40,
        heartbeatInterval: 30,
      });

      const receiveErr = new MockSQSError("failed");
      changeMessageVisibilityMock.mockRejectedValue(receiveErr);
      const errorListener = vi.fn();
      consumer.on("error", errorListener);

      consumer.start();
      await vi.advanceTimersByTimeAsync(75000);
      consumer.stop();

      const err = vi.mocked(errorListener).mock.calls[0][0];
      assert.ok(err);
      assert.equal(err.message, "Error changing visibility timeout: failed");
      assert.equal(err.queueUrl, QUEUE_URL);
      assert.deepEqual(err.messageIds, ["1"]);
    });

    it("includes queueUrl and messageIds in batch SQS errors", async () => {
      receiveMessageMock.mockResolvedValue({
        Messages: [
          { MessageId: "1", ReceiptHandle: "receipt-handle-1", Body: "body-1" },
          { MessageId: "2", ReceiptHandle: "receipt-handle-2", Body: "body-2" },
        ],
      });

      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessageBatch: () =>
          new Promise((resolve) => setTimeout(() => resolve(undefined), 75000)),
        sqs,
        batchSize: 2,
        visibilityTimeout: 40,
        heartbeatInterval: 30,
      });

      const receiveErr = new MockSQSError("failed");
      changeMessageVisibilityBatchMock.mockRejectedValue(receiveErr);
      const errorListener = vi.fn();
      consumer.on("error", errorListener);

      consumer.start();
      await vi.advanceTimersByTimeAsync(75000);
      consumer.stop();

      const err = vi.mocked(errorListener).mock.calls[0][0];
      assert.ok(err);
      assert.equal(err.message, "Error changing visibility timeout: failed");
      assert.equal(err.queueUrl, QUEUE_URL);
      assert.deepEqual(err.messageIds, ["1", "2"]);
    });

    it("includes undefined in error event when receiveMessage fails", async () => {
      const receiveErr = new Error("Receive error");
      receiveMessageMock.mockRejectedValue(receiveErr);

      const errorListener = vi.fn();
      consumer.on("error", errorListener);

      consumer.start();
      await pEvent(consumer, "error");
      consumer.stop();

      expect(errorListener).toHaveBeenCalledOnce();
      expect(errorListener).toHaveBeenCalledWith(expect.any(Error), undefined, {
        queueUrl: QUEUE_URL,
      });
    });

    it("includes undefined in error event when poll method catches an error", async () => {
      const pollError = new Error("Poll error");

      receiveMessageMock.mockResolvedValue({});

      const originalPrototype = Object.getPrototypeOf(consumer);
      const originalHandleSqsResponse = originalPrototype.handleSqsResponse;

      Object.defineProperty(originalPrototype, "handleSqsResponse", {
        value: vi.fn().mockImplementation(() => {
          throw pollError;
        }),
      });

      const errorListener = vi.fn();
      consumer.on("error", errorListener);

      consumer.start();
      await pEvent(consumer, "error");
      consumer.stop();

      Object.defineProperty(originalPrototype, "handleSqsResponse", {
        value: originalHandleSqsResponse,
      });

      expect(errorListener).toHaveBeenCalledOnce();
      expect(errorListener).toHaveBeenCalledWith(expect.any(Error), undefined, {
        queueUrl: QUEUE_URL,
      });
    });
  });

  describe("FIFO Queue Warning", () => {
    let warnStub;

    beforeEach(() => {
      warnStub = vi.spyOn(logger, "warn").mockImplementation(() => undefined);
    });

    it("emits a warning when starting with a FIFO queue URL", () => {
      consumer = new Consumer({
        queueUrl: "https://sqs.us-east-1.amazonaws.com/123456789012/queue.fifo",
        region: REGION,
        handleMessage,
        sqs,
      });

      consumer.start();
      consumer.stop();

      expect(warnStub).toHaveBeenCalledOnce();
      expect(warnStub).toHaveBeenCalledWith(
        "WARNING: A FIFO queue was detected. SQS Consumer does not guarantee FIFO queues will work as expected. Set 'suppressFifoWarning: true' to disable this warning.",
      );
    });

    it("does not emit warning for standard queue URLs", () => {
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage,
        sqs,
      });

      consumer.start();
      consumer.stop();

      expect(warnStub).not.toHaveBeenCalled();
    });

    it("suppresses warning when suppressFifoWarning option is true", () => {
      consumer = new Consumer({
        queueUrl: "https://sqs.us-east-1.amazonaws.com/123456789012/queue.fifo",
        region: REGION,
        handleMessage,
        sqs,
        suppressFifoWarning: true,
      });

      consumer.start();
      consumer.stop();

      expect(warnStub).not.toHaveBeenCalled();
    });

    it("emits warning on multiple start calls with FIFO queue", () => {
      consumer = new Consumer({
        queueUrl: "https://sqs.us-east-1.amazonaws.com/123456789012/queue.fifo",
        region: REGION,
        handleMessage,
        sqs,
      });

      consumer.start();
      consumer.stop();
      consumer.start();
      consumer.stop();

      expect(warnStub).toHaveBeenCalledTimes(2);
    });
  });

  describe("event listeners", () => {
    it("fires the event multiple times", async () => {
      receiveMessageMock.mockResolvedValue({});

      const handleEmpty = vi.fn().mockReturnValue(null);

      consumer.on("empty", handleEmpty);

      consumer.start();

      await vi.advanceTimersByTimeAsync(0);

      consumer.stop();

      await vi.runAllTimersAsync();

      expect(handleEmpty).toHaveBeenCalledTimes(2);
    });

    it("fires the events only once", async () => {
      receiveMessageMock.mockResolvedValue({});

      const handleEmpty = vi.fn().mockReturnValue(null);

      consumer.once("empty", handleEmpty);

      consumer.start();

      await vi.advanceTimersByTimeAsync(0);

      consumer.stop();

      await vi.runAllTimersAsync();

      expect(handleEmpty).toHaveBeenCalledOnce();
    });
  });

  describe(".stop", () => {
    it("stops the consumer polling for messages", async () => {
      const handleStop = vi.fn().mockReturnValue(null);

      consumer.on("stopped", handleStop);

      consumer.start();
      consumer.stop();

      await vi.runAllTimersAsync();

      expect(handleStop).toHaveBeenCalledOnce();
      expect(handleMessage).toHaveBeenCalledOnce();
    });

    it("clears the polling timeout when stopped", async () => {
      const clearTimeoutSpy = vi.spyOn(globalThis, "clearTimeout");

      consumer.start();
      await vi.advanceTimersByTimeAsync(0);
      consumer.stop();

      await vi.runAllTimersAsync();

      expect(clearTimeoutSpy).toHaveBeenCalledTimes(2);
    });

    it("fires a stopped event only once when stopped multiple times", async () => {
      const handleStop = vi.fn().mockReturnValue(null);

      consumer.on("stopped", handleStop);

      consumer.start();
      consumer.stop();
      consumer.stop();
      consumer.stop();
      await vi.runAllTimersAsync();

      expect(handleStop).toHaveBeenCalledOnce();
    });

    it("fires a stopped event a second time if started and stopped twice", async () => {
      const handleStop = vi.fn().mockReturnValue(null);

      consumer.on("stopped", handleStop);

      consumer.start();
      consumer.stop();
      consumer.start();
      consumer.stop();
      await vi.runAllTimersAsync();

      expect(handleStop).toHaveBeenCalledTimes(2);
    });

    it("aborts requests when the abort param is true", async () => {
      const handleStop = vi.fn().mockReturnValue(null);
      const handleAbort = vi.fn().mockReturnValue(null);

      consumer.on("stopped", handleStop);
      consumer.on("aborted", handleAbort);

      consumer.start();
      consumer.stop({ abort: true });

      await vi.runAllTimersAsync();

      expect(consumer.abortController?.signal.aborted).toBe(true);
      expect(handleMessage).toHaveBeenCalledOnce();
      expect(handleAbort).toHaveBeenCalledOnce();
      expect(handleStop).toHaveBeenCalledOnce();
    });

    it("waits for in-flight messages before emitting stopped (within timeout)", async () => {
      receiveMessageMock.mockResolvedValue({
        Messages: [{ MessageId: "1", ReceiptHandle: "receipt-handle-1", Body: "body-1" }],
      });
      const handleStop = vi.fn().mockReturnValue(null);
      const handleResponseProcessed = vi.fn().mockReturnValue(null);
      const waitingForPollingComplete = vi.fn().mockReturnValue(null);
      const waitingForPollingCompleteTimeoutExceeded = vi.fn().mockReturnValue(null);

      // A slow message handler
      handleMessage = vi
        .fn()
        .mockResolvedValue(new Promise((resolve) => setTimeout(resolve, 5000)));

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
      await vi.advanceTimersByTimeAsync(1);
      consumer.stop();

      await vi.runAllTimersAsync();

      expect(handleStop).toHaveBeenCalledOnce();
      expect(handleResponseProcessed).toHaveBeenCalledOnce();
      expect(handleMessage).toHaveBeenCalledOnce();
      expect(waitingForPollingComplete).toHaveBeenCalledTimes(5);
      expect(waitingForPollingCompleteTimeoutExceeded).not.toHaveBeenCalled();

      expect(handleMessage.mock.invocationCallOrder[0]).toBeLessThan(
        handleStop.mock.invocationCallOrder[0],
      );

      // handleResponseProcessed is called after handleMessage, indicating
      // messages were allowed to complete before 'stopped' was emitted
      expect(handleResponseProcessed.mock.invocationCallOrder[0]).toBeLessThan(
        handleStop.mock.invocationCallOrder[0],
      );
    });

    it("waits for in-flight messages before emitting stopped (timeout reached)", async () => {
      receiveMessageMock.mockResolvedValue({
        Messages: [{ MessageId: "1", ReceiptHandle: "receipt-handle-1", Body: "body-1" }],
      });
      const handleStop = vi.fn().mockReturnValue(null);
      const handleResponseProcessed = vi.fn().mockReturnValue(null);
      const waitingForPollingComplete = vi.fn().mockReturnValue(null);
      const waitingForPollingCompleteTimeoutExceeded = vi.fn().mockReturnValue(null);

      // A slow message handler
      handleMessage = vi
        .fn()
        .mockResolvedValue(new Promise((resolve) => setTimeout(resolve, 5000)));

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
      await vi.advanceTimersByTimeAsync(1);
      consumer.stop();

      await vi.runAllTimersAsync();

      expect(handleStop).toHaveBeenCalledOnce();
      expect(handleResponseProcessed).toHaveBeenCalledOnce();
      expect(handleMessage).toHaveBeenCalledOnce();
      expect(waitingForPollingComplete).toHaveBeenCalledOnce();
      expect(waitingForPollingCompleteTimeoutExceeded).toHaveBeenCalledOnce();
      expect(handleMessage.mock.invocationCallOrder[0]).toBeLessThan(
        handleStop.mock.invocationCallOrder[0],
      );

      // Stop was called before the message could be processed, because we reached timeout.
      expect(handleStop.mock.invocationCallOrder[0]).toBeLessThan(
        handleResponseProcessed.mock.invocationCallOrder[0],
      );
    });
  });

  describe("status", () => {
    it("returns the defaults before the consumer is started", () => {
      expect(consumer.status.isRunning).toBe(false);
      expect(consumer.status.isPolling).toBe(false);
    });

    it("returns true for `isRunning` if the consumer has not been stopped", () => {
      consumer.start();
      expect(consumer.status.isRunning).toBe(true);
      consumer.stop();
    });

    it("returns false for `isRunning` if the consumer has been stopped", () => {
      consumer.start();
      consumer.stop();
      expect(consumer.status.isRunning).toBe(false);
    });

    it("returns true for `isPolling` if the consumer is polling for messages", async () => {
      receiveMessageMock.mockResolvedValue({
        Messages: [{ MessageId: "1", ReceiptHandle: "receipt-handle-1", Body: "body-1" }],
      });
      consumer = new Consumer({
        queueUrl: QUEUE_URL,
        region: REGION,
        handleMessage: () => new Promise((resolve) => setTimeout(() => resolve(undefined), 20)),
        sqs,
      });

      consumer.start();
      await vi.advanceTimersByTimeAsync(1);
      expect(consumer.status.isPolling).toBe(true);
      consumer.stop();
      expect(consumer.status.isPolling).toBe(true);
      await vi.advanceTimersByTimeAsync(21);
      expect(consumer.status.isPolling).toBe(false);
    });
  });

  describe("updateOption", () => {
    it("updates the visibilityTimeout option and emits an event", () => {
      const optionUpdatedListener = vi.fn();
      consumer.on("option_updated", optionUpdatedListener);

      consumer.updateOption("visibilityTimeout", 45);

      assert.equal(consumer.visibilityTimeout, 45);

      expect(optionUpdatedListener).toHaveBeenCalledWith("visibilityTimeout", 45, {
        queueUrl: QUEUE_URL,
      });
    });

    it("does not update the visibilityTimeout if the value is less than the heartbeatInterval", () => {
      consumer = new Consumer({
        region: REGION,
        queueUrl: QUEUE_URL,
        handleMessage,
        heartbeatInterval: 30,
        visibilityTimeout: 60,
      });

      const optionUpdatedListener = vi.fn();
      consumer.on("option_updated", optionUpdatedListener);

      expect(() => {
        consumer.updateOption("visibilityTimeout", 30);
      }).toThrow("heartbeatInterval must be less than visibilityTimeout.");

      assert.equal(consumer.visibilityTimeout, 60);

      expect(optionUpdatedListener).not.toHaveBeenCalled();
    });

    it("updates the batchSize option and emits an event", () => {
      const optionUpdatedListener = vi.fn();
      consumer.on("option_updated", optionUpdatedListener);

      consumer.updateOption("batchSize", 4);

      assert.equal(consumer.batchSize, 4);

      expect(optionUpdatedListener).toHaveBeenCalledWith("batchSize", 4, {
        queueUrl: QUEUE_URL,
      });
    });

    it("does not update the batchSize if the value is more than 10", () => {
      const optionUpdatedListener = vi.fn();
      consumer.on("option_updated", optionUpdatedListener);

      expect(() => {
        consumer.updateOption("batchSize", 13);
      }).toThrow("batchSize must be between 1 and 10.");

      assert.equal(consumer.batchSize, 1);

      expect(optionUpdatedListener).not.toHaveBeenCalled();
    });

    it("does not update the batchSize if the value is less than 1", () => {
      const optionUpdatedListener = vi.fn();
      consumer.on("option_updated", optionUpdatedListener);

      expect(() => {
        consumer.updateOption("batchSize", 0);
      }).toThrow("batchSize must be between 1 and 10.");

      assert.equal(consumer.batchSize, 1);

      expect(optionUpdatedListener).not.toHaveBeenCalled();
    });

    it("updates the waitTimeSeconds option and emits an event", () => {
      const optionUpdatedListener = vi.fn();
      consumer.on("option_updated", optionUpdatedListener);

      consumer.updateOption("waitTimeSeconds", 18);

      assert.equal(consumer.waitTimeSeconds, 18);

      expect(optionUpdatedListener).toHaveBeenCalledWith("waitTimeSeconds", 18, {
        queueUrl: QUEUE_URL,
      });
    });

    it("does not update the batchSize if the value is less than 0", () => {
      const optionUpdatedListener = vi.fn();
      consumer.on("option_updated", optionUpdatedListener);

      expect(() => {
        consumer.updateOption("waitTimeSeconds", -1);
      }).toThrow("waitTimeSeconds must be between 0 and 20.");

      assert.equal(consumer.waitTimeSeconds, 20);

      expect(optionUpdatedListener).not.toHaveBeenCalled();
    });

    it("does not update the batchSize if the value is more than 20", () => {
      const optionUpdatedListener = vi.fn();
      consumer.on("option_updated", optionUpdatedListener);

      expect(() => {
        consumer.updateOption("waitTimeSeconds", 27);
      }).toThrow("waitTimeSeconds must be between 0 and 20.");

      assert.equal(consumer.waitTimeSeconds, 20);

      expect(optionUpdatedListener).not.toHaveBeenCalled();
    });

    it("updates the pollingWaitTimeMs option and emits an event", () => {
      const optionUpdatedListener = vi.fn();
      consumer.on("option_updated", optionUpdatedListener);

      consumer.updateOption("pollingWaitTimeMs", 1000);

      assert.equal(consumer.pollingWaitTimeMs, 1000);

      expect(optionUpdatedListener).toHaveBeenCalledWith("pollingWaitTimeMs", 1000, {
        queueUrl: QUEUE_URL,
      });
    });

    it("does not update the pollingWaitTimeMs if the value is less than 0", () => {
      const optionUpdatedListener = vi.fn();
      consumer.on("option_updated", optionUpdatedListener);

      expect(() => {
        consumer.updateOption("pollingWaitTimeMs", -1);
      }).toThrow("pollingWaitTimeMs must be greater than 0.");

      assert.equal(consumer.pollingWaitTimeMs, 0);

      expect(optionUpdatedListener).not.toHaveBeenCalled();
    });

    it("throws an error for an unknown option", () => {
      consumer = new Consumer({
        region: REGION,
        queueUrl: QUEUE_URL,
        handleMessage,
        visibilityTimeout: 60,
      });

      expect(() => {
        consumer.updateOption("unknown", "value");
      }).toThrow(`The update unknown cannot be updated`);
    });
  });

  describe("events", () => {
    it("logs a debug event when an event is emitted", async () => {
      const loggerDebug = vi.spyOn(logger, "debug").mockImplementation(() => undefined);

      consumer.start();
      consumer.stop();

      expect(loggerDebug).toHaveBeenCalledTimes(5);
      // Logged directly
      expect(loggerDebug).toHaveBeenCalledWith("starting");
      // Sent from the emitter
      expect(loggerDebug).toHaveBeenCalledWith("started", {
        queueUrl: QUEUE_URL,
      });
      // Logged directly
      expect(loggerDebug).toHaveBeenCalledWith("polling");
      // Logged directly
      expect(loggerDebug).toHaveBeenCalledWith("stopping");
      // Sent from the emitter
      expect(loggerDebug).toHaveBeenCalledWith("stopped", {
        queueUrl: QUEUE_URL,
      });
    });

    it("includes queueUrl in emitted events", async () => {
      const startedListener = vi.fn();
      const messageReceivedListener = vi.fn();
      const messageProcessedListener = vi.fn();
      const emptyListener = vi.fn();
      const stoppedListener = vi.fn();
      const errorListener = vi.fn();
      const processingErrorListener = vi.fn();

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

      handleMessage.mockRejectedValue(new Error("Processing error"));
      consumer.start();
      await pEvent(consumer, "processing_error");
      consumer.stop();

      expect(startedListener).toHaveBeenCalledWith({ queueUrl: QUEUE_URL });
      expect(messageReceivedListener).toHaveBeenCalledWith(response.Messages[0], {
        queueUrl: QUEUE_URL,
      });
      expect(messageProcessedListener).toHaveBeenCalledWith(response.Messages[0], {
        queueUrl: QUEUE_URL,
      });
      expect(stoppedListener).toHaveBeenCalledWith({ queueUrl: QUEUE_URL });
      expect(processingErrorListener).toHaveBeenCalledWith(
        expect.any(Error),
        response.Messages[0],
        { queueUrl: QUEUE_URL },
      );
    });
  });
});
