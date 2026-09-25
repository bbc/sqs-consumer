import type { Message } from "@aws-sdk/client-sqs";

import type { AWSError } from "./types.js";

const DEFAULT_TIMEOUT_ERROR_MESSAGE = "Operation timed out.";
const DEFAULT_STANDARD_ERROR_MESSAGE = "An unexpected error occurred:";

class SQSError extends Error {
  code?: string;
  cause?: unknown;
  statusCode?: number;
  service?: string;
  time?: Date;
  retryable?: boolean;
  fault?: AWSError["$fault"];
  response?: AWSError["$response"];
  metadata?: AWSError["$metadata"];
  queueUrl?: string;
  messageIds?: string[];

  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
  }
}

class TimeoutError extends Error {
  messageIds: string[];
  cause?: Error;
  time?: Date;

  constructor(message?: string) {
    const errorMessage = message === undefined ? DEFAULT_TIMEOUT_ERROR_MESSAGE : message;

    super(errorMessage);
    this.message = errorMessage;
    this.name = "TimeoutError";
    this.messageIds = [];
  }
}

class StandardError extends Error {
  messageIds: string[];
  cause?: Error;
  time?: Date;

  constructor(message?: string) {
    const errorMessage = message === undefined ? DEFAULT_STANDARD_ERROR_MESSAGE : message;

    super(errorMessage);
    this.message = errorMessage;
    this.name = "StandardError";
    this.messageIds = [];
  }
}

/**
 * List of SQS error codes that are considered connection errors.
 */
const CONNECTION_ERRORS = [
  "CredentialsError",
  "UnknownEndpoint",
  "AWS.SimpleQueueService.NonExistentQueue",
  "CredentialsProviderError",
  "InvalidAddress",
  "InvalidSecurity",
  "QueueDoesNotExist",
  "RequestThrottled",
  "OverLimit",
];

/**
 * Checks if the error provided should be treated as a connection error.
 * @param err The error that was received.
 */
function isConnectionError(err: Error): boolean {
  if (err instanceof SQSError) {
    return (
      err.statusCode === 403 || (err.code !== undefined && CONNECTION_ERRORS.includes(err.code))
    );
  }
  return false;
}

/**
 * Gets the message IDs from the message.
 * @param message The message that was received from SQS.
 */
function getMessageIds(message: Message | Message[]): string[] {
  if (Array.isArray(message)) {
    return message.flatMap((item) => (item.MessageId === undefined ? [] : [item.MessageId]));
  }
  return message.MessageId === undefined ? [] : [message.MessageId];
}

function toError(err: unknown): Error {
  if (err instanceof Error) {
    return err;
  }

  if (typeof err === "object" && err !== null && "message" in err) {
    const error = new Error(String(err.message));
    if ("name" in err && typeof err.name === "string") {
      error.name = err.name;
    }
    return error;
  }

  return new Error(String(err));
}

function isAWSError(err: unknown): err is AWSError {
  return typeof err === "object" && err !== null && "$metadata" in err;
}

/**
 * Formats an AWSError the the SQSError type.
 * @param err The error object that was received.
 * @param message The message to send with the error.
 */
function toSQSError(
  err: unknown,
  message: string,
  extendedAWSErrors: boolean,
  queueUrl?: string,
  sqsMessage?: Message | Message[],
): SQSError {
  const cause = toError(err);
  const sqsError = new SQSError(message);
  sqsError.cause = err;
  if (err instanceof Error || (typeof err === "object" && err !== null && "name" in err)) {
    sqsError.code = cause.name;
  }
  sqsError.time = new Date();

  if (isAWSError(err)) {
    sqsError.statusCode = err.$metadata?.httpStatusCode;
    sqsError.retryable = err.$retryable?.throttling;
    sqsError.service = err.$service;
    sqsError.fault = err.$fault;

    if (extendedAWSErrors) {
      sqsError.response = err.$response;
      sqsError.metadata = err.$metadata;
    }
  }

  if (queueUrl) {
    sqsError.queueUrl = queueUrl;
  }

  if (sqsMessage) {
    sqsError.messageIds = getMessageIds(sqsMessage);
  }

  return sqsError;
}

/**
 * Formats an Error to the StandardError type.
 * @param err The error object that was received.
 * @param message The message to send with the error.
 * @param sqsMessage The message that was received from SQS.
 */
function toStandardError(
  err: Error,
  message: string,
  sqsMessage: Message | Message[],
): StandardError {
  const error = new StandardError(message);
  error.cause = err;
  error.time = new Date();
  error.messageIds = getMessageIds(sqsMessage);

  return error;
}

/**
 * Formats an Error to the TimeoutError type.
 * @param err The error object that was received.
 * @param message The message to send with the error.
 * @param sqsMessage The message that was received from SQS.
 */
function toTimeoutError(
  err: TimeoutError,
  message: string,
  sqsMessage: Message | Message[],
): TimeoutError {
  const error = new TimeoutError(message);
  error.cause = err;
  error.time = new Date();
  error.messageIds = getMessageIds(sqsMessage);

  return error;
}

export {
  SQSError,
  StandardError,
  TimeoutError,
  isConnectionError,
  toSQSError,
  toStandardError,
  toTimeoutError,
  toError,
};
