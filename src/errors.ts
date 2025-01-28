import { Message } from "@aws-sdk/client-sqs";

import { AWSError } from "./types.js";

class SQSError extends Error {
  code: string;
  statusCode: number;
  service: string;
  time: Date;
  retryable: boolean;
  fault: AWSError["$fault"];
  response?: AWSError["$response"];
  metadata?: AWSError["$metadata"];

  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
  }
}

class TimeoutError extends Error {
  messageIds: string[];
  cause: Error;
  time: Date;

  constructor(message = "Operation timed out.") {
    super(message);
    this.message = message;
    this.name = "TimeoutError";
    this.messageIds = [];
  }
}

class StandardError extends Error {
  messageIds: string[];
  cause: Error;
  time: Date;

  constructor(message = "An unexpected error occurred:") {
    super(message);
    this.message = message;
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
    return err.statusCode === 403 || CONNECTION_ERRORS.includes(err.code);
  }
  return false;
}

/**
 * Formats an AWSError the the SQSError type.
 * @param err The error object that was received.
 * @param message The message to send with the error.
 */
function toSQSError(
  err: AWSError,
  message: string,
  extendedAWSErrors: boolean,
): SQSError {
  const sqsError = new SQSError(message);
  sqsError.code = err.name;
  sqsError.statusCode = err.$metadata?.httpStatusCode;
  sqsError.retryable = err.$retryable?.throttling;
  sqsError.service = err.$service;
  sqsError.fault = err.$fault;
  sqsError.time = new Date();

  if (extendedAWSErrors) {
    sqsError.response = err.$response;
    sqsError.metadata = err.$metadata;
  }

  return sqsError;
}

/**
 * Gets the message IDs from the message.
 * @param message The message that was received from SQS.
 */
function getMessageIds(message: Message | Message[]): string[] {
  if (Array.isArray(message)) {
    return message.map((m) => m.MessageId);
  }
  return [message.MessageId];
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
  TimeoutError,
  isConnectionError,
  toSQSError,
  toStandardError,
  toTimeoutError,
};
