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
  cause: Error;
  time: Date;

  constructor(message = "Operation timed out.") {
    super(message);
    this.message = message;
    this.name = "TimeoutError";
  }
}

class StandardError extends Error {
  cause: Error;
  time: Date;

  constructor(message = "An unexpected error occurred:") {
    super(message);
    this.message = message;
    this.name = "StandardError";
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
 * Formats an Error to the StandardError type.
 * @param err The error object that was received.
 * @param message The message to send with the error.
 */
function toStandardError(err: Error, message: string): StandardError {
  const error = new StandardError(message);
  error.cause = err;
  error.time = new Date();

  return error;
}

/**
 * Formats an Error to the TimeoutError type.
 * @param err The error object that was received.
 * @param message The message to send with the error.
 */
function toTimeoutError(err: TimeoutError, message: string): TimeoutError {
  const error = new TimeoutError(message);
  error.cause = err;
  error.time = new Date();

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
