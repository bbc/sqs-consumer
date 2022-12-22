import { AWSError } from './types';

class SQSError extends Error {
  code: string;
  statusCode: number;
  service: string;
  time: Date;
  retryable: boolean;
  fault: 'client' | 'server';

  constructor(message: string) {
    super(message);
    this.name = this.constructor.name;
  }
}

class TimeoutError extends Error {
  constructor(message = 'Operation timed out.') {
    super(message);
    this.message = message;
    this.name = 'TimeoutError';
  }
}

/**
 * Checks if the error provided should be treated as a connection error.
 * @param err The error that was received.
 */
function isConnectionError(err: Error): boolean {
  if (err instanceof SQSError) {
    return (
      err.statusCode === 403 ||
      err.code === 'CredentialsError' ||
      err.code === 'UnknownEndpoint' ||
      err.code === 'AWS.SimpleQueueService.NonExistentQueue'
    );
  }
  return false;
}

/**
 * Formats an AWSError the the SQSError type.
 * @param err The error object that was received.
 * @param message The message that the error occurred on.
 */
function toSQSError(err: AWSError, message: string): SQSError {
  const sqsError = new SQSError(message);
  sqsError.code = err.name;
  sqsError.statusCode = err.$metadata?.httpStatusCode;
  sqsError.retryable = err.$retryable?.throttling;
  sqsError.service = err.$service;
  sqsError.fault = err.$fault;
  sqsError.time = new Date();

  return sqsError;
}

export { SQSError, TimeoutError, isConnectionError, toSQSError };
