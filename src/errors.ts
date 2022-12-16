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

export { SQSError, TimeoutError };
