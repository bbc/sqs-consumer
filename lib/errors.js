'use strict';

class SQSError extends Error {
  constructor() {
    super(Array.from(arguments));
    this.name = this.constructor.name;
  }
}

class TimeoutError extends Error {
  constructor(message) {
    super(message);
    this.message = message || 'Operation timed out.';
    this.name = 'TimeoutError';
  }
}

module.exports = {
  SQSError,
  TimeoutError
};
