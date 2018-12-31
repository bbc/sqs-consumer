'use strict';

class SQSError extends Error {
  constructor() {
    super(Array.from(arguments));
    this.name = this.constructor.name;
  }
}

module.exports = SQSError;
