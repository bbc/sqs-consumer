"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.TimeoutError = exports.SQSError = void 0;
class SQSError extends Error {
    constructor(message) {
        super(message);
        this.name = this.constructor.name;
    }
}
exports.SQSError = SQSError;
class TimeoutError extends Error {
    constructor(message = 'Operation timed out.') {
        super(message);
        this.message = message;
        this.name = 'TimeoutError';
    }
}
exports.TimeoutError = TimeoutError;
