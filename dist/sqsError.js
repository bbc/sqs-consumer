"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
class SQSError extends Error {
    constructor(message) {
        super(message);
        this.name = this.constructor.name;
    }
}
exports.SQSError = SQSError;
