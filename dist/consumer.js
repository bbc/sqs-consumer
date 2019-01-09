'use strict';
Object.defineProperty(exports, "__esModule", { value: true });
const debug = require('debug')('sqs-consumer');
const aws_sdk_1 = require("aws-sdk");
const events_1 = require("events");
const bind_1 = require("./bind");
const sqsError_1 = require("./sqsError");
const requiredOptions = [
    'queueUrl',
    'handleMessage'
];
function validate(options) {
    requiredOptions.forEach((option) => {
        if (!options[option]) {
            throw new Error(`Missing SQS consumer option [' + ${option} + ]. `);
        }
    });
    if (options.batchSize > 10 || options.batchSize < 1) {
        throw new Error('SQS batchSize option must be between 1 and 10.');
    }
}
function isAuthenticationError(err) {
    if (err instanceof sqsError_1.SQSError) {
        const e = err;
        return (e.statusCode === 403 || e.code === 'CredentialsError');
    }
    return false;
}
function toSQSError(err, message) {
    if (err instanceof sqsError_1.SQSError) {
        const to = new sqsError_1.SQSError(message);
        const from = err;
        to.code = from.code;
        to.statusCode = from.statusCode;
        to.region = from.region;
        to.retryable = from.retryable;
        to.hostname = from.hostname;
        to.time = from.time;
        return to;
    }
    return new sqsError_1.SQSError(message);
}
function hasMessages(response) {
    return response.Messages && response.Messages.length > 0;
}
class Consumer extends events_1.EventEmitter {
    constructor(options) {
        super();
        validate(options);
        this.queueUrl = options.queueUrl;
        this.handleMessage = options.handleMessage;
        this.attributeNames = options.attributeNames || [];
        this.messageAttributeNames = options.messageAttributeNames || [];
        this.stopped = true;
        this.batchSize = options.batchSize || 1;
        this.visibilityTimeout = options.visibilityTimeout;
        this.terminateVisibilityTimeout = options.terminateVisibilityTimeout || false;
        this.waitTimeSeconds = options.waitTimeSeconds || 20;
        this.authenticationErrorTimeout = options.authenticationErrorTimeout || 10000;
        this.sqs = options.sqs || new aws_sdk_1.SQS({
            region: options.region || process.env.AWS_REGION || 'eu-west-1'
        });
        bind_1.auto(this);
    }
    static create(options) {
        return new Consumer(options);
    }
    start() {
        if (this.stopped) {
            debug('Starting consumer');
            this.stopped = false;
            this.poll();
        }
    }
    stop() {
        debug('Stopping consumer');
        this.stopped = true;
    }
    async receiveMessage(params) {
        try {
            return await this.sqs.receiveMessage(params).promise();
        }
        catch (err) {
            throw toSQSError(err, `SQS receive message failed: ${err.message}`);
        }
    }
    async deleteMessage(message) {
        debug('Deleting message %s', message.MessageId);
        const deleteParams = {
            QueueUrl: this.queueUrl,
            ReceiptHandle: message.ReceiptHandle
        };
        try {
            await this.sqs.deleteMessage(deleteParams).promise();
        }
        catch (err) {
            throw toSQSError(err, `SQS delete message failed: ${err.message}`);
        }
    }
    async handleSQSResponse(response) {
        debug('Received SQS response');
        debug(response);
        if (response) {
            if (hasMessages(response)) {
                await Promise.all(response.Messages.map(this.processMessage));
                this.emit('response_processed');
            }
            else {
                this.emit('empty');
            }
        }
        this.poll();
    }
    async processMessage(message) {
        this.emit('message_received', message);
        try {
            await this.executeHandler(message);
            await this.deleteMessage(message);
            this.emit('message_processed', message);
        }
        catch (err) {
            if (err.name === sqsError_1.SQSError.name) {
                this.emit('error', err, message);
            }
            else {
                this.emit('processing_error', err, message);
            }
            if (this.terminateVisibilityTimeout) {
                try {
                    await this.sqs
                        .changeMessageVisibility({
                        QueueUrl: this.queueUrl,
                        ReceiptHandle: message.ReceiptHandle,
                        VisibilityTimeout: 0
                    })
                        .promise();
                }
                catch (err) {
                    this.emit('error', err, message);
                }
            }
        }
    }
    async executeHandler(message) {
        try {
            await this.handleMessage(message);
        }
        catch (err) {
            throw new Error(`Unexpected message handler failure: ${err.message}`);
        }
    }
    async poll() {
        if (this.stopped) {
            this.emit('stopped');
            return;
        }
        debug('Polling for messages');
        try {
            const receiveParams = {
                QueueUrl: this.queueUrl,
                AttributeNames: this.attributeNames,
                MessageAttributeNames: this.messageAttributeNames,
                MaxNumberOfMessages: this.batchSize,
                WaitTimeSeconds: this.waitTimeSeconds,
                VisibilityTimeout: this.visibilityTimeout
            };
            const response = await this.receiveMessage(receiveParams);
            this.handleSQSResponse(response);
        }
        catch (err) {
            this.emit('error', err);
            if (isAuthenticationError(err)) {
                debug('There was an authentication error. Pausing before retrying.');
                return setTimeout(() => this.poll(), this.authenticationErrorTimeout);
            }
        }
    }
}
exports.Consumer = Consumer;
