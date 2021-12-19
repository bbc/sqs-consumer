"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Consumer = void 0;
const SQS = require("aws-sdk/clients/sqs");
const Debug = require("debug");
const events_1 = require("events");
const bind_1 = require("./bind");
const errors_1 = require("./errors");
const debug = Debug('sqs-consumer');
const requiredOptions = [
    'queueUrl',
    // only one of handleMessage / handleMessagesBatch is required
    'handleMessage|handleMessageBatch'
];
function createTimeout(duration) {
    let timeout;
    const pending = new Promise((_, reject) => {
        timeout = setTimeout(() => {
            reject(new errors_1.TimeoutError());
        }, duration);
    });
    return [timeout, pending];
}
function assertOptions(options) {
    requiredOptions.forEach((option) => {
        const possibilities = option.split('|');
        if (!possibilities.find((p) => options[p])) {
            throw new Error(`Missing SQS consumer option [ ${possibilities.join(' or ')} ].`);
        }
    });
    if (options.batchSize > 10 || options.batchSize < 1) {
        throw new Error('SQS batchSize option must be between 1 and 10.');
    }
    if (options.heartbeatInterval && !(options.heartbeatInterval < options.visibilityTimeout)) {
        throw new Error('heartbeatInterval must be less than visibilityTimeout.');
    }
}
function isConnectionError(err) {
    if (err instanceof errors_1.SQSError) {
        return (err.statusCode === 403 || err.code === 'CredentialsError' || err.code === 'UnknownEndpoint');
    }
    return false;
}
function toSQSError(err, message) {
    const sqsError = new errors_1.SQSError(message);
    sqsError.code = err.code;
    sqsError.statusCode = err.statusCode;
    sqsError.region = err.region;
    sqsError.retryable = err.retryable;
    sqsError.hostname = err.hostname;
    sqsError.time = err.time;
    return sqsError;
}
function hasMessages(response) {
    return response.Messages && response.Messages.length > 0;
}
class Consumer extends events_1.EventEmitter {
    constructor(options) {
        super();
        assertOptions(options);
        this.queueUrl = options.queueUrl;
        this.handleMessage = options.handleMessage;
        this.handleMessageBatch = options.handleMessageBatch;
        this.handleMessageTimeout = options.handleMessageTimeout;
        this.attributeNames = options.attributeNames || [];
        this.messageAttributeNames = options.messageAttributeNames || [];
        this.stopped = true;
        this.batchSize = options.batchSize || 1;
        this.visibilityTimeout = options.visibilityTimeout;
        this.terminateVisibilityTimeout = options.terminateVisibilityTimeout || false;
        this.heartbeatInterval = options.heartbeatInterval;
        this.waitTimeSeconds = options.waitTimeSeconds || 20;
        this.authenticationErrorTimeout = options.authenticationErrorTimeout || 10000;
        this.pollingWaitTimeMs = options.pollingWaitTimeMs || 0;
        this.sqs = options.sqs || new SQS({
            region: options.region || process.env.AWS_REGION || 'eu-west-1'
        });
        (0, bind_1.autoBind)(this);
    }
    emit(event, ...args) {
        return super.emit(event, ...args);
    }
    on(event, listener) {
        return super.on(event, listener);
    }
    once(event, listener) {
        return super.once(event, listener);
    }
    get isRunning() {
        return !this.stopped;
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
    async handleSqsResponse(response) {
        debug('Received SQS response');
        debug(response);
        if (response) {
            if (hasMessages(response)) {
                if (this.handleMessageBatch) {
                    // prefer handling messages in batch when available
                    await this.processMessageBatch(response.Messages);
                }
                else {
                    await Promise.all(response.Messages.map(this.processMessage));
                }
                this.emit('response_processed');
            }
            else {
                this.emit('empty');
            }
        }
    }
    async processMessage(message) {
        this.emit('message_received', message);
        let heartbeat;
        try {
            if (this.heartbeatInterval) {
                heartbeat = this.startHeartbeat(async () => {
                    return this.changeVisabilityTimeout(message, this.visibilityTimeout);
                });
            }
            await this.executeHandler(message);
            this.emit('message_processed', message);
        }
        catch (err) {
            this.emitError(err, message);
            if (this.terminateVisibilityTimeout) {
                await this.changeVisabilityTimeout(message, 0);
            }
        }
        finally {
            clearInterval(heartbeat);
        }
    }
    async receiveMessage(params) {
        try {
            return await this.sqs
                .receiveMessage(params)
                .promise();
        }
        catch (err) {
            throw toSQSError(err, `SQS receive message failed: ${err.message}`);
        }
    }
    async executeHandler(message) {
        let timeout;
        let pending;
        try {
            if (this.handleMessageTimeout) {
                [timeout, pending] = createTimeout(this.handleMessageTimeout);
                await Promise.race([
                    this.handleMessage(message),
                    pending
                ]);
            }
            else {
                await this.handleMessage(message);
            }
        }
        catch (err) {
            if (err instanceof errors_1.TimeoutError) {
                err.message = `Message handler timed out after ${this.handleMessageTimeout}ms: Operation timed out.`;
            }
            else if (err instanceof Error) {
                err.message = `Unexpected message handler failure: ${err.message}`;
            }
            throw err;
        }
        finally {
            clearTimeout(timeout);
        }
    }
    async changeVisabilityTimeout(message, timeout) {
        try {
            return this.sqs
                .changeMessageVisibility({
                QueueUrl: this.queueUrl,
                ReceiptHandle: message.ReceiptHandle,
                VisibilityTimeout: timeout
            })
                .promise();
        }
        catch (err) {
            this.emit('error', err, message);
        }
    }
    emitError(err, message) {
        if (err.name === errors_1.SQSError.name) {
            this.emit('error', err, message);
        }
        else if (err instanceof errors_1.TimeoutError) {
            this.emit('timeout_error', err, message);
        }
        else {
            this.emit('processing_error', err, message);
        }
    }
    poll() {
        if (this.stopped) {
            this.emit('stopped');
            return;
        }
        debug('Polling for messages');
        const receiveParams = {
            QueueUrl: this.queueUrl,
            AttributeNames: this.attributeNames,
            MessageAttributeNames: this.messageAttributeNames,
            MaxNumberOfMessages: this.batchSize,
            WaitTimeSeconds: this.waitTimeSeconds,
            VisibilityTimeout: this.visibilityTimeout
        };
        let currentPollingTimeout = this.pollingWaitTimeMs;
        this.receiveMessage(receiveParams)
            .then(this.handleSqsResponse)
            .catch((err) => {
            this.emit('error', err);
            if (isConnectionError(err)) {
                debug('There was an authentication error. Pausing before retrying.');
                currentPollingTimeout = this.authenticationErrorTimeout;
            }
            return;
        }).then(() => {
            setTimeout(this.poll, currentPollingTimeout);
        }).catch((err) => {
            this.emit('error', err);
        });
    }
    async processMessageBatch(messages) {
        messages.forEach((message) => {
            this.emit('message_received', message);
        });
        let heartbeat;
        try {
            if (this.heartbeatInterval) {
                heartbeat = this.startHeartbeat(async () => {
                    return this.changeVisabilityTimeoutBatch(messages, this.visibilityTimeout);
                });
            }
            await this.executeBatchHandler(messages);
            messages.forEach((message) => {
                this.emit('message_processed', message);
            });
        }
        catch (err) {
            this.emit('error', err, messages);
            if (this.terminateVisibilityTimeout) {
                await this.changeVisabilityTimeoutBatch(messages, 0);
            }
        }
        finally {
            clearInterval(heartbeat);
        }
    }
    async executeBatchHandler(messages) {
        try {
            await this.handleMessageBatch(messages);
        }
        catch (err) {
            err.message = `Unexpected message handler failure: ${err.message}`;
            throw err;
        }
    }
    async changeVisabilityTimeoutBatch(messages, timeout) {
        const params = {
            QueueUrl: this.queueUrl,
            Entries: messages.map((message) => ({
                Id: message.MessageId,
                ReceiptHandle: message.ReceiptHandle,
                VisibilityTimeout: timeout
            }))
        };
        try {
            return this.sqs
                .changeMessageVisibilityBatch(params)
                .promise();
        }
        catch (err) {
            this.emit('error', err, messages);
        }
    }
    startHeartbeat(heartbeatFn) {
        return setInterval(() => {
            heartbeatFn();
        }, this.heartbeatInterval * 1000);
    }
}
exports.Consumer = Consumer;
