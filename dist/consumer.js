"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const SQS = require("aws-sdk/clients/sqs");
const Debug = require("debug");
const crypto = require("crypto");
const events_1 = require("events");
const bind_1 = require("./bind");
const errors_1 = require("./errors");
const debug = Debug('sqs-consumer');
const requiredOptions = [
    'queueUrl',
    // only one of handleMessage / handleMessagesBatch is required
    'handleMessage|handleMessageBatch',
];
function generateUuid() {
    return crypto.randomBytes(16).toString('hex');
}
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
}
function isConnectionError(err) {
    if (err instanceof errors_1.SQSError) {
        return err.statusCode === 403 || err.code === 'CredentialsError' || err.code === 'UnknownEndpoint';
    }
    return false;
}
function isNonExistentQueueError(err) {
    if (err instanceof errors_1.SQSError) {
        return err.code === 'AWS.SimpleQueueService.NonExistentQueue';
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
function addMessageUuidToError(error, message) {
    try {
        const messageBody = JSON.parse(message.Body);
        const messageUuid = messageBody && messageBody.payload && messageBody.payload.uuid;
        error.messageUuid = messageUuid;
    }
    catch (err) { }
}
class Consumer extends events_1.EventEmitter {
    constructor(options) {
        super();
        assertOptions(options);
        this.queueUrl = options.queueUrl;
        this.handleMessage = options.handleMessage;
        this.handleMessageBatch = options.handleMessageBatch;
        this.pollingStartedInstrumentCallback = options.pollingStartedInstrumentCallback;
        this.pollingFinishedInstrumentCallback = options.pollingFinishedInstrumentCallback;
        this.batchStartedInstrumentCallBack = options.batchStartedInstrumentCallBack;
        this.batchFinishedInstrumentCallBack = options.batchFinishedInstrumentCallBack;
        this.batchFailedInstrumentCallBack = options.batchFailedInstrumentCallBack;
        this.handleMessageTimeout = options.handleMessageTimeout;
        this.attributeNames = options.attributeNames || [];
        this.messageAttributeNames = options.messageAttributeNames || [];
        this.stopped = true;
        this.batchSize = options.batchSize || 1;
        this.concurrencyLimit = options.concurrencyLimit || 30;
        this.freeConcurrentSlots = this.concurrencyLimit;
        this.visibilityTimeout = options.visibilityTimeout;
        this.terminateVisibilityTimeout = options.terminateVisibilityTimeout || false;
        this.waitTimeSeconds = options.waitTimeSeconds || 20;
        this.authenticationErrorTimeout = options.authenticationErrorTimeout || 10000;
        this.pollingWaitTimeMs = options.pollingWaitTimeMs || 0;
        this.msDelayOnEmptyBatchSize = options.msDelayOnEmptyBatchSize || 5;
        this.sqs =
            options.sqs ||
                new SQS({
                    region: options.region || process.env.AWS_REGION || 'eu-west-1',
                });
        bind_1.autoBind(this);
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
    setBatchSize(newBatchSize) {
        this.batchSize = newBatchSize;
    }
    setConcurrencyLimit(newConcurrencyLimit) {
        const concurrencyLimitDiff = newConcurrencyLimit - this.concurrencyLimit;
        const newFreeConcurrentSlots = Math.max(0, this.freeConcurrentSlots + concurrencyLimitDiff);
        this.concurrencyLimit = newConcurrencyLimit;
        this.freeConcurrentSlots = newFreeConcurrentSlots;
        this.reportConcurrencyUsage(this.freeConcurrentSlots);
    }
    setPollingWaitTimeMs(newPollingWaitTimeMs) {
        this.pollingWaitTimeMs = newPollingWaitTimeMs;
    }
    async reportMessageFromBatchFinished(message, error) {
        debug('Message from batch has finished');
        this.freeConcurrentSlots++;
        this.reportConcurrencyUsage(this.freeConcurrentSlots);
        try {
            if (error)
                throw error;
            await this.deleteMessage(message);
            this.emit('message_processed', message, this.queueUrl);
        }
        catch (err) {
            this.emitError(err, message);
        }
    }
    reportNumberOfMessagesReceived(numberOfMessages) {
        debug('Reducing number of messages received from freeConcurrentSlots');
        this.freeConcurrentSlots = this.freeConcurrentSlots - numberOfMessages;
        this.reportConcurrencyUsage(this.freeConcurrentSlots);
    }
    async handleSqsResponse(response) {
        debug('Received SQS response');
        debug(response);
        const hasResponseWithMessages = !!response && hasMessages(response);
        const numberOfMessages = hasResponseWithMessages ? response.Messages.length : 0;
        if (this.pollingFinishedInstrumentCallback) {
            // instrument pod how many messages received
            this.pollingFinishedInstrumentCallback({
                instanceId: process.env.HOSTNAME,
                queueUrl: this.queueUrl,
                messagesReceived: numberOfMessages,
                freeConcurrentSlots: this.freeConcurrentSlots,
            });
        }
        if (response) {
            if (hasMessages(response)) {
                if (this.handleMessageBatch) {
                    // prefer handling messages in batch when available
                    await this.processMessageBatch(response.Messages);
                }
                else {
                    await Promise.all(response.Messages.map(this.processMessage));
                }
                this.emit('response_processed', this.queueUrl);
            }
            else {
                this.emit('empty', this.queueUrl);
            }
        }
    }
    async processMessage(message) {
        this.emit('message_received', message, this.queueUrl);
        try {
            await this.executeHandler(message);
            await this.deleteMessage(message);
            this.emit('message_processed', message, this.queueUrl);
        }
        catch (err) {
            this.emitError(err, message);
            if (this.terminateVisibilityTimeout) {
                try {
                    await this.terminateVisabilityTimeout(message);
                }
                catch (err) {
                    this.emit('error', err, message, this.queueUrl);
                }
            }
        }
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
            ReceiptHandle: message.ReceiptHandle,
        };
        try {
            await this.sqs.deleteMessage(deleteParams).promise();
        }
        catch (err) {
            throw toSQSError(err, `SQS delete message failed: ${err.message}`);
        }
    }
    async executeHandler(message) {
        let timeout;
        let pending;
        try {
            if (this.handleMessageTimeout) {
                [timeout, pending] = createTimeout(this.handleMessageTimeout);
                await Promise.race([this.handleMessage(message), pending]);
            }
            else {
                await this.handleMessage(message);
            }
        }
        catch (err) {
            addMessageUuidToError(err, message);
            if (err instanceof errors_1.TimeoutError) {
                err.message = `Message handler timed out after ${this.handleMessageTimeout}ms: Operation timed out.`;
            }
            else {
                err.message = `Unexpected message handler failure: ${err.message}`;
            }
            throw err;
        }
        finally {
            clearTimeout(timeout);
        }
    }
    async terminateVisabilityTimeout(message) {
        return this.sqs
            .changeMessageVisibility({
            QueueUrl: this.queueUrl,
            ReceiptHandle: message.ReceiptHandle,
            VisibilityTimeout: 0,
        })
            .promise();
    }
    emitError(err, message) {
        if (err.name === errors_1.SQSError.name) {
            this.emit('error', err, message, this.queueUrl);
        }
        else if (err instanceof errors_1.TimeoutError) {
            this.emit('timeout_error', err, message, this.queueUrl);
        }
        else {
            this.emit('processing_error', err, message, this.queueUrl);
        }
    }
    poll() {
        if (this.stopped) {
            this.emit('stopped', this.queueUrl);
            return;
        }
        const pollBatchSize = Math.min(this.batchSize, this.freeConcurrentSlots);
        debug('Polling for messages');
        if (this.pollingStartedInstrumentCallback) {
            this.pollingStartedInstrumentCallback({
                instanceId: process.env.HOSTNAME,
                queueUrl: this.queueUrl,
                pollBatchSize,
                freeConcurrentSlots: this.freeConcurrentSlots,
            });
        }
        let currentPollingTimeout = this.pollingWaitTimeMs;
        if (pollBatchSize > 0) {
            const receiveParams = {
                QueueUrl: this.queueUrl,
                AttributeNames: this.attributeNames,
                MessageAttributeNames: this.messageAttributeNames,
                MaxNumberOfMessages: pollBatchSize,
                WaitTimeSeconds: this.waitTimeSeconds,
                VisibilityTimeout: this.visibilityTimeout,
            };
            this.receiveMessage(receiveParams)
                .then(this.handleSqsResponse)
                .catch((err) => {
                this.emit('unhandled_error', err, this.queueUrl);
                if (isNonExistentQueueError(err)) {
                    throw new Error(`Could not receive messages - non existent queue - ${this.queueUrl}`);
                }
                if (isConnectionError(err)) {
                    debug('There was an authentication error. Pausing before retrying.');
                    currentPollingTimeout = this.authenticationErrorTimeout;
                }
                return;
            })
                .then(() => {
                setTimeout(this.poll, currentPollingTimeout);
            })
                .catch((err) => {
                this.emit('unhandled_error', err, this.queueUrl);
            });
        }
        else {
            setTimeout(this.poll, this.msDelayOnEmptyBatchSize);
        }
    }
    async processMessageBatch(messages) {
        messages.forEach((message) => {
            this.emit('message_received', message, this.queueUrl);
        });
        this.reportNumberOfMessagesReceived(messages.length);
        const batchUuid = generateUuid();
        if (this.batchStartedInstrumentCallBack) {
            this.batchStartedInstrumentCallBack({
                instanceId: process.env.HOSTNAME,
                queueUrl: this.queueUrl,
                batchUuid,
                numberOfMessages: messages.length,
                freeConcurrentSlots: this.freeConcurrentSlots,
            });
        }
        this.handleMessageBatch(messages, this)
            .then(() => {
            if (this.batchFinishedInstrumentCallBack) {
                this.batchFinishedInstrumentCallBack({
                    instanceId: process.env.HOSTNAME,
                    queueUrl: this.queueUrl,
                    batchUuid,
                    numberOfMessages: messages.length,
                    freeConcurrentSlots: this.freeConcurrentSlots,
                });
            }
        })
            .catch((err) => {
            if (this.batchFailedInstrumentCallBack) {
                this.batchFailedInstrumentCallBack({
                    instanceId: process.env.HOSTNAME,
                    queueUrl: this.queueUrl,
                    batchUuid,
                    numberOfMessages: messages.length,
                    freeConcurrentSlots: this.freeConcurrentSlots,
                    error: err,
                });
            }
        });
    }
    reportConcurrencyUsage(currentFreeConcurrencySlots) {
        this.emit('concurrency_usage_updated', currentFreeConcurrencySlots, this.concurrencyLimit, this.queueUrl);
    }
}
exports.Consumer = Consumer;
