'use strict';

const EventEmitter = require('events').EventEmitter;
const auto = require('auto-bind');
const AWS = require('aws-sdk');
const debug = require('debug')('sqs-consumer');
const requiredOptions = [
  'queueUrl',
  'handleMessage'
];

class SQSError extends Error {
  constructor() {
    super(Array.from(arguments));
    this.name = this.constructor.name;
  }
}

function validate(options) {
  requiredOptions.forEach((option) => {
    if (!options[option]) {
      throw new Error('Missing SQS consumer option [' + option + '].');
    }
  });

  if (options.batchSize > 10 || options.batchSize < 1) {
    throw new Error('SQS batchSize option must be between 1 and 10.');
  }
}

function isAuthenticationError(err) {
  return (err.statusCode === 403 || err.code === 'CredentialsError');
}

class Consumer extends EventEmitter {
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

    this.sqs = options.sqs || new AWS.SQS({
      region: options.region || process.env.AWS_REGION || 'eu-west-1'
    });

    auto(this);
  }

  static create(options) {
    return new Consumer(options);
  }

  start() {
    if (this.stopped) {
      debug('Starting consumer');
      this.stopped = false;
      this._poll();
    }
  }

  stop() {
    debug('Stopping consumer');
    this.stopped = true;
  }

  _poll() {
    const receiveParams = {
      QueueUrl: this.queueUrl,
      AttributeNames: this.attributeNames,
      MessageAttributeNames: this.messageAttributeNames,
      MaxNumberOfMessages: this.batchSize,
      WaitTimeSeconds: this.waitTimeSeconds,
      VisibilityTimeout: this.visibilityTimeout
    };

    if (this.stopped) {
      this.emit('stopped');
      return;
    }
    debug('Polling for messages');
    this.sqs.receiveMessage(receiveParams)
      .promise()
      .then((data) => this._handleSqsResponse(data))
      .catch((err) => {
        this.emit('error', new SQSError('SQS receive message failed: ' + err.message));
        if (isAuthenticationError(err)) {
          debug('There was an authentication error. Pausing before retrying.');
          return setTimeout(() => this._poll(), this.authenticationErrorTimeout);
        }
      });
  }

  _handleSqsResponse(response) {
    const consumer = this;

    debug('Received SQS response');
    debug(response);

    if (response && response.Messages && response.Messages.length > 0) {
      Promise.all(response.Messages.map(this._processMessage)).then(() => {
        // start polling again once all of the messages have been processed
        consumer.emit('response_processed');
        consumer._poll();
      });
    } else if (response && !response.Messages) {
      this.emit('empty');
      this._poll();
    } else {
      // there were no messages, so start polling again
      this._poll();
    }
  }

  _processMessage(message) {
    const consumer = this;

    this.emit('message_received', message);
    return Promise.resolve()
      .then(() => consumer.handleMessage(message))
      .catch(
        (err) => Promise.reject(new Error('Unexpected message handler failure: ' + err.message))
      )
      .then(() => consumer._deleteMessage(message))
      .then(() => consumer.emit('message_processed', message))
      .catch((err) => {
        if (err.name === SQSError.name) {
          consumer.emit('error', err, message);
        } else {
          consumer.emit('processing_error', err, message);
        }

        if (consumer.terminateVisibilityTimeout) {
          return consumer.sqs
            .changeMessageVisibility({
              QueueUrl: consumer.queueUrl,
              ReceiptHandle: message.ReceiptHandle,
              VisibilityTimeout: 0
            })
            .promise()
            .catch((err) => consumer.emit('error', err, message));
        }
        return;
      });
  }

  _deleteMessage(message) {
    const deleteParams = {
      QueueUrl: this.queueUrl,
      ReceiptHandle: message.ReceiptHandle
    };

    debug('Deleting message %s', message.MessageId);
    return this.sqs
      .deleteMessage(deleteParams)
      .promise()
      .catch((err) => Promise.reject(new SQSError('SQS delete message failed: ' + err.message)));
  }
}

module.exports = Consumer;
