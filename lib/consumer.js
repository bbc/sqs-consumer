'use strict';

const EventEmitter = require('events').EventEmitter;
const debug = require('debug')('sqs-consumer');

const SQS = require('./sqs');
const auto = require('./bind');
const SQSError = require('./sqsError');

const requiredOptions = [
  'queueUrl',
  'handleMessage'
];

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

    this.sqs = options.sqs || new SQS({
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

  async _poll() {
    const receiveParams = {
      QueueUrl: this.queueUrl,
      AttributeNames: this.attributeNames,
      MessageAttributeNames: this.messageAttributeNames,
      MaxNumberOfMessages: this.batchSize,
      WaitTimeSeconds: this.waitTimeSeconds,
      VisibilityTimeout: this.visibilityTimeout
    };

    if (!this.stopped) {
      debug('Polling for messages');
      const response = await this.sqs.receiveMessage(receiveParams).promise();
      try {
        this._handleSqsResponse(response);
      } catch (err) {
        this.emit('error', new SQSError('SQS receive message failed: ' + err.message));
        if (isAuthenticationError(err)) {
          debug('There was an authentication error. Pausing before retrying.');
          setTimeout(this._poll.bind(this), this.authenticationErrorTimeout);
        }
      }
    } else {
      this.emit('stopped');
    }
  }

  _handleSqsResponse(response) {
    debug('Received SQS response');
    debug(response);

    if (response && response.Messages && response.Messages.length > 0) {
      Promise.all(response.Messages.map(this._processMessage))
        .then(() => {
          this.emit('response_processed');
          this._poll();
        });

    } else if (response && !response.Messages) {
      this.emit('empty');
      this._poll();

    } else {
      // there were no messages, so start polling again
      this._poll();
    }
  }

  async consume(message) {
    try {
      await this.handleMessage(message);
    } catch (err) {
      throw new Error('Unexpected message handler failure: ' + err.message);
    }
  }

  async _processMessage(message) {
    const consumer = this;

    this.emit('message_received', message);

    try {
      await consumer.consume(message);
      await consumer._deleteMessage(message);
      consumer.emit('message_processed', message);

    } catch (err) {
      if (err.name === SQSError.name) {
        consumer.emit('error', err, message);
      } else {
        consumer.emit('processing_error', err, message);
      }

      if (consumer.terminateVisibilityTimeout) {
        await consumer.sqs.changeMessageVisibility({
          QueueUrl: consumer.queueUrl,
          ReceiptHandle: message.ReceiptHandle,
          VisibilityTimeout: 0
        }).promise();
      }
      return;
    }
  }

  async _deleteMessage(message) {
    const deleteParams = {
      QueueUrl: this.queueUrl,
      ReceiptHandle: message.ReceiptHandle
    };

    debug('Deleting message %s', message.MessageId);

    try {
      await this.sqs.deleteMessage(deleteParams).promise();
    } catch (err) {
      throw new SQSError('SQS delete message failed: ' + err.message);
    }
  }
}

module.exports = Consumer;
