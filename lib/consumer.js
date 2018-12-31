'use strict';

const EventEmitter = require('events').EventEmitter;
const async = require('async');
const AWS = require('aws-sdk');
const debug = require('debug')('sqs-consumer');

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

    if (!this.stopped) {
      debug('Polling for messages');
      this.sqs.receiveMessage(receiveParams, this._handleSqsResponse);
    } else {
      this.emit('stopped');
    }
  }

  _handleSqsResponse(err, response) {
    const consumer = this;

    if (err) {
      this.emit('error', new SQSError('SQS receive message failed: ' + err.message));
    }

    debug('Received SQS response');
    debug(response);

    if (response && response.Messages && response.Messages.length > 0) {
      async.each(response.Messages, this._processMessage, () => {
        // start polling again once all of the messages have been processed
        consumer.emit('response_processed');
        consumer._poll();
      });
    } else if (response && !response.Messages) {
      this.emit('empty');
      this._poll();
    } else if (err && isAuthenticationError(err)) {
      // there was an authentication error, so wait a bit before repolling
      debug('There was an authentication error. Pausing before retrying.');
      setTimeout(this._poll.bind(this), this.authenticationErrorTimeout);
    } else {
      // there were no messages, so start polling again
      this._poll();
    }
  }

  _processMessage(message, cb) {
    const consumer = this;

    this.emit('message_received', message);
    async.series([
      function handleMessage(done) {
        try {
          consumer.handleMessage(message, done);
        } catch (err) {
          done(new Error('Unexpected message handler failure: ' + err.message));
        }
      },
      function deleteMessage(done) {
        consumer._deleteMessage(message, done);
      }
    ], (err) => {
      if (err) {
        if (err.name === SQSError.name) {
          consumer.emit('error', err, message);
        } else {
          consumer.emit('processing_error', err, message);
        }

        if (consumer.terminateVisibilityTimeout) {
          consumer.sqs.changeMessageVisibility({
            QueueUrl: consumer.queueUrl,
            ReceiptHandle: message.ReceiptHandle,
            VisibilityTimeout: 0
          }, (err) => {
            if (err) consumer.emit('error', err, message);
            cb();
          });
          return;
        }
      } else {
        consumer.emit('message_processed', message);
      }
      cb();
    });
  }

  _deleteMessage(message, cb) {
    const deleteParams = {
      QueueUrl: this.queueUrl,
      ReceiptHandle: message.ReceiptHandle
    };

    debug('Deleting message %s', message.MessageId);
    this.sqs.deleteMessage(deleteParams, (err) => {
      if (err) return cb(new SQSError('SQS delete message failed: ' + err.message));

      cb();
    });
  }
}

module.exports = Consumer;
