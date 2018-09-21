'use strict';

const EventEmitter = require('events').EventEmitter;
const async = require('async');
const auto = require('auto-bind');
const AWS = require('aws-sdk');
const debug = require('debug')('sqs-consumer');
const requiredOptions = [
  'queueUrl',
  // only one of handleMessage / handleMessagesBatch is required
  'handleMessage|handleMessagesBatch'
];

class SQSError extends Error {
  constructor() {
    super(Array.from(arguments));
    this.name = this.constructor.name;
  }
}

function validate(options) {
  requiredOptions.forEach((option) => {
    const possibilities = option.split('|');
    if (!possibilities.find((p) => options[p])) {
      throw new Error('Missing SQS consumer option [' + possibilities.join(' or ') + '].');
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
    this.handleMessagesBatch = options.handleMessagesBatch;
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
      if (consumer.handleMessagesBatch) {
        // prefer handling messages in batch when available
        this._processMessagesBatch(response.Messages, this._endProcessing);
      } else {
        async.each(response.Messages, this._processMessage, this._endProcessing);
      }
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

  _processMessagesBatch(messages, cb) {
    const consumer = this;

    messages.forEach((message) => {
      this.emit('message_received', message);
    });

    async.series([
      function handleMessagesBatch(done) {
        try {
          consumer.handleMessagesBatch(messages, done);
        } catch (err) {
          done(new Error('Unexpected message handler failure: ' + err.message));
        }
      },
      function deleteMessagesBatch(done) {
        consumer._deleteMessagesBatch(messages, done);
      }
    ], (err) => {
      if (err) {
        if (err.name === SQSError.name) {
          consumer.emit('error', err, messages);
        } else {
          consumer.emit('processing_error', err, messages);
        }

        if (consumer.terminateVisibilityTimeout) {
          consumer.sqs.changeMessageVisibilityBatch({
            QueueUrl: consumer.queueUrl,
            Entries: messages.map((message) => ({
              ReceiptHandle: message.ReceiptHandle,
              VisibilityTimeout: 0
            }))
          }, (err) => {
            if (err) consumer.emit('error', err, messages);
            cb();
          });
          return;
        }
      } else {
        messages.map((message) => {
          consumer.emit('message_processed', message);
        });
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

  _deleteMessagesBatch(messages, cb) {
    const deleteParams = {
      QueueUrl: this.queueUrl,
      Entries: messages.map((message) => ({
        Id: message.MessageId,
        ReceiptHandle: message.ReceiptHandle
      }))
    };

    debug('Deleting messages %s', messages.map((msg) => msg.MessageId).join(' ,'));
    this.sqs.deleteMessageBatch(deleteParams, (err) => {
      if (err) return cb(new SQSError('SQS delete message batch failed: ' + err.message));

      cb();
    });
  }

  _endProcessing() {
    // start polling again once all of the messages have been processed
    this.emit('response_processed');
    this._poll();
  }

}

module.exports = Consumer;
