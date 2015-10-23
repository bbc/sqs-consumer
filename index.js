'use strict';

var EventEmitter = require('events').EventEmitter;
var util = require('util');
var async = require('async');
var AWS = require('aws-sdk');
var debug = require('debug')('sqs-consumer');
var requiredOptions = [
    'queueUrl',
    'handleMessage'
  ];

function validate(options) {
  requiredOptions.forEach(function (option) {
    if (!options[option]) {
      throw new Error('Missing SQS consumer option [' + option + '].');
    }
  });

  if (options.batchSize > 10 || options.batchSize < 1) {
    throw new Error('SQS batchSize option must be between 1 and 10.');
  }
}

/**
 * An SQS consumer.
 * @param {object} options
 * @param {string} options.queueUrl
 * @param {string} options.region
 * @param {function} options.handleMessage
 * @param {array} options.messageAttributeNames
 * @param {number} options.batchSize
 * @param {object} options.sqs
 * @param {number} options.visibilityTimeout
 * @param {number} options.waitTimeSeconds
 */
function Consumer(options) {
  validate(options);

  this.queueUrl = options.queueUrl;
  this.handleMessage = options.handleMessage;
  this.messageAttributeNames = options.messageAttributeNames || [];
  this.stopped = true;
  this.batchSize = options.batchSize || 1;
  this.visibilityTimeout = options.visibilityTimeout;
  this.waitTimeSeconds = options.waitTimeSeconds || 20;

  this.sqs = options.sqs || new AWS.SQS({
    region: options.region || 'eu-west-1'
  });

  this._handleSqsResponseBound = this._handleSqsResponse.bind(this);
  this._processMessageBound = this._processMessage.bind(this);
}

util.inherits(Consumer, EventEmitter);

/**
 * Construct a new Consumer
 */
Consumer.create = function (options) {
  return new Consumer(options);
};

/**
 * Start polling for messages.
 */
Consumer.prototype.start = function () {
  if (this.stopped) {
    debug('Starting consumer');
    this.stopped = false;
    this._poll();
  }
};

/**
 * Stop polling for messages.
 */
Consumer.prototype.stop = function () {
  debug('Stopping consumer');
  this.stopped = true;
};

Consumer.prototype._poll = function () {
  var receiveParams = {
    QueueUrl: this.queueUrl,
    MessageAttributeNames: this.messageAttributeNames,
    MaxNumberOfMessages: this.batchSize,
    WaitTimeSeconds: this.waitTimeSeconds,
    VisibilityTimeout: this.visibilityTimeout
  };

  if (!this.stopped) {
    debug('Polling for messages');
    this.sqs.receiveMessage(receiveParams, this._handleSqsResponseBound);
  }
};

Consumer.prototype._handleSqsResponse = function (err, response) {
  if (err) this.emit('error', new Error('SQS receive message failed: ' + err.message));

  var consumer = this;

  debug('Received SQS response');
  debug(response);

  if (response && response.Messages && response.Messages.length > 0) {
    async.each(response.Messages, this._processMessageBound, function () {
      // start polling again once all of the messages have been processed
      consumer._poll();
    });
  } else {
    // there were no messages, so start polling again
    this._poll();
  }
};

Consumer.prototype._processMessage = function (message, cb) {
  var consumer = this;

  this.emit('message_received', message);
  async.series([
    function handleMessage(done) {
      consumer.handleMessage(message, done);
    },
    function deleteMessage(done) {
      consumer._deleteMessage(message, done);
    }
  ], function (err) {
    if (err) {
      consumer.emit('error', err);
    } else {
      consumer.emit('message_processed', message);
    }

    cb();
  });
};

Consumer.prototype._deleteMessage = function (message, cb) {
  var deleteParams = {
    QueueUrl: this.queueUrl,
    ReceiptHandle: message.ReceiptHandle
  };

  debug('Deleting message %s', message.MessageId);
  this.sqs.deleteMessage(deleteParams, function (err) {
    if (err) return cb(new Error('SQS delete message failed: ' + err.message));

    cb();
  });
};

module.exports = Consumer;
