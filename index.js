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

  var MAX_NUM_MESSAGES = 10;

/**
 * Construct a new SQSError
 */
function SQSError(message) {
  Error.captureStackTrace(this, this.constructor);
  this.name = this.constructor.name;
  this.message = (message || '');
}
util.inherits(SQSError, Error);

function validate(options) {
  requiredOptions.forEach(function (option) {
    if (!options[option]) {
      throw new Error('Missing SQS consumer option [' + option + '].');
    }
  });

  if (options.batchSize < 1) {
    throw new Error('SQS batchSize option must be greater than 0.');
  }
}

function isAuthenticationError(err) {
  return (err.statusCode === 403 || err.code === 'CredentialsError');
}

/**
 * An SQS consumer.
 * @param {object} options
 * @param {string} options.queueUrl
 * @param {string} options.region
 * @param {function} options.handleMessage
 * @param {array} options.attributeNames
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
  this.attributeNames = options.attributeNames || [];
  this.messageAttributeNames = options.messageAttributeNames || [];
  this.stopped = true;
  this.batchSize = options.batchSize || 1;
  this.visibilityTimeout = options.visibilityTimeout;
  this.waitTimeSeconds = options.waitTimeSeconds || 20;
  this.authenticationErrorTimeout = options.authenticationErrorTimeout || 10000;

  this.sqs = options.sqs || new AWS.SQS({
    region: options.region || process.env.AWS_REGION || 'eu-west-1'
  });

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

    var numReceives = Math.floor(this.batchSize / MAX_NUM_MESSAGES);
    var remainder = this.batchSize % MAX_NUM_MESSAGES;

    for (var i = 0; i < numReceives; i++) {
      this._poll(MAX_NUM_MESSAGES);
    }

    if (remainder) {
      this._poll(remainder);
    }
  }
};

/**
 * Stop polling for messages.
 */
Consumer.prototype.stop = function () {
  debug('Stopping consumer');
  this.stopped = true;
};

Consumer.prototype._poll = function (batchSize) {
  var consumer = this;

  var receiveParams = {
    QueueUrl: this.queueUrl,
    AttributeNames: this.attributeNames,
    MessageAttributeNames: this.messageAttributeNames,
    MaxNumberOfMessages: batchSize,
    WaitTimeSeconds: this.waitTimeSeconds,
    VisibilityTimeout: this.visibilityTimeout
  };

  if (!this.stopped) {
    debug('Polling for messages');
    this.sqs.receiveMessage(receiveParams, function (err, response) {
      consumer._handleSqsResponse(err, response, function () {
        consumer._poll(batchSize);
      });
    });
  } else {
    this.emit('stopped');
  }
};

Consumer.prototype._handleSqsResponse = function (err, response, cb) {
  if (err) {
    this.emit('error', new SQSError('SQS receive message failed: ' + err.message));
  }

  debug('Received SQS response');
  debug(response);

  if (response && response.Messages && response.Messages.length > 0) {
    async.each(response.Messages, this._processMessageBound, function() {
      cb();
    });
  } else if (response && !response.Messages) {
    this.emit('empty');
    cb();
  } else if (err && isAuthenticationError(err)) {
    // there was an authentication error, so wait a bit before repolling
    debug('There was an authentication error. Pausing before retrying.');
    setTimeout(cb, this.authenticationErrorTimeout);
  } else {
    // there were no messages, so start polling again
    cb();
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
      if (err.name === SQSError.name) {
        consumer.emit('error', err, message);
      } else {
        consumer.emit('processing_error', err, message);
      }
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
    if (err) return cb(new SQSError('SQS delete message failed: ' + err.message));

    cb();
  });
};

module.exports = Consumer;
