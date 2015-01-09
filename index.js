var EventEmitter = require('events').EventEmitter;
var util = require('util');
var _ = require('lodash');
var AWS = require('aws-sdk');
var debug = require('debug')('sqs-consumer');
var requiredOptions = [
    'queueUrl',
    'region',
    'handleMessage'
  ];

function validate(options) {
  requiredOptions.forEach(function (option) {
    if (!options[option]) {
      throw new Error('Missing SQS consumer option [' + option + '].');
    }
  });
}

/**
 * An SQS consumer.
 * @param {object} options
 * @param {string} options.queueUrl
 * @param {string} options.region
 * @param {function} options.handleMessage
 * @param {number} options.waitTime
 * @param {object} options.sqs
 */
function Consumer(options) {
  validate(options);

  this.queueUrl = options.queueUrl;
  this.handleMessage = options.handleMessage;
  this.stopped = true;
  this.sqs = options.sqs || new AWS.SQS({
    region: options.region
  });
  this.poll = _.throttle(this._poll.bind(this), options.waitTime || 100);
}

util.inherits(Consumer, EventEmitter);

/**
 * Start polling for messages.
 */
Consumer.prototype.start = function () {
  if (this.stopped) {
    debug('Starting consumer');
    this.stopped = false;
    this.poll();
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
    MaxNumberOfMessages: 1,
    WaitTimeSeconds: 20
  };

  if (!this.stopped) {
    debug('Polling for messages');
    this.sqs.receiveMessage(receiveParams, this._handleSqsResponse.bind(this));
  }
};

Consumer.prototype._handleSqsResponse = function (err, response) {
  if (err) this.emit('error', err);

  debug('Received SQS response');
  debug(response);
  if (response && response.Messages && response.Messages.length > 0) {
    var message = response.Messages[0];

    this.emit('message_received', message);
    this._handleSqsMessage(message);
  }

  // Poll for another message
  this.poll();
};

Consumer.prototype._handleSqsMessage = function (message) {
  var consumer = this;

  this.handleMessage(message, function (err) {
    if (err) return consumer.emit('error', err);

    consumer._deleteMessage(message);
  });
};

Consumer.prototype._deleteMessage = function (message) {
  var consumer = this;
  var deleteParams = {
    QueueUrl: this.queueUrl,
    ReceiptHandle: message.ReceiptHandle
  };

  debug('Deleting message %s', message.MessageId);
  this.sqs.deleteMessage(deleteParams, function (err) {
    if (err) return consumer.emit('error', err);

    consumer.emit('message_processed', message);
  });
};

module.exports = Consumer;