'use strict';

var EventEmitter = require('events').EventEmitter;
var util = require('util');
var AWS = require('aws-sdk');
var debug = require('debug')('sqs-consumer');
var requiredOptions = [
    'queueUrl',
    'handleMessage'
  ];

const VISIBILITY_TIMEOUT_FACTOR = 0.75;
const MAX_DURATION_OF_MESSAGE = 11 * 60 * 60 * 1000; // 11 hours

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

  if (options.batchSize > 10 || options.batchSize < 1) {
    throw new Error('SQS batchSize option must be between 1 and 10.');
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

  this.currentQueueIndex = 0;
  this.numberActive = 0;
  this.maxNumberActive = options.batchSize || 1;

  this.queueUrls = [].concat(options.queueUrl);
  this.handleMessage = options.handleMessage;
  this.attributeNames = options.attributeNames || [];
  this.messageAttributeNames = options.messageAttributeNames || [];
  this.stopped = true;
  this.maxDurationOfMessage = options.maxDurationOfMessage || MAX_DURATION_OF_MESSAGE;
  this.visibilityTimeout = options.visibilityTimeout || 30;
  this.initialVisibilityTimeout = options.initialVisibilityTimeout || this.visibilityTimeout;
  this.terminateVisibilityTimeout = options.terminateVisibilityTimeout || false;
  
  this.waitTimeSeconds = options.waitTimeSeconds || 20;
  if (!Array.isArray(this.waitTimeSeconds)) {
    this.waitTimeSeconds = this.queueUrls.map(() => this.waitTimeSeconds);
  }

  this.authenticationErrorTimeout = options.authenticationErrorTimeout || 10000;

  this.sqs = options.sqs || new AWS.SQS({
    region: options.region || process.env.AWS_REGION || 'eu-west-1'
  });
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
  if (!this.stopped) {
    this.emit('stopped');
    this.stopped = true;
  }
};

Consumer.prototype._poll = function () {
  if (this.stopped) {
    return;
  }

  if (this.numberActive < this.maxNumberActive) {
    clearTimeout(this._pollDebounce);
    this._pollDebounce = setTimeout(() => {
      let index = this.currentQueueIndex++;
      if (this.currentQueueIndex > this.queueUrls.length) {
        this.currentQueueIndex = 0;
      }

      let url = this.queueUrls[index];
      this._pollQueue(url, index);
    }, 0);
  }
};

Consumer.prototype._pollQueue = function(queueUrl, index) {
  if (this.stopped) {
    return;
  }

  var receiveParams = {
    QueueUrl: queueUrl,
    AttributeNames: this.attributeNames,
    MessageAttributeNames: this.messageAttributeNames,
    MaxNumberOfMessages: this.maxNumberActive - this.numberActive,
    WaitTimeSeconds: this.waitTimeSeconds[index],
    VisibilityTimeout: this.initialVisibilityTimeout
  };

  this.sqs.receiveMessage(receiveParams, (err, response) => {
    if (err) {    
      this.emit('error', new SQSError('SQS receive message failed: ' + err.message));
    }

    debug('Received SQS response');
    debug(response);

    if (response && response.Messages && response.Messages.length > 0) {
      response.Messages.forEach(message => this._processMessage(message, queueUrl));
    } 
    else if (response && !response.Messages) {
      this.emit('empty');
      this._poll();
    } 
    else if (err && isAuthenticationError(err)) {
      // there was an authentication error, so wait a bit before repolling
      debug('There was an authentication error. Pausing before retrying.');
      setTimeout(this._poll.bind(this), this.authenticationErrorTimeout);
    } 
    else {
      // there were no messages, so start polling again
      this._poll();
    }
  });
};

Consumer.prototype._processMessage = function (message, queueUrl) {
  // make sure we aren't over-extending ourselves
  if (this.stopped || this.numberActive >= this.maxNumberActive) {
    this._cancelProcessingMessage(message, queueUrl);
    return;
  }

  // mark us as having worked on this message --- 
  // it is VERY important that all paths out of this processing will
  // decrement the counter
  this.numberActive++;
  this.emit('message_received', message);

  let startTime = new Date().getTime(),
      hasDecremented = false, 
      keepAliveTimeout = null, 
      keepAlive = () => {

        // if the message takes a REALLY long time to process,
        // at some point SQS stops being able to extend the visibility timeout...
        // so we have to just delete the message and hope it finishes.  
        // this is not ideal for the super long term, but it better than the message
        // becoming visible again and being processed again.
        if (this.maxDurationOfMessage < new Date().getTime() - startTime) {
          this._deleteMessage(message, queueUrl);
          return;
        }

        this.sqs.changeMessageVisibility({
          QueueUrl: queueUrl,
          ReceiptHandle: message.ReceiptHandle,
          VisibilityTimeout: this.visibilityTimeout
        }, err => {
          if (hasDecremented) return;
          debug(err, message);
        });

        keepAliveTimeout = setTimeout(keepAlive, this.visibilityTimeout * VISIBILITY_TIMEOUT_FACTOR * 1000);
      };

  let done = err => {
    if (!hasDecremented) {
      this.numberActive--;
      hasDecremented = true;
    }
    clearTimeout(keepAliveTimeout);

    if (err) {
      if (err.name === SQSError.name) {
        this.emit('error', err, message);
      } else {
        this.emit('processing_error', err, message);
      }

      if (this.terminateVisibilityTimeout) {
        this._cancelProcessingMessage(message, queueUrl);
      }
      else {
        this._poll();
      }
    }
    else {
      this._deleteMessage(message, queueUrl);
      this.emit('message_processed', message);
    }
  };

  keepAlive();

  try {
    this.handleMessage(message, done, queueUrl);
  }
  catch (err) {
    done(new Error('Unexpected message handler failure: ' + err.message));
  }
};

Consumer.prototype._deleteMessage = function (message, queueUrl) {
  var deleteParams = {
    QueueUrl: queueUrl,
    ReceiptHandle: message.ReceiptHandle
  };

  debug('Deleting message %s', message.MessageId);
  this.sqs.deleteMessage(deleteParams, err => {
    if (err) this.emit('error', new SQSError('SQS delete message failed: ' + err.message));
    this._poll();
  });
};

Consumer.prototype._cancelProcessingMessage = function (message, queueUrl) {
  this.sqs.changeMessageVisibility({
    QueueUrl: queueUrl,
    ReceiptHandle: message.ReceiptHandle,
    VisibilityTimeout: 0
  }, err => {
    if (err) this.emit('error', err, message);
    this._poll();
  });
};

module.exports = Consumer;
