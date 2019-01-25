const debug = require('debug')('sqs-consumer');

import { AWSError, SQS } from 'aws-sdk';
// tslint:disable-next-line:no-submodule-imports
import { PromiseResult } from 'aws-sdk/lib/request';
import { EventEmitter } from 'events';
import { auto } from './bind';
import { SQSError } from './sqsError';

type ReceieveMessageResponse = PromiseResult<SQS.Types.ReceiveMessageResult, AWSError>;
type SQSMessage = SQS.Types.Message;

const requiredOptions = [
  'queueUrl',
  'handleMessage'
];

function validate(options: any): void {
  requiredOptions.forEach((option) => {
    if (!options[option]) {
      throw new Error(`Missing SQS consumer option ['${option}'].`);
    }
  });

  if (options.batchSize > 10 || options.batchSize < 1) {
    throw new Error('SQS batchSize option must be between 1 and 10.');
  }
}

function isAuthenticationError(err: any): Boolean {
  return (err.statusCode === 403 || err.code === 'CredentialsError');
}

function toSQSError(err: AWSError, message: string): SQSError {
  const sqsError = new SQSError(message);
  sqsError.code = err.code;
  sqsError.statusCode = err.statusCode;
  sqsError.region = err.region;
  sqsError.retryable = err.retryable;
  sqsError.hostname = err.hostname;
  sqsError.time = err.time;

  return sqsError;
}

function hasMessages(response: any): any {
  return response.Messages && response.Messages.length > 0;
}

export class Consumer extends EventEmitter {
  private queueUrl: string;
  private handleMessage: (message: any) => Promise<void>;
  private attributeNames: string[];
  private messageAttributeNames: string[];
  private stopped: boolean;
  private batchSize: number;
  private visibilityTimeout: boolean;
  private waitTimeSeconds: number;
  private authenticationErrorTimeout: number;
  private terminateVisibilityTimeout: boolean;
  private sqs: SQS;

  constructor(options: any) {

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

  static create(options: any): Consumer {
    return new Consumer(options);
  }

  start(): void {
    if (this.stopped) {
      debug('Starting consumer');
      this.stopped = false;
      this._poll();
    }
  }

  stop(): void {
    debug('Stopping consumer');
    this.stopped = true;
  }

  async _poll(): Promise<void> {
    if (this.stopped) {
      this.emit('stopped');
      return;
    }

    debug('Polling for messages');
    try {
      const receiveParams = {
        QueueUrl: this.queueUrl,
        AttributeNames: this.attributeNames,
        MessageAttributeNames: this.messageAttributeNames,
        MaxNumberOfMessages: this.batchSize,
        WaitTimeSeconds: this.waitTimeSeconds,
        VisibilityTimeout: this.visibilityTimeout
      };

      const response = await this._receiveMessage(receiveParams);
      this._handleSqsResponse(response);

    } catch (err) {
      this.emit('error', err);
      if (isAuthenticationError(err)) {
        debug('There was an authentication error. Pausing before retrying.');
        setTimeout(() => this._poll(), this.authenticationErrorTimeout);
        return;
      }
    }
  }

  async _handleSqsResponse(response: ReceieveMessageResponse): Promise<void> {
    debug('Received SQS response');
    debug(response);

    if (response) {
      if (hasMessages(response)) {
        await Promise.all(response.Messages.map(this._processMessage));
        this.emit('response_processed');
      } else {
        this.emit('empty');
      }
    }

    this._poll();
  }

  async _processMessage(message: SQSMessage): Promise<void> {
    this.emit('message_received', message);

    try {
      await this._executeHandler(message);
      await this._deleteMessage(message);
      this.emit('message_processed', message);
    } catch (err) {
      if (err.name === SQSError.name) {
        this.emit('error', err, message);
      } else {
        this.emit('processing_error', err, message);
      }

      if (this.terminateVisibilityTimeout) {
        try {
          await this._terminateVisabilityTimeout(message);
        } catch (err) {
          this.emit('error', err, message);
        }
      }
    }
  }

  async _receiveMessage(params: any): Promise<ReceieveMessageResponse> {
    try {
      return await this.sqs.receiveMessage(params).promise();
    } catch (err) {
      throw toSQSError(err, `SQS receive message failed: ${err.message}`);
    }
  }

  async _terminateVisabilityTimeout(message: SQSMessage): Promise<PromiseResult<any, AWSError>> {
    return this.sqs
      .changeMessageVisibility({
        QueueUrl: this.queueUrl,
        ReceiptHandle: message.ReceiptHandle,
        VisibilityTimeout: 0
      })
      .promise();
  }

  async _deleteMessage(message: SQSMessage): Promise<void> {
    debug('Deleting message %s', message.MessageId);

    const deleteParams = {
      QueueUrl: this.queueUrl,
      ReceiptHandle: message.ReceiptHandle
    };

    try {
      await this.sqs.deleteMessage(deleteParams).promise();
    } catch (err) {
      throw toSQSError(err, `SQS delete message failed: ${err.message}`);
    }
  }

  async _executeHandler(message: any): Promise<any> {
    try {
      await this.handleMessage(message);
    } catch (err) {
      err.message = `Unexpected message handler failure: ${err.message}`;
      throw err;
    }
  }
}
