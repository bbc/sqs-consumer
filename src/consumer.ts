'use strict';

const debug = require('debug')('sqs-consumer');

import { AWSError, SQS } from 'aws-sdk';
// tslint:disable-next-line:no-submodule-imports
import { PromiseResult } from 'aws-sdk/lib/request';
import { EventEmitter } from 'events';
import { auto } from './bind';
import { SQSError } from './sqsError';

const requiredOptions = [
  'queueUrl',
  'handleMessage'
];

type ReceieveMessageResponse = PromiseResult<SQS.Types.ReceiveMessageResult, AWSError>;
type SQSMessage = SQS.Types.Message;

function validate(options: any): void {
  requiredOptions.forEach((option) => {
    if (!options[option]) {
      throw new Error(`Missing SQS consumer option [' + ${option} + ]. `);
    }
  });

  if (options.batchSize > 10 || options.batchSize < 1) {
    throw new Error('SQS batchSize option must be between 1 and 10.');
  }
}

function isAuthenticationError(err: Error): boolean {
  if (err instanceof SQSError) {
    const e: SQSError = err;
    return (e.statusCode === 403 || e.code === 'CredentialsError');
  }
  return false;
}

function toSQSError(err: Error, message: string): SQSError {
  if (err instanceof SQSError) {
    const to = new SQSError(message);
    const from: SQSError = err;
    to.code = from.code;
    to.statusCode = from.statusCode;
    to.region = from.region;
    to.retryable = from.retryable;
    to.hostname = from.hostname;
    to.time = from.time;
    return to;
  }
  return new SQSError(message);
}

function hasMessages(response: any): boolean {
  return response.Messages && response.Messages.length > 0;
}

export class Consumer extends EventEmitter {
  private queueUrl: string;
  private handleMessage: (message: SQSMessage) => Promise<void>;
  private attributeNames: string[];
  private messageAttributeNames: string[];
  private stopped: boolean;
  private batchSize: number;
  private visibilityTimeout: boolean;
  private waitTimeSeconds: number;
  private authenticationErrorTimeout: number;
  private terminateVisibilityTimeout: boolean;
  private sqs: any;

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

  public start(): void {
    if (this.stopped) {
      debug('Starting consumer');
      this.stopped = false;
      this.poll();
    }
  }

  public stop(): void {
    debug('Stopping consumer');
    this.stopped = true;
  }

  private async receiveMessage(params: any): Promise<ReceieveMessageResponse> {
    try {
      return await this.sqs.receiveMessage(params).promise();
    } catch (err) {
      throw toSQSError(err, `SQS receive message failed: ${err.message}`);
    }
  }

  private async deleteMessage(message: SQSMessage): Promise<void> {
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

  private async handleSQSResponse(response: ReceieveMessageResponse): Promise<void> {
    debug('Received SQS response');
    debug(response);

    if (response) {
      if (hasMessages(response)) {
        await Promise.all(response.Messages.map(this.processMessage));
        this.emit('response_processed');
      } else {
        this.emit('empty');
      }
    }

    this.poll();
  }

  private async processMessage(message: SQSMessage): Promise<void> {
    this.emit('message_received', message);

    try {
      await this.executeHandler(message);
      await this.deleteMessage(message);
      this.emit('message_processed', message);
    } catch (err) {
      if (err.name === SQSError.name) {
        this.emit('error', err, message);
      } else {
        this.emit('processing_error', err, message);
      }

      if (this.terminateVisibilityTimeout) {
        try {
          await this.sqs
            .changeMessageVisibility({
              QueueUrl: this.queueUrl,
              ReceiptHandle: message.ReceiptHandle,
              VisibilityTimeout: 0
            })
            .promise();
        } catch (err) {
          this.emit('error', err, message);
        }
      }
    }
  }

  private async executeHandler(message: SQSMessage): Promise<void> {
    try {
      await this.handleMessage(message);
    } catch (err) {
      throw new Error(`Unexpected message handler failure: ${err.message}`);
    }
  }

  private async poll(): Promise<any> {
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

      const response = await this.receiveMessage(receiveParams);
      this.handleSQSResponse(response);

    } catch (err) {
      this.emit('error', err);
      if (isAuthenticationError(err)) {
        debug('There was an authentication error. Pausing before retrying.');
        return setTimeout(() => this.poll(), this.authenticationErrorTimeout);
      }
    }
  }
}
