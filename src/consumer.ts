const debug = require('debug')('sqs-consumer');

// tslint:disable:no-submodule-imports
import { AWSError } from 'aws-sdk';
import * as SQS from 'aws-sdk/clients/sqs';
import { PromiseResult } from 'aws-sdk/lib/request';
import { EventEmitter } from 'events';
import { autoBind } from './bind';
import { SQSError, TimeoutError } from './errors';

type ReceieveMessageResponse = PromiseResult<SQS.Types.ReceiveMessageResult, AWSError>;
type SQSMessage = SQS.Types.Message;
type ReceiveMessageRequest = SQS.Types.ReceiveMessageRequest;

const requiredOptions = [
  'queueUrl',
  'handleMessage'
];

interface TimeoutResonse {
  timeout: NodeJS.Timeout;
  pending: Promise<void>;
}

function createTimeout(duration: number): TimeoutResonse[] {
  let timeout;
  const pending = new Promise((_, reject) => {
    timeout = setTimeout((): void => {
      reject(new TimeoutError());
    }, duration);
  });
  return [timeout, pending];
}

function assertOptions(options: ConsumerOptions): void {
  requiredOptions.forEach((option) => {
    if (!options[option]) {
      throw new Error(`Missing SQS consumer option ['${option}'].`);
    }
  });

  if (options.batchSize > 10 || options.batchSize < 1) {
    throw new Error('SQS batchSize option must be between 1 and 10.');
  }
}

function isAuthenticationError(err: Error): Boolean {
  if (err instanceof SQSError) {
    return (err.statusCode === 403 || err.code === 'CredentialsError');
  }
  return false;
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

function hasMessages(response: ReceieveMessageResponse): boolean {
  return response.Messages && response.Messages.length > 0;
}

export interface ConsumerOptions {
  queueUrl?: string;
  attributeNames?: string[];
  messageAttributeNames?: string[];
  stopped?: boolean;
  batchSize?: number;
  visibilityTimeout?: number;
  waitTimeSeconds?: number;
  authenticationErrorTimeout?: number;
  terminateVisibilityTimeout?: boolean;
  sqs?: SQS;
  region?: string;
  handleMessageTimeout?: number;
  handleMessage(message: SQSMessage): Promise<void>;
}

export class Consumer extends EventEmitter {
  private queueUrl: string;
  private handleMessage: (message: SQSMessage) => Promise<void>;
  private handleMessageTimeout: number;
  private attributeNames: string[];
  private messageAttributeNames: string[];
  private stopped: boolean;
  private batchSize: number;
  private visibilityTimeout: number;
  private waitTimeSeconds: number;
  private authenticationErrorTimeout: number;
  private terminateVisibilityTimeout: boolean;
  private sqs: SQS;

  constructor(options: ConsumerOptions) {
    super();
    assertOptions(options);

    this.queueUrl = options.queueUrl;
    this.handleMessage = options.handleMessage;
    this.handleMessageTimeout = options.handleMessageTimeout;
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

    autoBind(this);
  }

  public static create(options: ConsumerOptions): Consumer {
    return new Consumer(options);
  }

  public async start(): Promise<void> {
    if (this.stopped) {
      debug('Starting consumer');
      this.stopped = false;
      await this.poll();
    }
  }

  public stop(): void {
    debug('Stopping consumer');
    this.stopped = true;
  }

  private async handleSqsResponse(response: ReceieveMessageResponse): Promise<void> {
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

    await this.poll();
  }

  private async processMessage(message: SQSMessage): Promise<void> {
    this.emit('message_received', message);

    try {
      await this.executeHandler(message);
      await this.deleteMessage(message);
      this.emit('message_processed', message);
    } catch (err) {
      this.emitError(err, message);

      if (this.terminateVisibilityTimeout) {
        try {
          await this.terminateVisabilityTimeout(message);
        } catch (err) {
          this.emit('error', err, message);
        }
      }
    }
  }

  private async receiveMessage(params: ReceiveMessageRequest): Promise<ReceieveMessageResponse> {
    try {
      return await this.sqs
        .receiveMessage(params)
        .promise();
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
      await this.sqs
        .deleteMessage(deleteParams)
        .promise();
    } catch (err) {
      throw toSQSError(err, `SQS delete message failed: ${err.message}`);
    }
  }

  private async executeHandler(message: SQSMessage): Promise<void> {
    let timeout;
    let pending;
    try {
      if (this.handleMessageTimeout) {
        [timeout, pending] = createTimeout(this.handleMessageTimeout);
        await Promise.race([
          this.handleMessage(message),
          pending
        ]);
      } else {
        await this.handleMessage(message);
      }
    } catch (err) {
      if (err instanceof TimeoutError) {
        err.message = `Message handler timed out after ${this.handleMessageTimeout}ms: Operation timed out.`;
      } else {
        err.message = `Unexpected message handler failure: ${err.message}`;
      }
      throw err;
    } finally {
      clearTimeout(timeout);
    }
  }

  private async terminateVisabilityTimeout(message: SQSMessage): Promise<PromiseResult<any, AWSError>> {
    return this.sqs
      .changeMessageVisibility({
        QueueUrl: this.queueUrl,
        ReceiptHandle: message.ReceiptHandle,
        VisibilityTimeout: 0
      })
      .promise();
  }

  private emitError(err: Error, message: SQSMessage): void {
    if (err.name === SQSError.name) {
      this.emit('error', err, message);
    } else if (err instanceof TimeoutError) {
      this.emit('timeout_error', err, message);
    } else {
      this.emit('processing_error', err, message);
    }
  }

  private async poll(): Promise<void> {
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
      await this.handleSqsResponse(response);

    } catch (err) {
      this.emit('error', err);
      if (isAuthenticationError(err)) {
        debug('There was an authentication error. Pausing before retrying.');
        setTimeout(async () => this.poll(), await this.authenticationErrorTimeout);
      }
    }
  }
}
