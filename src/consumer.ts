import { AWSError } from 'aws-sdk';
import * as SQS from 'aws-sdk/clients/sqs';
import { PromiseResult } from 'aws-sdk/lib/request';
import * as Debug from 'debug';
import * as crypto from 'crypto';
import { EventEmitter } from 'events';
import { autoBind } from './bind';
import { SQSError, TimeoutError } from './errors';

const debug = Debug('sqs-consumer');

type ReceieveMessageResponse = PromiseResult<SQS.Types.ReceiveMessageResult, AWSError>;
type SQSMessage = SQS.Types.Message;
type ReceiveMessageRequest = SQS.Types.ReceiveMessageRequest;

const requiredOptions = [
  'queueUrl',
  // only one of handleMessage / handleMessagesBatch is required
  'handleMessage|handleMessageBatch',
];

interface TimeoutResponse {
  timeout: NodeJS.Timeout;
  pending: Promise<void>;
}

function generateUuid(): string {
  return crypto.randomBytes(16).toString('hex');
}

function createTimeout(duration: number): TimeoutResponse[] {
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
    const possibilities = option.split('|');
    if (!possibilities.find((p) => options[p])) {
      throw new Error(`Missing SQS consumer option [ ${possibilities.join(' or ')} ].`);
    }
  });

  if (options.batchSize > 10 || options.batchSize < 1) {
    throw new Error('SQS batchSize option must be between 1 and 10.');
  }
}

function isConnectionError(err: Error): Boolean {
  if (err instanceof SQSError) {
    return err.statusCode === 403 || err.code === 'CredentialsError' || err.code === 'UnknownEndpoint';
  }
  return false;
}

function isNonExistentQueueError(err: Error): Boolean {
  if (err instanceof SQSError) {
    return err.code === 'AWS.SimpleQueueService.NonExistentQueue';
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

function addMessageUuidToError(error, message): void {
  try {
    const messageBody = JSON.parse(message.Body);
    const messageUuid = messageBody && messageBody.payload && messageBody.payload.uuid;

    error.messageUuid = messageUuid;
  } catch (err) {}
}

export interface ConsumerOptions {
  queueUrl?: string;
  attributeNames?: string[];
  messageAttributeNames?: string[];
  stopped?: boolean;
  concurrencyLimit?: number;
  batchSize?: number;
  visibilityTimeout?: number;
  waitTimeSeconds?: number;
  authenticationErrorTimeout?: number;
  pollingWaitTimeMs?: number;
  msDelayOnEmptyBatchSize?: number;
  terminateVisibilityTimeout?: boolean;
  sqs?: SQS;
  region?: string;
  handleMessageTimeout?: number;
  handleMessage?(message: SQSMessage): Promise<void>;
  handleMessageBatch?(messages: SQSMessage[], consumer: Consumer): Promise<void>;
  pollingStartedInstrumentCallback?(eventData: object): void;
  pollingFinishedInstrumentCallback?(eventData: object): void;
  batchStartedInstrumentCallBack?(eventData: object): void;
  batchFinishedInstrumentCallBack?(eventData: object): void;
  batchFailedInstrumentCallBack?(eventData: object): void;
}

export class Consumer extends EventEmitter {
  private queueUrl: string;
  private handleMessage: (message: SQSMessage) => Promise<void>;
  private handleMessageBatch: (message: SQSMessage[], consumer: Consumer) => Promise<void>;
  private pollingStartedInstrumentCallback?: (eventData: object) => void;
  private pollingFinishedInstrumentCallback?: (eventData: object) => void;
  private batchStartedInstrumentCallBack?: (eventData: object) => void;
  private batchFinishedInstrumentCallBack?: (eventData: object) => void;
  private batchFailedInstrumentCallBack?: (eventData: object) => void;
  private handleMessageTimeout: number;
  private attributeNames: string[];
  private messageAttributeNames: string[];
  private stopped: boolean;
  private concurrencyLimit: number;
  private freeConcurrentSlots: number;
  private batchSize: number;
  private visibilityTimeout: number;
  private waitTimeSeconds: number;
  private authenticationErrorTimeout: number;
  private pollingWaitTimeMs: number;
  private msDelayOnEmptyBatchSize: number;
  private terminateVisibilityTimeout: boolean;
  private sqs: SQS;

  constructor(options: ConsumerOptions) {
    super();
    assertOptions(options);
    this.queueUrl = options.queueUrl;
    this.handleMessage = options.handleMessage;
    this.handleMessageBatch = options.handleMessageBatch;
    this.pollingStartedInstrumentCallback = options.pollingStartedInstrumentCallback;
    this.pollingFinishedInstrumentCallback = options.pollingFinishedInstrumentCallback;
    this.batchStartedInstrumentCallBack = options.batchStartedInstrumentCallBack;
    this.batchFinishedInstrumentCallBack = options.batchFinishedInstrumentCallBack;
    this.batchFailedInstrumentCallBack = options.batchFailedInstrumentCallBack;
    this.handleMessageTimeout = options.handleMessageTimeout;
    this.attributeNames = options.attributeNames || [];
    this.messageAttributeNames = options.messageAttributeNames || [];
    this.stopped = true;
    this.batchSize = options.batchSize || 1;
    this.concurrencyLimit = options.concurrencyLimit || 30;
    this.freeConcurrentSlots = this.concurrencyLimit;
    this.visibilityTimeout = options.visibilityTimeout;
    this.terminateVisibilityTimeout = options.terminateVisibilityTimeout || false;
    this.waitTimeSeconds = options.waitTimeSeconds || 20;
    this.authenticationErrorTimeout = options.authenticationErrorTimeout || 10000;
    this.pollingWaitTimeMs = options.pollingWaitTimeMs || 0;
    this.msDelayOnEmptyBatchSize = options.msDelayOnEmptyBatchSize || 5;

    this.sqs =
      options.sqs ||
      new SQS({
        region: options.region || process.env.AWS_REGION || 'eu-west-1',
      });

    autoBind(this);
  }

  public get isRunning(): boolean {
    return !this.stopped;
  }

  public static create(options: ConsumerOptions): Consumer {
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

  public setBatchSize(newBatchSize: number): void {
    this.batchSize = newBatchSize;
  }

  public setConcurrencyLimit(newConcurrencyLimit: number): void {
    const concurrencyLimitDiff = newConcurrencyLimit - this.concurrencyLimit;
    const newFreeConcurrentSlots = Math.max(0, this.freeConcurrentSlots + concurrencyLimitDiff);

    this.concurrencyLimit = newConcurrencyLimit;
    this.freeConcurrentSlots = newFreeConcurrentSlots;
  }

  public async reportMessageFromBatchFinished(message: SQSMessage, error: Error): Promise<void> {
    debug('Message from batch has finished');

    this.freeConcurrentSlots++;

    try {
      if (error) throw error;

      await this.deleteMessage(message);
      this.emit('message_processed', message, this.queueUrl);
    } catch (err) {
      this.emitError(err, message);
    }
  }

  private reportNumberOfMessagesReceived(numberOfMessages: number): void {
    debug('Reducing number of messages received from freeConcurrentSlots');
    this.freeConcurrentSlots = this.freeConcurrentSlots - numberOfMessages;
  }

  private async handleSqsResponse(response: ReceieveMessageResponse): Promise<void> {
    debug('Received SQS response');
    debug(response);

    const hasResponseWithMessages = !!response && hasMessages(response);
    const numberOfMessages = hasResponseWithMessages ? response.Messages.length : 0;

    if (this.pollingFinishedInstrumentCallback) {
      // instrument pod how many messages received
      this.pollingFinishedInstrumentCallback({
        instanceId: process.env.HOSTNAME,
        queueUrl: this.queueUrl,
        messagesReceived: numberOfMessages,
        freeConcurrentSlots: this.freeConcurrentSlots,
      });
    }

    if (response) {
      if (hasMessages(response)) {
        if (this.handleMessageBatch) {
          // prefer handling messages in batch when available
          await this.processMessageBatch(response.Messages);
        } else {
          await Promise.all(response.Messages.map(this.processMessage));
        }
        this.emit('response_processed', this.queueUrl);
      } else {
        this.emit('empty', this.queueUrl);
      }
    }
  }

  private async processMessage(message: SQSMessage): Promise<void> {
    this.emit('message_received', message, this.queueUrl);

    try {
      await this.executeHandler(message);
      await this.deleteMessage(message);
      this.emit('message_processed', message, this.queueUrl);
    } catch (err) {
      this.emitError(err, message);

      if (this.terminateVisibilityTimeout) {
        try {
          await this.terminateVisabilityTimeout(message);
        } catch (err) {
          this.emit('error', err, message, this.queueUrl);
        }
      }
    }
  }

  private async receiveMessage(params: ReceiveMessageRequest): Promise<ReceieveMessageResponse> {
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
      ReceiptHandle: message.ReceiptHandle,
    };

    try {
      await this.sqs.deleteMessage(deleteParams).promise();
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
        await Promise.race([this.handleMessage(message), pending]);
      } else {
        await this.handleMessage(message);
      }
    } catch (err) {
      addMessageUuidToError(err, message);
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
        VisibilityTimeout: 0,
      })
      .promise();
  }

  private emitError(err: Error, message: SQSMessage): void {
    if (err.name === SQSError.name) {
      this.emit('error', err, message, this.queueUrl);
    } else if (err instanceof TimeoutError) {
      this.emit('timeout_error', err, message, this.queueUrl);
    } else {
      this.emit('processing_error', err, message, this.queueUrl);
    }
  }

  private poll(): void {
    if (this.stopped) {
      this.emit('stopped', this.queueUrl);
      return;
    }

    const pollBatchSize = Math.min(this.batchSize, this.freeConcurrentSlots);

    debug('Polling for messages');
    if (this.pollingStartedInstrumentCallback) {
      this.pollingStartedInstrumentCallback({
        instanceId: process.env.HOSTNAME,
        queueUrl: this.queueUrl,
        pollBatchSize,
        freeConcurrentSlots: this.freeConcurrentSlots,
      });
    }

    let currentPollingTimeout = this.pollingWaitTimeMs;

    if (pollBatchSize > 0) {
      const receiveParams = {
        QueueUrl: this.queueUrl,
        AttributeNames: this.attributeNames,
        MessageAttributeNames: this.messageAttributeNames,
        MaxNumberOfMessages: pollBatchSize,
        WaitTimeSeconds: this.waitTimeSeconds,
        VisibilityTimeout: this.visibilityTimeout,
      };

      this.receiveMessage(receiveParams)
        .then(this.handleSqsResponse)
        .catch((err) => {
          this.emit('unhandled_error', err, this.queueUrl);
          if (isNonExistentQueueError(err)) {
            throw new Error(`Could not receive messages - non existent queue - ${this.queueUrl}`);
          }
          if (isConnectionError(err)) {
            debug('There was an authentication error. Pausing before retrying.');
            currentPollingTimeout = this.authenticationErrorTimeout;
          }

          return;
        })
        .then(() => {
          setTimeout(this.poll, currentPollingTimeout);
        })
        .catch((err) => {
          this.emit('unhandled_error', err, this.queueUrl);
        });
    } else {
      setTimeout(this.poll, this.msDelayOnEmptyBatchSize);
    }
  }

  private async processMessageBatch(messages: SQSMessage[]): Promise<void> {
    messages.forEach((message) => {
      this.emit('message_received', message, this.queueUrl);
    });

    this.reportNumberOfMessagesReceived(messages.length);
    const batchUuid = generateUuid();

    if (this.batchStartedInstrumentCallBack) {
      this.batchStartedInstrumentCallBack({
        instanceId: process.env.HOSTNAME,
        queueUrl: this.queueUrl,
        batchUuid,
        numberOfMessages: messages.length,
        freeConcurrentSlots: this.freeConcurrentSlots,
      });
    }

    this.handleMessageBatch(messages, this)
      .then(() => {
        if (this.batchFinishedInstrumentCallBack) {
          this.batchFinishedInstrumentCallBack({
            instanceId: process.env.HOSTNAME,
            queueUrl: this.queueUrl,
            batchUuid,
            numberOfMessages: messages.length,
            freeConcurrentSlots: this.freeConcurrentSlots,
          });
        }
      })
      .catch((err) => {
        if (this.batchFailedInstrumentCallBack) {
          this.batchFailedInstrumentCallBack({
            instanceId: process.env.HOSTNAME,
            queueUrl: this.queueUrl,
            batchUuid,
            numberOfMessages: messages.length,
            freeConcurrentSlots: this.freeConcurrentSlots,
            error: err,
          });
        }
      });
  }
}
