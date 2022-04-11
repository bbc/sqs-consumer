import { AWSError } from 'aws-sdk';
import SQS from 'aws-sdk/clients/sqs';
import { PromiseResult } from 'aws-sdk/lib/request';
import Debug from 'debug';
import { EventEmitter } from 'events';
import { autoBind } from './bind';
import { SQSError, TimeoutError } from './errors';
import fastq from 'fastq';

const debug = Debug('sqs-consumer');

type ReceieveMessageResponse = PromiseResult<SQS.Types.ReceiveMessageResult, AWSError>;
type ReceiveMessageRequest = SQS.Types.ReceiveMessageRequest;
export type SQSMessage = SQS.Types.Message;

const requiredOptions = [
  'queueUrl',
  // only one of handleMessage / handleMessagesBatch is required
  'handleMessage|handleMessageBatch'
];

interface TimeoutResponse {
  timeout: NodeJS.Timeout;
  pending: Promise<void>;
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

  if (options.heartbeatInterval && !(options.heartbeatInterval < options.visibilityTimeout)) {
    throw new Error('heartbeatInterval must be less than visibilityTimeout.');
  }
}

function isConnectionError(err: Error): boolean {
  if (err instanceof SQSError) {
    return (err.statusCode === 403 || err.code === 'CredentialsError' || err.code === 'UnknownEndpoint');
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
  concurrency?: number;
  bufferMessages?: boolean;
  visibilityTimeout?: number;
  waitTimeSeconds?: number;
  authenticationErrorTimeout?: number;
  pollingWaitTimeMs?: number;
  terminateVisibilityTimeout?: boolean;
  heartbeatInterval?: number;
  sqs?: SQS;
  region?: string;
  handleMessageTimeout?: number;
  handleMessage?(message: SQSMessage): Promise<void>;
  handleMessageBatch?(messages: SQSMessage[]): Promise<void>;
}

interface Events {
  'response_processed': [];
  'empty': [];
  'message_received': [SQSMessage];
  'message_processed': [SQSMessage];
  'error': [Error, void | SQSMessage | SQSMessage[]];
  'timeout_error': [Error, SQSMessage];
  'processing_error': [Error, SQSMessage];
  'stopped': [];
}

enum POLLING_STATUS {
  ACTIVE,
  WAITING,
  INACTIVE,
  READY
}

export class Consumer extends EventEmitter {
  private queueUrl: string;
  private handleMessage: (message: SQSMessage) => Promise<void>;
  private handleMessageBatch: (message: SQSMessage[]) => Promise<void>;
  private handleMessageTimeout: number;
  private attributeNames: string[];
  private messageAttributeNames: string[];
  private stopped: boolean;
  private batchSize: number;
  private concurrency: number;
  private bufferMessages: boolean;
  private visibilityTimeout: number;
  private waitTimeSeconds: number;
  private authenticationErrorTimeout: number;
  private pollingWaitTimeMs: number;
  private terminateVisibilityTimeout: boolean;
  private heartbeatInterval: number;
  private sqs: SQS;
  private workQueue: fastq.queueAsPromised;
  private pollingStatus: POLLING_STATUS;

  constructor(options: ConsumerOptions) {
    super();
    assertOptions(options);
    this.queueUrl = options.queueUrl;
    this.handleMessage = options.handleMessage;
    this.handleMessageBatch = options.handleMessageBatch;
    this.handleMessageTimeout = options.handleMessageTimeout;
    this.attributeNames = options.attributeNames || [];
    this.messageAttributeNames = options.messageAttributeNames || [];
    this.stopped = true;
    this.batchSize = options.batchSize || 1;
    this.concurrency = options.concurrency || (this.handleMessageBatch ? 1 : this.batchSize);
    this.bufferMessages = options.bufferMessages ?? !!options.concurrency,
    this.visibilityTimeout = options.visibilityTimeout;
    this.terminateVisibilityTimeout = options.terminateVisibilityTimeout || false;
    this.heartbeatInterval = options.heartbeatInterval;
    this.waitTimeSeconds = options.waitTimeSeconds || 20;
    this.authenticationErrorTimeout = options.authenticationErrorTimeout || 10000;
    this.pollingWaitTimeMs = options.pollingWaitTimeMs || 0;
    this.pollingStatus = POLLING_STATUS.INACTIVE;
    this.workQueue = this.handleMessageBatch ?
      fastq.promise(this.executeBatchHandler.bind(this), this.concurrency) : fastq.promise(this.executeHandler.bind(this), this.concurrency);

    this.sqs = options.sqs || new SQS({
      region: options.region || process.env.AWS_REGION || 'eu-west-1'
    });

    autoBind(this);
  }

  emit<T extends keyof Events>(event: T, ...args: Events[T]) {
    return super.emit(event, ...args);
  }

  on<T extends keyof Events>(event: T, listener: (...args: Events[T]) => void): this {
    return super.on(event, listener);
  }

  once<T extends keyof Events>(event: T, listener: (...args: Events[T]) => void): this {
    return super.once(event, listener);
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

  private async handleSqsResponse(response: ReceieveMessageResponse): Promise<void> {
    debug('Received SQS response');
    debug(response);

    if (response) {
      if (hasMessages(response)) {
        if (this.handleMessageBatch) {
          // prefer handling messages in batch when available
          await this.processMessageBatch(response.Messages);
        } else {
          await Promise.all(response.Messages.map(this.processMessage));
        }
        this.emit('response_processed');
      } else {
        this.emit('empty');
      }
    }
  }

  private async processMessage(message: SQSMessage): Promise<void> {
    this.emit('message_received', message);

    let heartbeat;
    try {
      if (this.heartbeatInterval) {
        heartbeat = this.startHeartbeat(async () => {
          return this.changeVisabilityTimeout(message, this.visibilityTimeout);
        });
      }
      debug('pushed');
      await this.workQueue.push(message);
      debug('done');
      await this.deleteMessage(message);
      this.emit('message_processed', message);
    } catch (err) {
      this.emitError(err, message);

      if (this.terminateVisibilityTimeout) {
        await this.changeVisabilityTimeout(message, 0);
      }
    } finally {
      clearInterval(heartbeat);
      this.queuePoll();
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
      } else if (err instanceof Error) {
        err.message = `Unexpected message handler failure: ${err.message}`;
      }
      throw err;
    } finally {
      clearTimeout(timeout);
    }
  }

  private async changeVisabilityTimeout(message: SQSMessage, timeout: number): Promise<PromiseResult<any, AWSError>> {
    try {
      return this.sqs
        .changeMessageVisibility({
          QueueUrl: this.queueUrl,
          ReceiptHandle: message.ReceiptHandle,
          VisibilityTimeout: timeout
        })
        .promise();
    } catch (err) {
      this.emit('error', err, message);
    }
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

  private poll(): void {
    if (this.stopped) {
      this.pollingStatus === POLLING_STATUS.INACTIVE;
      this.emit('stopped');
      return;
    }

    if (this.pollingStatus === POLLING_STATUS.ACTIVE) {
      debug('sqs polling already in progress');
      return;
    }

    if (!this.bufferMessages && (this.workQueue as any).running() > 0) {
      debug('work queue is not yet empty. not polling');
      this.pollingStatus = POLLING_STATUS.READY;
      return;
    }

    if (this.workQueue.length() > 0) {
      debug('unstarted work in queue. not polling');
      this.pollingStatus = POLLING_STATUS.READY;
      return;
    }

    if ((this.workQueue as any).running() >= this.concurrency) {
      debug('work queue at capacity, no need to poll');
      this.pollingStatus = POLLING_STATUS.READY;
      return;
    }

    this.pollingStatus = POLLING_STATUS.ACTIVE;
    debug('Polling for messages');
    const receiveParams = {
      QueueUrl: this.queueUrl,
      AttributeNames: this.attributeNames,
      MessageAttributeNames: this.messageAttributeNames,
      MaxNumberOfMessages: this.batchSize,
      WaitTimeSeconds: this.waitTimeSeconds,
      VisibilityTimeout: this.visibilityTimeout
    };

    let currentPollingTimeout = this.pollingWaitTimeMs;
    this.receiveMessage(receiveParams)
      .catch((err) => {
        this.emit('error', err);
        if (isConnectionError(err)) {
          debug('There was an authentication error. Pausing before retrying.');
          currentPollingTimeout = this.authenticationErrorTimeout;
        }
        return;
      })
      .then((message) => {
        this.queuePoll(currentPollingTimeout);
        if (message) return this.handleSqsResponse(message);
      })
      .catch((err) => {
        this.emit('error', err);
      }).finally(() => {
        if (this.pollingStatus === POLLING_STATUS.ACTIVE) {
          this.pollingStatus = POLLING_STATUS.INACTIVE;
        }
      });
  }

  private queuePoll(timeout?: number) {
    if (this.pollingStatus !== POLLING_STATUS.WAITING) {
      this.pollingStatus = POLLING_STATUS.WAITING;
      setTimeout(this.poll, timeout ?? this.pollingWaitTimeMs);
    }
  }

  private async processMessageBatch(messages: SQSMessage[]): Promise<void> {
    messages.forEach((message) => {
      this.emit('message_received', message);
    });

    let heartbeat;
    try {
      if (this.heartbeatInterval) {
        heartbeat = this.startHeartbeat(async () => {
          return this.changeVisabilityTimeoutBatch(messages, this.visibilityTimeout);
        });
      }
      await this.workQueue.push(messages);
      await this.deleteMessageBatch(messages);
      messages.forEach((message) => {
        this.emit('message_processed', message);
      });
    } catch (err) {
      this.emit('error', err, messages);

      if (this.terminateVisibilityTimeout) {
        await this.changeVisabilityTimeoutBatch(messages, 0);
      }
    } finally {
      clearInterval(heartbeat);
      this.queuePoll();
    }
  }

  private async deleteMessageBatch(messages: SQSMessage[]): Promise<void> {
    debug('Deleting messages %s', messages.map((msg) => msg.MessageId).join(' ,'));

    const deleteParams = {
      QueueUrl: this.queueUrl,
      Entries: messages.map((message) => ({
        Id: message.MessageId,
        ReceiptHandle: message.ReceiptHandle
      }))
    };

    try {
      await this.sqs
        .deleteMessageBatch(deleteParams)
        .promise();
    } catch (err) {
      throw toSQSError(err, `SQS delete message failed: ${err.message}`);
    }
  }

  private async executeBatchHandler(messages: SQSMessage[]): Promise<void> {
    try {
      await this.handleMessageBatch(messages);
    } catch (err) {
      err.message = `Unexpected message handler failure: ${err.message}`;
      throw err;
    }
  }

  private async changeVisabilityTimeoutBatch(messages: SQSMessage[], timeout: number): Promise<PromiseResult<any, AWSError>> {
    const params = {
      QueueUrl: this.queueUrl,
      Entries: messages.map((message) => ({
        Id: message.MessageId,
        ReceiptHandle: message.ReceiptHandle,
        VisibilityTimeout: timeout
      }))
    };
    try {
      return this.sqs
        .changeMessageVisibilityBatch(params)
        .promise();
    } catch (err) {
      this.emit('error', err, messages);
    }
  }

  private startHeartbeat(heartbeatFn: () => void): NodeJS.Timeout {
    return setInterval(() => {
      heartbeatFn();
    }, this.heartbeatInterval * 1000);
  }
}
