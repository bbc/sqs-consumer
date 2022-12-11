import {
  SQSClient,
  Message,
  ChangeMessageVisibilityCommand,
  ChangeMessageVisibilityCommandInput,
  ChangeMessageVisibilityCommandOutput,
  ChangeMessageVisibilityBatchCommand,
  ChangeMessageVisibilityBatchCommandInput,
  ChangeMessageVisibilityBatchCommandOutput,
  DeleteMessageCommand,
  DeleteMessageCommandInput,
  DeleteMessageBatchCommand,
  DeleteMessageBatchCommandInput,
  ReceiveMessageCommand,
  ReceiveMessageCommandInput,
  ReceiveMessageCommandOutput
} from '@aws-sdk/client-sqs';
import Debug from 'debug';
import { EventEmitter } from 'events';

import { AWSError } from './types';
import { autoBind } from './bind';
import { SQSError, TimeoutError } from './errors';

const debug = Debug('sqs-consumer');

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
      throw new Error(
        `Missing SQS consumer option [ ${possibilities.join(' or ')} ].`
      );
    }
  });

  if (options.batchSize > 10 || options.batchSize < 1) {
    throw new Error('SQS batchSize option must be between 1 and 10.');
  }

  if (
    options.heartbeatInterval &&
    !(options.heartbeatInterval < options.visibilityTimeout)
  ) {
    throw new Error('heartbeatInterval must be less than visibilityTimeout.');
  }
}

function isConnectionError(err: Error): boolean {
  if (err instanceof SQSError) {
    return (
      err.statusCode === 403 ||
      err.code === 'CredentialsError' ||
      err.code === 'UnknownEndpoint'
    );
  }
  return false;
}

function toSQSError(err: AWSError, message: string): SQSError {
  const sqsError = new SQSError(message);
  sqsError.code = err.name;
  sqsError.statusCode = err.$metadata?.httpStatusCode;
  sqsError.retryable = err.$retryable?.throttling;
  sqsError.service = err.$service;
  sqsError.fault = err.$fault;
  sqsError.time = new Date();

  return sqsError;
}

function hasMessages(response: ReceiveMessageCommandOutput): boolean {
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
  pollingWaitTimeMs?: number;
  terminateVisibilityTimeout?: boolean;
  heartbeatInterval?: number;
  sqs?: SQSClient;
  region?: string;
  handleMessageTimeout?: number;
  shouldDeleteMessages?: boolean;
  handleMessage?(message: Message): Promise<void>;
  handleMessageBatch?(messages: Message[]): Promise<void>;
}

interface Events {
  response_processed: [];
  empty: [];
  message_received: [Message];
  message_processed: [Message];
  error: [Error, void | Message | Message[]];
  timeout_error: [Error, Message];
  processing_error: [Error, Message];
  stopped: [];
}

export class Consumer extends EventEmitter {
  private queueUrl: string;
  private handleMessage: (message: Message) => Promise<void>;
  private handleMessageBatch: (message: Message[]) => Promise<void>;
  private handleMessageTimeout: number;
  private attributeNames: string[];
  private messageAttributeNames: string[];
  private stopped: boolean;
  private batchSize: number;
  private visibilityTimeout: number;
  private waitTimeSeconds: number;
  private authenticationErrorTimeout: number;
  private pollingWaitTimeMs: number;
  private terminateVisibilityTimeout: boolean;
  private heartbeatInterval: number;
  private sqs: SQSClient;
  private shouldDeleteMessages: boolean;

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
    this.visibilityTimeout = options.visibilityTimeout;
    this.terminateVisibilityTimeout =
      options.terminateVisibilityTimeout || false;
    this.heartbeatInterval = options.heartbeatInterval;
    this.waitTimeSeconds = options.waitTimeSeconds ?? 20;
    this.authenticationErrorTimeout =
      options.authenticationErrorTimeout ?? 10000;
    this.pollingWaitTimeMs = options.pollingWaitTimeMs ?? 0;
    this.shouldDeleteMessages = options.shouldDeleteMessages ?? true;

    this.sqs =
      options.sqs ||
      new SQSClient({
        region: options.region || process.env.AWS_REGION || 'eu-west-1'
      });

    autoBind(this);
  }

  emit<T extends keyof Events>(event: T, ...args: Events[T]) {
    return super.emit(event, ...args);
  }

  on<T extends keyof Events>(
    event: T,
    listener: (...args: Events[T]) => void
  ): this {
    return super.on(event, listener);
  }

  once<T extends keyof Events>(
    event: T,
    listener: (...args: Events[T]) => void
  ): this {
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

  private async handleSqsResponse(
    response: ReceiveMessageCommandOutput
  ): Promise<void> {
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

  private async processMessage(message: Message): Promise<void> {
    this.emit('message_received', message);

    let heartbeat;
    try {
      if (this.heartbeatInterval) {
        heartbeat = this.startHeartbeat(async () => {
          return this.changeVisibilityTimeout(message, this.visibilityTimeout);
        });
      }
      await this.executeHandler(message);
      await this.deleteMessage(message);
      this.emit('message_processed', message);
    } catch (err) {
      this.emitError(err, message);

      if (this.terminateVisibilityTimeout) {
        await this.changeVisibilityTimeout(message, 0);
      }
    } finally {
      clearInterval(heartbeat);
    }
  }

  private async receiveMessage(
    params: ReceiveMessageCommandInput
  ): Promise<ReceiveMessageCommandOutput> {
    try {
      return await this.sqs.send(new ReceiveMessageCommand(params));
    } catch (err) {
      throw toSQSError(err, `SQS receive message failed: ${err.message}`);
    }
  }

  private async deleteMessage(message: Message): Promise<void> {
    if (!this.shouldDeleteMessages) {
      debug(
        'Skipping message delete since shouldDeleteMessages is set to false'
      );
      return;
    }
    debug('Deleting message %s', message.MessageId);

    const deleteParams: DeleteMessageCommandInput = {
      QueueUrl: this.queueUrl,
      ReceiptHandle: message.ReceiptHandle
    };

    try {
      await this.sqs.send(new DeleteMessageCommand(deleteParams));
    } catch (err) {
      throw toSQSError(err, `SQS delete message failed: ${err.message}`);
    }
  }

  private async executeHandler(message: Message): Promise<void> {
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

  private async changeVisibilityTimeout(
    message: Message,
    timeout: number
  ): Promise<ChangeMessageVisibilityCommandOutput> {
    try {
      const input: ChangeMessageVisibilityCommandInput = {
        QueueUrl: this.queueUrl,
        ReceiptHandle: message.ReceiptHandle,
        VisibilityTimeout: timeout
      };
      return await this.sqs.send(new ChangeMessageVisibilityCommand(input));
    } catch (err) {
      this.emit(
        'error',
        toSQSError(err, `Error changing visibility timeout: ${err.message}`),
        message
      );
    }
  }

  private emitError(err: Error, message: Message): void {
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
      this.emit('stopped');
      return;
    }

    debug('Polling for messages');
    const receiveParams: ReceiveMessageCommandInput = {
      QueueUrl: this.queueUrl,
      AttributeNames: this.attributeNames,
      MessageAttributeNames: this.messageAttributeNames,
      MaxNumberOfMessages: this.batchSize,
      WaitTimeSeconds: this.waitTimeSeconds,
      VisibilityTimeout: this.visibilityTimeout
    };

    let currentPollingTimeout = this.pollingWaitTimeMs;
    this.receiveMessage(receiveParams)
      .then(this.handleSqsResponse)
      .catch((err) => {
        this.emit('error', err);
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
        this.emit('error', err);
      });
  }

  private async processMessageBatch(messages: Message[]): Promise<void> {
    messages.forEach((message) => {
      this.emit('message_received', message);
    });

    let heartbeat;
    try {
      if (this.heartbeatInterval) {
        heartbeat = this.startHeartbeat(async () => {
          return this.changeVisibilityTimeoutBatch(
            messages,
            this.visibilityTimeout
          );
        });
      }
      await this.executeBatchHandler(messages);
      await this.deleteMessageBatch(messages);
      messages.forEach((message) => {
        this.emit('message_processed', message);
      });
    } catch (err) {
      this.emit('error', err, messages);

      if (this.terminateVisibilityTimeout) {
        await this.changeVisibilityTimeoutBatch(messages, 0);
      }
    } finally {
      clearInterval(heartbeat);
    }
  }

  private async deleteMessageBatch(messages: Message[]): Promise<void> {
    if (!this.shouldDeleteMessages) {
      debug(
        'Skipping message delete since shouldDeleteMessages is set to false'
      );
      return;
    }
    debug(
      'Deleting messages %s',
      messages.map((msg) => msg.MessageId).join(' ,')
    );

    const deleteParams: DeleteMessageBatchCommandInput = {
      QueueUrl: this.queueUrl,
      Entries: messages.map((message) => ({
        Id: message.MessageId,
        ReceiptHandle: message.ReceiptHandle
      }))
    };

    try {
      await this.sqs.send(new DeleteMessageBatchCommand(deleteParams));
    } catch (err) {
      throw toSQSError(err, `SQS delete message failed: ${err.message}`);
    }
  }

  private async executeBatchHandler(messages: Message[]): Promise<void> {
    try {
      await this.handleMessageBatch(messages);
    } catch (err) {
      err.message = `Unexpected message handler failure: ${err.message}`;
      throw err;
    }
  }

  private async changeVisibilityTimeoutBatch(
    messages: Message[],
    timeout: number
  ): Promise<ChangeMessageVisibilityBatchCommandOutput> {
    const params: ChangeMessageVisibilityBatchCommandInput = {
      QueueUrl: this.queueUrl,
      Entries: messages.map((message) => ({
        Id: message.MessageId,
        ReceiptHandle: message.ReceiptHandle,
        VisibilityTimeout: timeout
      }))
    };
    try {
      return await this.sqs.send(
        new ChangeMessageVisibilityBatchCommand(params)
      );
    } catch (err) {
      this.emit(
        'error',
        toSQSError(err, `Error changing visibility timeout: ${err.message}`),
        messages
      );
    }
  }

  private startHeartbeat(heartbeatFn: () => void): NodeJS.Timeout {
    return setInterval(() => {
      heartbeatFn();
    }, this.heartbeatInterval * 1000);
  }
}
