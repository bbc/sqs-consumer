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

import { ConsumerOptions, Events } from './types';
import { createTimeout } from './timeout';
import { autoBind } from './bind';
import {
  SQSError,
  TimeoutError,
  toSQSError,
  isConnectionError
} from './errors';
import { assertOptions, hasMessages } from './validation';

const debug = Debug('sqs-consumer');

/**
 * [Usage](https://bbc.github.io/sqs-consumer/index.html#usage)
 */
export class Consumer extends EventEmitter {
  private queueUrl: string;
  private handleMessage: (message: Message) => Promise<Message | void>;
  private handleMessageBatch: (message: Message[]) => Promise<Message[] | void>;
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

  /**
   * Emits an event with the provided arguments
   * @param event The name of the event to emit
   */
  emit<T extends keyof Events>(event: T, ...args: Events[T]) {
    return super.emit(event, ...args);
  }

  /**
   * Trigger a listener on all emitted events
   * @param event The name of the event to listen to
   * @param listener A function to trigger when the event is emitted
   */
  on<T extends keyof Events>(
    event: T,
    listener: (...args: Events[T]) => void
  ): this {
    return super.on(event, listener);
  }

  /**
   * Trigger a listener only once for an emitted event
   * @param event The name of the event to listen to
   * @param listener A function to trigger when the event is emitted
   */
  once<T extends keyof Events>(
    event: T,
    listener: (...args: Events[T]) => void
  ): this {
    return super.once(event, listener);
  }

  /**
   * Returns the current polling state of the consumer: `true` if it is actively polling, `false` if it is not.
   */
  public get isRunning(): boolean {
    return !this.stopped;
  }

  /**
   * Creates a new SQS consumer.
   */
  public static create(options: ConsumerOptions): Consumer {
    return new Consumer(options);
  }

  /**
   * Start polling the queue for messages.
   */
  public start(): void {
    if (this.stopped) {
      debug('Starting consumer');
      this.stopped = false;
      this.poll();
    }
  }

  /**
   * Stop polling the queue for messages (pre existing requests will still be made until concluded).
   */
  public stop(): void {
    debug('Stopping consumer');
    this.stopped = true;
  }

  /**
   * Handles the response from AWS SQS, determining if we should proceed to
   * the message handler.
   * @param response The output from AWS SQS
   */
  private async handleSqsResponse(
    response: ReceiveMessageCommandOutput
  ): Promise<void> {
    debug('Received SQS response');
    debug(response);

    if (response) {
      if (hasMessages(response)) {
        if (this.handleMessageBatch) {
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

  /**
   * Process a message that has been received from SQS. This will execute the message
   * handler and delete the message once complete.
   * @param message The message that was delivered from SQS
   */
  private async processMessage(message: Message): Promise<void> {
    this.emit('message_received', message);

    let heartbeat;
    try {
      if (this.heartbeatInterval) {
        heartbeat = this.startHeartbeat(async () => {
          return this.changeVisibilityTimeout(message, this.visibilityTimeout);
        });
      }

      const ackedMessage = await this.executeHandler(message);

      if (ackedMessage?.MessageId === message.MessageId) {
        await this.deleteMessage(message);

        this.emit('message_processed', message);
      }
    } catch (err) {
      this.emitError(err, message);

      if (this.terminateVisibilityTimeout) {
        await this.changeVisibilityTimeout(message, 0);
      }
    } finally {
      clearInterval(heartbeat);
    }
  }

  /**
   * Send a request to SQS to retrieve messages
   * @param params The required params to receive messages from SQS
   */
  private async receiveMessage(
    params: ReceiveMessageCommandInput
  ): Promise<ReceiveMessageCommandOutput> {
    try {
      return await this.sqs.send(new ReceiveMessageCommand(params));
    } catch (err) {
      throw toSQSError(err, `SQS receive message failed: ${err.message}`);
    }
  }

  /**
   * Delete a single message from SQS
   * @param message The message to delete from the SQS queue
   */
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

  /**
   * Trigger the applications handleMessage function
   * @param message The message that was received from SQS
   */
  private async executeHandler(message: Message): Promise<Message> {
    let timeout;
    let pending;
    try {
      let result;

      if (this.handleMessageTimeout) {
        [timeout, pending] = createTimeout(this.handleMessageTimeout);
        result = await Promise.race([this.handleMessage(message), pending]);
      } else {
        result = await this.handleMessage(message);
      }

      if (result instanceof Object) {
        return result;
      }

      return message;
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

  /**
   * Change the visibility timeout on a message
   * @param message The message to change the value of
   * @param timeout The new timeout that should be set
   */
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

  /**
   * Emit one of the consumer's error events depending on the error received.
   * @param err The error object to forward on
   * @param message The message that the error occurred on
   */
  private emitError(err: Error, message: Message): void {
    if (err.name === SQSError.name) {
      this.emit('error', err, message);
    } else if (err instanceof TimeoutError) {
      this.emit('timeout_error', err, message);
    } else {
      this.emit('processing_error', err, message);
    }
  }

  /**
   * Poll for new messages from SQS
   */
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

  /**
   * Process a batch of messages from the SQS queue.
   * @param message The messages that were delivered from SQS
   */
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
      const ackedMessages = await this.executeBatchHandler(messages);

      if (ackedMessages.length > 0) {
        await this.deleteMessageBatch(ackedMessages);
      }

      ackedMessages.forEach((message) => {
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

  /**
   * Delete a batch of messages from the SQS queue.
   * @param message The messages that should be deleted from SQS
   */
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

  /**
   * Execute the application's message batch handler
   * @param message The messages that should be forwarded from the SQS queue
   */
  private async executeBatchHandler(messages: Message[]): Promise<Message[]> {
    try {
      const result = await this.handleMessageBatch(messages);

      if (result instanceof Object) {
        return result;
      }

      return messages;
    } catch (err) {
      err.message = `Unexpected message handler failure: ${err.message}`;
      throw err;
    }
  }

  /**
   * Change the visibility timeout on a batch of messages
   * @param message The messages to change the value of
   * @param timeout The new timeout that should be set
   */
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

  /**
   * Trigger a function on a set interval
   * @param heartbeatFn The function that should be triggered
   */
  private startHeartbeat(heartbeatFn: () => void): NodeJS.Timeout {
    return setInterval(() => {
      heartbeatFn();
    }, this.heartbeatInterval * 1000);
  }
}
