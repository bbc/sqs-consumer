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

import { ConsumerOptions, TypedEventEmitter, StopOptions } from './types';
import { autoBind } from './bind';
import {
  SQSError,
  TimeoutError,
  toSQSError,
  isConnectionError
} from './errors';
import { assertOptions, hasMessages } from './validation';
import { abortController } from './controllers';

const debug = Debug('sqs-consumer');

/**
 * [Usage](https://bbc.github.io/sqs-consumer/index.html#usage)
 */
export class Consumer extends TypedEventEmitter {
  private pollingTimeoutId: NodeJS.Timeout | undefined = undefined;
  private heartbeatTimeoutId: NodeJS.Timeout | undefined = undefined;
  private handleMessageTimeoutId: NodeJS.Timeout | undefined = undefined;
  private stopped = true;
  private queueUrl: string;
  private handleMessage: (message: Message) => Promise<Message | void>;
  private handleMessageBatch: (message: Message[]) => Promise<Message[] | void>;
  private sqs: SQSClient;
  private handleMessageTimeout: number;
  private attributeNames: string[];
  private messageAttributeNames: string[];
  private shouldDeleteMessages: boolean;
  private batchSize: number;
  private visibilityTimeout: number;
  private terminateVisibilityTimeout: boolean;
  private waitTimeSeconds: number;
  private authenticationErrorTimeout: number;
  private pollingWaitTimeMs: number;
  private heartbeatInterval: number;

  constructor(options: ConsumerOptions) {
    super();
    assertOptions(options);
    this.queueUrl = options.queueUrl;
    this.handleMessage = options.handleMessage;
    this.handleMessageBatch = options.handleMessageBatch;
    this.handleMessageTimeout = options.handleMessageTimeout;
    this.attributeNames = options.attributeNames || [];
    this.messageAttributeNames = options.messageAttributeNames || [];
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
  public stop(options: StopOptions): void {
    if (this.stopped) {
      debug('Consumer was already stopped');
      return;
    }

    debug('Stopping consumer');
    this.stopped = true;

    if (this.pollingTimeoutId) {
      clearTimeout(this.pollingTimeoutId);
      this.pollingTimeoutId = undefined;
    }

    if (options?.abort) {
      debug('Aborting SQS requests');

      abortController.abort();

      this.emit('aborted');
    }

    this.emit('stopped');
  }

  /**
   * Returns the current polling state of the consumer: `true` if it is actively polling, `false` if it is not.
   */
  public get isRunning(): boolean {
    return !this.stopped;
  }

  /**
   * Emit one of the consumer's error events depending on the error received.
   * @param err The error object to forward on
   * @param message The message that the error occurred on
   */
  private emitError(err: Error, message?: Message): void {
    if (!message) {
      this.emit('error', err);
    } else if (err.name === SQSError.name) {
      this.emit('error', err, message);
    } else if (err instanceof TimeoutError) {
      this.emit('timeout_error', err, message);
    } else {
      this.emit('processing_error', err, message);
    }
  }

  /**
   * A reusable options object for sqs.send that's used to avoid duplication.
   */
  private sqsSendOptions = {
    abortSignal: abortController.signal
  };

  /**
   * Poll for new messages from SQS
   */
  private poll(): void {
    if (this.stopped) {
      debug('Poll was called while consumer was stopped, cancelling poll...');
      return;
    }

    debug('Polling for messages');

    let currentPollingTimeout = this.pollingWaitTimeMs;
    this.receiveMessage({
      QueueUrl: this.queueUrl,
      AttributeNames: this.attributeNames,
      MessageAttributeNames: this.messageAttributeNames,
      MaxNumberOfMessages: this.batchSize,
      WaitTimeSeconds: this.waitTimeSeconds,
      VisibilityTimeout: this.visibilityTimeout
    })
      .then(this.handleSqsResponse)
      .catch((err) => {
        this.emitError(err);
        if (isConnectionError(err)) {
          debug('There was an authentication error. Pausing before retrying.');
          currentPollingTimeout = this.authenticationErrorTimeout;
        }
        return;
      })
      .then(() => {
        if (this.pollingTimeoutId) {
          clearTimeout(this.pollingTimeoutId);
        }
        this.pollingTimeoutId = setTimeout(this.poll, currentPollingTimeout);
      })
      .catch((err) => {
        this.emitError(err);
      });
  }

  /**
   * Send a request to SQS to retrieve messages
   * @param params The required params to receive messages from SQS
   */
  private async receiveMessage(
    params: ReceiveMessageCommandInput
  ): Promise<ReceiveMessageCommandOutput> {
    try {
      return await this.sqs.send(
        new ReceiveMessageCommand(params),
        this.sqsSendOptions
      );
    } catch (err) {
      throw toSQSError(err, `SQS receive message failed: ${err.message}`);
    }
  }

  /**
   * Handles the response from AWS SQS, determining if we should proceed to
   * the message handler.
   * @param response The output from AWS SQS
   */
  private async handleSqsResponse(
    response: ReceiveMessageCommandOutput
  ): Promise<void> {
    if (hasMessages(response)) {
      if (this.handleMessageBatch) {
        await this.processMessageBatch(response.Messages);
      } else {
        await Promise.all(response.Messages.map(this.processMessage));
      }

      this.emit('response_processed');
    } else if (response) {
      this.emit('empty');
    }
  }

  /**
   * Process a message that has been received from SQS. This will execute the message
   * handler and delete the message once complete.
   * @param message The message that was delivered from SQS
   */
  private async processMessage(message: Message): Promise<void> {
    try {
      this.emit('message_received', message);

      if (this.heartbeatInterval) {
        this.heartbeatTimeoutId = this.startHeartbeat(message);
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
      clearInterval(this.heartbeatTimeoutId);
      this.heartbeatTimeoutId = undefined;
    }
  }

  /**
   * Process a batch of messages from the SQS queue.
   * @param messages The messages that were delivered from SQS
   */
  private async processMessageBatch(messages: Message[]): Promise<void> {
    try {
      messages.forEach((message) => {
        this.emit('message_received', message);
      });

      if (this.heartbeatInterval) {
        this.heartbeatTimeoutId = this.startHeartbeat(null, messages);
      }

      const ackedMessages = await this.executeBatchHandler(messages);

      if (ackedMessages?.length > 0) {
        await this.deleteMessageBatch(ackedMessages);

        ackedMessages.forEach((message) => {
          this.emit('message_processed', message);
        });
      }
    } catch (err) {
      this.emit('error', err, messages);

      if (this.terminateVisibilityTimeout) {
        await this.changeVisibilityTimeoutBatch(messages, 0);
      }
    } finally {
      clearInterval(this.heartbeatTimeoutId);
      this.heartbeatTimeoutId = undefined;
    }
  }

  /**
   * Trigger a function on a set interval
   * @param heartbeatFn The function that should be triggered
   */
  private startHeartbeat(
    message?: Message,
    messages?: Message[]
  ): NodeJS.Timeout {
    return setInterval(() => {
      if (this.handleMessageBatch) {
        return this.changeVisibilityTimeoutBatch(
          messages,
          this.visibilityTimeout
        );
      } else {
        return this.changeVisibilityTimeout(message, this.visibilityTimeout);
      }
    }, this.heartbeatInterval * 1000);
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
      return await this.sqs.send(
        new ChangeMessageVisibilityCommand(input),
        this.sqsSendOptions
      );
    } catch (err) {
      this.emit(
        'error',
        toSQSError(err, `Error changing visibility timeout: ${err.message}`),
        message
      );
    }
  }

  /**
   * Change the visibility timeout on a batch of messages
   * @param messages The messages to change the value of
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
        new ChangeMessageVisibilityBatchCommand(params),
        this.sqsSendOptions
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
   * Trigger the applications handleMessage function
   * @param message The message that was received from SQS
   */
  private async executeHandler(message: Message): Promise<Message> {
    try {
      let result;

      if (this.handleMessageTimeout) {
        const pending = new Promise((_, reject) => {
          this.handleMessageTimeoutId = setTimeout((): void => {
            reject(new TimeoutError());
          }, this.handleMessageTimeout);
        });
        result = await Promise.race([this.handleMessage(message), pending]);
      } else {
        result = await this.handleMessage(message);
      }

      return result instanceof Object ? result : message;
    } catch (err) {
      err.message =
        err instanceof TimeoutError
          ? `Message handler timed out after ${this.handleMessageTimeout}ms: Operation timed out.`
          : `Unexpected message handler failure: ${err.message}`;
      throw err;
    } finally {
      if (this.handleMessageTimeoutId) {
        clearTimeout(this.handleMessageTimeoutId);
      }
    }
  }

  /**
   * Execute the application's message batch handler
   * @param messages The messages that should be forwarded from the SQS queue
   */
  private async executeBatchHandler(messages: Message[]): Promise<Message[]> {
    try {
      const result = await this.handleMessageBatch(messages);

      return result instanceof Object ? result : messages;
    } catch (err) {
      err.message = `Unexpected message handler failure: ${err.message}`;
      throw err;
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
      await this.sqs.send(
        new DeleteMessageCommand(deleteParams),
        this.sqsSendOptions
      );
    } catch (err) {
      throw toSQSError(err, `SQS delete message failed: ${err.message}`);
    }
  }

  /**
   * Delete a batch of messages from the SQS queue.
   * @param messages The messages that should be deleted from SQS
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
      await this.sqs.send(
        new DeleteMessageBatchCommand(deleteParams),
        this.sqsSendOptions
      );
    } catch (err) {
      throw toSQSError(err, `SQS delete message failed: ${err.message}`);
    }
  }
}
