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
  ReceiveMessageCommandOutput,
  QueueAttributeName,
  MessageSystemAttributeName,
} from "@aws-sdk/client-sqs";

import { ConsumerOptions, StopOptions, UpdatableOptions } from "./types.js";
import { TypedEventEmitter } from "./emitter.js";
import {
  SQSError,
  TimeoutError,
  toStandardError,
  toTimeoutError,
  toSQSError,
  isConnectionError,
} from "./errors.js";
import { validateOption, assertOptions, hasMessages } from "./validation.js";
import { logger } from "./logger.js";

/**
 * [Usage](https://bbc.github.io/sqs-consumer/index.html#usage)
 */
export class Consumer extends TypedEventEmitter {
  private pollingTimeoutId: NodeJS.Timeout | undefined = undefined;
  private stopped = true;
  private queueUrl: string;
  private handleMessage: (message: Message) => Promise<Message | void>;
  private handleMessageBatch: (message: Message[]) => Promise<Message[] | void>;
  private preReceiveMessageCallback?: () => Promise<void>;
  private postReceiveMessageCallback?: () => Promise<void>;
  private sqs: SQSClient;
  private handleMessageTimeout: number;
  private attributeNames: QueueAttributeName[];
  private messageAttributeNames: string[];
  private messageSystemAttributeNames: MessageSystemAttributeName[];
  private shouldDeleteMessages: boolean;
  private alwaysAcknowledge: boolean;
  private batchSize: number;
  private visibilityTimeout: number;
  private terminateVisibilityTimeout: boolean | number;
  private waitTimeSeconds: number;
  private authenticationErrorTimeout: number;
  private pollingWaitTimeMs: number;
  private pollingCompleteWaitTimeMs: number;
  private heartbeatInterval: number;
  private isPolling = false;
  private stopRequestedAtTimestamp: number;
  public abortController: AbortController;
  private extendedAWSErrors: boolean;

  constructor(options: ConsumerOptions) {
    super();
    assertOptions(options);
    this.queueUrl = options.queueUrl;
    this.handleMessage = options.handleMessage;
    this.handleMessageBatch = options.handleMessageBatch;
    this.preReceiveMessageCallback = options.preReceiveMessageCallback;
    this.postReceiveMessageCallback = options.postReceiveMessageCallback;
    this.handleMessageTimeout = options.handleMessageTimeout;
    this.attributeNames = options.attributeNames || [];
    this.messageAttributeNames = options.messageAttributeNames || [];
    this.messageSystemAttributeNames =
      options.messageSystemAttributeNames || [];
    this.batchSize = options.batchSize || 1;
    this.visibilityTimeout = options.visibilityTimeout;
    this.terminateVisibilityTimeout =
      options.terminateVisibilityTimeout || false;
    this.heartbeatInterval = options.heartbeatInterval;
    this.waitTimeSeconds = options.waitTimeSeconds ?? 20;
    this.authenticationErrorTimeout =
      options.authenticationErrorTimeout ?? 10000;
    this.pollingWaitTimeMs = options.pollingWaitTimeMs ?? 0;
    this.pollingCompleteWaitTimeMs = options.pollingCompleteWaitTimeMs ?? 0;
    this.shouldDeleteMessages = options.shouldDeleteMessages ?? true;
    this.alwaysAcknowledge = options.alwaysAcknowledge ?? false;
    this.extendedAWSErrors = options.extendedAWSErrors ?? false;
    this.sqs =
      options.sqs ||
      new SQSClient({
        useQueueUrlAsEndpoint: options.useQueueUrlAsEndpoint ?? true,
        region: options.region || process.env.AWS_REGION || "eu-west-1",
      });
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
      // Create a new abort controller each time the consumer is started
      this.abortController = new AbortController();
      logger.debug("starting");
      this.stopped = false;
      this.emit("started");
      this.poll();
    }
  }

  /**
   * A reusable options object for sqs.send that's used to avoid duplication.
   */
  private get sqsSendOptions(): { abortSignal: AbortSignal } {
    return {
      // return the current abortController signal or a fresh signal that has not been aborted.
      // This effectively defaults the signal sent to the AWS SDK to not aborted
      abortSignal: this.abortController?.signal || new AbortController().signal,
    };
  }

  /**
   * Stop polling the queue for messages (pre existing requests will still be made until concluded).
   */
  public stop(options?: StopOptions): void {
    if (this.stopped) {
      logger.debug("already_stopped");
      return;
    }

    logger.debug("stopping");
    this.stopped = true;

    if (this.pollingTimeoutId) {
      clearTimeout(this.pollingTimeoutId);
      this.pollingTimeoutId = undefined;
    }

    if (options?.abort) {
      logger.debug("aborting");
      this.abortController.abort();
      this.emit("aborted");
    }

    this.stopRequestedAtTimestamp = Date.now();
    this.waitForPollingToComplete();
  }

  /**
   * Wait for final poll and in flight messages to complete.
   * @private
   */
  private waitForPollingToComplete(): void {
    if (!this.isPolling || !(this.pollingCompleteWaitTimeMs > 0)) {
      this.emit("stopped");
      return;
    }

    const exceededTimeout: boolean =
      Date.now() - this.stopRequestedAtTimestamp >
      this.pollingCompleteWaitTimeMs;
    if (exceededTimeout) {
      this.emit("waiting_for_polling_to_complete_timeout_exceeded");
      this.emit("stopped");
      return;
    }

    this.emit("waiting_for_polling_to_complete");
    setTimeout(() => this.waitForPollingToComplete(), 1000);
  }

  /**
   * Returns the current status of the consumer.
   * This includes whether it is running or currently polling.
   */
  public get status(): {
    isRunning: boolean;
    isPolling: boolean;
  } {
    return {
      isRunning: !this.stopped,
      isPolling: this.isPolling,
    };
  }

  /**
   * Validates and then updates the provided option to the provided value.
   * @param option The option to validate and then update
   * @param value The value to set the provided option to
   */
  public updateOption(
    option: UpdatableOptions,
    value: ConsumerOptions[UpdatableOptions],
  ): void {
    validateOption(option, value, this, true);

    this[option] = value;

    this.emit("option_updated", option, value);
  }

  /**
   * Emit one of the consumer's error events depending on the error received.
   * @param err The error object to forward on
   * @param message The message that the error occurred on
   */
  private emitError(err: Error, message?: Message): void {
    if (!message) {
      this.emit("error", err);
    } else if (err.name === SQSError.name) {
      this.emit("error", err, message);
    } else if (err instanceof TimeoutError) {
      this.emit("timeout_error", err, message);
    } else {
      this.emit("processing_error", err, message);
    }
  }

  /**
   * Poll for new messages from SQS
   */
  private poll(): void {
    if (this.stopped) {
      logger.debug("cancelling_poll", {
        detail:
          "Poll was called while consumer was stopped, cancelling poll...",
      });
      return;
    }

    logger.debug("polling");

    this.isPolling = true;

    let currentPollingTimeout: number = this.pollingWaitTimeMs;
    this.receiveMessage({
      QueueUrl: this.queueUrl,
      AttributeNames: this.attributeNames,
      MessageAttributeNames: this.messageAttributeNames,
      MessageSystemAttributeNames: this.messageSystemAttributeNames,
      MaxNumberOfMessages: this.batchSize,
      WaitTimeSeconds: this.waitTimeSeconds,
      VisibilityTimeout: this.visibilityTimeout,
    })
      .then((output: ReceiveMessageCommandOutput) =>
        this.handleSqsResponse(output),
      )
      .catch((err): void => {
        this.emitError(err);
        if (isConnectionError(err)) {
          logger.debug("authentication_error", {
            code: err.code || "Unknown",
            detail:
              "There was an authentication error. Pausing before retrying.",
          });
          currentPollingTimeout = this.authenticationErrorTimeout;
        }
        return;
      })
      .then((): void => {
        if (this.pollingTimeoutId) {
          clearTimeout(this.pollingTimeoutId);
        }
        this.pollingTimeoutId = setTimeout(
          () => this.poll(),
          currentPollingTimeout,
        );
      })
      .catch((err): void => {
        this.emitError(err);
      })
      .finally((): void => {
        this.isPolling = false;
      });
  }

  /**
   * Send a request to SQS to retrieve messages
   * @param params The required params to receive messages from SQS
   */
  private async receiveMessage(
    params: ReceiveMessageCommandInput,
  ): Promise<ReceiveMessageCommandOutput> {
    try {
      if (this.preReceiveMessageCallback) {
        await this.preReceiveMessageCallback();
      }
      const result: ReceiveMessageCommandOutput = await this.sqs.send(
        new ReceiveMessageCommand(params),
        this.sqsSendOptions,
      );
      if (this.postReceiveMessageCallback) {
        await this.postReceiveMessageCallback();
      }

      return result;
    } catch (err) {
      throw toSQSError(
        err,
        `SQS receive message failed: ${err.message}`,
        this.extendedAWSErrors,
      );
    }
  }

  /**
   * Handles the response from AWS SQS, determining if we should proceed to
   * the message handler.
   * @param response The output from AWS SQS
   */
  private async handleSqsResponse(
    response: ReceiveMessageCommandOutput,
  ): Promise<void> {
    if (hasMessages(response)) {
      if (this.handleMessageBatch) {
        await this.processMessageBatch(response.Messages);
      } else {
        await Promise.all(
          response.Messages.map((message: Message) =>
            this.processMessage(message),
          ),
        );
      }

      this.emit("response_processed");
    } else if (response) {
      this.emit("empty");
    }
  }

  /**
   * Process a message that has been received from SQS. This will execute the message
   * handler and delete the message once complete.
   * @param message The message that was delivered from SQS
   */
  private async processMessage(message: Message): Promise<void> {
    let heartbeatTimeoutId: NodeJS.Timeout | undefined = undefined;

    try {
      this.emit("message_received", message);

      if (this.heartbeatInterval) {
        heartbeatTimeoutId = this.startHeartbeat(message);
      }

      const ackedMessage: Message = await this.executeHandler(message);

      if (ackedMessage?.MessageId === message.MessageId) {
        await this.deleteMessage(message);

        this.emit("message_processed", message);
      }
    } catch (err) {
      this.emitError(err, message);

      if (this.terminateVisibilityTimeout !== false) {
        const timeout =
          this.terminateVisibilityTimeout === true
            ? 0
            : this.terminateVisibilityTimeout;
        await this.changeVisibilityTimeout(message, timeout);
      }
    } finally {
      if (this.heartbeatInterval) {
        clearInterval(heartbeatTimeoutId);
      }
    }
  }

  /**
   * Process a batch of messages from the SQS queue.
   * @param messages The messages that were delivered from SQS
   */
  private async processMessageBatch(messages: Message[]): Promise<void> {
    let heartbeatTimeoutId: NodeJS.Timeout | undefined = undefined;

    try {
      messages.forEach((message: Message): void => {
        this.emit("message_received", message);
      });

      if (this.heartbeatInterval) {
        heartbeatTimeoutId = this.startHeartbeat(null, messages);
      }

      const ackedMessages: Message[] = await this.executeBatchHandler(messages);

      if (ackedMessages?.length > 0) {
        await this.deleteMessageBatch(ackedMessages);

        ackedMessages.forEach((message: Message): void => {
          this.emit("message_processed", message);
        });
      }
    } catch (err) {
      this.emit("error", err, messages);

      if (this.terminateVisibilityTimeout !== false) {
        const timeout =
          this.terminateVisibilityTimeout === true
            ? 0
            : this.terminateVisibilityTimeout;
        await this.changeVisibilityTimeoutBatch(messages, timeout);
      }
    } finally {
      clearInterval(heartbeatTimeoutId);
    }
  }

  /**
   * Trigger a function on a set interval
   * @param heartbeatFn The function that should be triggered
   */
  private startHeartbeat(
    message?: Message,
    messages?: Message[],
  ): NodeJS.Timeout {
    return setInterval(() => {
      if (this.handleMessageBatch) {
        return this.changeVisibilityTimeoutBatch(
          messages,
          this.visibilityTimeout,
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
    timeout: number,
  ): Promise<ChangeMessageVisibilityCommandOutput> {
    try {
      const input: ChangeMessageVisibilityCommandInput = {
        QueueUrl: this.queueUrl,
        ReceiptHandle: message.ReceiptHandle,
        VisibilityTimeout: timeout,
      };
      return await this.sqs.send(
        new ChangeMessageVisibilityCommand(input),
        this.sqsSendOptions,
      );
    } catch (err) {
      this.emit(
        "error",
        toSQSError(
          err,
          `Error changing visibility timeout: ${err.message}`,
          this.extendedAWSErrors,
        ),
        message,
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
    timeout: number,
  ): Promise<ChangeMessageVisibilityBatchCommandOutput> {
    const params: ChangeMessageVisibilityBatchCommandInput = {
      QueueUrl: this.queueUrl,
      Entries: messages.map((message: Message) => ({
        Id: message.MessageId,
        ReceiptHandle: message.ReceiptHandle,
        VisibilityTimeout: timeout,
      })),
    };
    try {
      return await this.sqs.send(
        new ChangeMessageVisibilityBatchCommand(params),
        this.sqsSendOptions,
      );
    } catch (err) {
      this.emit(
        "error",
        toSQSError(
          err,
          `Error changing visibility timeout: ${err.message}`,
          this.extendedAWSErrors,
        ),
        messages,
      );
    }
  }

  /**
   * Trigger the applications handleMessage function
   * @param message The message that was received from SQS
   */
  private async executeHandler(message: Message): Promise<Message> {
    let handleMessageTimeoutId: NodeJS.Timeout | undefined = undefined;

    try {
      let result;

      if (this.handleMessageTimeout) {
        const pending: Promise<void> = new Promise((_, reject): void => {
          handleMessageTimeoutId = setTimeout((): void => {
            reject(new TimeoutError());
          }, this.handleMessageTimeout);
        });
        result = await Promise.race([this.handleMessage(message), pending]);
      } else {
        result = await this.handleMessage(message);
      }

      return !this.alwaysAcknowledge && result instanceof Object
        ? result
        : message;
    } catch (err) {
      if (err instanceof TimeoutError) {
        throw toTimeoutError(
          err,
          `Message handler timed out after ${this.handleMessageTimeout}ms: Operation timed out.`,
        );
      } else if (err instanceof Error) {
        throw toStandardError(
          err,
          `Unexpected message handler failure: ${err.message}`,
        );
      }
      throw err;
    } finally {
      if (handleMessageTimeoutId) {
        clearTimeout(handleMessageTimeoutId);
      }
    }
  }

  /**
   * Execute the application's message batch handler
   * @param messages The messages that should be forwarded from the SQS queue
   */
  private async executeBatchHandler(messages: Message[]): Promise<Message[]> {
    try {
      const result: void | Message[] = await this.handleMessageBatch(messages);

      return !this.alwaysAcknowledge && result instanceof Object
        ? result
        : messages;
    } catch (err) {
      if (err instanceof Error) {
        throw toStandardError(
          err,
          `Unexpected message handler failure: ${err.message}`,
        );
      }
      throw err;
    }
  }

  /**
   * Delete a single message from SQS
   * @param message The message to delete from the SQS queue
   */
  private async deleteMessage(message: Message): Promise<void> {
    if (!this.shouldDeleteMessages) {
      logger.debug("skipping_delete", {
        detail:
          "Skipping message delete since shouldDeleteMessages is set to false",
      });
      return;
    }
    logger.debug("deleting_message", { messageId: message.MessageId });

    const deleteParams: DeleteMessageCommandInput = {
      QueueUrl: this.queueUrl,
      ReceiptHandle: message.ReceiptHandle,
    };

    try {
      await this.sqs.send(
        new DeleteMessageCommand(deleteParams),
        this.sqsSendOptions,
      );
    } catch (err) {
      throw toSQSError(
        err,
        `SQS delete message failed: ${err.message}`,
        this.extendedAWSErrors,
      );
    }
  }

  /**
   * Delete a batch of messages from the SQS queue.
   * @param messages The messages that should be deleted from SQS
   */
  private async deleteMessageBatch(messages: Message[]): Promise<void> {
    if (!this.shouldDeleteMessages) {
      logger.debug("skipping_delete", {
        detail:
          "Skipping message delete since shouldDeleteMessages is set to false",
      });
      return;
    }
    logger.debug("deleting_messages", {
      messageIds: messages.map((msg: Message) => msg.MessageId),
    });

    const deleteParams: DeleteMessageBatchCommandInput = {
      QueueUrl: this.queueUrl,
      Entries: messages.map((message: Message) => ({
        Id: message.MessageId,
        ReceiptHandle: message.ReceiptHandle,
      })),
    };

    try {
      await this.sqs.send(
        new DeleteMessageBatchCommand(deleteParams),
        this.sqsSendOptions,
      );
    } catch (err) {
      throw toSQSError(
        err,
        `SQS delete message failed: ${err.message}`,
        this.extendedAWSErrors,
      );
    }
  }
}
