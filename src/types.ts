import { SQSClient, Message } from '@aws-sdk/client-sqs';
import { EventEmitter } from 'events';

export interface ConsumerOptions {
  /**
   * The SQS queue URL.
   */
  queueUrl: string;
  /**
   * List of queue attributes to retrieve (i.e.
   * `['All', 'ApproximateFirstReceiveTimestamp', 'ApproximateReceiveCount']`).
   * @defaultvalue `[]`
   */
  attributeNames?: string[];
  /**
   * List of message attributes to retrieve (i.e. `['name', 'address']`).
   * @defaultvalue `[]`
   */
  messageAttributeNames?: string[];
  /** @hidden */
  stopped?: boolean;
  /**
   * The number of messages to request from SQS when polling (default `1`).
   *
   * This cannot be higher than the
   * [AWS limit of 10](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/quotas-messages.html).
   * @defaultvalue `1`
   */
  batchSize?: number;
  /**
   * The duration (in seconds) that the received messages are hidden from subsequent
   * retrieve requests after being retrieved by a ReceiveMessage request.
   */
  visibilityTimeout?: number;
  /**
   * The duration (in seconds) for which the call will wait for a message to arrive in
   * the queue before returning.
   * @defaultvalue `20`
   */
  waitTimeSeconds?: number;
  /**
   * The duration (in milliseconds) to wait before retrying after an authentication error.
   * @defaultvalue `10000`
   */
  authenticationErrorTimeout?: number;
  /**
   * The duration (in milliseconds) to wait before repolling the queue.
   * @defaultvalue `0`
   */
  pollingWaitTimeMs?: number;
  /**
   * If true, sets the message visibility timeout to 0 after a `processing_error`.
   * @defaultvalue `false`
   */
  terminateVisibilityTimeout?: boolean;
  /**
   * The interval (in seconds) between requests to extend the message visibility timeout.
   *
   * On each heartbeat the visibility is extended by adding `visibilityTimeout` to
   * the number of seconds since the start of the handler function.
   *
   * This value must less than `visibilityTimeout`.
   */
  heartbeatInterval?: number;
  /**
   * An optional [SQS Client](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-sqs/classes/sqsclient.html)
   * object to use if you need to configure the client manually.
   */
  sqs?: SQSClient;
  /**
   * The AWS region.
   * @defaultValue `eu-west-1`
   */
  region?: string;
  /**
   * Time in ms to wait for `handleMessage` to process a message before timing out.
   *
   * Emits `timeout_error` on timeout. By default, if `handleMessage` times out,
   * the unprocessed message returns to the end of the queue.
   */
  handleMessageTimeout?: number;
  /**
   * Default to `true`, if you don't want the package to delete messages from sqs
   * set this to `false`.
   * @defaultvalue `true`
   */
  shouldDeleteMessages?: boolean;
  /**
   * An `async` function (or function that returns a `Promise`) to be called whenever
   * a message is received.
   *
   * In the case that you need to acknowledge the message, return an object containing
   * the MessageId that you'd like to acknowledge.
   */
  handleMessage?(message: Message): Promise<Message | void>;
  /**
   * An `async` function (or function that returns a `Promise`) to be called whenever
   * a batch of messages is received. Similar to `handleMessage` but will receive the
   * list of messages, not each message individually.
   *
   * **If both are set, `handleMessageBatch` overrides `handleMessage`**.
   *
   * In the case that you need to ack only some of the messages, return an array with
   * the successful messages only.
   */
  handleMessageBatch?(messages: Message[]): Promise<Message[] | void>;
}

export interface StopOptions {
  /**
   * Default to `false`, if you want the stop action to also abort requests to SQS
   * set this to `true`.
   * @defaultvalue `false`
   */
  abort?: boolean;
}

export interface Events {
  /**
   * Fired after one batch of items (up to `batchSize`) has been successfully processed.
   */
  response_processed: [];
  /**
   * Fired when the queue is empty (All messages have been consumed).
   */
  empty: [];
  /**
   * Fired when a message is received.
   */
  message_received: [Message];
  /**
   * Fired when a message is successfully processed and removed from the queue.
   */
  message_processed: [Message];
  /**
   * Fired when an error occurs interacting with the queue.
   *
   * If the error correlates to a message, that message is included in Params
   */
  error: [Error, void | Message | Message[]];
  /**
   * Fired when `handleMessageTimeout` is supplied as an option and if
   * `handleMessage` times out.
   */
  timeout_error: [Error, Message];
  /**
   * Fired when an error occurs processing the message.
   */
  processing_error: [Error, Message];
  /**
   * Fired when requests to SQS were aborted.
   */
  aborted: [];
  /**
   * Fired when the consumer finally stops its work.
   */
  stopped: [];
}

export class TypedEventEmitter extends EventEmitter {
  /**
   * Trigger a listener on all emitted events
   * @param event The name of the event to listen to
   * @param listener A function to trigger when the event is emitted
   */
  on<E extends keyof Events>(
    event: E,
    listener: (...args: Events[E]) => void
  ): this {
    return super.on(event, listener);
  }
  /**
   * Trigger a listener only once for an emitted event
   * @param event The name of the event to listen to
   * @param listener A function to trigger when the event is emitted
   */
  once<E extends keyof Events>(
    event: E,
    listener: (...args: Events[E]) => void
  ): this {
    return super.on(event, listener);
  }
  /**
   * Emits an event with the provided arguments
   * @param event The name of the event to emit
   */
  emit<E extends keyof Events>(event: E, ...args: Events[E]): boolean {
    return super.emit(event, ...args);
  }
}

export type AWSError = {
  /**
   * Name, eg. ConditionalCheckFailedException
   */
  name: string;

  /**
   * Human-readable error response message
   */
  message: string;

  /**
   * Non-standard stacktrace
   */
  stack?: string;

  /**
   * Whether the client or server are at fault.
   */
  readonly $fault?: 'client' | 'server';

  /**
   * The service that encountered the exception.
   */
  readonly $service?: string;

  /**
   * Indicates that an error MAY be retried by the client.
   */
  readonly $retryable?: {
    /**
     * Indicates that the error is a retryable throttling error.
     */
    readonly throttling?: boolean;
  };

  $metadata?: {
    /**
     * The status code of the last HTTP response received for this operation.
     */
    httpStatusCode?: number;

    /**
     * A unique identifier for the last request sent for this operation. Often
     * requested by AWS service teams to aid in debugging.
     */
    requestId?: string;

    /**
     * A secondary identifier for the last request sent. Used for debugging.
     */
    extendedRequestId?: string;

    /**
     * A tertiary identifier for the last request sent. Used for debugging.
     */
    cfId?: string;

    /**
     * The number of times this operation was attempted.
     */
    attempts?: number;

    /**
     * The total amount of time (in milliseconds) that was spent waiting between
     * retry attempts.
     */
    totalRetryDelay?: number;
  };
};
