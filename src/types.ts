import {
  SQSClient,
  Message,
  QueueAttributeName,
  MessageSystemAttributeName,
} from "@aws-sdk/client-sqs";

/**
 * The options for the consumer.
 */
export interface ConsumerOptions {
  /**
   * The SQS queue URL.
   */
  queueUrl: string;
  /**
   * List of queue attributes to retrieve, see [AWS docs](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/Package/-aws-sdk-client-sqs/Variable/QueueAttributeName/).
   * @defaultvalue `[]`
   */
  attributeNames?: QueueAttributeName[];
  /**
   * List of message attributes to retrieve (i.e. `['name', 'address']`).
   * @defaultvalue `[]`
   */
  messageAttributeNames?: string[];
  /**
   * A list of attributes that need to be returned along with each message.
   * @defaultvalue `[]`
   */
  messageSystemAttributeNames?: MessageSystemAttributeName[];
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
   * If you want the stop action to wait for the final poll to complete and in-flight messages
   * to be processed before emitting 'stopped' set this to the max amount of time to wait.
   * @defaultvalue `0`
   */
  pollingCompleteWaitTimeMs?: number;
  /**
   * If true, sets the message visibility timeout to 0 after a `processing_error`. You can
   * also specify a different timeout using a number.
   * @defaultvalue `false`
   */
  terminateVisibilityTimeout?: boolean | number;
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
   * @defaultValue process.env.AWS_REGION || `eu-west-1`
   */
  region?: string;
  /**
   * Set this value to false to ignore the `queueUrl` and use the
   * client's resolved endpoint, which may be a custom endpoint.
   * @defaultValue `true`
   */
  useQueueUrlAsEndpoint?: boolean;
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
   * By default, the consumer will treat an empty object or array from either of the
   * handlers as a acknowledgement of no messages and will not delete those messages as
   * a result. Set this to `true` to always acknowledge all messages no matter the returned
   * value.
   * @defaultvalue `false`
   */
  alwaysAcknowledge?: boolean;
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
  /**
   * An `async` function (or function that returns a `Promise`) to be called right
   * before the SQS Client sends a receive message command.
   *
   * This function is usefull if SQS Client module exports have been modified, for
   * example to add middlewares.
   */
  preReceiveMessageCallback?(): Promise<void>;
  /**
   * An `async` function (or function that returns a `Promise`) to be called right
   * after the SQS Client sends a receive message command.
   *
   * This function is usefull if SQS Client module exports have been modified, for
   * example to add middlewares.
   */
  postReceiveMessageCallback?(): Promise<void>;
  /**
   * Set this to `true` if you want to receive additional information about the error
   * that occurred from AWS, such as the response and metadata.
   */
  extendedAWSErrors?: boolean;
}

/**
 * A subset of the ConsumerOptions that can be updated at runtime.
 */
export type UpdatableOptions =
  | "visibilityTimeout"
  | "batchSize"
  | "waitTimeSeconds"
  | "pollingWaitTimeMs";

/**
 * The options for the stop method.
 */
export interface StopOptions {
  /**
   * Default to `false`, if you want the stop action to also abort requests to SQS
   * set this to `true`.
   * @defaultvalue `false`
   */
  abort?: boolean;
}

/**
 * These are the events that the consumer emits.
 */
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
   * Fired when the consumer starts its work..
   */
  started: [];
  /**
   * Fired when the consumer finally stops its work.
   */
  stopped: [];
  /**
   * Fired when an option is updated
   */
  option_updated: [UpdatableOptions, ConsumerOptions[UpdatableOptions]];
  /**
   * Fired when the Consumer is waiting for polling to complete before stopping.
   */
  waiting_for_polling_to_complete: [];
  /**
   * Fired when the Consumer has waited for polling to complete and is stopping due to a timeout.
   */
  waiting_for_polling_to_complete_timeout_exceeded: [];
}

/**
 * The error object that is emitted with error events from AWS.
 */
export type AWSError = {
  /**
   * Name, eg. ConditionalCheckFailedException
   */
  readonly name: string;

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
  readonly $fault: "client" | "server";

  /**
   * Represents an HTTP message as received in reply to a request
   */
  readonly $response?: {
    /**
     * The status code of the HTTP response.
     */
    statusCode?: number;
    /**
     * The headers of the HTTP message.
     */
    headers: Record<string, string>;
    /**
     * The body of the HTTP message.
     * Can be: ArrayBuffer | ArrayBufferView | string | Uint8Array | Readable | ReadableStream
     */
    body?: any;
  };

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

  readonly $metadata: {
    /**
     * The status code of the last HTTP response received for this operation.
     */
    readonly httpStatusCode?: number;

    /**
     * A unique identifier for the last request sent for this operation. Often
     * requested by AWS service teams to aid in debugging.
     */
    readonly requestId?: string;

    /**
     * A secondary identifier for the last request sent. Used for debugging.
     */
    readonly extendedRequestId?: string;

    /**
     * A tertiary identifier for the last request sent. Used for debugging.
     */
    readonly cfId?: string;

    /**
     * The number of times this operation was attempted.
     */
    readonly attempts?: number;

    /**
     * The total amount of time (in milliseconds) that was spent waiting between
     * retry attempts.
     */
    readonly totalRetryDelay?: number;
  };
};
