import { SQSClient, Message } from '@aws-sdk/client-sqs';

export interface ConsumerOptions {
  queueUrl: string;
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
  handleMessageBatch?(messages: Message[]): Promise<Message[] | void>;
}

export interface Events {
  response_processed: [];
  empty: [];
  message_received: [Message];
  message_processed: [Message];
  error: [Error, void | Message | Message[]];
  timeout_error: [Error, Message];
  processing_error: [Error, Message];
  stopped: [];
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
