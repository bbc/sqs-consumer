/// <reference types="node" />
import * as SQS from 'aws-sdk/clients/sqs';
import { EventEmitter } from 'events';
export declare type SQSMessage = SQS.Types.Message;
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
export declare class Consumer extends EventEmitter {
    private queueUrl;
    private handleMessage;
    private handleMessageBatch;
    private handleMessageTimeout;
    private attributeNames;
    private messageAttributeNames;
    private stopped;
    private batchSize;
    private visibilityTimeout;
    private waitTimeSeconds;
    private authenticationErrorTimeout;
    private pollingWaitTimeMs;
    private terminateVisibilityTimeout;
    private heartbeatInterval;
    private sqs;
    constructor(options: ConsumerOptions);
    emit<T extends keyof Events>(event: T, ...args: Events[T]): boolean;
    on<T extends keyof Events>(event: T, listener: (...args: Events[T]) => void): this;
    once<T extends keyof Events>(event: T, listener: (...args: Events[T]) => void): this;
    get isRunning(): boolean;
    static create(options: ConsumerOptions): Consumer;
    start(): void;
    stop(): void;
    private handleSqsResponse;
    private processMessage;
    private receiveMessage;
    private executeHandler;
    private changeVisabilityTimeout;
    private emitError;
    private poll;
    private processMessageBatch;
    private executeBatchHandler;
    private changeVisabilityTimeoutBatch;
    private startHeartbeat;
}
export {};
