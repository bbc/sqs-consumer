/// <reference types="node" />
import { EventEmitter } from 'events';
export declare class Consumer extends EventEmitter {
    private queueUrl;
    private handleMessage;
    private attributeNames;
    private messageAttributeNames;
    private stopped;
    private batchSize;
    private visibilityTimeout;
    private waitTimeSeconds;
    private authenticationErrorTimeout;
    private terminateVisibilityTimeout;
    private sqs;
    constructor(options: any);
    static create(options: any): Consumer;
    start(): void;
    stop(): void;
    private receiveMessage;
    private deleteMessage;
    private handleSQSResponse;
    private processMessage;
    private executeHandler;
    private poll;
}
