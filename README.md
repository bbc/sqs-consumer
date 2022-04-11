# sqs-consumer

[![NPM downloads](https://img.shields.io/npm/dm/sqs-consumer.svg?style=flat)](https://npmjs.org/package/sqs-consumer)
[![Build Status](https://travis-ci.org/bbc/sqs-consumer.svg)](https://travis-ci.org/bbc/sqs-consumer) 
[![Code Climate](https://codeclimate.com/github/BBC/sqs-consumer/badges/gpa.svg)](https://codeclimate.com/github/BBC/sqs-consumer) 
[![Test Coverage](https://codeclimate.com/github/BBC/sqs-consumer/badges/coverage.svg)](https://codeclimate.com/github/BBC/sqs-consumer)

Build SQS-based applications without the boilerplate. Just define an async function that handles the SQS message processing.

## Installation

```bash
npm add sqs-consumer
```

Note: This library assumes you are using [AWS SDK v3](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-sqs/index.html). If you are using v2, please install v5.5.0:

```bash
npm add sqs-consumer@5.5.0
```

## Usage

```js
import { Consumer } from 'sqs-consumer';

const app = Consumer.create({
  queueUrl: 'https://sqs.eu-west-1.amazonaws.com/account-id/queue-name',
  handleMessage: async (message) => {
    // do some work with `message`
  }
});

app.on('error', (err) => {
  console.error(err.message);
});

app.on('processing_error', (err) => {
  console.error(err.message);
});

app.start();
```

* The queue is polled continuously for messages using [long polling](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html).
* Messages are deleted from the queue once the handler function has completed successfully.
* Throwing an error (or returning a rejected promise) from the handler function will cause the message to be left on the queue. An [SQS redrive policy](http://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/SQSDeadLetterQueue.html) can be used to move messages that cannot be processed to a dead letter queue.
* By default messages are processed one at a time – a new message won't be received until the first one has been processed. To process messages in parallel, use the `batchSize` option [detailed below](#options).
* If you need to add specific configuration options to the SQS client, you may pass an instantiated client to the Consumer constructor:
```js
import { Consumer } from 'sqs-consumer';
import { SQSClient } from '@aws-sdk/client-sqs';

const app = Consumer.create({
  queueUrl: 'https://sqs.eu-west-1.amazonaws.com/account-id/queue-name',
  handleMessage: async (message) => {
    // do some work with `message`
  },
  sqs: new SQSClient({
    region: 'my-region',
    requestHandler: MyCustomHttpHandler
  })
});

app.on('error', (err) => {
  console.error(err.message);
});

app.on('processing_error', (err) => {
  console.error(err.message);
});

app.start();
```

### Credentials

By default the consumer will look for AWS credentials in the places [specified by the AWS SDK](http://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html#Setting_AWS_Credentials). The simplest option is to export your credentials as environment variables:

```bash
export AWS_SECRET_ACCESS_KEY=...
export AWS_ACCESS_KEY_ID=...
```

If you need to specify your credentials manually, you can use a pre-configured instance of the [SQS Client](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-sqs/classes/sqsclient.html) client and chose from [a valid credentials provider](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/)


```js
import { Consumer } from 'sqs-consumer';
import { SQSClient } from '@aws-sdk/client-sqs';
import { fromIni } from '@aws-sdk/credential-provider-ini';

const credentials = fromIni({
  profile: 'my-profile'
});

const region = 'my-region';

const app = Consumer.create({
  queueUrl: 'https://sqs.eu-west-1.amazonaws.com/account-id/queue-name',
  handleMessage: async (message) => {
    // ...
  },
  sqs: new SQSClient({ region, credentials })
});

app.on('error', (err) => {
  console.error(err.message);
});

app.on('processing_error', (err) => {
  console.error(err.message);
});

app.on('timeout_error', (err) => {
 console.error(err.message);
});

app.start();
```

## API

### `Consumer.create(options)`

Creates a new SQS consumer.

#### Options

* `queueUrl` - _String_ - The SQS queue URL
* `region` - _String_ - The AWS region (default `eu-west-1`)
* `handleMessage` - _Function_ - An `async` function (or function that returns a `Promise`) to be called whenever a message is received. Receives an SQS message object as it's first argument.
* `handleMessageBatch` - _Function_ - An `async` function (or function that returns a `Promise`) to be called whenever a batch of messages is received. Similar to `handleMessage` but will receive the list of messages, not each message individually. **If both are set, `handleMessageBatch` overrides `handleMessage`**.
* `handleMessageTimeout` - _Number_ - Time in ms to wait for `handleMessage` to process a message before timing out. Emits `timeout_error` on timeout. By default, if `handleMessage` times out, the unprocessed message returns to the end of the queue.
* `attributeNames` - _Array_ - List of queue attributes to retrieve (i.e. `['All', 'ApproximateFirstReceiveTimestamp', 'ApproximateReceiveCount']`).
* `messageAttributeNames` - _Array_ - List of message attributes to retrieve (i.e. `['name', 'address']`).
* `batchSize` - _Number_ - The number of messages to request from SQS when polling (default `1`). This cannot be higher than the AWS limit of 10.
* `visibilityTimeout` - _Number_ - The duration (in seconds) that the received messages are hidden from subsequent retrieve requests after being retrieved by a ReceiveMessage request.
* `heartbeatInterval` - _Number_ - The interval (in seconds) between requests to extend the message visibility timeout. On each heartbeat the visibility is extended by adding `visibilityTimeout` to the number of seconds since the start of the handler function. This value must less than `visibilityTimeout`.
* `terminateVisibilityTimeout` - _Boolean_ - If true, sets the message visibility timeout to 0 after a `processing_error` (defaults to `false`).
* `waitTimeSeconds` - _Number_ - The duration (in seconds) for which the call will wait for a message to arrive in the queue before returning.
* `authenticationErrorTimeout` - _Number_ - The duration (in milliseconds) to wait before retrying after an authentication error (defaults to `10000`).
* `pollingWaitTimeMs` - _Number_ - The duration (in milliseconds) to wait before repolling the queue (defaults to `0`).
* `sqs` - _Object_ - An optional [SQS Client](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-sqs/classes/sqsclient.html) object to use if you need to configure the client manually

### `consumer.start()`

Start polling the queue for messages.

### `consumer.stop()`

Stop polling the queue for messages.

### `consumer.isRunning`  

Returns the current polling state of the consumer: `true` if it is actively polling, `false` if it is not.

### Events

Each consumer is an [`EventEmitter`](http://nodejs.org/api/events.html) and emits the following events:

|Event|Params|Description|
|-----|------|-----------|
|`error`|`err`, `[message]`|Fired when an error occurs interacting with the queue. If the error correlates to a message, that error is included in Params|
|`processing_error`|`err`, `message`|Fired when an error occurs processing the message.|
|`timeout_error`|`err`, `message`|Fired when `handleMessageTimeout` is supplied as an option and if `handleMessage` times out.|
|`message_received`|`message`|Fired when a message is received.|
|`message_processed`|`message`|Fired when a message is successfully processed and removed from the queue.|
|`response_processed`|None|Fired after one batch of items (up to `batchSize`) has been successfully processed.|
|`stopped`|None|Fired when the consumer finally stops its work.|
|`empty`|None|Fired when the queue is empty (All messages have been consumed).|

### AWS IAM Permissions

Consumer will receive and delete messages from the SQS queue. Ensure `sqs:ReceiveMessage` and `sqs:DeleteMessage` access is granted on the queue being consumed.


### Contributing 
See contributing [guidelines](https://github.com/bbc/sqs-consumer/blob/master/.github/CONTRIBUTING.md).
