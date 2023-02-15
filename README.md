# sqs-consumer

[![NPM downloads](https://img.shields.io/npm/dm/sqs-consumer.svg?style=flat)](https://npmjs.org/package/sqs-consumer)
[![Build Status](https://github.com/bbc/sqs-consumer/actions/workflows/test.yml/badge.svg?branch=main)](https://github.com/bbc/sqs-consumer/actions/workflows/test.yml)
[![Maintainability](https://api.codeclimate.com/v1/badges/16ec3f59e73bc898b7ff/maintainability)](https://codeclimate.com/github/bbc/sqs-consumer/maintainability)
[![Test Coverage](https://api.codeclimate.com/v1/badges/16ec3f59e73bc898b7ff/test_coverage)](https://codeclimate.com/github/bbc/sqs-consumer/test_coverage)

Build SQS-based applications without the boilerplate. Just define an async function that handles the SQS message processing.

## Installation

To install this package, simply enter the following command into your terminal (or the variant of whatever package manager you are using):

```bash
npm install --save-dev sqs-consumer
```

> **Note**
> This library assumes you are using [AWS SDK v3](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-sqs/index.html). If you are using v2, please install v5.8.0:
>
> ```bash
> npm install sqs-consumer@5.8.0 --save-dev
> ```

### Node version

From v7 and above, this library will only support Node v16 or above. If you are still using Node 14, please use a previous version of the library.

This decision was made due to the removal of security support from the Node.JS team from April 30th, 2023.

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

- The queue is polled continuously for messages using [long polling](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-long-polling.html).
- Messages are deleted from the queue once the handler function has completed successfully.
- Throwing an error (or returning a rejected promise) from the handler function will cause the message to be left on the queue. An [SQS redrive policy](https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/SQSDeadLetterQueue.html) can be used to move messages that cannot be processed to a dead letter queue.
- By default messages are processed one at a time – a new message won't be received until the first one has been processed. To process messages in parallel, use the `batchSize` option [detailed below](#options).
- By default, messages that are sent to the `handleMessage` and `handleMessageBatch` functions will be considered as processed if they return without an error. To acknowledge individual messages, please return the message that you want to acknowledge if you are using `handleMessage` or the messages for `handleMessageBatch`. It's also important to await any processing that you are doing to ensure that messages are processed one at a time.

### Credentials

By default the consumer will look for AWS credentials in the places [specified by the AWS SDK](https://docs.aws.amazon.com/AWSJavaScriptSDK/guide/node-configuring.html#Setting_AWS_Credentials). The simplest option is to export your credentials as environment variables:

```bash
export AWS_SECRET_ACCESS_KEY=...
export AWS_ACCESS_KEY_ID=...
```

If you need to specify your credentials manually, you can use a pre-configured instance of the [SQS Client](https://docs.aws.amazon.com/AWSJavaScriptSDK/v3/latest/clients/client-sqs/classes/sqsclient.html) client.

```js
import { Consumer } from 'sqs-consumer';
import { SQSClient } from '@aws-sdk/client-sqs';

const app = Consumer.create({
  queueUrl: 'https://sqs.eu-west-1.amazonaws.com/account-id/queue-name',
  handleMessage: async (message) => {
    // ...
  },
  sqs: new SQSClient({
    region: 'my-region',
    credentials: {
      accessKeyId: 'yourAccessKey',
      secretAccessKey: 'yourSecret'
    }
  })
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

### AWS IAM Permissions

Consumer will receive and delete messages from the SQS queue. Ensure `sqs:ReceiveMessage`, `sqs:DeleteMessage`, `sqs:DeleteMessageBatch`, `sqs:ChangeMessageVisibility` and `sqs:ChangeMessageVisibilityBatch` access is granted on the queue being consumed.

## API

### `Consumer.create(options)`

Creates a new SQS consumer using the [defined options](https://bbc.github.io/sqs-consumer/interfaces/ConsumerOptions.html).

### `consumer.start()`

Start polling the queue for messages.

### `consumer.stop(options)`

Stop polling the queue for messages. [You can find the options definition here](https://bbc.github.io/sqs-consumer/interfaces/StopOptions.html).

By default, the value of `abort` is set to `false` which means pre existing requests to AWS SQS will still be made until they have concluded. If you would like to abort these requests instead, pass the abort value as `true`, like so:

`consumer.stop({ abort: true })`

### `consumer.isRunning`

Returns the current polling state of the consumer: `true` if it is actively polling, `false` if it is not.

### Events

Each consumer is an [`EventEmitter`](https://nodejs.org/api/events.html) and [emits these events](https://bbc.github.io/sqs-consumer/interfaces/Events.html).

## Contributing

We welcome and appreciate contributions for anyone who would like to take the time to fix a bug or implement a new feature.

But before you get started, [please read the contributing guidelines](https://github.com/bbc/sqs-consumer/blob/main/.github/CONTRIBUTING.md) and [code of conduct](https://github.com/bbc/sqs-consumer/blob/main/.github/CODE_OF_CONDUCT.md).

## License

SQS Consumer is distributed under the Apache License, Version 2.0, see [LICENSE](https://github.com/bbc/sqs-consumer/blob/main/LICENSE) for more information.
