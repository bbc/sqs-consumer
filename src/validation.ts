import { ReceiveMessageCommandOutput } from '@aws-sdk/client-sqs';

import { ConsumerOptions } from './types';

const requiredOptions = [
  'queueUrl',
  // only one of handleMessage / handleMessagesBatch is required
  'handleMessage|handleMessageBatch'
];

const MIN_BATCH_SIZE = 1;
const MAX_BATCH_SIZE = 10;
const MIN_WAIT_TIME_SECONDS = 1;
const MAX_WAIT_TIME_SECONDS = 20;

function validateOption(
  option: string,
  value: any,
  allOptions: { [key: string]: any },
  strict?: boolean
): void {
  switch (option) {
    case 'batchSize':
      if (value > MAX_BATCH_SIZE || value < MIN_BATCH_SIZE) {
        throw new Error('batchSize must be between 1 and 10.');
      }
      break;
    case 'concurrency':
      if (value < allOptions.batchSize) {
        throw new Error(
          'concurrency must not be less than the batchSize value.'
        );
      }
      break;
    case 'heartbeatInterval':
      if (
        !allOptions.visibilityTimeout ||
        value >= allOptions.visibilityTimeout
      ) {
        throw new Error(
          'heartbeatInterval must be less than visibilityTimeout.'
        );
      }
      break;
    case 'visibilityTimeout':
      if (
        allOptions.heartbeatInterval &&
        value <= allOptions.heartbeatInterval
      ) {
        throw new Error(
          'heartbeatInterval must be less than visibilityTimeout.'
        );
      }
      break;
    case 'waitTimeSeconds':
      if (value < MIN_WAIT_TIME_SECONDS || value > MAX_WAIT_TIME_SECONDS) {
        throw new Error('waitTimeSeconds must be between 0 and 20.');
      }
      break;
    default:
      if (strict) {
        throw new Error(`The update ${option} cannot be updated`);
      }
      break;
  }
}

/**
 * Ensure that the required options have been set.
 * @param options The options that have been set by the application.
 */
function assertOptions(options: ConsumerOptions): void {
  requiredOptions.forEach((option) => {
    const possibilities = option.split('|');
    if (!possibilities.find((p) => options[p])) {
      throw new Error(
        `Missing SQS consumer option [ ${possibilities.join(' or ')} ].`
      );
    }
  });

  if (options.batchSize) {
    validateOption('batchSize', options.batchSize, options);
  }
  if (options.concurrency) {
    validateOption('concurrency', options.concurrency, options);
  }
  if (options.heartbeatInterval) {
    validateOption('heartbeatInterval', options.heartbeatInterval, options);
  }
}

/**
 * Determine if the response from SQS has messages in it.
 * @param response The response from SQS.
 */
function hasMessages(response: ReceiveMessageCommandOutput): boolean {
  return response.Messages && response.Messages.length > 0;
}

export {
  MIN_BATCH_SIZE,
  MAX_BATCH_SIZE,
  hasMessages,
  assertOptions,
  validateOption
};
