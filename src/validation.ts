import { ReceiveMessageCommandOutput } from '@aws-sdk/client-sqs';

import { ConsumerOptions } from './types';

const requiredOptions = [
  'queueUrl',
  // only one of handleMessage / handleMessagesBatch is required
  'handleMessage|handleMessageBatch'
];

function assertOptions(options: ConsumerOptions): void {
  requiredOptions.forEach((option) => {
    const possibilities = option.split('|');
    if (!possibilities.find((p) => options[p])) {
      throw new Error(
        `Missing SQS consumer option [ ${possibilities.join(' or ')} ].`
      );
    }
  });

  if (options.batchSize > 10 || options.batchSize < 1) {
    throw new Error('SQS batchSize option must be between 1 and 10.');
  }

  if (
    options.heartbeatInterval &&
    !(options.heartbeatInterval < options.visibilityTimeout)
  ) {
    throw new Error('heartbeatInterval must be less than visibilityTimeout.');
  }
}

function hasMessages(response: ReceiveMessageCommandOutput): boolean {
  return response.Messages && response.Messages.length > 0;
}

export { hasMessages, assertOptions };
