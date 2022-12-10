const { Endpoint, SQS } = require('aws-sdk');

const sqsConfig: {
  endpoint?: any;
  accessKeyId?: string;
  secretAccessKey?: string;
  region: string;
} = {
  region: 'eu-west-1'
};

if (process.env.SQS_ENDPOINT) {
  sqsConfig.endpoint = new Endpoint(process.env.SQS_ENDPOINT);
} else if (process.env.NODE_ENV === 'development') {
  sqsConfig.endpoint = new Endpoint('http://localhost:4566');
}
if (process.env.SQS_ACCESS_KEY_ID) {
  sqsConfig.accessKeyId = process.env.SQS_ACCESS_KEY_ID;
} else if (process.env.NODE_ENV === 'development') {
  sqsConfig.accessKeyId = 'na';
}
if (process.env.SQS_SECRET_ACCESS_KEY) {
  sqsConfig.secretAccessKey = process.env.SQS_SECRET_ACCESS_KEY;
} else if (process.env.NODE_ENV === 'development') {
  sqsConfig.secretAccessKey = 'na';
}

exports.sqs =
  !sqsConfig.endpoint && process.env.NODE_ENV === 'development'
    ? null
    : new SQS(sqsConfig);

exports.QUEUE_NAME = process.env.SQS_QUEUE_NAME || 'sqs-consumer-data';

exports.QUEUE_URL =
  process.env.SQS_QUEUE_URL ||
  'http://localhost:4566/000000000000/sqs-consumer-data';

exports.sqsConfig = sqsConfig;

export {};
