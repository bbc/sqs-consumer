#!/usr/bin/env bash

set -euo pipefail

SQS_ENDPOINT="http://localhost:9324"
QUEUE_NAME="sqs-consumer-data"
QUEUE_URL="${SQS_ENDPOINT}/000000000000/${QUEUE_NAME}"

docker compose --file ./test/scripts/docker-compose.yml up -d --remove-orphans

if [ $? -eq 0 ]
then
  echo "Successfully started docker"
else
  echo "Could not start docker" >&2
  exit 1
fi

export AWS_ACCESS_KEY_ID="key"
export AWS_SECRET_ACCESS_KEY="secret"
export AWS_DEFAULT_REGION="eu-west-1"

echo "Waiting for ElasticMQ, attempting every 5s"
until aws --endpoint-url="${SQS_ENDPOINT}" sqs list-queues --region eu-west-1 > /dev/null 2>&1; do
    printf '.'
    sleep 5
done

aws --endpoint-url="${SQS_ENDPOINT}" sqs create-queue \
  --queue-name "${QUEUE_NAME}" \
  --region eu-west-1 \
  --attributes VisibilityTimeout=30 > /dev/null

until [ "$(aws --endpoint-url="${SQS_ENDPOINT}" sqs get-queue-url --queue-name "${QUEUE_NAME}" --region eu-west-1 --query QueueUrl --output text 2> /dev/null)" = "${QUEUE_URL}" ]; do
  printf '.'
  sleep 5
done

echo " ElasticMQ is up!"
