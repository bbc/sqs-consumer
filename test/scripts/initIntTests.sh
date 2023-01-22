docker-compose --file ./test/scripts/docker-compose.yml up -d

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

echo "Waiting for SQS at address $QUEUE_URL, attempting every 5s"
until $(aws --endpoint-url=http://localhost:4566 sqs get-queue-url --queue-name sqs-consumer-data --region eu-west-1 | grep "{
    "QueueUrl": "http://localhost:4566/000000000000/sqs-consumer-data"
}" > /dev/null); do
    printf '.'
    sleep 5
done
echo ' Success: Reached SQS'
