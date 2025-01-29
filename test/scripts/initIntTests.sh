#!/bin/sh
SCRIPT_DIR=$(cd "$(dirname "$0")" && pwd)

# Setup AWS credentials
mkdir -p ~/.aws
cat > ~/.aws/credentials << EOF
[default]
aws_access_key_id = key
aws_secret_access_key = secret
region = eu-west-1
EOF

cd "${SCRIPT_DIR}"
podman-compose up -d

if [ $? -eq 0 ]
then
  echo "Successfully started podman containers"
else
  echo "Could not start podman containers" >&2
  exit 1
fi

# Export these for LocalStack
export AWS_ACCESS_KEY_ID="key"
export AWS_SECRET_ACCESS_KEY="secret"
export AWS_DEFAULT_REGION="eu-west-1"
# Disable AWS CLI pager
export AWS_PAGER=""

echo "Waiting for SQS, attempting every 5s"
until aws --endpoint-url=http://localhost:4566 sqs get-queue-url --queue-name sqs-consumer-data --region eu-west-1 | grep "{
    \"QueueUrl\": \"http://localhost:4566/000000000000/sqs-consumer-data\"
}" > /dev/null; do
    printf '.'
    sleep 5
done
echo ' Service is up!'
