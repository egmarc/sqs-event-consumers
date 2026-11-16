#!/bin/sh
set -eu

for queue in db-deletion s3-deletion; do
  dlq_url=$(awslocal sqs create-queue --queue-name "$queue-dlq" \
    --attributes MessageRetentionPeriod=1209600 --query QueueUrl --output text)
  dlq_arn=$(awslocal sqs get-queue-attributes --queue-url "$dlq_url" \
    --attribute-names QueueArn --query Attributes.QueueArn --output text)
  awslocal sqs create-queue --queue-name "$queue-queue" \
    --attributes "{\"VisibilityTimeout\":\"360\",\"MessageRetentionPeriod\":\"86400\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$dlq_arn\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"}"
done
awslocal s3api create-bucket --bucket deletion-objects
touch /tmp/deletion-ready
