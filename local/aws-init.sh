#!/bin/sh
set -eu

create_queue_pair() {
  queue_name=$1
  dlq_name=$2
  dlq_url=$(awslocal sqs create-queue --queue-name "$dlq_name" \
    --attributes MessageRetentionPeriod=1209600 --query QueueUrl --output text)
  dlq_arn=$(awslocal sqs get-queue-attributes --queue-url "$dlq_url" \
    --attribute-names QueueArn --query Attributes.QueueArn --output text)
  awslocal sqs create-queue --queue-name "$queue_name" \
    --attributes "{\"VisibilityTimeout\":\"360\",\"MessageRetentionPeriod\":\"86400\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"$dlq_arn\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"}"
}

create_queue_pair "$DB_QUEUE_NAME" "$DB_DLQ_NAME"
create_queue_pair "$S3_QUEUE_NAME" "$S3_DLQ_NAME"
printf '%s\n' "$ALLOWED_S3_BUCKETS" | tr ',' '\n' | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' | while IFS= read -r bucket; do
  [ -n "$bucket" ] || continue
  if [ "$AWS_REGION" = us-east-1 ]; then
    awslocal s3api create-bucket --bucket "$bucket"
  else
    awslocal s3api create-bucket --bucket "$bucket" --create-bucket-configuration "LocationConstraint=$AWS_REGION"
  fi
done
touch /tmp/deletion-ready
