#!/bin/bash

# Send a test message to SQS
echo "Sending test message to SQS..."

QUEUE_URL="http://localhost:4566/000000000000/batch-job-queue"
MESSAGE_BODY='{"id": "'$(date +%s)'", "timestamp": "'$(date -Iseconds)'", "data": "This is a test message for batch job processing"}'

aws --endpoint-url=http://localhost:4566 sqs send-message \
    --queue-url "$QUEUE_URL" \
    --message-body "$MESSAGE_BODY" \
    --region us-east-1

echo "Test message sent!"
echo "Message body: $MESSAGE_BODY"