param()

# Send a test message to SQS
Write-Host "Sending test message to SQS..."

$QUEUE_URL = "http://localhost:4566/000000000000/batch-job-queue"
$id = [string](Get-Date -UFormat %s)
$timestamp = (Get-Date).ToString("o")
$MESSAGE_BODY = "{`"id`": `"$id`", `"timestamp`": `"$timestamp`", `"data`": `"This is a test message for batch job processing`"}"

aws --endpoint-url=http://localhost:4566 sqs send-message `
    --queue-url $QUEUE_URL `
    --message-body '$MESSAGE_BODY' `
    --region us-east-1

Write-Host "Test message sent!"
Write-Host ("Message body: {0}" -f $MESSAGE_BODY)
