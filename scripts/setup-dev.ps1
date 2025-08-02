param()

# Start LocalStack and Jaeger
echo "Starting LocalStack and Jaeger..."
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to be ready..."
Start-Sleep -Seconds 10

# Check if LocalStack is ready
while (-not (Invoke-WebRequest -Uri "http://localhost:4566/_localstack/health" -UseBasicParsing -ErrorAction SilentlyContinue)) {
    echo "Waiting for LocalStack..."
    Start-Sleep -Seconds 2
}

# Check if Jaeger is ready
while (-not (Invoke-WebRequest -Uri "http://localhost:16686" -UseBasicParsing -ErrorAction SilentlyContinue)) {
    echo "Waiting for Jaeger..."
    Start-Sleep -Seconds 2
}

echo "Services are ready!"
echo "LocalStack: http://localhost:4566"
echo "Jaeger UI: http://localhost:16686"

# Create AWS resources
echo "Creating AWS resources..."
aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name batch-job-queue --region us-east-1
aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name batch-job-dlq --region us-east-1
aws --endpoint-url=http://localhost:4566 s3 mb s3://batch-job-bucket --region us-east-1

echo "Setup complete!"
