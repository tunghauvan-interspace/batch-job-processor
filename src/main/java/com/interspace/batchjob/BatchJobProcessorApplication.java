package com.interspace.batchjob;

import com.interspace.batchjob.config.AppConfig;
import com.interspace.batchjob.config.AwsClientFactory;
import com.interspace.batchjob.service.RetryService;
import com.interspace.batchjob.service.S3Uploader;
import com.interspace.batchjob.service.SqsPoller;
import com.interspace.batchjob.telemetry.TelemetryConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.CreateQueueRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;

public class BatchJobProcessorApplication {
    private static final Logger logger = LoggerFactory.getLogger(BatchJobProcessorApplication.class);

    public static void main(String[] args) {
        logger.info("Starting Batch Job Processor Application");

        try {
            // Initialize OpenTelemetry
            TelemetryConfig.initialize();
            logger.info("OpenTelemetry initialized");

            // Create AWS clients
            SqsClient sqsClient = AwsClientFactory.createSqsClient();
            S3Client s3Client = AwsClientFactory.createS3Client();
            logger.info("AWS clients created");

            // Initialize AWS resources
            initializeAwsResources(sqsClient, s3Client);

            // Create services
            RetryService retryService = new RetryService();
            S3Uploader s3Uploader = new S3Uploader(s3Client);
            SqsPoller sqsPoller = new SqsPoller(sqsClient, s3Uploader, retryService);

            // Add shutdown hook for graceful shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Shutting down application...");
                sqsPoller.stopPolling();
                sqsClient.close();
                s3Client.close();
                logger.info("Application shutdown complete");
            }));

            // Start polling (this will run indefinitely)
            sqsPoller.startPolling();

        } catch (Exception e) {
            logger.error("Application startup failed", e);
            System.exit(1);
        }
    }

    private static void initializeAwsResources(SqsClient sqsClient, S3Client s3Client) {
        logger.info("Initializing AWS resources...");
        
        // Create SQS queues if they don't exist
        createQueueIfNotExists(sqsClient, "batch-job-queue");
        createQueueIfNotExists(sqsClient, "batch-job-dlq");
        
        // Create S3 bucket if it doesn't exist
        createBucketIfNotExists(s3Client, AppConfig.getS3BucketName());
        
        logger.info("AWS resources initialized");
    }

    private static void createQueueIfNotExists(SqsClient sqsClient, String queueName) {
        try {
            GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                    .queueName(queueName)
                    .build();
            sqsClient.getQueueUrl(getQueueRequest);
            logger.info("Queue already exists: {}", queueName);
        } catch (Exception e) {
            logger.info("Creating queue: {}", queueName);
            CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .build();
            sqsClient.createQueue(createQueueRequest);
            logger.info("Queue created: {}", queueName);
        }
    }

    private static void createBucketIfNotExists(S3Client s3Client, String bucketName) {
        try {
            HeadBucketRequest headBucketRequest = HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            s3Client.headBucket(headBucketRequest);
            logger.info("Bucket already exists: {}", bucketName);
        } catch (Exception e) {
            logger.info("Creating bucket: {}", bucketName);
            CreateBucketRequest createBucketRequest = CreateBucketRequest.builder()
                    .bucket(bucketName)
                    .build();
            s3Client.createBucket(createBucketRequest);
            logger.info("Bucket created: {}", bucketName);
        }
    }
}