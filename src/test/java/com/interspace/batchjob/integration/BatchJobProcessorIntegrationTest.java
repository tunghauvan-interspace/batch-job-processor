package com.interspace.batchjob.integration;

import com.interspace.batchjob.service.RetryService;
import com.interspace.batchjob.service.S3Uploader;
import com.interspace.batchjob.service.SqsPoller;
import com.interspace.batchjob.telemetry.TelemetryConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SQS;

@Testcontainers
class BatchJobProcessorIntegrationTest {

    @Container
    static LocalStackContainer localstack = new LocalStackContainer(DockerImageName.parse("localstack/localstack:latest"))
            .withServices(S3, SQS);

    private SqsClient sqsClient;
    private S3Client s3Client;
    private String queueUrl;
    private String bucketName;

    @BeforeEach
    void setUp() {
        // Create AWS clients
        sqsClient = SqsClient.builder()
                .endpointOverride(localstack.getEndpointOverride(SQS))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
                ))
                .region(Region.of(localstack.getRegion()))
                .build();

        s3Client = S3Client.builder()
                .endpointOverride(localstack.getEndpointOverride(S3))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(localstack.getAccessKey(), localstack.getSecretKey())
                ))
                .region(Region.of(localstack.getRegion()))
                .forcePathStyle(true)
                .build();

        // Create SQS queue
        CreateQueueResponse queueResponse = sqsClient.createQueue(CreateQueueRequest.builder()
                .queueName("test-queue")
                .build());
        queueUrl = queueResponse.queueUrl();

        // Create S3 bucket
        bucketName = "test-bucket";
        s3Client.createBucket(CreateBucketRequest.builder()
                .bucket(bucketName)
                .build());
    }

    @Test
    void testSqsToS3Flow() throws Exception {
        // Given
        RetryService retryService = new RetryService();
        S3Uploader s3Uploader = new S3Uploader(s3Client);

        String messageBody = "{\"id\": \"test-123\", \"data\": \"test message content\"}";

        // Send message to SQS
        sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .build());

        // When - manually process one message cycle
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(1)
                .messageAttributeNames("All")
                .build();

        ReceiveMessageResponse response = sqsClient.receiveMessage(receiveRequest);
        List<Message> messages = response.messages();
        
        assertFalse(messages.isEmpty(), "Should receive at least one message");
        
        Message message = messages.get(0);
        
        // Process the message manually
        retryService.executeWithRetry(() -> {
            String objectKey = "messages/" + message.messageId() + ".json";
            s3Uploader.uploadToS3(bucketName, objectKey, message.body());
            
            DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            
            sqsClient.deleteMessage(deleteRequest);
            return null;
        });

        // Then
        // Verify message was processed and uploaded to S3
        ListObjectsV2Response objects = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucketName)
                .build());

        assertFalse(objects.contents().isEmpty(), "S3 bucket should contain uploaded objects");
        
        // Verify the uploaded content
        S3Object uploadedObject = objects.contents().get(0);
        assertTrue(uploadedObject.key().startsWith("messages/"), "Object key should start with 'messages/'");
        assertTrue(uploadedObject.key().endsWith(".json"), "Object key should end with '.json'");

        // Verify queue is empty (message was deleted after successful processing)
        ReceiveMessageResponse receiveResponse = sqsClient.receiveMessage(ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .waitTimeSeconds(1)
                .build());
        
        assertTrue(receiveResponse.messages().isEmpty(), "Queue should be empty after successful processing");
    }
}