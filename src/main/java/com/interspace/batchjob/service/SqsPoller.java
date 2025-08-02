package com.interspace.batchjob.service;

import com.interspace.batchjob.config.AppConfig;
import com.interspace.batchjob.telemetry.TelemetryConfig;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class SqsPoller {
    private static final Logger logger = LoggerFactory.getLogger(SqsPoller.class);
    private final SqsClient sqsClient;
    private final S3Uploader s3Uploader;
    private final RetryService retryService;
    private final Tracer tracer;
    private volatile boolean running = false;

    public SqsPoller(SqsClient sqsClient, S3Uploader s3Uploader, RetryService retryService) {
        this.sqsClient = sqsClient;
        this.s3Uploader = s3Uploader;
        this.retryService = retryService;
        this.tracer = TelemetryConfig.getTracer();
    }

    public void startPolling() {
        running = true;
        logger.info("Starting SQS polling for queue: {}", AppConfig.getSqsQueueUrl());
        
        while (running) {
            try {
                pollMessages();
                Thread.sleep(1000); // Short delay between polling cycles
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("Error during polling cycle", e);
                try {
                    Thread.sleep(5000); // Longer delay on error
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        }
    }

    public void stopPolling() {
        running = false;
        logger.info("Stopping SQS polling");
    }

    public void pollMessages() {
        Span span = tracer.spanBuilder("sqs.poll_messages")
                .setAttribute("sqs.queue.url", AppConfig.getSqsQueueUrl())
                .startSpan();

        try (var scope = span.makeCurrent()) {
            ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                    .queueUrl(AppConfig.getSqsQueueUrl())
                    .maxNumberOfMessages(10)
                    .waitTimeSeconds(20) // Long polling
                    .messageAttributeNames("All")
                    .build();

            ReceiveMessageResponse response = sqsClient.receiveMessage(receiveRequest);
            List<Message> messages = response.messages();
            
            span.setAttribute("sqs.messages.received", messages.size());
            logger.info("Received {} messages from SQS", messages.size());

            for (Message message : messages) {
                processMessage(message);
            }
            
            span.setStatus(StatusCode.OK);
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            logger.error("Error polling messages from SQS", e);
            throw e;
        } finally {
            span.end();
        }
    }

    private void processMessage(Message message) {
        String messageId = message.messageId();
        Span span = tracer.spanBuilder("sqs.process_message")
                .setAttribute("sqs.message.id", messageId)
                .setAttribute("sqs.queue.url", AppConfig.getSqsQueueUrl())
                .startSpan();

        try (var scope = span.makeCurrent()) {
            logger.info("Processing message: {}", messageId);
            
            // Process message with retry logic
            retryService.executeWithRetry(() -> {
                // Upload message content to S3
                String objectKey = "messages/" + messageId + ".json";
                s3Uploader.uploadToS3(AppConfig.getS3BucketName(), objectKey, message.body());
                
                // Delete message from SQS after successful processing
                deleteMessage(message);
                return null;
            });
            
            span.setStatus(StatusCode.OK);
            logger.info("Successfully processed message: {}", messageId);
            
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            logger.error("Failed to process message: {}", messageId, e);
            
            // Send to DLQ after max retries
            sendToDlq(message, e.getMessage());
        } finally {
            span.end();
        }
    }

    private void deleteMessage(Message message) {
        DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                .queueUrl(AppConfig.getSqsQueueUrl())
                .receiptHandle(message.receiptHandle())
                .build();
        
        sqsClient.deleteMessage(deleteRequest);
        logger.debug("Deleted message from SQS: {}", message.messageId());
    }

    private void sendToDlq(Message message, String errorMessage) {
        Span span = tracer.spanBuilder("sqs.send_to_dlq")
                .setAttribute("sqs.message.id", message.messageId())
                .setAttribute("sqs.dlq.url", AppConfig.getSqsDlqUrl())
                .setAttribute("error.message", errorMessage)
                .startSpan();

        try (var scope = span.makeCurrent()) {
            SendMessageRequest dlqRequest = SendMessageRequest.builder()
                    .queueUrl(AppConfig.getSqsDlqUrl())
                    .messageBody(message.body())
                    .messageAttributes(message.messageAttributes())
                    .build();

            sqsClient.sendMessage(dlqRequest);
            
            // Delete original message after sending to DLQ
            deleteMessage(message);
            
            span.setStatus(StatusCode.OK);
            logger.warn("Sent message to DLQ: {}", message.messageId());
            
        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            logger.error("Failed to send message to DLQ: {}", message.messageId(), e);
        } finally {
            span.end();
        }
    }
}