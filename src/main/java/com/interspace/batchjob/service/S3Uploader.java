package com.interspace.batchjob.service;

import com.interspace.batchjob.telemetry.TelemetryConfig;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

public class S3Uploader {
    private static final Logger logger = LoggerFactory.getLogger(S3Uploader.class);
    private final S3Client s3Client;
    private final Tracer tracer;

    public S3Uploader(S3Client s3Client) {
        this.s3Client = s3Client;
        this.tracer = TelemetryConfig.getTracer();
    }

    public void uploadToS3(String bucketName, String objectKey, String data) {
        Span span = tracer.spanBuilder("s3.upload")
                .setAttribute("s3.bucket.name", bucketName)
                .setAttribute("s3.object.key", objectKey)
                .setAttribute("s3.object.size", data.length())
                .startSpan();

        try (var scope = span.makeCurrent()) {
            logger.info("Uploading data to S3: bucket={}, key={}", bucketName, objectKey);

            PutObjectRequest putObjectRequest = PutObjectRequest.builder()
                    .bucket(bucketName)
                    .key(objectKey)
                    .contentType("application/json")
                    .build();

            PutObjectResponse response = s3Client.putObject(putObjectRequest, RequestBody.fromString(data));
            
            span.setAttribute("s3.etag", response.eTag());
            span.setStatus(StatusCode.OK);
            
            logger.info("Successfully uploaded to S3: bucket={}, key={}, etag={}", 
                       bucketName, objectKey, response.eTag());

        } catch (Exception e) {
            span.setStatus(StatusCode.ERROR, e.getMessage());
            logger.error("Failed to upload to S3: bucket={}, key={}", bucketName, objectKey, e);
            throw new RuntimeException("Failed to upload to S3", e);
        } finally {
            span.end();
        }
    }
}