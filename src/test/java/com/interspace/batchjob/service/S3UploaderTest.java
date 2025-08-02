package com.interspace.batchjob.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class S3UploaderTest {

    @Mock
    private S3Client s3Client;

    private S3Uploader s3Uploader;

    @BeforeEach
    void setUp() {
        s3Uploader = new S3Uploader(s3Client);
    }

    @Test
    void uploadToS3_Success() {
        // Given
        String bucketName = "test-bucket";
        String objectKey = "test-key";
        String data = "test-data";
        
        PutObjectResponse response = PutObjectResponse.builder()
                .eTag("test-etag")
                .build();
        
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenReturn(response);

        // When
        s3Uploader.uploadToS3(bucketName, objectKey, data);

        // Then
        verify(s3Client, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }

    @Test
    void uploadToS3_Failure() {
        // Given
        String bucketName = "test-bucket";
        String objectKey = "test-key";
        String data = "test-data";
        
        when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
                .thenThrow(new RuntimeException("S3 error"));

        // When & Then
        assertThrows(RuntimeException.class, () -> {
            s3Uploader.uploadToS3(bucketName, objectKey, data);
        });
        
        verify(s3Client, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    }
}