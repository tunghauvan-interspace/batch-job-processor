package com.interspace.batchjob.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class SqsPollerTest {

    @Mock
    private SqsClient sqsClient;
    
    @Mock
    private S3Uploader s3Uploader;
    
    @Mock
    private RetryService retryService;

    private SqsPoller sqsPoller;

    @BeforeEach
    void setUp() {
        sqsPoller = new SqsPoller(sqsClient, s3Uploader, retryService);
    }

    @Test
    void pollMessages_Success() throws Exception {
        // Given
        Message message1 = Message.builder()
                .messageId("msg1")
                .body("test message 1")
                .receiptHandle("receipt1")
                .build();
        
        Message message2 = Message.builder()
                .messageId("msg2")
                .body("test message 2")
                .receiptHandle("receipt2")
                .build();

        ReceiveMessageResponse response = ReceiveMessageResponse.builder()
                .messages(Arrays.asList(message1, message2))
                .build();

        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(response);
        
        when(retryService.executeWithRetry(any()))
                .thenAnswer(invocation -> {
                    try {
                        return invocation.getArgument(0, java.util.concurrent.Callable.class).call();
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

        // When
        sqsPoller.pollMessages();

        // Then
        verify(sqsClient, times(1)).receiveMessage(any(ReceiveMessageRequest.class));
        verify(retryService, times(2)).executeWithRetry(any());
        verify(sqsClient, times(2)).deleteMessage(any(DeleteMessageRequest.class));
    }

    @Test
    void pollMessages_NoMessages() throws Exception {
        // Given
        ReceiveMessageResponse response = ReceiveMessageResponse.builder()
                .messages(Collections.emptyList())
                .build();

        when(sqsClient.receiveMessage(any(ReceiveMessageRequest.class)))
                .thenReturn(response);

        // When
        sqsPoller.pollMessages();

        // Then
        verify(sqsClient, times(1)).receiveMessage(any(ReceiveMessageRequest.class));
        verify(retryService, never()).executeWithRetry(any());
        verify(sqsClient, never()).deleteMessage(any(DeleteMessageRequest.class));
    }
}