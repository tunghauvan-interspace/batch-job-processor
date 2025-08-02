package com.interspace.batchjob.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class RetryServiceTest {

    private RetryService retryService;

    @BeforeEach
    void setUp() {
        retryService = new RetryService();
    }

    @Test
    void executeWithRetry_SuccessOnFirstAttempt() throws Exception {
        // Given
        Callable<String> operation = () -> "success";

        // When
        String result = retryService.executeWithRetry(operation);

        // Then
        assertEquals("success", result);
    }

    @Test
    void executeWithRetry_SuccessOnSecondAttempt() throws Exception {
        // Given
        AtomicInteger attempts = new AtomicInteger(0);
        Callable<String> operation = () -> {
            if (attempts.incrementAndGet() == 1) {
                throw new RuntimeException("First attempt failed");
            }
            return "success";
        };

        // When
        String result = retryService.executeWithRetry(operation);

        // Then
        assertEquals("success", result);
        assertEquals(2, attempts.get());
    }

    @Test
    void executeWithRetry_FailsAfterMaxAttempts() {
        // Given
        AtomicInteger attempts = new AtomicInteger(0);
        Callable<String> operation = () -> {
            attempts.incrementAndGet();
            throw new RuntimeException("Always fails");
        };

        // When & Then
        RuntimeException exception = assertThrows(RuntimeException.class, () -> {
            retryService.executeWithRetry(operation);
        });
        
        assertEquals("Always fails", exception.getMessage());
        assertEquals(3, attempts.get()); // Should attempt 3 times (max attempts)
    }
}