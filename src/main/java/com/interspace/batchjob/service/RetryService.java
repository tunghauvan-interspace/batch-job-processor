package com.interspace.batchjob.service;

import com.interspace.batchjob.config.AppConfig;
import com.interspace.batchjob.telemetry.TelemetryConfig;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class RetryService {
    private static final Logger logger = LoggerFactory.getLogger(RetryService.class);
    private final Tracer tracer;
    private final int maxAttempts;
    private final long initialDelayMs;
    private final long maxDelayMs;

    public RetryService() {
        this.tracer = TelemetryConfig.getTracer();
        this.maxAttempts = AppConfig.getMaxRetryAttempts();
        this.initialDelayMs = AppConfig.getInitialDelayMs();
        this.maxDelayMs = AppConfig.getMaxDelayMs();
    }

    public <T> T executeWithRetry(Callable<T> operation) throws Exception {
        Exception lastException = null;
        
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            Span span = tracer.spanBuilder("retry.attempt")
                    .setAttribute("retry.attempt_number", attempt)
                    .setAttribute("retry.max_attempts", maxAttempts)
                    .startSpan();

            try (var scope = span.makeCurrent()) {
                T result = operation.call();
                span.setStatus(StatusCode.OK);
                logger.debug("Operation succeeded on attempt {}", attempt);
                return result;
                
            } catch (Exception e) {
                lastException = e;
                span.setStatus(StatusCode.ERROR, e.getMessage());
                span.setAttribute("error.type", e.getClass().getSimpleName());
                
                logger.warn("Operation failed on attempt {} of {}: {}", 
                           attempt, maxAttempts, e.getMessage());
                
                if (attempt < maxAttempts) {
                    long delayMs = calculateDelay(attempt);
                    span.setAttribute("retry.delay_ms", delayMs);
                    
                    try {
                        Thread.sleep(delayMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Retry interrupted", ie);
                    }
                }
            } finally {
                span.end();
            }
        }
        
        logger.error("Operation failed after {} attempts", maxAttempts);
        throw lastException;
    }

    private long calculateDelay(int attempt) {
        // Exponential backoff with jitter
        long delay = Math.min(initialDelayMs * (1L << (attempt - 1)), maxDelayMs);
        
        // Add jitter (±25%)
        double jitter = 0.25 * (2.0 * Math.random() - 1.0);
        delay = (long) (delay * (1.0 + jitter));
        
        return Math.max(delay, 100); // Minimum 100ms delay
    }
}