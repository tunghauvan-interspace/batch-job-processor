package com.interspace.batchjob.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class AppConfig {
    private static final Properties properties = new Properties();
    
    static {
        try (InputStream input = AppConfig.class.getClassLoader().getResourceAsStream("application.properties")) {
            if (input != null) {
                properties.load(input);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to load application properties", e);
        }
    }
    
    public static String getSqsQueueUrl() {
        return properties.getProperty("app.sqs.queue-url");
    }
    
    public static String getSqsDlqUrl() {
        return properties.getProperty("app.sqs.dlq-url");
    }
    
    public static String getS3BucketName() {
        return properties.getProperty("app.s3.bucket-name");
    }
    
    public static String getS3Region() {
        return properties.getProperty("app.s3.region");
    }
    
    public static int getMaxRetryAttempts() {
        return Integer.parseInt(properties.getProperty("app.retry.max-attempts", "3"));
    }
    
    public static long getInitialDelayMs() {
        return Long.parseLong(properties.getProperty("app.retry.initial-delay-ms", "1000"));
    }
    
    public static long getMaxDelayMs() {
        return Long.parseLong(properties.getProperty("app.retry.max-delay-ms", "30000"));
    }
    
    public static String getAwsRegion() {
        return properties.getProperty("aws.region");
    }
    
    public static String getAwsEndpoint() {
        return properties.getProperty("aws.endpoint");
    }
    
    public static String getAwsAccessKeyId() {
        return properties.getProperty("aws.access-key-id");
    }
    
    public static String getAwsSecretAccessKey() {
        return properties.getProperty("aws.secret-access-key");
    }
}