package com.interspace.batchjob.config;

import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.sqs.SqsClient;

import java.net.URI;

public class AwsClientFactory {
    
    public static SqsClient createSqsClient() {
        return SqsClient.builder()
                .region(Region.of(AppConfig.getAwsRegion()))
                .endpointOverride(URI.create(AppConfig.getAwsEndpoint()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                                AppConfig.getAwsAccessKeyId(),
                                AppConfig.getAwsSecretAccessKey()
                        )
                ))
                .build();
    }
    
    public static S3Client createS3Client() {
        return S3Client.builder()
                .region(Region.of(AppConfig.getAwsRegion()))
                .endpointOverride(URI.create(AppConfig.getAwsEndpoint()))
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create(
                                AppConfig.getAwsAccessKeyId(),
                                AppConfig.getAwsSecretAccessKey()
                        )
                ))
                .forcePathStyle(true) // Required for LocalStack
                .build();
    }
}