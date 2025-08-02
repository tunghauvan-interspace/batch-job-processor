package com.interspace.batchjob.telemetry;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.exporter.jaeger.JaegerGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;

public class TelemetryConfig {
    private static final String SERVICE_NAME = "batch-job-processor";
    private static final String SERVICE_VERSION = "1.0.0";
    private static final String JAEGER_ENDPOINT = "http://localhost:14250";
    
    private static OpenTelemetry openTelemetry;
    private static Tracer tracer;
    
    public static void initialize() {
        Resource resource = Resource.getDefault()
                .merge(Resource.create(Attributes.of(
                        AttributeKey.stringKey("service.name"), SERVICE_NAME,
                        AttributeKey.stringKey("service.version"), SERVICE_VERSION)));

        JaegerGrpcSpanExporter jaegerExporter = JaegerGrpcSpanExporter.builder()
                .setEndpoint(JAEGER_ENDPOINT)
                .build();

        SdkTracerProvider tracerProvider = SdkTracerProvider.builder()
                .addSpanProcessor(BatchSpanProcessor.builder(jaegerExporter).build())
                .setResource(resource)
                .build();

        openTelemetry = OpenTelemetrySdk.builder()
                .setTracerProvider(tracerProvider)
                .build();
        
        tracer = openTelemetry.getTracer(SERVICE_NAME);
    }
    
    public static OpenTelemetry getOpenTelemetry() {
        if (openTelemetry == null) {
            initialize();
        }
        return openTelemetry;
    }
    
    public static Tracer getTracer() {
        if (tracer == null) {
            initialize();
        }
        return tracer;
    }
}