package com.myexampleproject.paymentservice.service;

import com.myexampleproject.common.dto.PaymentMethod;
import com.myexampleproject.common.event.OrderValidatedEvent;
import com.myexampleproject.common.event.PaymentFailedEvent;
import com.myexampleproject.common.event.PaymentProcessedEvent;
import com.myorg.lsf.eventing.LsfPublishOptions;
import com.myorg.lsf.eventing.LsfPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class PaymentService {

    public static final String PAYMENT_PROCESSED_ENVELOPE_TOPIC = "payment-processed-envelope-topic";
    public static final String PAYMENT_FAILED_ENVELOPE_TOPIC = "payment-failed-envelope-topic";
    public static final String PAYMENT_PROCESSED_EVENT_TYPE = "payment.processed.v1";
    public static final String PAYMENT_FAILED_EVENT_TYPE = "payment.failed.v1";
    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    private final LsfPublisher lsfPublisher;
    private final MeterRegistry meterRegistry;
    private final long demoProcessingDelayMs;
    private final ScheduledExecutorService scheduler;

    @Autowired
    public PaymentService(
            LsfPublisher lsfPublisher,
            MeterRegistry meterRegistry,
            @Value("${app.payment.demo-processing-delay-seconds:5}") long demoProcessingDelaySeconds
    ) {
        this.lsfPublisher = lsfPublisher;
        this.meterRegistry = meterRegistry;
        this.demoProcessingDelayMs = Math.max(0L, demoProcessingDelaySeconds * 1000L);
        this.scheduler = createScheduler(this.demoProcessingDelayMs);
    }

    PaymentService(LsfPublisher lsfPublisher, MeterRegistry meterRegistry) {
        this.lsfPublisher = lsfPublisher;
        this.meterRegistry = meterRegistry;
        this.demoProcessingDelayMs = 0L;
        this.scheduler = null;
    }

    PaymentService(LsfPublisher lsfPublisher) {
        this(lsfPublisher, null);
    }

    public void processValidatedOrder(OrderValidatedEvent event, String source, String eventId) {
        String orderNumber = event.getOrderNumber();
        String safeEventId = eventId == null ? "legacy" : eventId;
        PaymentMethod paymentMethod = resolvePaymentMethod(event.getPaymentMethod());

        log.info(
                "Processing validated order {} from {} (eventId={}) with paymentMethod={}",
                orderNumber,
                source,
                safeEventId,
                paymentMethod
        );

        switch (paymentMethod) {
            case MOCK_FAIL -> scheduleResult(
                    orderNumber,
                    paymentMethod,
                    () -> publishFailure(orderNumber, eventId, "Mock payment failed by selected scenario.")
            );
            case MOCK_TIMEOUT -> log.warn(
                    "Payment timeout scenario selected for order {}. No payment result will be published; saga timeout will handle compensation.",
                    orderNumber
            );
            case COD -> {
                log.info(
                        "COD selected for order {}. Publishing success immediately without demo delay.",
                        orderNumber
                );
                publishSuccess(orderNumber, eventId, "COD");
            }
            case MOCK_SUCCESS -> scheduleResult(
                    orderNumber,
                    paymentMethod,
                    () -> publishSuccess(orderNumber, eventId, "MOCK")
            );
        }
    }

    @PreDestroy
    void shutdownScheduler() {
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
    }

    private void scheduleResult(String orderNumber, PaymentMethod paymentMethod, Runnable task) {
        if (demoProcessingDelayMs <= 0 || scheduler == null) {
            task.run();
            return;
        }

        log.info(
                "Scheduling {} payment result for order {} after {} ms.",
                paymentMethod,
                orderNumber,
                demoProcessingDelayMs
        );

        scheduler.schedule(() -> {
            try {
                task.run();
            } catch (Exception exception) {
                log.error("Failed to publish scheduled payment result for order {}", orderNumber, exception);
            }
        }, demoProcessingDelayMs, TimeUnit.MILLISECONDS);
    }

    private void publishSuccess(String orderNumber, String causationId, String prefix) {
        String paymentId = prefix + "-" + UUID.randomUUID();
        PaymentProcessedEvent successEvent = new PaymentProcessedEvent(orderNumber, paymentId);
        publishEnvelopeResult(
                PAYMENT_PROCESSED_ENVELOPE_TOPIC,
                PAYMENT_PROCESSED_EVENT_TYPE,
                orderNumber,
                successEvent,
                causationId,
                "processed"
        );
        log.info("Payment SUCCESS for order {}. paymentId={}", orderNumber, paymentId);
    }

    private void publishFailure(String orderNumber, String causationId, String reason) {
        PaymentFailedEvent failedEvent = new PaymentFailedEvent(orderNumber, reason);
        publishEnvelopeResult(
                PAYMENT_FAILED_ENVELOPE_TOPIC,
                PAYMENT_FAILED_EVENT_TYPE,
                orderNumber,
                failedEvent,
                causationId,
                "failed"
        );
        log.warn("Payment FAILED for order {}. reason={}", orderNumber, reason);
    }

    private void publishEnvelopeResult(
            String topic,
            String eventType,
            String orderNumber,
            Object payload,
            String causationId,
            String result
    ) {
        lsfPublisher.publish(
                topic,
                orderNumber,
                eventType,
                orderNumber,
                payload,
                LsfPublishOptions.builder()
                        .correlationId(orderNumber)
                        .causationId(causationId)
                        .build()
        );
        recordPublishMetric("envelope", result);
    }

    private void recordPublishMetric(String path, String result) {
        if (meterRegistry == null) {
            return;
        }

        meterRegistry.counter(
                "payment_result_publish_total",
                "path", path,
                "result", result
        ).increment();
    }

    private PaymentMethod resolvePaymentMethod(PaymentMethod paymentMethod) {
        return paymentMethod == null ? PaymentMethod.defaultMethod() : paymentMethod;
    }

    private static ScheduledExecutorService createScheduler(long demoProcessingDelayMs) {
        if (demoProcessingDelayMs <= 0) {
            return null;
        }

        return Executors.newSingleThreadScheduledExecutor(runnable -> {
            Thread thread = new Thread(runnable, "payment-demo-scheduler");
            thread.setDaemon(true);
            return thread;
        });
    }
}
