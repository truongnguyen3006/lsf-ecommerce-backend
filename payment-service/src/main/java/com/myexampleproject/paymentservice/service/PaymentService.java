package com.myexampleproject.paymentservice.service;

import com.myexampleproject.common.event.OrderValidatedEvent;
import com.myexampleproject.common.event.PaymentFailedEvent;
import com.myexampleproject.common.event.PaymentProcessedEvent;
import com.myorg.lsf.eventing.LsfPublishOptions;
import com.myorg.lsf.eventing.LsfPublisher;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
@Slf4j
public class PaymentService {

    public static final String PAYMENT_PROCESSED_ENVELOPE_TOPIC = "payment-processed-envelope-topic";
    public static final String PAYMENT_FAILED_ENVELOPE_TOPIC = "payment-failed-envelope-topic";
    public static final String PAYMENT_PROCESSED_EVENT_TYPE = "payment.processed.v1";
    public static final String PAYMENT_FAILED_EVENT_TYPE = "payment.failed.v1";

    private final LsfPublisher lsfPublisher;
    private final MeterRegistry meterRegistry;

    @Autowired
    public PaymentService(
            LsfPublisher lsfPublisher,
            MeterRegistry meterRegistry
    ) {
        this.lsfPublisher = lsfPublisher;
        this.meterRegistry = meterRegistry;
    }

    PaymentService(LsfPublisher lsfPublisher) {
        this(lsfPublisher, null);
    }

    public void processValidatedOrder(OrderValidatedEvent event, String source, String eventId) {
        String orderNumber = event.getOrderNumber();
        String safeEventId = eventId == null ? "legacy" : eventId;

        log.info("Processing validated order {} from {} (eventId={})", orderNumber, source, safeEventId);

        boolean paymentSuccess = processPayment(event);
        if (paymentSuccess) {
            String paymentId = UUID.randomUUID().toString();
            PaymentProcessedEvent successEvent = new PaymentProcessedEvent(orderNumber, paymentId);
            publishEnvelopeResult(
                    PAYMENT_PROCESSED_ENVELOPE_TOPIC,
                    PAYMENT_PROCESSED_EVENT_TYPE,
                    orderNumber,
                    successEvent,
                    eventId,
                    "processed"
            );
            log.info("Payment SUCCESS for order {} from {}. paymentId={}", orderNumber, source, paymentId);
            return;
        }

        PaymentFailedEvent failedEvent = new PaymentFailedEvent(
                orderNumber,
                "Payment gateway declined."
        );
        publishEnvelopeResult(
                PAYMENT_FAILED_ENVELOPE_TOPIC,
                PAYMENT_FAILED_EVENT_TYPE,
                orderNumber,
                failedEvent,
                eventId,
                "failed"
        );
        log.warn("Payment FAILED for order {} from {}. reason={}", orderNumber, source, failedEvent.getReason());
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

    private boolean processPayment(OrderValidatedEvent event) {
        return simulatePaymentDecision(event);
    }

    private boolean simulatePaymentDecision(OrderValidatedEvent event) {
        int totalQty = event.getItems().stream().mapToInt(item -> item.getQuantity()).sum();
        return totalQty != 2;
    }
}
