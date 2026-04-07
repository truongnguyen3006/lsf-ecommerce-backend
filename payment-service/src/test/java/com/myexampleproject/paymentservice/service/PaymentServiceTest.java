package com.myexampleproject.paymentservice.service;

import com.myexampleproject.common.dto.OrderLineItemRequest;
import com.myexampleproject.common.event.OrderValidatedEvent;
import com.myexampleproject.common.event.PaymentFailedEvent;
import com.myexampleproject.common.event.PaymentProcessedEvent;
import com.myorg.lsf.eventing.LsfPublisher;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock
    private LsfPublisher lsfPublisher;

    @Test
    void shouldPublishPaymentProcessedToEnvelopeTopic() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        PaymentService service = new PaymentService(lsfPublisher, meterRegistry);
        OrderValidatedEvent event = new OrderValidatedEvent(
                "ORDER-1",
                List.of(new OrderLineItemRequest("SKU-1", 1))
        );

        service.processValidatedOrder(event, "lsf-envelope", "evt-1");

        verify(lsfPublisher).publish(
                eq(PaymentService.PAYMENT_PROCESSED_ENVELOPE_TOPIC),
                eq("ORDER-1"),
                eq(PaymentService.PAYMENT_PROCESSED_EVENT_TYPE),
                eq("ORDER-1"),
                argThat(payload ->
                        payload instanceof PaymentProcessedEvent processedEvent
                                && "ORDER-1".equals(processedEvent.getOrderNumber())
                                && processedEvent.getPaymentId() != null
                                && !processedEvent.getPaymentId().isBlank()
                ),
                argThat(options ->
                        options != null
                                && "ORDER-1".equals(options.getCorrelationId())
                                && "evt-1".equals(options.getCausationId())
                )
        );
        assertThat(meterRegistry.find("payment_result_publish_total")
                .tags("path", "legacy", "result", "processed")
                .counter()).isNull();
        assertThat(meterRegistry.get("payment_result_publish_total")
                .tag("path", "envelope")
                .tag("result", "processed")
                .counter()
                .count()).isEqualTo(1.0);
    }

    @Test
    void shouldPublishPaymentFailedToEnvelopeTopic() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        PaymentService service = new PaymentService(lsfPublisher, meterRegistry);
        OrderValidatedEvent event = new OrderValidatedEvent(
                "ORDER-2",
                List.of(
                        new OrderLineItemRequest("SKU-1", 1),
                        new OrderLineItemRequest("SKU-2", 1)
                )
        );

        service.processValidatedOrder(event, "lsf-envelope", null);

        verify(lsfPublisher).publish(
                eq(PaymentService.PAYMENT_FAILED_ENVELOPE_TOPIC),
                eq("ORDER-2"),
                eq(PaymentService.PAYMENT_FAILED_EVENT_TYPE),
                eq("ORDER-2"),
                argThat(payload ->
                        payload instanceof PaymentFailedEvent failedEvent
                                && "ORDER-2".equals(failedEvent.getOrderNumber())
                                && "Payment gateway declined.".equals(failedEvent.getReason())
                ),
                argThat(options ->
                        options != null
                                && "ORDER-2".equals(options.getCorrelationId())
                                && options.getCausationId() == null
                )
        );
        assertThat(meterRegistry.find("payment_result_publish_total")
                .tags("path", "legacy", "result", "failed")
                .counter()).isNull();
        assertThat(meterRegistry.get("payment_result_publish_total")
                .tag("path", "envelope")
                .tag("result", "failed")
                .counter()
                .count()).isEqualTo(1.0);
    }
}
