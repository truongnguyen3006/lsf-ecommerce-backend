package com.myexampleproject.orderservice.service;

import com.myexampleproject.common.event.PaymentFailedEvent;
import com.myexampleproject.common.event.PaymentProcessedEvent;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class OrderPaymentResultProcessor {

    private final OrderSagaStateService orderSagaStateService;
    private final Counter ordersCompletedCounter;

    public OrderPaymentResultProcessor(OrderSagaStateService orderSagaStateService, MeterRegistry meterRegistry) {
        this.orderSagaStateService = orderSagaStateService;
        this.ordersCompletedCounter = Counter.builder("orders_processed_total")
                .tag("status", "completed")
                .description("Total successful orders")
                .register(meterRegistry);
    }

    public void handlePaymentSuccess(PaymentProcessedEvent event, String source, String eventId) {
        boolean changed = orderSagaStateService.markCompletedAndEnqueueConfirm(event.getOrderNumber());

        if (changed) {
            ordersCompletedCounter.increment();
            log.info(
                    "Order {} status updated to COMPLETED via {} payment result (eventId={}).",
                    event.getOrderNumber(),
                    source,
                    eventId == null ? "legacy" : eventId
            );
        }
    }

    public void handlePaymentFailure(PaymentFailedEvent event, String source, String eventId) {
        boolean changed = orderSagaStateService.markFailedAndEnqueueRelease(
                event.getOrderNumber(),
                "PAYMENT_FAILED",
                "payment: " + event.getReason()
        );

        if (changed) {
            log.warn(
                    "Order {} status updated to PAYMENT_FAILED via {} payment result (eventId={}).",
                    event.getOrderNumber(),
                    source,
                    eventId == null ? "legacy" : eventId
            );
        }
    }
}
