package com.myexampleproject.orderservice.service;

import com.myexampleproject.common.event.PaymentFailedEvent;
import com.myexampleproject.common.event.PaymentProcessedEvent;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.eventing.LsfEventHandler;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(
        name = "app.order.workflow.mode",
        havingValue = "legacy"
)
@RequiredArgsConstructor
public class OrderPaymentResultEventHandler {

    static final String PAYMENT_PROCESSED_EVENT_TYPE = "payment.processed.v1";
    static final String PAYMENT_FAILED_EVENT_TYPE = "payment.failed.v1";

    private final OrderPaymentResultProcessor orderPaymentResultProcessor;

    @LsfEventHandler(
            value = PAYMENT_PROCESSED_EVENT_TYPE,
            payload = PaymentProcessedEvent.class
    )
    public void handleProcessed(EventEnvelope envelope, PaymentProcessedEvent payload) {
        orderPaymentResultProcessor.handlePaymentSuccess(payload, "lsf-envelope", envelope.getEventId());
    }

    @LsfEventHandler(
            value = PAYMENT_FAILED_EVENT_TYPE,
            payload = PaymentFailedEvent.class
    )
    public void handleFailed(EventEnvelope envelope, PaymentFailedEvent payload) {
        orderPaymentResultProcessor.handlePaymentFailure(payload, "lsf-envelope", envelope.getEventId());
    }
}
