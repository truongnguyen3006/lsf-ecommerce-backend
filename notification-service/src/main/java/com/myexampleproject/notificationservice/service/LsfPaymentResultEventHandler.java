package com.myexampleproject.notificationservice.service;

import com.myexampleproject.common.event.PaymentFailedEvent;
import com.myexampleproject.common.event.PaymentProcessedEvent;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.eventing.LsfEventHandler;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class LsfPaymentResultEventHandler {

    static final String PAYMENT_PROCESSED_EVENT_TYPE = "payment.processed.v1";
    static final String PAYMENT_FAILED_EVENT_TYPE = "payment.failed.v1";

    private final NotificationPaymentResultDispatcher dispatcher;

    @LsfEventHandler(
            value = PAYMENT_PROCESSED_EVENT_TYPE,
            payload = PaymentProcessedEvent.class
    )
    public void handleProcessed(EventEnvelope envelope, PaymentProcessedEvent payload) {
        dispatcher.publishPaymentSuccess(payload, "lsf-envelope", envelope.getEventId());
    }

    @LsfEventHandler(
            value = PAYMENT_FAILED_EVENT_TYPE,
            payload = PaymentFailedEvent.class
    )
    public void handleFailed(EventEnvelope envelope, PaymentFailedEvent payload) {
        dispatcher.publishPaymentFailure(payload, "lsf-envelope", envelope.getEventId());
    }
}
