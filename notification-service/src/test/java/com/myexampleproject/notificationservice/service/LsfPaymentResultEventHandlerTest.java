package com.myexampleproject.notificationservice.service;

import com.myexampleproject.common.event.PaymentFailedEvent;
import com.myexampleproject.common.event.PaymentProcessedEvent;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class LsfPaymentResultEventHandlerTest {

    @Mock
    private NotificationPaymentResultDispatcher dispatcher;

    @InjectMocks
    private LsfPaymentResultEventHandler handler;

    @Test
    void shouldDelegateProcessedEnvelopeToDispatcher() {
        EventEnvelope envelope = EventEnvelope.builder()
                .eventId("evt-processed-1")
                .eventType("payment.processed.v1")
                .aggregateId("ORDER-1")
                .build();
        PaymentProcessedEvent payload = new PaymentProcessedEvent("ORDER-1", "PAY-1");

        handler.handleProcessed(envelope, payload);

        verify(dispatcher).publishPaymentSuccess(payload, "lsf-envelope", "evt-processed-1");
    }

    @Test
    void shouldDelegateFailedEnvelopeToDispatcher() {
        EventEnvelope envelope = EventEnvelope.builder()
                .eventId("evt-failed-1")
                .eventType("payment.failed.v1")
                .aggregateId("ORDER-2")
                .build();
        PaymentFailedEvent payload = new PaymentFailedEvent("ORDER-2", "declined");

        handler.handleFailed(envelope, payload);

        verify(dispatcher).publishPaymentFailure(payload, "lsf-envelope", "evt-failed-1");
    }
}
