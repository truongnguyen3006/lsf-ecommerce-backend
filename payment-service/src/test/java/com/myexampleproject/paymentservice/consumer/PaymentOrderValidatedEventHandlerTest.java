package com.myexampleproject.paymentservice.consumer;

import com.myexampleproject.common.dto.OrderLineItemRequest;
import com.myexampleproject.common.event.OrderValidatedEvent;
import com.myexampleproject.paymentservice.service.PaymentService;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class PaymentOrderValidatedEventHandlerTest {

    @Mock
    private PaymentService paymentService;

    @Test
    void shouldDelegateEnvelopeEventToPaymentService() {
        PaymentOrderValidatedEventHandler handler = new PaymentOrderValidatedEventHandler(paymentService);
        EventEnvelope envelope = EventEnvelope.builder()
                .eventId("evt-validated-1")
                .eventType("order.validated.v1")
                .aggregateId("ORDER-1")
                .build();
        OrderValidatedEvent payload = new OrderValidatedEvent(
                "ORDER-1",
                List.of(new OrderLineItemRequest("SKU-1", 1))
        );

        handler.handle(envelope, payload);

        verify(paymentService).processValidatedOrder(payload, "lsf-envelope", "evt-validated-1");
    }
}
