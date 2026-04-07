package com.myexampleproject.paymentservice.consumer;

import com.myexampleproject.common.event.OrderValidatedEvent;
import com.myexampleproject.paymentservice.service.PaymentService;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.eventing.LsfEventHandler;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class PaymentOrderValidatedEventHandler {

    static final String ORDER_VALIDATED_EVENT_TYPE = "order.validated.v1";

    private final PaymentService paymentService;

    @LsfEventHandler(
            value = ORDER_VALIDATED_EVENT_TYPE,
            payload = OrderValidatedEvent.class
    )
    public void handle(EventEnvelope envelope, OrderValidatedEvent payload) {
        paymentService.processValidatedOrder(payload, "lsf-envelope", envelope.getEventId());
    }
}
