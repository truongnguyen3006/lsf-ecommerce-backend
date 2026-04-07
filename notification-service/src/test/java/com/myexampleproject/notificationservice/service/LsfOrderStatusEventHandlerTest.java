package com.myexampleproject.notificationservice.service;

import com.myexampleproject.common.event.OrderStatusEvent;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class LsfOrderStatusEventHandlerTest {

    @Mock
    private SimpMessagingTemplate messagingTemplate;

    @InjectMocks
    private LsfOrderStatusEventHandler handler;

    @Test
    void shouldPublishOrderStatusMessageToWebSocketTopic() {
        EventEnvelope envelope = EventEnvelope.builder()
                .eventId("evt-1")
                .eventType("ecommerce.order.status.v1")
                .aggregateId("order-123")
                .build();
        OrderStatusEvent payload = new OrderStatusEvent("ORDER-123", "COMPLETED");

        handler.handle(envelope, payload);

        verify(messagingTemplate).convertAndSend(
                eq("/topic/order/ORDER-123"),
                (Object) argThat(message ->
                        message instanceof LsfOrderStatusEventHandler.NotificationMessage notificationMessage
                                && "COMPLETED".equals(notificationMessage.status())
                )
        );
    }
}
