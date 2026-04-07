package com.myexampleproject.notificationservice.service;

import com.myexampleproject.common.event.PaymentFailedEvent;
import com.myexampleproject.common.event.PaymentProcessedEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.util.Map;

import static org.mockito.ArgumentMatchers.eq;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class NotificationPaymentResultDispatcherTest {

    @Mock
    private SimpMessagingTemplate messagingTemplate;

    @InjectMocks
    private NotificationPaymentResultDispatcher dispatcher;

    @Test
    void shouldPublishPaymentSuccessMessage() {
        dispatcher.publishPaymentSuccess(new PaymentProcessedEvent("ORDER-1", "PAY-1"), "lsf-envelope", "evt-1");

        ArgumentCaptor<Object> payloadCaptor = ArgumentCaptor.forClass(Object.class);
        verify(messagingTemplate).convertAndSend(
                eq("/topic/order/ORDER-1"),
                payloadCaptor.capture()
        );
        assertThat(payloadCaptor.getValue()).isInstanceOf(Map.class);
        Map<?, ?> payload = (Map<?, ?>) payloadCaptor.getValue();
        assertThat(payload.get("status")).isEqualTo("COMPLETED");
    }

    @Test
    void shouldPublishPaymentFailureMessage() {
        dispatcher.publishPaymentFailure(
                new PaymentFailedEvent("ORDER-2", "declined"),
                "legacy-topic",
                null
        );

        ArgumentCaptor<Object> payloadCaptor = ArgumentCaptor.forClass(Object.class);
        verify(messagingTemplate).convertAndSend(
                eq("/topic/order/ORDER-2"),
                payloadCaptor.capture()
        );
        assertThat(payloadCaptor.getValue()).isInstanceOf(Map.class);
        Map<?, ?> payload = (Map<?, ?>) payloadCaptor.getValue();
        assertThat(payload.get("status")).isEqualTo("PAYMENT_FAILED");
        assertThat(String.valueOf(payload.get("message")))
                .contains("declined");
    }
}
