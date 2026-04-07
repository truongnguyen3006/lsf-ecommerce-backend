package com.myexampleproject.notificationservice.service;

import com.myexampleproject.common.event.PaymentFailedEvent;
import com.myexampleproject.common.event.PaymentProcessedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationPaymentResultDispatcher {

    private final SimpMessagingTemplate messagingTemplate;

    public void publishPaymentSuccess(PaymentProcessedEvent event, String source, String eventId) {
        log.info(
                "Notification payment success for order {} from {} (eventId={})",
                event.getOrderNumber(),
                source,
                eventId == null ? "legacy" : eventId
        );
        messagingTemplate.convertAndSend(
                "/topic/order/" + event.getOrderNumber(),
                Map.of("status", "COMPLETED", "message", "Thanh toÃ¡n thÃ nh cÃ´ng! ÄÆ¡n hÃ ng hoÃ n táº¥t.")
        );
    }

    public void publishPaymentFailure(PaymentFailedEvent event, String source, String eventId) {
        log.warn(
                "Notification payment failure for order {} from {} (eventId={})",
                event.getOrderNumber(),
                source,
                eventId == null ? "legacy" : eventId
        );
        messagingTemplate.convertAndSend(
                "/topic/order/" + event.getOrderNumber(),
                Map.of("status", "PAYMENT_FAILED", "message", "Thanh toÃ¡n lá»—i: " + event.getReason())
        );
    }
}
