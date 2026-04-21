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
                Map.of(
                        "status",
                        "COMPLETED",
                        "message",
                        "Thanh toán đã thành công. Hệ thống đang xác nhận giữ chỗ để hoàn tất đơn hàng."
                )
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
                Map.of(
                        "status",
                        "PAYMENT_FAILED",
                        "message",
                        "Thanh toán không thành công. Hệ thống đang hoàn lại giữ chỗ: " + event.getReason()
                )
        );
    }
}
