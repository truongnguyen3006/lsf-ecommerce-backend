package com.myexampleproject.notificationservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myexampleproject.common.event.OrderFailedEvent;
import com.myexampleproject.common.event.OrderPlacedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationService {

    private final SimpMessagingTemplate messagingTemplate;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "order-placed-topic",
            groupId = "notification-group",
            containerFactory = "orderPlacedKafkaListenerContainerFactory"
    )
    public void handleOrderPlaced(@Payload OrderPlacedEvent event) {
        log.info("Order placed: {}", event.getOrderNumber());
        messagingTemplate.convertAndSend(
                "/topic/order/" + event.getOrderNumber(),
                Map.of("status", "PENDING", "message", "ÄÆ¡n hÃ ng Ä‘Ã£ Ä‘Æ°á»£c tiáº¿p nháº­n!")
        );
    }

    @KafkaListener(
            topics = "order-failed-topic",
            groupId = "notification-group",
            containerFactory = "orderFailedRawKafkaListenerContainerFactory"
    )
    public void handleOrderFailed(ConsumerRecord<String, String> record) {
        try {
            String json = cleanJson(record.value());
            OrderFailedEvent event = objectMapper.readValue(json, OrderFailedEvent.class);

            log.warn("Notification: Inventory Failed for Order {}", event.getOrderNumber());
            messagingTemplate.convertAndSend(
                    "/topic/order/" + event.getOrderNumber(),
                    Map.of("status", "FAILED", "message", "Háº¿t hÃ ng: " + event.getReason())
            );
        } catch (Exception e) {
            log.error("Lá»—i parse OrderFailedEvent: {}", e.getMessage(), e);
        }
    }

    private String cleanJson(String raw) {
        if (raw == null) {
            return "";
        }
        int jsonStart = raw.indexOf('{');
        return jsonStart >= 0 ? raw.substring(jsonStart) : raw;
    }
}
