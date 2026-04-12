package com.myexampleproject.orderservice.service;

import com.myexampleproject.common.event.InventoryCheckRequest;
import com.myexampleproject.common.event.OrderPlacedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaOrderWorkflowPublisher implements OrderWorkflowPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    public void publishOrderPlaced(OrderPlacedEvent event) {
        kafkaTemplate.send(OrderMessagingConstants.ORDER_PLACED_TOPIC, event.getOrderNumber(), event)
                .whenComplete((metadata, exception) -> {
                    if (exception != null) {
                        log.error(
                                "Failed to publish order placed event for order {}: {}",
                                event.getOrderNumber(),
                                exception.getMessage()
                        );
                    }
                });
    }

    @Override
    public void publishInventoryCheckRequest(InventoryCheckRequest event) {
        kafkaTemplate.send(
                OrderMessagingConstants.INVENTORY_CHECK_REQUEST_TOPIC,
                event.getItem().getSkuCode(),
                event
        ).whenComplete((metadata, exception) -> {
            if (exception != null) {
                log.error(
                        "Failed to publish inventory check request for order {} sku {}: {}",
                        event.getOrderNumber(),
                        event.getItem().getSkuCode(),
                        exception.getMessage()
                );
            }
        });
    }
}
