package com.myexampleproject.inventoryservice.service;

import com.myexampleproject.common.event.InventoryAdjustmentEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaInventoryAdjustmentPublisher implements InventoryAdjustmentPublisher {

    private static final String INVENTORY_ADJUSTMENT_TOPIC = "inventory-adjustment-topic";

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    public void publish(InventoryAdjustmentEvent event) {
        kafkaTemplate.send(INVENTORY_ADJUSTMENT_TOPIC, event.getSkuCode(), event)
                .whenComplete((metadata, exception) -> {
                    if (exception != null) {
                        log.error(
                                "Failed to publish inventory adjustment for sku {}: {}",
                                event.getSkuCode(),
                                exception.getMessage()
                        );
                    }
                });
    }
}
