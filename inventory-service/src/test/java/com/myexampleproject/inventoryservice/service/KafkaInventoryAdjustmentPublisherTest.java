package com.myexampleproject.inventoryservice.service;

import com.myexampleproject.common.event.InventoryAdjustmentEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaInventoryAdjustmentPublisherTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Test
    void shouldPublishAdjustmentToLegacyTopic() {
        KafkaInventoryAdjustmentPublisher publisher = new KafkaInventoryAdjustmentPublisher(kafkaTemplate);
        InventoryAdjustmentEvent event = new InventoryAdjustmentEvent("SKU-1", -2, "reservation");

        when(kafkaTemplate.send(eq("inventory-adjustment-topic"), anyString(), eq(event)))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publish(event);

        verify(kafkaTemplate).send("inventory-adjustment-topic", "SKU-1", event);
    }
}
