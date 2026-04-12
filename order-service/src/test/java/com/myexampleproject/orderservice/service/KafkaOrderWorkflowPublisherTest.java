package com.myexampleproject.orderservice.service;

import com.myexampleproject.common.dto.OrderLineItemRequest;
import com.myexampleproject.common.event.InventoryCheckRequest;
import com.myexampleproject.common.event.OrderPlacedEvent;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaOrderWorkflowPublisherTest {

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Test
    void shouldPublishOrderPlacedToLegacyTopic() {
        KafkaOrderWorkflowPublisher publisher = new KafkaOrderWorkflowPublisher(kafkaTemplate);
        OrderPlacedEvent event = new OrderPlacedEvent(
                "ORDER-1",
                "user-1",
                List.of(OrderLineItemRequest.builder().skuCode("SKU-1").quantity(2).build())
        );

        when(kafkaTemplate.send(eq(OrderMessagingConstants.ORDER_PLACED_TOPIC), anyString(), eq(event)))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publishOrderPlaced(event);

        verify(kafkaTemplate).send(OrderMessagingConstants.ORDER_PLACED_TOPIC, "ORDER-1", event);
    }

    @Test
    void shouldPublishInventoryCheckRequestToLegacyTopic() {
        KafkaOrderWorkflowPublisher publisher = new KafkaOrderWorkflowPublisher(kafkaTemplate);
        InventoryCheckRequest event = new InventoryCheckRequest(
                "ORDER-2",
                OrderLineItemRequest.builder().skuCode("SKU-8").quantity(1).build()
        );

        when(kafkaTemplate.send(eq(OrderMessagingConstants.INVENTORY_CHECK_REQUEST_TOPIC), anyString(), eq(event)))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publishInventoryCheckRequest(event);

        verify(kafkaTemplate).send(OrderMessagingConstants.INVENTORY_CHECK_REQUEST_TOPIC, "SKU-8", event);
    }
}
