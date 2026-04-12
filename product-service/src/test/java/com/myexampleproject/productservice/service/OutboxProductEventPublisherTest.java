package com.myexampleproject.productservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myexampleproject.common.event.ProductCacheEvent;
import com.myexampleproject.common.event.ProductCreatedEvent;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.outbox.OutboxWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OutboxProductEventPublisherTest {

    @Mock
    private OutboxWriter outboxWriter;

    @Test
    void publishProductCreatedShouldAppendOutboxEnvelope() {
        ProductOutboxEnvelopeFactory factory = new ProductOutboxEnvelopeFactory(new ObjectMapper(), "product-service");
        OutboxProductEventPublisher publisher = new OutboxProductEventPublisher(outboxWriter, factory);
        ProductCreatedEvent event = ProductCreatedEvent.builder()
                .skuCode("SKU-1")
                .initialQuantity(5)
                .build();

        when(outboxWriter.append(any(EventEnvelope.class), anyString(), anyString())).thenReturn(1L);

        publisher.publishProductCreated(event);

        ArgumentCaptor<EventEnvelope> envelopeCaptor = ArgumentCaptor.forClass(EventEnvelope.class);
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);

        verify(outboxWriter).append(envelopeCaptor.capture(), topicCaptor.capture(), keyCaptor.capture());

        assertEquals(ProductEventConstants.PRODUCT_CREATED_TOPIC, topicCaptor.getValue());
        assertEquals("SKU-1", keyCaptor.getValue());
        assertEquals(ProductEventConstants.PRODUCT_CREATED_EVENT_TYPE, envelopeCaptor.getValue().getEventType());
        assertEquals("SKU-1", envelopeCaptor.getValue().getPayload().get("skuCode").asText());
        assertEquals(5, envelopeCaptor.getValue().getPayload().get("initialQuantity").asInt());
    }

    @Test
    void publishProductCacheUpdateShouldAppendOutboxEnvelope() {
        ProductOutboxEnvelopeFactory factory = new ProductOutboxEnvelopeFactory(new ObjectMapper(), "product-service");
        OutboxProductEventPublisher publisher = new OutboxProductEventPublisher(outboxWriter, factory);
        ProductCacheEvent event = ProductCacheEvent.builder()
                .skuCode("SKU-2")
                .name("Air Max")
                .price(new BigDecimal("150.00"))
                .imageUrl("image.jpg")
                .color("Black")
                .size("44")
                .build();

        when(outboxWriter.append(any(EventEnvelope.class), anyString(), anyString())).thenReturn(1L);

        publisher.publishProductCacheUpdate(event);

        ArgumentCaptor<EventEnvelope> envelopeCaptor = ArgumentCaptor.forClass(EventEnvelope.class);
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);

        verify(outboxWriter).append(envelopeCaptor.capture(), topicCaptor.capture(), keyCaptor.capture());

        assertEquals(ProductEventConstants.PRODUCT_CACHE_UPDATE_TOPIC, topicCaptor.getValue());
        assertEquals("SKU-2", keyCaptor.getValue());
        assertEquals(ProductEventConstants.PRODUCT_CACHE_UPDATED_EVENT_TYPE, envelopeCaptor.getValue().getEventType());
        assertEquals("Air Max", envelopeCaptor.getValue().getPayload().get("name").asText());
        assertEquals("Black", envelopeCaptor.getValue().getPayload().get("color").asText());
    }
}
