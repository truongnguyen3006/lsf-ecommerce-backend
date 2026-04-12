package com.myexampleproject.productservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myexampleproject.common.event.ProductCacheEvent;
import com.myexampleproject.common.event.ProductCreatedEvent;
import com.myorg.lsf.contracts.core.conventions.CoreHeaders;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.outbox.mysql.JdbcOutboxRepository;
import com.myorg.lsf.outbox.mysql.LsfOutboxMySqlProperties;
import com.myorg.lsf.outbox.mysql.OutboxRow;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProductRawPayloadOutboxPublisherTest {

    @Mock
    private JdbcOutboxRepository outboxRepository;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Mock
    private TransactionTemplate transactionTemplate;

    private ObjectMapper objectMapper;
    private LsfOutboxMySqlProperties properties;
    private ProductRawPayloadOutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        properties = new LsfOutboxMySqlProperties();
        properties.setEnabled(true);
        properties.getPublisher().setEnabled(false);
        properties.getPublisher().setClaimStrategy(LsfOutboxMySqlProperties.Publisher.ClaimStrategy.SKIP_LOCKED);

        when(transactionTemplate.execute(any())).thenAnswer(invocation -> {
            TransactionCallback<?> callback = invocation.getArgument(0);
            return callback.doInTransaction(mock(TransactionStatus.class));
        });
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            Consumer<TransactionStatus> consumer = invocation.getArgument(0);
            consumer.accept(mock(TransactionStatus.class));
            return null;
        }).when(transactionTemplate).executeWithoutResult(any());

        doReturn(CompletableFuture.completedFuture(null))
                .when(kafkaTemplate)
                .send(any(ProducerRecord.class));

        publisher = new ProductRawPayloadOutboxPublisher(
                properties,
                outboxRepository,
                kafkaTemplate,
                objectMapper,
                transactionTemplate,
                Clock.fixed(Instant.parse("2026-04-08T00:00:00Z"), ZoneOffset.UTC),
                new SimpleMeterRegistry(),
                "product-service"
        );
    }

    @Test
    void runOnceShouldPublishRawProductCreatedPayload() throws Exception {
        EventEnvelope envelope = EventEnvelope.builder()
                .eventId("evt-created-1")
                .eventType(ProductEventConstants.PRODUCT_CREATED_EVENT_TYPE)
                .aggregateId("SKU-1")
                .correlationId("SKU-1")
                .payload(objectMapper.valueToTree(new ProductCreatedEvent("SKU-1", 7)))
                .build();

        when(outboxRepository.claimBatchSkipLocked(anyString(), any(Instant.class), any(Instant.class), anyInt()))
                .thenReturn(1);
        when(outboxRepository.findClaimed(anyString(), any(Instant.class), anyInt()))
                .thenReturn(List.of(new OutboxRow(
                        1L,
                        ProductEventConstants.PRODUCT_CREATED_TOPIC,
                        "SKU-1",
                        "evt-created-1",
                        objectMapper.writeValueAsString(envelope),
                        0
                )));

        publisher.runOnce();

        ArgumentCaptor<ProducerRecord<String, Object>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(recordCaptor.capture());
        verify(outboxRepository).markSent(anyLong(), any(Instant.class));

        ProducerRecord<String, Object> record = recordCaptor.getValue();
        assertEquals(ProductEventConstants.PRODUCT_CREATED_TOPIC, record.topic());
        assertEquals("SKU-1", record.key());
        assertInstanceOf(ProductCreatedEvent.class, record.value());
        assertEquals(7, ((ProductCreatedEvent) record.value()).getInitialQuantity());
        assertEquals("evt-created-1", new String(record.headers().lastHeader(CoreHeaders.EVENT_ID).value()));
        assertEquals(
                ProductEventConstants.PRODUCT_CREATED_EVENT_TYPE,
                new String(record.headers().lastHeader(CoreHeaders.EVENT_TYPE).value())
        );
    }

    @Test
    void runOnceShouldPublishRawProductCachePayload() throws Exception {
        EventEnvelope envelope = EventEnvelope.builder()
                .eventId("evt-cache-1")
                .eventType(ProductEventConstants.PRODUCT_CACHE_UPDATED_EVENT_TYPE)
                .aggregateId("SKU-2")
                .correlationId("SKU-2")
                .payload(objectMapper.valueToTree(ProductCacheEvent.builder()
                        .skuCode("SKU-2")
                        .name("Air Max")
                        .price(new BigDecimal("150.00"))
                        .imageUrl("image.jpg")
                        .color("Black")
                        .size("44")
                        .build()))
                .build();

        when(outboxRepository.claimBatchSkipLocked(anyString(), any(Instant.class), any(Instant.class), anyInt()))
                .thenReturn(1);
        when(outboxRepository.findClaimed(anyString(), any(Instant.class), anyInt()))
                .thenReturn(List.of(new OutboxRow(
                        2L,
                        ProductEventConstants.PRODUCT_CACHE_UPDATE_TOPIC,
                        "SKU-2",
                        "evt-cache-1",
                        objectMapper.writeValueAsString(envelope),
                        0
                )));

        publisher.runOnce();

        ArgumentCaptor<ProducerRecord<String, Object>> recordCaptor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(recordCaptor.capture());

        ProducerRecord<String, Object> record = recordCaptor.getValue();
        assertEquals(ProductEventConstants.PRODUCT_CACHE_UPDATE_TOPIC, record.topic());
        assertEquals("SKU-2", record.key());
        assertInstanceOf(ProductCacheEvent.class, record.value());
        assertEquals("Air Max", ((ProductCacheEvent) record.value()).getName());
    }
}
