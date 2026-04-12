package com.myexampleproject.productservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.myorg.lsf.contracts.core.conventions.CoreHeaders;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.outbox.mysql.JdbcOutboxRepository;
import com.myorg.lsf.outbox.mysql.LsfOutboxMySqlProperties;
import com.myorg.lsf.outbox.mysql.OutboxRow;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.util.StringUtils;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Temporary bridge for product-service raw Kafka contracts.
 *
 * <p>The framework source in {@code D:\IdeaProjects\lsf-parent-fixed} currently exposes
 * {@link com.myorg.lsf.outbox.OutboxWriter} for appending envelopes, but it does not expose a public
 * extension point to publish raw payloads from the MySQL outbox publisher path. product-service still
 * has to emit raw {@code ProductCreatedEvent} and {@code ProductCacheEvent} payloads to preserve
 * downstream contracts, so this adapter reads the framework-managed outbox rows and republishes the
 * raw payload with public LSF header conventions.</p>
 */
@Service
@ConditionalOnProperty(prefix = "lsf.outbox", name = "enabled", havingValue = "true")
public class ProductRawPayloadOutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(ProductRawPayloadOutboxPublisher.class);

    private final LsfOutboxMySqlProperties properties;
    private final JdbcOutboxRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final TransactionTemplate transactionTemplate;
    private final Clock clock;
    private final Counter sentCounter;
    private final Counter retryCounter;
    private final Counter failedCounter;
    private final Counter publishedAliasCounter;
    private final Counter failedAliasCounter;
    private final String instanceId = "product-outbox-" + UUID.randomUUID();

    public ProductRawPayloadOutboxPublisher(
            LsfOutboxMySqlProperties properties,
            JdbcOutboxRepository outboxRepository,
            KafkaTemplate<String, Object> kafkaTemplate,
            ObjectMapper objectMapper,
            TransactionTemplate transactionTemplate,
            Clock productOutboxClock,
            MeterRegistry meterRegistry,
            @Value("${spring.application.name:product-service}") String applicationName
    ) {
        this.properties = properties;
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.transactionTemplate = transactionTemplate;
        this.clock = productOutboxClock;

        Tags tags = Tags.of("service", applicationName, "table", properties.getTable());
        this.sentCounter = Counter.builder("lsf.outbox.sent").tags(tags).register(meterRegistry);
        this.retryCounter = Counter.builder("lsf.outbox.retry").tags(tags).register(meterRegistry);
        this.failedCounter = Counter.builder("lsf.outbox.fail").tags(tags).register(meterRegistry);
        this.publishedAliasCounter = Counter.builder("outbox.published").tags(tags).register(meterRegistry);
        this.failedAliasCounter = Counter.builder("outbox.failed").tags(tags).register(meterRegistry);
    }

    @Scheduled(
            initialDelayString = "#{@productOutboxSchedule.initialDelayMs}",
            fixedDelayString = "#{@productOutboxSchedule.pollIntervalMs}"
    )
    public void scheduledLoop() {
        if (properties.getPublisher().isEnabled() || !properties.getPublisher().isSchedulingEnabled()) {
            return;
        }
        runOnce();
    }

    public void runOnce() {
        if (properties.getPublisher().isEnabled()) {
            return;
        }

        Instant now = clock.instant().truncatedTo(ChronoUnit.MILLIS);
        Instant leaseUntil = now.plus(properties.getPublisher().getLease());

        Integer claimed = transactionTemplate.execute(status -> claimBatch(now, leaseUntil));
        if (claimed == null || claimed <= 0) {
            return;
        }

        List<OutboxRow> rows = outboxRepository.findClaimed(
                instanceId,
                now,
                properties.getPublisher().getBatchSize()
        );
        if (rows.isEmpty()) {
            return;
        }

        for (OutboxRow row : rows) {
            try {
                EventEnvelope envelope = objectMapper.readValue(row.envelopeJson(), EventEnvelope.class);
                Object payload = deserializePayload(envelope);

                ProducerRecord<String, Object> record = new ProducerRecord<>(row.topic(), row.msgKey(), payload);
                addHeaders(record, envelope);

                kafkaTemplate.send(record)
                        .get(properties.getPublisher().getSendTimeout().toMillis(), TimeUnit.MILLISECONDS);

                Instant sentAt = clock.instant();
                transactionTemplate.executeWithoutResult(status -> outboxRepository.markSent(row.id(), sentAt));
                sentCounter.increment();
                publishedAliasCounter.increment();

                log.debug(
                        "PRODUCT OUTBOX SENT -> id={}, eventId={}, topic={}, payloadType={}",
                        row.id(),
                        row.eventId(),
                        row.topic(),
                        payload.getClass().getSimpleName()
                );
            } catch (Exception exception) {
                handleFailure(row, exception);
            }
        }
    }

    private int claimBatch(Instant now, Instant leaseUntil) {
        if (properties.getPublisher().getClaimStrategy() == LsfOutboxMySqlProperties.Publisher.ClaimStrategy.SKIP_LOCKED) {
            return outboxRepository.claimBatchSkipLocked(
                    instanceId,
                    now,
                    leaseUntil,
                    properties.getPublisher().getBatchSize()
            );
        }

        return outboxRepository.claimBatch(
                instanceId,
                now,
                leaseUntil,
                properties.getPublisher().getBatchSize()
        );
    }

    private Object deserializePayload(EventEnvelope envelope) throws JsonProcessingException {
        return switch (envelope.getEventType()) {
            case ProductEventConstants.PRODUCT_CREATED_EVENT_TYPE ->
                    objectMapper.treeToValue(envelope.getPayload(), com.myexampleproject.common.event.ProductCreatedEvent.class);
            case ProductEventConstants.PRODUCT_CACHE_UPDATED_EVENT_TYPE ->
                    objectMapper.treeToValue(envelope.getPayload(), com.myexampleproject.common.event.ProductCacheEvent.class);
            default -> throw new IllegalStateException("Unsupported product outbox event type: " + envelope.getEventType());
        };
    }

    private void handleFailure(OutboxRow row, Exception exception) {
        int nextRetryCount = row.retryCount() + 1;
        String error = safeErr(exception);

        if (nextRetryCount >= properties.getPublisher().getMaxRetries()) {
            transactionTemplate.executeWithoutResult(status -> outboxRepository.markFailed(row.id(), error));
            failedCounter.increment();
            failedAliasCounter.increment();
            log.error(
                    "PRODUCT OUTBOX FAILED -> id={}, eventId={}, topic={}, retries={}, error={}",
                    row.id(),
                    row.eventId(),
                    row.topic(),
                    nextRetryCount,
                    error
            );
            return;
        }

        Instant nextAttempt = clock.instant().plus(backoff(nextRetryCount));
        retryCounter.increment();
        transactionTemplate.executeWithoutResult(status -> outboxRepository.markRetry(row.id(), nextAttempt, error));
        log.warn(
                "PRODUCT OUTBOX RETRY -> id={}, eventId={}, topic={}, retry={}, nextAttempt={}, error={}",
                row.id(),
                row.eventId(),
                row.topic(),
                nextRetryCount,
                nextAttempt,
                error
        );
    }

    private Duration backoff(int retryCount) {
        Duration base = properties.getPublisher().getBackoffBase();
        Duration max = properties.getPublisher().getBackoffMax();
        long baseMillis = Math.max(1L, base.toMillis());
        int exponent = Math.max(0, retryCount - 1);
        long multiplier = 1L << Math.min(30, exponent);
        long millis = Math.min(baseMillis * multiplier, max.toMillis());
        return Duration.ofMillis(millis);
    }

    private String safeErr(Throwable throwable) {
        Throwable root = throwable;
        while (root.getCause() != null && root.getCause() != root) {
            root = root.getCause();
        }

        String message = root.getClass().getSimpleName() + ": " + (root.getMessage() == null ? "" : root.getMessage());
        return message.length() > 2000 ? message.substring(0, 2000) : message;
    }

    private void addHeaders(ProducerRecord<String, Object> record, EventEnvelope envelope) {
        record.headers().add(new RecordHeader(CoreHeaders.EVENT_ID, bytes(envelope.getEventId())));
        record.headers().add(new RecordHeader(CoreHeaders.EVENT_TYPE, bytes(envelope.getEventType())));

        if (StringUtils.hasText(envelope.getCorrelationId())) {
            record.headers().add(new RecordHeader(CoreHeaders.CORRELATION_ID, bytes(envelope.getCorrelationId())));
        }
        if (StringUtils.hasText(envelope.getCausationId())) {
            record.headers().add(new RecordHeader(CoreHeaders.CAUSATION_ID, bytes(envelope.getCausationId())));
        }
        if (StringUtils.hasText(envelope.getRequestId())) {
            record.headers().add(new RecordHeader(CoreHeaders.REQUEST_ID, bytes(envelope.getRequestId())));
        }

        Map<String, String> traceHeaders = envelope.getTraceHeaders();
        if (traceHeaders != null) {
            traceHeaders.forEach((key, value) -> {
                if (StringUtils.hasText(key) && StringUtils.hasText(value)) {
                    record.headers().add(new RecordHeader(key, bytes(value)));
                }
            });
        }
    }

    private byte[] bytes(String value) {
        return value == null ? new byte[0] : value.getBytes(StandardCharsets.UTF_8);
    }
}
