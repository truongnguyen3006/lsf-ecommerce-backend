package com.myexampleproject.inventoryservice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.myexampleproject.inventoryservice.dto.OrderReservationItemView;
import com.myexampleproject.inventoryservice.dto.OrderReservationSummaryResponse;
import com.myorg.lsf.quota.api.QuotaDecision;
import com.myorg.lsf.quota.api.QuotaResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryOrderReservationService {

    private static final String RECORD_PREFIX = "demo:inventory:reservation:record:";
    private static final String ORDER_INDEX_PREFIX = "demo:inventory:reservation:order:";
    private static final Duration RECORD_TTL = Duration.ofHours(24);

    private final StringRedisTemplate redisTemplate;
    private final ObjectMapper objectMapper;

    public void markReserved(
            String orderNumber,
            String skuCode,
            int quantity,
            String quotaKey,
            String requestId,
            QuotaResult result
    ) {
        if (result.decision() != QuotaDecision.ACCEPTED && result.decision() != QuotaDecision.DUPLICATE) {
            return;
        }

        long nowMs = System.currentTimeMillis();
        InventoryReservationViewRecord existing = readRecord(orderNumber, skuCode).orElse(null);
        InventoryReservationViewRecord record = new InventoryReservationViewRecord(
                orderNumber,
                skuCode,
                quantity,
                quotaKey,
                requestId,
                "RESERVED",
                existing != null && existing.reservedAtMs() > 0 ? existing.reservedAtMs() : nowMs,
                result.holdUntilEpochMs(),
                existing == null ? null : existing.confirmedAtMs(),
                existing == null ? null : existing.releasedAtMs(),
                null,
                nowMs
        );
        writeRecord(record);
    }

    public void markConfirmed(String orderNumber, String skuCode, QuotaResult result) {
        if (result.decision() != QuotaDecision.ACCEPTED && result.decision() != QuotaDecision.DUPLICATE) {
            return;
        }

        long nowMs = System.currentTimeMillis();
        InventoryReservationViewRecord existing = readRecord(orderNumber, skuCode).orElse(null);
        InventoryReservationViewRecord record = new InventoryReservationViewRecord(
                orderNumber,
                skuCode,
                existing == null ? 0 : existing.quantity(),
                existing == null ? "" : existing.quotaKey(),
                existing == null ? orderNumber + ":" + skuCode : existing.requestId(),
                "CONFIRMED",
                existing == null ? nowMs : existing.reservedAtMs(),
                existing == null ? 0L : existing.expiresAtMs(),
                nowMs,
                existing == null ? null : existing.releasedAtMs(),
                existing == null ? null : existing.reason(),
                nowMs
        );
        writeRecord(record);
    }

    public void markReleased(String orderNumber, String skuCode, String reason, QuotaResult result) {
        InventoryReservationViewRecord existing = readRecord(orderNumber, skuCode).orElse(null);
        if (existing == null) {
            return;
        }

        long nowMs = System.currentTimeMillis();
        if (result.decision() == QuotaDecision.ACCEPTED) {
            writeRecord(new InventoryReservationViewRecord(
                    orderNumber,
                    skuCode,
                    existing.quantity(),
                    existing.quotaKey(),
                    existing.requestId(),
                    "RELEASED",
                    existing.reservedAtMs(),
                    existing.expiresAtMs(),
                    existing.confirmedAtMs(),
                    nowMs,
                    reason,
                    nowMs
            ));
            return;
        }

        if (isExpired(existing, nowMs)) {
            writeRecord(new InventoryReservationViewRecord(
                    orderNumber,
                    skuCode,
                    existing.quantity(),
                    existing.quotaKey(),
                    existing.requestId(),
                    existing.state(),
                    existing.reservedAtMs(),
                    existing.expiresAtMs(),
                    existing.confirmedAtMs(),
                    existing.releasedAtMs(),
                    reason,
                    nowMs
            ));
        }
    }

    public OrderReservationSummaryResponse getOrderReservationSummary(String orderNumber) {
        long nowMs = System.currentTimeMillis();
        List<OrderReservationItemView> items = loadOrderRecords(orderNumber).stream()
                .map(record -> toItemView(record, nowMs))
                .sorted(Comparator.comparing(OrderReservationItemView::skuCode))
                .toList();

        if (items.isEmpty()) {
            return OrderReservationSummaryResponse.builder()
                    .orderNumber(orderNumber)
                    .state("NOT_FOUND")
                    .reservedAtMs(0L)
                    .expiresAtMs(0L)
                    .remainingMs(0L)
                    .countdownActive(false)
                    .items(List.of())
                    .build();
        }

        long reservedAtMs = items.stream()
                .mapToLong(OrderReservationItemView::reservedAtMs)
                .filter(value -> value > 0)
                .min()
                .orElse(0L);
        long expiresAtMs = items.stream()
                .filter(item -> "RESERVED".equals(item.state()) || "EXPIRED".equals(item.state()))
                .mapToLong(OrderReservationItemView::expiresAtMs)
                .filter(value -> value > 0)
                .min()
                .orElse(0L);
        long remainingMs = expiresAtMs > 0 ? Math.max(expiresAtMs - nowMs, 0L) : 0L;
        String state = resolveSummaryState(items);

        return OrderReservationSummaryResponse.builder()
                .orderNumber(orderNumber)
                .state(state)
                .reservedAtMs(reservedAtMs)
                .expiresAtMs(expiresAtMs)
                .remainingMs("RESERVED".equals(state) ? remainingMs : 0L)
                .countdownActive("RESERVED".equals(state))
                .items(items)
                .build();
    }

    private List<InventoryReservationViewRecord> loadOrderRecords(String orderNumber) {
        Set<String> members = redisTemplate.opsForSet().members(orderIndexKey(orderNumber));
        if (members == null || members.isEmpty()) {
            return List.of();
        }

        return members.stream()
                .map(this::readByKey)
                .flatMap(Optional::stream)
                .toList();
    }

    private OrderReservationItemView toItemView(InventoryReservationViewRecord record, long nowMs) {
        String state = resolveItemState(record, nowMs);
        long remainingMs = "RESERVED".equals(state)
                ? Math.max(record.expiresAtMs() - nowMs, 0L)
                : 0L;

        return OrderReservationItemView.builder()
                .orderNumber(record.orderNumber())
                .skuCode(record.skuCode())
                .quantity(record.quantity())
                .state(state)
                .reservedAtMs(record.reservedAtMs())
                .expiresAtMs(record.expiresAtMs())
                .confirmedAtMs(record.confirmedAtMs())
                .releasedAtMs(record.releasedAtMs())
                .remainingMs(remainingMs)
                .reason(record.reason())
                .quotaKey(record.quotaKey())
                .requestId(record.requestId())
                .build();
    }

    private String resolveSummaryState(List<OrderReservationItemView> items) {
        if (items.stream().allMatch(item -> "CONFIRMED".equals(item.state()))) {
            return "CONFIRMED";
        }
        if (items.stream().anyMatch(item -> "RESERVED".equals(item.state()))) {
            return "RESERVED";
        }
        if (items.stream().allMatch(item -> "RELEASED".equals(item.state()))) {
            return "RELEASED";
        }
        if (items.stream().allMatch(item -> "EXPIRED".equals(item.state()))) {
            return "EXPIRED";
        }
        if (items.stream().anyMatch(item -> "EXPIRED".equals(item.state()))) {
            return "EXPIRED";
        }
        if (items.stream().anyMatch(item -> "RELEASED".equals(item.state()))) {
            return "RELEASED";
        }
        return "UNKNOWN";
    }

    private String resolveItemState(InventoryReservationViewRecord record, long nowMs) {
        if ("RESERVED".equals(record.state()) && isExpired(record, nowMs)) {
            return "EXPIRED";
        }
        return record.state();
    }

    private boolean isExpired(InventoryReservationViewRecord record, long nowMs) {
        return record.expiresAtMs() > 0 && record.expiresAtMs() <= nowMs;
    }

    private Optional<InventoryReservationViewRecord> readRecord(String orderNumber, String skuCode) {
        return readByKey(recordKey(orderNumber, skuCode));
    }

    private Optional<InventoryReservationViewRecord> readByKey(String key) {
        String payload = redisTemplate.opsForValue().get(key);
        if (payload == null || payload.isBlank()) {
            return Optional.empty();
        }

        try {
            return Optional.of(objectMapper.readValue(payload, InventoryReservationViewRecord.class));
        } catch (JsonProcessingException exception) {
            log.warn("Failed to read reservation view record at key {}", key, exception);
            return Optional.empty();
        }
    }

    private void writeRecord(InventoryReservationViewRecord record) {
        String key = recordKey(record.orderNumber(), record.skuCode());
        String indexKey = orderIndexKey(record.orderNumber());

        try {
            redisTemplate.opsForValue().set(key, objectMapper.writeValueAsString(record), RECORD_TTL);
            redisTemplate.opsForSet().add(indexKey, key);
            redisTemplate.expire(indexKey, RECORD_TTL);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Failed to serialize reservation view record for " + key, exception);
        }
    }

    private String recordKey(String orderNumber, String skuCode) {
        return RECORD_PREFIX + orderNumber + ":" + skuCode;
    }

    private String orderIndexKey(String orderNumber) {
        return ORDER_INDEX_PREFIX + orderNumber;
    }
}
