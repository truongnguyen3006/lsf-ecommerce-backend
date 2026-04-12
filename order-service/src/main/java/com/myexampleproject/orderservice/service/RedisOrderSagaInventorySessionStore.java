package com.myexampleproject.orderservice.service;

import com.myexampleproject.common.event.InventoryCheckResult;
import com.myexampleproject.orderservice.config.OrderWorkflowProperties;
import com.myorg.lsf.saga.SagaReplyFanInSession;
import com.myorg.lsf.saga.SagaReplyFanInSignal;
import com.myorg.lsf.saga.SagaReplyFanInSupport;
import com.myorg.lsf.saga.SagaReplyFanInOutcome;
import com.myorg.lsf.saga.SagaReplyFanInUpdate;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.Clock;
import java.time.Duration;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

@Service
public class RedisOrderSagaInventorySessionStore implements OrderSagaInventorySessionStore {

    private static final String KEY_PREFIX = "lsf:saga:order:inventory:";
    private static final String INDEX_KEY = KEY_PREFIX + "index";

    private final RedisTemplate<String, Object> redisTemplate;
    private final OrderWorkflowProperties workflowProperties;

    private final Clock clock;

    public RedisOrderSagaInventorySessionStore(
            RedisTemplate<String, Object> redisTemplate,
            OrderWorkflowProperties workflowProperties,
            @Qualifier("lsfSagaClock") Clock clock
    ) {
        this.redisTemplate = redisTemplate;
        this.workflowProperties = workflowProperties;
        this.clock = clock;
    }

    @Override
    public boolean start(String orderNumber, int totalItems, String commandEventId, String requestId) {
        String key = key(orderNumber);
        long nowMs = clock.millis();
        Duration ttl = workflowProperties.getSaga().getAggregatorTtl();

        if (Boolean.TRUE.equals(redisTemplate.hasKey(key))) {
            SagaReplyFanInSession existing = read(key, orderNumber, null);
            if (!existing.isExpired(nowMs)) {
                return false;
            }
            delete(orderNumber);
        }

        write(key, SagaReplyFanInSupport.start(orderNumber, totalItems, commandEventId, requestId, nowMs, ttl), ttl);
        redisTemplate.opsForSet().add(INDEX_KEY, orderNumber);
        return true;
    }

    @Override
    public Optional<SagaReplyFanInUpdate> applyResult(InventoryCheckResult result) {
        String orderNumber = result.getOrderNumber();
        String key = key(orderNumber);
        if (Boolean.FALSE.equals(redisTemplate.hasKey(key))) {
            redisTemplate.opsForSet().remove(INDEX_KEY, orderNumber);
            return Optional.empty();
        }

        long nowMs = clock.millis();
        Duration ttl = workflowProperties.getSaga().getAggregatorTtl();
        SagaReplyFanInSession current = read(key, orderNumber, null);

        if (current.isExpired(nowMs)) {
            delete(orderNumber);
            return Optional.empty();
        }

        SagaReplyFanInUpdate update = SagaReplyFanInSupport.apply(
                current,
                result.isSuccess()
                        ? SagaReplyFanInSignal.successful()
                        : SagaReplyFanInSignal.failure(resolveFailureReason(result)),
                nowMs,
                ttl
        );
        if (update.shouldIgnore()) {
            if (update.outcome() == SagaReplyFanInOutcome.EXPIRED) {
                delete(orderNumber);
            }
            return Optional.of(update);
        }

        write(key, update.session(), ttl);
        return Optional.of(update);
    }

    @Override
    public Optional<SagaReplyFanInSession> find(String orderNumber) {
        String key = key(orderNumber);
        if (Boolean.FALSE.equals(redisTemplate.hasKey(key))) {
            redisTemplate.opsForSet().remove(INDEX_KEY, orderNumber);
            return Optional.empty();
        }

        SagaReplyFanInSession session = read(key, orderNumber, null);
        if (session.isExpired(clock.millis())) {
            delete(orderNumber);
            return Optional.empty();
        }
        return Optional.of(session);
    }

    @Override
    public List<SagaReplyFanInSession> findPending(int limit) {
        Set<Object> members = redisTemplate.opsForSet().members(INDEX_KEY);
        if (members == null || members.isEmpty()) {
            return List.of();
        }

        long nowMs = clock.millis();
        return members.stream()
                .map(String::valueOf)
                .map(this::find)
                .flatMap(Optional::stream)
                .filter(session -> !session.isExpired(nowMs))
                .sorted(Comparator.comparingLong(SagaReplyFanInSession::updatedAtMs).reversed())
                .limit(Math.max(1, limit))
                .toList();
    }

    @Override
    public long pendingCount() {
        Set<Object> members = redisTemplate.opsForSet().members(INDEX_KEY);
        if (members == null || members.isEmpty()) {
            return 0L;
        }

        long nowMs = clock.millis();
        return members.stream()
                .map(String::valueOf)
                .map(this::find)
                .flatMap(Optional::stream)
                .filter(session -> !session.isExpired(nowMs))
                .count();
    }

    @Override
    public int purgeExpired(long nowMs, int limit) {
        Set<Object> members = redisTemplate.opsForSet().members(INDEX_KEY);
        if (members == null || members.isEmpty()) {
            return 0;
        }

        int purged = 0;
        for (Object member : members) {
            if (purged >= Math.max(1, limit)) {
                break;
            }

            String orderNumber = String.valueOf(member);
            String key = key(orderNumber);

            if (Boolean.FALSE.equals(redisTemplate.hasKey(key))) {
                redisTemplate.opsForSet().remove(INDEX_KEY, orderNumber);
                purged++;
                continue;
            }

            SagaReplyFanInSession session = read(key, orderNumber, null);
            if (!session.isExpired(nowMs)) {
                continue;
            }

            redisTemplate.delete(key);
            redisTemplate.opsForSet().remove(INDEX_KEY, orderNumber);
            purged++;
        }

        return purged;
    }

    @Override
    public void delete(String orderNumber) {
        redisTemplate.delete(key(orderNumber));
        redisTemplate.opsForSet().remove(INDEX_KEY, orderNumber);
    }

    private SagaReplyFanInSession read(String key, String orderNumber, Long receivedOverride) {
        int expectedReplies = toInt(redisTemplate.opsForHash().get(key, "expectedReplies"));
        int receivedReplies = receivedOverride == null
                ? toInt(redisTemplate.opsForHash().get(key, "receivedReplies"))
                : receivedOverride.intValue();
        boolean failed = toBoolean(redisTemplate.opsForHash().get(key, "failed"));
        String failureReason = toString(redisTemplate.opsForHash().get(key, "failureReason"));
        String commandEventId = toString(redisTemplate.opsForHash().get(key, "commandEventId"));
        String requestId = toString(redisTemplate.opsForHash().get(key, "requestId"));
        long createdAtMs = toLong(redisTemplate.opsForHash().get(key, "createdAtMs"));
        long updatedAtMs = toLong(redisTemplate.opsForHash().get(key, "updatedAtMs"));
        long expiresAtMs = toLong(redisTemplate.opsForHash().get(key, "expiresAtMs"));
        return new SagaReplyFanInSession(
                orderNumber,
                expectedReplies,
                receivedReplies,
                failed,
                failureReason,
                commandEventId,
                requestId,
                createdAtMs,
                updatedAtMs,
                expiresAtMs
        );
    }

    private void write(String key, SagaReplyFanInSession session, Duration ttl) {
        redisTemplate.opsForHash().put(key, "expectedReplies", session.expectedReplies());
        redisTemplate.opsForHash().put(key, "receivedReplies", session.receivedReplies());
        redisTemplate.opsForHash().put(key, "failed", session.failed());
        redisTemplate.opsForHash().put(key, "failureReason", session.failureReason());
        redisTemplate.opsForHash().put(key, "commandEventId", session.commandEventId() == null ? "" : session.commandEventId());
        redisTemplate.opsForHash().put(key, "requestId", session.requestId() == null ? "" : session.requestId());
        redisTemplate.opsForHash().put(key, "createdAtMs", session.createdAtMs());
        redisTemplate.opsForHash().put(key, "updatedAtMs", session.updatedAtMs());
        redisTemplate.opsForHash().put(key, "expiresAtMs", session.expiresAtMs());
        redisTemplate.expire(key, ttl);
    }

    private String key(String orderNumber) {
        return KEY_PREFIX + orderNumber;
    }

    private int toInt(Object value) {
        if (value instanceof Integer integerValue) {
            return integerValue;
        }
        if (value instanceof Long longValue) {
            return longValue.intValue();
        }
        if (value instanceof String stringValue && !stringValue.isBlank()) {
            return Integer.parseInt(stringValue);
        }
        return 0;
    }

    private long toLong(Object value) {
        if (value instanceof Long longValue) {
            return longValue;
        }
        if (value instanceof Integer integerValue) {
            return integerValue.longValue();
        }
        if (value instanceof String stringValue && !stringValue.isBlank()) {
            return Long.parseLong(stringValue);
        }
        return 0L;
    }

    private boolean toBoolean(Object value) {
        if (value instanceof Boolean booleanValue) {
            return booleanValue;
        }
        if (value instanceof String stringValue) {
            return Boolean.parseBoolean(stringValue);
        }
        return false;
    }

    private String toString(Object value) {
        return value == null ? "" : String.valueOf(value);
    }

    private String resolveFailureReason(InventoryCheckResult result) {
        return result.getReason() == null ? "Inventory reservation failed" : result.getReason();
    }
}
