package com.myexampleproject.orderservice.service;

import com.myexampleproject.orderservice.config.OrderWorkflowProperties;
import com.myorg.lsf.saga.LsfSagaProperties;
import com.myorg.lsf.saga.SagaSql;
import com.myorg.lsf.saga.SagaStatus;
import com.myorg.lsf.saga.SagaReplyFanInSession;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class OrderSagaAdminService {

    private static final List<String> FAILURE_STATUSES = List.of(
            SagaStatus.FAILED.name(),
            SagaStatus.TIMED_OUT.name(),
            SagaStatus.COMPENSATION_FAILED.name()
    );

    private final JdbcTemplate jdbcTemplate;
    private final OrderWorkflowProperties workflowProperties;
    private final LsfSagaProperties sagaProperties;
    private final OrderSagaInventorySessionStore sessionStore;
    private final OrderSagaEvidenceMetrics evidenceMetrics;

    private final Clock clock;

    public OrderSagaAdminService(
            JdbcTemplate jdbcTemplate,
            OrderWorkflowProperties workflowProperties,
            LsfSagaProperties sagaProperties,
            OrderSagaInventorySessionStore sessionStore,
            OrderSagaEvidenceMetrics evidenceMetrics,
            @Qualifier("lsfSagaClock") Clock clock
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.workflowProperties = workflowProperties;
        this.sagaProperties = sagaProperties;
        this.sessionStore = sessionStore;
        this.evidenceMetrics = evidenceMetrics;
        this.clock = clock;
    }

    public OrderSagaAdminSnapshot snapshot() {
        String table = SagaSql.validateTableName(sagaProperties.getJdbc().getTable());
        long nowMs = clock.millis();

        int purged = sessionStore.purgeExpired(
                nowMs,
                Math.max(1, workflowProperties.getSaga().getAggregatorCleanupBatch())
        );
        evidenceMetrics.recordAggregatorExpired(purged);

        Map<String, Long> summary = loadSummary(table);
        int recentLimit = Math.max(1, workflowProperties.getSaga().getRecentLimit());
        int pendingLimit = Math.max(1, workflowProperties.getSaga().getPendingAggregatorLimit());
        OrderSagaRuntimeCounters runtimeCounters = evidenceMetrics.snapshot();

        return new OrderSagaAdminSnapshot(
                workflowProperties.getMode().name(),
                workflowProperties.getDefaultMode().name(),
                workflowProperties.getRollbackMode().name(),
                workflowProperties.isRollbackAvailable(),
                sagaProperties.isEnabled(),
                sagaProperties.getStore().name(),
                sagaProperties.getTransport().getMode().name(),
                summary,
                new OrderSagaTimeoutSummary(
                        summary.getOrDefault(SagaStatus.TIMED_OUT.name(), 0L),
                        summary.getOrDefault(SagaStatus.COMPENSATION_FAILED.name(), 0L),
                        countOverdueInstances(table, nowMs)
                ),
                new OrderSagaAggregatorSnapshot(
                        sessionStore.pendingCount(),
                        runtimeCounters.aggregatorDuplicateStartsIgnoredSinceBoot(),
                        runtimeCounters.aggregatorLateResultsIgnoredSinceBoot(),
                        runtimeCounters.aggregatorExpiredSinceBoot(),
                        sessionStore.findPending(pendingLimit).stream()
                                .map(session -> mapPendingSession(session, nowMs))
                                .toList()
                ),
                runtimeCounters,
                loadRecentInstances(table, recentLimit),
                loadRecentInstancesByStatuses(table, FAILURE_STATUSES, recentLimit),
                loadRecentInstancesByStatuses(table, List.of(SagaStatus.COMPENSATED.name()), recentLimit),
                loadOverdueInstances(table, nowMs, recentLimit)
        );
    }

    private Map<String, Long> loadSummary(String table) {
        Map<String, Long> summary = new LinkedHashMap<>();
        for (SagaStatus status : SagaStatus.values()) {
            summary.put(status.name(), 0L);
        }

        String sql = "SELECT status, COUNT(*) AS total FROM " + table + " GROUP BY status";
        jdbcTemplate.queryForList(sql).forEach(row -> {
            Object status = row.get("status");
            Object total = row.get("total");
            if (status != null && total instanceof Number number) {
                summary.put(String.valueOf(status), number.longValue());
            }
        });
        return summary;
    }

    private List<OrderSagaInstanceView> loadRecentInstances(String table, int limit) {
        String sql = """
                SELECT saga_id, definition_name, status, phase, current_step, correlation_id,
                       failure_reason, next_timeout_at_ms, created_at_ms, updated_at_ms
                FROM %s
                ORDER BY updated_at_ms DESC
                LIMIT ?
                """.formatted(table);
        return jdbcTemplate.query(sql, this::mapInstance, limit);
    }

    private List<OrderSagaInstanceView> loadRecentInstancesByStatuses(String table, List<String> statuses, int limit) {
        if (statuses.isEmpty()) {
            return List.of();
        }

        String placeholders = String.join(", ", statuses.stream().map(status -> "?").toList());
        String sql = """
                SELECT saga_id, definition_name, status, phase, current_step, correlation_id,
                       failure_reason, next_timeout_at_ms, created_at_ms, updated_at_ms
                FROM %s
                WHERE status IN (%s)
                ORDER BY updated_at_ms DESC
                LIMIT ?
                """.formatted(table, placeholders);

        Object[] params = new Object[statuses.size() + 1];
        for (int index = 0; index < statuses.size(); index++) {
            params[index] = statuses.get(index);
        }
        params[statuses.size()] = limit;

        return jdbcTemplate.query(sql, this::mapInstance, params);
    }

    private List<OrderSagaInstanceView> loadOverdueInstances(String table, long nowMs, int limit) {
        String sql = """
                SELECT saga_id, definition_name, status, phase, current_step, correlation_id,
                       failure_reason, next_timeout_at_ms, created_at_ms, updated_at_ms
                FROM %s
                WHERE status IN (?, ?, ?)
                  AND next_timeout_at_ms IS NOT NULL
                  AND next_timeout_at_ms <= ?
                ORDER BY next_timeout_at_ms ASC, updated_at_ms DESC
                LIMIT ?
                """.formatted(table);
        return jdbcTemplate.query(
                sql,
                this::mapInstance,
                SagaStatus.RUNNING.name(),
                SagaStatus.WAITING.name(),
                SagaStatus.COMPENSATING.name(),
                nowMs,
                limit
        );
    }

    private long countOverdueInstances(String table, long nowMs) {
        String sql = """
                SELECT COUNT(*)
                FROM %s
                WHERE status IN (?, ?, ?)
                  AND next_timeout_at_ms IS NOT NULL
                  AND next_timeout_at_ms <= ?
                """.formatted(table);
        Long total = jdbcTemplate.queryForObject(
                sql,
                Long.class,
                SagaStatus.RUNNING.name(),
                SagaStatus.WAITING.name(),
                SagaStatus.COMPENSATING.name(),
                nowMs
        );
        return total == null ? 0L : total;
    }

    private OrderSagaPendingSessionView mapPendingSession(SagaReplyFanInSession session, long nowMs) {
        return new OrderSagaPendingSessionView(
                session.aggregateId(),
                session.expectedReplies(),
                session.receivedReplies(),
                session.failed(),
                session.failureReason(),
                session.createdAtMs(),
                session.updatedAtMs(),
                session.expiresAtMs(),
                session.ageMs(nowMs)
        );
    }

    private OrderSagaInstanceView mapInstance(ResultSet rs, int rowNum) throws SQLException {
        long nextTimeoutAtMs = rs.getLong("next_timeout_at_ms");
        Long nextTimeout = rs.wasNull() ? null : nextTimeoutAtMs;
        return new OrderSagaInstanceView(
                rs.getString("saga_id"),
                rs.getString("definition_name"),
                rs.getString("status"),
                rs.getString("phase"),
                rs.getString("current_step"),
                rs.getString("correlation_id"),
                rs.getString("failure_reason"),
                nextTimeout,
                rs.getLong("created_at_ms"),
                rs.getLong("updated_at_ms")
        );
    }
}
