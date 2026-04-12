package com.myexampleproject.orderservice.service;

import com.myexampleproject.orderservice.config.OrderWorkflowMode;
import com.myexampleproject.orderservice.config.OrderWorkflowProperties;
import com.myorg.lsf.saga.SagaReplyFanInSession;
import com.myorg.lsf.saga.SagaReplyFanInUpdate;
import com.myorg.lsf.saga.LsfSagaProperties;
import com.myorg.lsf.saga.SagaStoreMode;
import com.myorg.lsf.saga.SagaTransportMode;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import javax.sql.DataSource;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class OrderSagaAdminServiceTest {

    @Test
    void snapshotShouldExposeTimeoutCompensationAndAggregatorEvidence() {
        long nowMs = 1_716_000_000_000L;
        DataSource dataSource = new EmbeddedDatabaseBuilder()
                .generateUniqueName(true)
                .setType(EmbeddedDatabaseType.H2)
                .build();
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.execute("""
                CREATE TABLE lsf_saga_instance (
                    saga_id VARCHAR(128) PRIMARY KEY,
                    definition_name VARCHAR(128) NOT NULL,
                    status VARCHAR(32) NOT NULL,
                    phase VARCHAR(32) NOT NULL,
                    current_step_index INTEGER,
                    compensation_step_index INTEGER,
                    current_step VARCHAR(128),
                    correlation_id VARCHAR(128),
                    request_id VARCHAR(128),
                    causation_id VARCHAR(128),
                    last_event_id VARCHAR(128),
                    failure_reason VARCHAR(2000),
                    state_json CLOB NOT NULL,
                    steps_json CLOB NOT NULL,
                    next_timeout_at_ms BIGINT,
                    created_at_ms BIGINT NOT NULL,
                    updated_at_ms BIGINT NOT NULL,
                    version BIGINT NOT NULL
                )
                """);

        insertSaga(jdbcTemplate, "ORDER-COMPLETE", "COMPLETED", "FORWARD", null, null, nowMs - 10_000, nowMs - 9_000);
        insertSaga(jdbcTemplate, "ORDER-COMPENSATED", "COMPENSATED", "COMPENSATION", null, "payment timeout", nowMs - 8_000, nowMs - 7_000);
        insertSaga(jdbcTemplate, "ORDER-FAILED", "FAILED", "FORWARD", null, "inventory rejected", nowMs - 6_000, nowMs - 5_000);
        insertSaga(jdbcTemplate, "ORDER-TIMEOUT", "TIMED_OUT", "FORWARD", null, "Saga step timed out: paymentProcessing", nowMs - 4_000, nowMs - 3_000);
        insertSaga(jdbcTemplate, "ORDER-OVERDUE", "WAITING", "FORWARD", nowMs - 200, null, nowMs - 2_000, nowMs - 1_000);

        OrderWorkflowProperties workflowProperties = new OrderWorkflowProperties();
        workflowProperties.setMode(OrderWorkflowMode.LEGACY);
        workflowProperties.getSaga().setRecentLimit(5);
        workflowProperties.getSaga().setPendingAggregatorLimit(5);
        workflowProperties.getSaga().setAggregatorCleanupBatch(10);

        LsfSagaProperties sagaProperties = new LsfSagaProperties();
        sagaProperties.setStore(SagaStoreMode.JDBC);
        sagaProperties.getTransport().setMode(SagaTransportMode.DIRECT);
        sagaProperties.getJdbc().setTable("lsf_saga_instance");

        TestOrderSagaInventorySessionStore sessionStore = new TestOrderSagaInventorySessionStore();
        sessionStore.add(new SagaReplyFanInSession("ORDER-PENDING", 2, 1, false, "", "evt-1", "req-1", nowMs - 500, nowMs - 200, nowMs + 5_000));
        sessionStore.add(new SagaReplyFanInSession("ORDER-EXPIRED", 1, 0, false, "", "evt-2", "req-2", nowMs - 5_000, nowMs - 4_000, nowMs - 100));

        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        OrderSagaEvidenceMetrics evidenceMetrics = new OrderSagaEvidenceMetrics(
                meterRegistry,
                jdbcTemplate,
                sessionStore,
                sagaProperties
        );
        evidenceMetrics.recordWorkflowModeStart(OrderWorkflowMode.LEGACY);
        evidenceMetrics.recordWorkflowModeStart(OrderWorkflowMode.LSF_SAGA);
        evidenceMetrics.recordSagaStarted();
        evidenceMetrics.recordSagaCompleted();
        evidenceMetrics.recordSagaFailed();
        evidenceMetrics.recordSagaCompensated();
        evidenceMetrics.recordAggregatorDuplicateStart();
        evidenceMetrics.recordAggregatorLateResultIgnored();

        OrderSagaAdminService service = new OrderSagaAdminService(
                jdbcTemplate,
                workflowProperties,
                sagaProperties,
                sessionStore,
                evidenceMetrics,
                Clock.fixed(Instant.ofEpochMilli(nowMs), ZoneOffset.UTC)
        );

        OrderSagaAdminSnapshot snapshot = service.snapshot();

        assertThat(snapshot.workflowMode()).isEqualTo("LEGACY");
        assertThat(snapshot.defaultWorkflowMode()).isEqualTo("LSF_SAGA");
        assertThat(snapshot.rollbackWorkflowMode()).isEqualTo("LEGACY");
        assertThat(snapshot.rollbackAvailable()).isTrue();
        assertThat(snapshot.summary()).containsEntry("COMPLETED", 1L);
        assertThat(snapshot.summary()).containsEntry("COMPENSATED", 1L);
        assertThat(snapshot.summary()).containsEntry("FAILED", 1L);
        assertThat(snapshot.summary()).containsEntry("TIMED_OUT", 1L);
        assertThat(snapshot.timeoutSummary().timedOutCount()).isEqualTo(1L);
        assertThat(snapshot.timeoutSummary().overdueCount()).isEqualTo(1L);
        assertThat(snapshot.recentCompensations()).extracting(OrderSagaInstanceView::sagaId)
                .contains("ORDER-COMPENSATED");
        assertThat(snapshot.recentFailures()).extracting(OrderSagaInstanceView::sagaId)
                .contains("ORDER-FAILED", "ORDER-TIMEOUT");
        assertThat(snapshot.overdueInstances()).extracting(OrderSagaInstanceView::sagaId)
                .containsExactly("ORDER-OVERDUE");
        assertThat(snapshot.aggregator().pendingCount()).isEqualTo(1L);
        assertThat(snapshot.aggregator().expiredSinceBoot()).isEqualTo(1L);
        assertThat(snapshot.aggregator().pendingSessions()).extracting(OrderSagaPendingSessionView::orderNumber)
                .containsExactly("ORDER-PENDING");
        assertThat(snapshot.runtimeCounters().legacyStartsSinceBoot()).isEqualTo(1L);
        assertThat(snapshot.runtimeCounters().sagaModeStartsSinceBoot()).isEqualTo(1L);
        assertThat(snapshot.runtimeCounters().sagaStartedSinceBoot()).isEqualTo(1L);
        assertThat(snapshot.runtimeCounters().sagaCompletedSinceBoot()).isEqualTo(1L);
        assertThat(snapshot.runtimeCounters().sagaFailedSinceBoot()).isEqualTo(1L);
        assertThat(snapshot.runtimeCounters().sagaCompensatedSinceBoot()).isEqualTo(1L);
        assertThat(snapshot.runtimeCounters().aggregatorExpiredSinceBoot()).isEqualTo(1L);
    }

    private static void insertSaga(
            JdbcTemplate jdbcTemplate,
            String sagaId,
            String status,
            String phase,
            Long nextTimeoutAtMs,
            String failureReason,
            long createdAtMs,
            long updatedAtMs
    ) {
        jdbcTemplate.update("""
                        INSERT INTO lsf_saga_instance (
                            saga_id, definition_name, status, phase, current_step_index, compensation_step_index, current_step,
                            correlation_id, request_id, causation_id, last_event_id, failure_reason,
                            state_json, steps_json, next_timeout_at_ms, created_at_ms, updated_at_ms, version
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                sagaId,
                "order-checkout-saga",
                status,
                phase,
                0,
                null,
                "paymentProcessing",
                sagaId,
                "req-" + sagaId,
                "cause-" + sagaId,
                "evt-" + sagaId,
                failureReason,
                "{}",
                "[]",
                nextTimeoutAtMs,
                createdAtMs,
                updatedAtMs,
                0L
        );
    }

    static class TestOrderSagaInventorySessionStore implements OrderSagaInventorySessionStore {

        private final List<SagaReplyFanInSession> sessions = new ArrayList<>();

        void add(SagaReplyFanInSession session) {
            sessions.add(session);
        }

        @Override
        public boolean start(String orderNumber, int totalItems, String commandEventId, String requestId) {
            throw new UnsupportedOperationException("Not needed for admin snapshot test");
        }

        @Override
        public Optional<SagaReplyFanInUpdate> applyResult(com.myexampleproject.common.event.InventoryCheckResult result) {
            throw new UnsupportedOperationException("Not needed for admin snapshot test");
        }

        @Override
        public Optional<SagaReplyFanInSession> find(String orderNumber) {
            return sessions.stream().filter(session -> session.aggregateId().equals(orderNumber)).findFirst();
        }

        @Override
        public List<SagaReplyFanInSession> findPending(int limit) {
            return sessions.stream().limit(limit).toList();
        }

        @Override
        public long pendingCount() {
            return sessions.size();
        }

        @Override
        public int purgeExpired(long nowMs, int limit) {
            List<SagaReplyFanInSession> expired = sessions.stream()
                    .filter(session -> session.isExpired(nowMs))
                    .limit(limit)
                    .toList();
            sessions.removeAll(expired);
            return expired.size();
        }

        @Override
        public void delete(String orderNumber) {
            find(orderNumber).ifPresent(sessions::remove);
        }
    }
}
