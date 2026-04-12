package com.myexampleproject.orderservice.service;

import com.myexampleproject.orderservice.config.OrderWorkflowMode;
import com.myorg.lsf.saga.LsfSagaProperties;
import com.myorg.lsf.saga.SagaSql;
import com.myorg.lsf.saga.SagaStatus;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

@Service
public class OrderSagaEvidenceMetrics {

    private final JdbcTemplate jdbcTemplate;
    private final OrderSagaInventorySessionStore sessionStore;
    private final String sagaTable;
    private final Counter legacyStartsCounter;
    private final Counter sagaModeStartsCounter;
    private final Counter sagaStartedCounter;
    private final Counter sagaCompletedCounter;
    private final Counter sagaFailedCounter;
    private final Counter sagaCompensatedCounter;
    private final Counter aggregatorDuplicateStartsCounter;
    private final Counter aggregatorLateResultsIgnoredCounter;
    private final Counter aggregatorExpiredCounter;

    public OrderSagaEvidenceMetrics(
            MeterRegistry meterRegistry,
            JdbcTemplate jdbcTemplate,
            OrderSagaInventorySessionStore sessionStore,
            LsfSagaProperties sagaProperties
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.sessionStore = sessionStore;
        this.sagaTable = SagaSql.validateTableName(sagaProperties.getJdbc().getTable());
        this.legacyStartsCounter = Counter.builder("lsf.order.workflow.starts")
                .description("Order workflow starts by mode")
                .tag("mode", "legacy")
                .register(meterRegistry);
        this.sagaModeStartsCounter = Counter.builder("lsf.order.workflow.starts")
                .description("Order workflow starts by mode")
                .tag("mode", "lsf_saga")
                .register(meterRegistry);
        this.sagaStartedCounter = Counter.builder("lsf.order.saga.started.total")
                .description("Total saga starts observed by order-service")
                .register(meterRegistry);
        this.sagaCompletedCounter = Counter.builder("lsf.order.saga.completed.total")
                .description("Total saga completions observed by order-service")
                .register(meterRegistry);
        this.sagaFailedCounter = Counter.builder("lsf.order.saga.failed.total")
                .description("Total saga terminal failures observed by order-service")
                .register(meterRegistry);
        this.sagaCompensatedCounter = Counter.builder("lsf.order.saga.compensated.total")
                .description("Total compensated sagas observed by order-service")
                .register(meterRegistry);
        this.aggregatorDuplicateStartsCounter = Counter.builder("lsf.order.saga.aggregator.duplicate_starts.total")
                .description("Duplicate inventory bridge starts ignored by order-service")
                .register(meterRegistry);
        this.aggregatorLateResultsIgnoredCounter = Counter.builder("lsf.order.saga.aggregator.late_results_ignored.total")
                .description("Late or duplicate inventory replies ignored by order-service")
                .register(meterRegistry);
        this.aggregatorExpiredCounter = Counter.builder("lsf.order.saga.aggregator.expired.total")
                .description("Expired inventory bridge sessions purged by order-service")
                .register(meterRegistry);

        Gauge.builder("lsf.order.saga.aggregator.pending", sessionStore, OrderSagaInventorySessionStore::pendingCount)
                .description("Current pending inventory bridge sessions")
                .register(meterRegistry);

        for (SagaStatus status : SagaStatus.values()) {
            Gauge.builder("lsf.order.saga.instances", () -> countByStatus(status))
                    .description("Current persisted saga instances by terminal/runtime status")
                    .tag("status", status.name().toLowerCase())
                    .register(meterRegistry);
        }
    }

    public void recordWorkflowModeStart(OrderWorkflowMode mode) {
        if (mode == OrderWorkflowMode.LSF_SAGA) {
            sagaModeStartsCounter.increment();
            return;
        }
        legacyStartsCounter.increment();
    }

    public void recordSagaStarted() {
        sagaStartedCounter.increment();
    }

    public void recordSagaCompleted() {
        sagaCompletedCounter.increment();
    }

    public void recordSagaFailed() {
        sagaFailedCounter.increment();
    }

    public void recordSagaCompensated() {
        sagaCompensatedCounter.increment();
    }

    public void recordAggregatorDuplicateStart() {
        aggregatorDuplicateStartsCounter.increment();
    }

    public void recordAggregatorLateResultIgnored() {
        aggregatorLateResultsIgnoredCounter.increment();
    }

    public void recordAggregatorExpired(int purgedCount) {
        if (purgedCount > 0) {
            aggregatorExpiredCounter.increment(purgedCount);
        }
    }

    public OrderSagaRuntimeCounters snapshot() {
        return new OrderSagaRuntimeCounters(
                Math.round(legacyStartsCounter.count()),
                Math.round(sagaModeStartsCounter.count()),
                Math.round(sagaStartedCounter.count()),
                Math.round(sagaCompletedCounter.count()),
                Math.round(sagaFailedCounter.count()),
                Math.round(sagaCompensatedCounter.count()),
                Math.round(aggregatorDuplicateStartsCounter.count()),
                Math.round(aggregatorLateResultsIgnoredCounter.count()),
                Math.round(aggregatorExpiredCounter.count())
        );
    }

    private double countByStatus(SagaStatus status) {
        try {
            String sql = "SELECT COUNT(*) FROM " + sagaTable + " WHERE status = ?";
            Long total = jdbcTemplate.queryForObject(sql, Long.class, status.name());
            return total == null ? 0D : total.doubleValue();
        } catch (RuntimeException exception) {
            return 0D;
        }
    }
}
