package com.myexampleproject.orderservice.service;

import java.util.List;
import java.util.Map;

public record OrderSagaAdminSnapshot(
        String workflowMode,
        String defaultWorkflowMode,
        String rollbackWorkflowMode,
        boolean rollbackAvailable,
        boolean sagaEnabled,
        String storeMode,
        String transportMode,
        Map<String, Long> summary,
        OrderSagaTimeoutSummary timeoutSummary,
        OrderSagaAggregatorSnapshot aggregator,
        OrderSagaRuntimeCounters runtimeCounters,
        List<OrderSagaInstanceView> recentInstances,
        List<OrderSagaInstanceView> recentFailures,
        List<OrderSagaInstanceView> recentCompensations,
        List<OrderSagaInstanceView> overdueInstances
) {
}
