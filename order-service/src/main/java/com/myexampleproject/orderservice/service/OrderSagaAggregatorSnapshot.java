package com.myexampleproject.orderservice.service;

import java.util.List;

public record OrderSagaAggregatorSnapshot(
        long pendingCount,
        long duplicateStartsIgnoredSinceBoot,
        long lateResultsIgnoredSinceBoot,
        long expiredSinceBoot,
        List<OrderSagaPendingSessionView> pendingSessions
) {
}
