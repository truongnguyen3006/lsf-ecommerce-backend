package com.myexampleproject.orderservice.service;

public record OrderSagaRuntimeCounters(
        long legacyStartsSinceBoot,
        long sagaModeStartsSinceBoot,
        long sagaStartedSinceBoot,
        long sagaCompletedSinceBoot,
        long sagaFailedSinceBoot,
        long sagaCompensatedSinceBoot,
        long aggregatorDuplicateStartsIgnoredSinceBoot,
        long aggregatorLateResultsIgnoredSinceBoot,
        long aggregatorExpiredSinceBoot
) {
}
