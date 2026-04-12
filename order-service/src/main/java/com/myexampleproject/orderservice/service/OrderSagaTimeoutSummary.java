package com.myexampleproject.orderservice.service;

public record OrderSagaTimeoutSummary(
        long timedOutCount,
        long compensationFailedCount,
        long overdueCount
) {
}
