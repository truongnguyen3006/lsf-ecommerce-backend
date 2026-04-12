package com.myexampleproject.orderservice.service;

public record OrderSagaInstanceView(
        String sagaId,
        String definitionName,
        String status,
        String phase,
        String currentStep,
        String correlationId,
        String failureReason,
        Long nextTimeoutAtMs,
        long createdAtMs,
        long updatedAtMs
) {
}
