package com.myexampleproject.orderservice.service;

public record OrderSagaPendingSessionView(
        String orderNumber,
        int totalItems,
        int receivedItems,
        boolean failed,
        String failureReason,
        long createdAtMs,
        long updatedAtMs,
        long expiresAtMs,
        long ageMs
) {
}
