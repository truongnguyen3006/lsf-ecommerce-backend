package com.myexampleproject.inventoryservice.service;

public record InventoryReservationViewRecord(
        String orderNumber,
        String skuCode,
        int quantity,
        String quotaKey,
        String requestId,
        String state,
        long reservedAtMs,
        long expiresAtMs,
        Long confirmedAtMs,
        Long releasedAtMs,
        String reason,
        long updatedAtMs
) {
}
