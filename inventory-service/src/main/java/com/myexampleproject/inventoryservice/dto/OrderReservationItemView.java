package com.myexampleproject.inventoryservice.dto;

import lombok.Builder;

@Builder
public record OrderReservationItemView(
        String orderNumber,
        String skuCode,
        int quantity,
        String state,
        long reservedAtMs,
        long expiresAtMs,
        Long confirmedAtMs,
        Long releasedAtMs,
        long remainingMs,
        String reason,
        String quotaKey,
        String requestId
) {
}
