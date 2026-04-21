package com.myexampleproject.inventoryservice.dto;

import lombok.Builder;

import java.util.List;

@Builder
public record OrderReservationSummaryResponse(
        String orderNumber,
        String state,
        long reservedAtMs,
        long expiresAtMs,
        long remainingMs,
        boolean countdownActive,
        List<OrderReservationItemView> items
) {
}
