package com.myexampleproject.orderservice.service;

public record OrderInventoryReleaseRequestedCommand(
        String orderNumber,
        String targetStatus,
        String reason
) {
}
