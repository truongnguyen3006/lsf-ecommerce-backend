package com.myexampleproject.orderservice.service;

public record OrderInventoryReleaseCompletedReply(
        String orderNumber,
        String status
) {
}
