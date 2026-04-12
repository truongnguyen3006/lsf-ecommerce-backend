package com.myexampleproject.orderservice.service;

public record OrderInventoryValidationFailedReply(
        String orderNumber,
        String reason
) {
}
