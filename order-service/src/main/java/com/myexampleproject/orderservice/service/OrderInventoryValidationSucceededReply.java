package com.myexampleproject.orderservice.service;

public record OrderInventoryValidationSucceededReply(
        String orderNumber,
        int checkedItems
) {
}
