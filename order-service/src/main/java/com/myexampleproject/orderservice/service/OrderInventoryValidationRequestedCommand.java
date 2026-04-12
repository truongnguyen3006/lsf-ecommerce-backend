package com.myexampleproject.orderservice.service;

import com.myexampleproject.common.dto.OrderLineItemRequest;

import java.util.List;

public record OrderInventoryValidationRequestedCommand(
        String orderNumber,
        List<OrderLineItemRequest> items
) {
}
