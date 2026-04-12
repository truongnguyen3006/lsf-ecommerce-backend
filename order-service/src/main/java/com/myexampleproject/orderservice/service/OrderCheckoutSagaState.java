package com.myexampleproject.orderservice.service;

import com.myexampleproject.common.dto.OrderLineItemRequest;

import java.util.List;

public record OrderCheckoutSagaState(
        String orderNumber,
        List<OrderLineItemRequest> items,
        boolean inventoryValidated,
        boolean paymentSucceeded,
        boolean compensationCompleted,
        String failureStatus,
        String failureReason,
        String lastMessage
) {

    public static OrderCheckoutSagaState initial(String orderNumber, List<OrderLineItemRequest> items) {
        return new OrderCheckoutSagaState(
                orderNumber,
                List.copyOf(items),
                false,
                false,
                false,
                null,
                null,
                "created"
        );
    }

    public OrderCheckoutSagaState withInventoryValidated() {
        return new OrderCheckoutSagaState(
                orderNumber,
                items,
                true,
                paymentSucceeded,
                compensationCompleted,
                failureStatus,
                failureReason,
                "inventory validated"
        );
    }

    public OrderCheckoutSagaState withInventoryFailure(String reason) {
        return withFailure("FAILED", reason, "inventory validation failed");
    }

    public OrderCheckoutSagaState withPaymentSucceeded() {
        return new OrderCheckoutSagaState(
                orderNumber,
                items,
                inventoryValidated,
                true,
                compensationCompleted,
                failureStatus,
                failureReason,
                "payment succeeded"
        );
    }

    public OrderCheckoutSagaState withPaymentFailure(String reason) {
        return withFailure("PAYMENT_FAILED", reason, "payment failed");
    }

    public OrderCheckoutSagaState withFailure(String status, String reason, String message) {
        return new OrderCheckoutSagaState(
                orderNumber,
                items,
                inventoryValidated,
                paymentSucceeded,
                compensationCompleted,
                status,
                reason,
                message
        );
    }

    public OrderCheckoutSagaState withCompensationCompleted() {
        return new OrderCheckoutSagaState(
                orderNumber,
                items,
                inventoryValidated,
                paymentSucceeded,
                true,
                failureStatus,
                failureReason,
                "inventory released"
        );
    }

    public String compensationStatusOr(String fallback) {
        return failureStatus == null || failureStatus.isBlank() ? fallback : failureStatus;
    }

    public String compensationReasonOr(String fallback) {
        return failureReason == null || failureReason.isBlank() ? fallback : failureReason;
    }
}
