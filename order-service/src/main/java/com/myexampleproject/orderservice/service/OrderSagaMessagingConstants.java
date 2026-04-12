package com.myexampleproject.orderservice.service;

public final class OrderSagaMessagingConstants {

    public static final String INVENTORY_VALIDATION_REQUESTED_EVENT_TYPE =
            "order.inventory.validation.requested.v1";
    public static final String INVENTORY_VALIDATED_EVENT_TYPE =
            "order.inventory.validated.v1";
    public static final String INVENTORY_VALIDATION_FAILED_EVENT_TYPE =
            "order.inventory.validation.failed.v1";
    public static final String INVENTORY_RELEASE_REQUESTED_EVENT_TYPE =
            "order.inventory.release.requested.v1";
    public static final String INVENTORY_RELEASE_COMPLETED_EVENT_TYPE =
            "order.inventory.release.completed.v1";

    private OrderSagaMessagingConstants() {
    }
}
