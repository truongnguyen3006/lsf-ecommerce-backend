package com.myexampleproject.orderservice.service;

public final class OrderMessagingConstants {

    public static final String ORDER_PLACED_TOPIC = "order-placed-topic";
    public static final String INVENTORY_CHECK_REQUEST_TOPIC = "inventory-check-request-topic";
    public static final String CART_CHECKOUT_RAW_EVENT_TYPE = "cart.checkout.raw.v1";
    public static final String ORDER_PLACED_RAW_EVENT_TYPE = "order.placed.raw.v1";

    private OrderMessagingConstants() {
    }
}
