package com.myexampleproject.orderservice.service;

import com.myexampleproject.common.event.CartCheckoutEvent;
import com.myexampleproject.common.event.OrderPlacedEvent;
import com.myexampleproject.orderservice.config.CartMapper;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.eventing.LsfEventHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class OrderLegacyEventHandler {

    private final OrderService orderService;

    @LsfEventHandler(
            value = OrderMessagingConstants.CART_CHECKOUT_RAW_EVENT_TYPE,
            payload = CartCheckoutEvent.class
    )
    public void handleCartCheckout(EventEnvelope envelope, CartCheckoutEvent event) {
        try {
            orderService.placeOrder(CartMapper.fromCart(event), event.getUserId());
        } catch (Exception exception) {
            log.error(
                    "Failed to process raw cart checkout in LSF adapter. eventId={}, userId={}",
                    envelope.getEventId(),
                    event.getUserId(),
                    exception
            );
        }
    }

    @LsfEventHandler(
            value = OrderMessagingConstants.ORDER_PLACED_RAW_EVENT_TYPE,
            payload = OrderPlacedEvent.class
    )
    public void handleOrderPlaced(EventEnvelope envelope, OrderPlacedEvent event) {
        try {
            orderService.handleOrderPlacement(envelope, event);
        } catch (Exception exception) {
            log.error(
                    "Failed to process raw order placed event in LSF adapter. eventId={}, orderNumber={}",
                    envelope.getEventId(),
                    event.getOrderNumber(),
                    exception
            );
        }
    }
}
