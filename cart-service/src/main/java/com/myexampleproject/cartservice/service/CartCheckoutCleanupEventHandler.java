package com.myexampleproject.cartservice.service;

import com.myexampleproject.common.event.CartCheckoutEvent;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.eventing.LsfEventHandler;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class CartCheckoutCleanupEventHandler {

    private final CartService cartService;

    @LsfEventHandler(
            value = CartEventingConstants.CHECKOUT_EVENT_TYPE,
            payload = CartCheckoutEvent.class
    )
    public void handle(EventEnvelope envelope, CartCheckoutEvent event) {
        try {
            cartService.cleanupAfterCheckout(event);
        } catch (Exception exception) {
            log.error(
                    "CLEANUP FAILED: userId={}, eventId={}, error={}",
                    event.getUserId(),
                    envelope.getEventId(),
                    exception.getMessage(),
                    exception
            );
        }
    }
}
