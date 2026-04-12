package com.myexampleproject.cartservice.service;

import com.myexampleproject.common.event.CartCheckoutEvent;

public interface CartCheckoutEventPublisher {

    void publish(CartCheckoutEvent event);
}
