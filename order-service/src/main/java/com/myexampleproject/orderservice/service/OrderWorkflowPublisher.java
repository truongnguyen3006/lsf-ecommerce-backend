package com.myexampleproject.orderservice.service;

import com.myexampleproject.common.event.InventoryCheckRequest;
import com.myexampleproject.common.event.OrderPlacedEvent;

public interface OrderWorkflowPublisher {

    void publishOrderPlaced(OrderPlacedEvent event);

    void publishInventoryCheckRequest(InventoryCheckRequest event);
}
