package com.myexampleproject.inventoryservice.service;

import com.myexampleproject.common.event.InventoryAdjustmentEvent;

public interface InventoryAdjustmentPublisher {

    void publish(InventoryAdjustmentEvent event);
}
