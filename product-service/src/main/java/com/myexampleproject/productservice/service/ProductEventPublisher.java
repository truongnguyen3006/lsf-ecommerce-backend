package com.myexampleproject.productservice.service;

import com.myexampleproject.common.event.ProductCacheEvent;
import com.myexampleproject.common.event.ProductCreatedEvent;

public interface ProductEventPublisher {

    void publishProductCreated(ProductCreatedEvent event);

    void publishProductCacheUpdate(ProductCacheEvent event);
}
