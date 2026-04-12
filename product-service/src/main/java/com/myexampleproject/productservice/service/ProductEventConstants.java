package com.myexampleproject.productservice.service;

final class ProductEventConstants {

    static final String PRODUCT_CREATED_TOPIC = "product-created-topic";
    static final String PRODUCT_CACHE_UPDATE_TOPIC = "product-cache-update-topic";

    static final String PRODUCT_CREATED_EVENT_TYPE = "product.created.v1";
    static final String PRODUCT_CACHE_UPDATED_EVENT_TYPE = "product.cache.updated.v1";

    private ProductEventConstants() {
    }
}
