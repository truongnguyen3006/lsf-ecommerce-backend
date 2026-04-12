package com.myexampleproject.productservice.service;

import com.myexampleproject.common.event.ProductCacheEvent;
import com.myexampleproject.common.event.ProductCreatedEvent;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.outbox.OutboxWriter;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class OutboxProductEventPublisher implements ProductEventPublisher {

    private final OutboxWriter outboxWriter;
    private final ProductOutboxEnvelopeFactory envelopeFactory;

    public OutboxProductEventPublisher(OutboxWriter outboxWriter, ProductOutboxEnvelopeFactory envelopeFactory) {
        this.outboxWriter = outboxWriter;
        this.envelopeFactory = envelopeFactory;
    }

    @Override
    public void publishProductCreated(ProductCreatedEvent event) {
        String skuCode = requireSku(event.getSkuCode());
        EventEnvelope envelope = envelopeFactory.wrap(
                ProductEventConstants.PRODUCT_CREATED_EVENT_TYPE,
                skuCode,
                skuCode,
                event
        );
        outboxWriter.append(envelope, ProductEventConstants.PRODUCT_CREATED_TOPIC, skuCode);
    }

    @Override
    public void publishProductCacheUpdate(ProductCacheEvent event) {
        String skuCode = requireSku(event.getSkuCode());
        EventEnvelope envelope = envelopeFactory.wrap(
                ProductEventConstants.PRODUCT_CACHE_UPDATED_EVENT_TYPE,
                skuCode,
                skuCode,
                event
        );
        outboxWriter.append(envelope, ProductEventConstants.PRODUCT_CACHE_UPDATE_TOPIC, skuCode);
    }

    private String requireSku(String skuCode) {
        if (!StringUtils.hasText(skuCode)) {
            throw new IllegalArgumentException("skuCode must not be blank");
        }
        return skuCode;
    }
}
