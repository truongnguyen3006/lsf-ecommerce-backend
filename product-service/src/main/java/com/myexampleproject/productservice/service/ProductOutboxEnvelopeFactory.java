package com.myexampleproject.productservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myorg.lsf.contracts.core.envelope.EnvelopeBuilder;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class ProductOutboxEnvelopeFactory {

    private final ObjectMapper objectMapper;
    private final String producerName;

    public ProductOutboxEnvelopeFactory(
            ObjectMapper objectMapper,
            @Value("${spring.application.name:product-service}") String producerName
    ) {
        this.objectMapper = objectMapper;
        this.producerName = producerName;
    }

    public EventEnvelope wrap(String eventType, String aggregateId, String correlationId, Object payload) {
        return EnvelopeBuilder.wrap(
                objectMapper,
                eventType,
                1,
                aggregateId,
                correlationId,
                null,
                null,
                producerName,
                payload
        );
    }
}
