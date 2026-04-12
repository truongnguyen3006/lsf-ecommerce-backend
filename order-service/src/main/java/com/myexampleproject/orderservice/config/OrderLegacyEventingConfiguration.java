package com.myexampleproject.orderservice.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.myexampleproject.common.event.CartCheckoutEvent;
import com.myexampleproject.common.event.OrderPlacedEvent;
import com.myexampleproject.orderservice.service.OrderMessagingConstants;
import com.myorg.lsf.contracts.core.envelope.EnvelopeBuilder;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.eventing.JacksonPayloadConverter;
import com.myorg.lsf.eventing.PayloadConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;

@Configuration(proxyBeanMethods = false)
public class OrderLegacyEventingConfiguration {

    @Bean
    public PayloadConverter payloadConverter(
            ObjectMapper objectMapper,
            @Value("${spring.application.name:order-service}") String applicationName
    ) {
        JacksonPayloadConverter delegate = new JacksonPayloadConverter(objectMapper);
        String producer = applicationName + ".raw-consumer-adapter";

        return value -> {
            CartCheckoutEvent cartCheckoutEvent = toCartCheckoutEvent(objectMapper, value);
            if (cartCheckoutEvent != null) {
                return wrap(
                        objectMapper,
                        producer,
                        OrderMessagingConstants.CART_CHECKOUT_RAW_EVENT_TYPE,
                        cartCheckoutEvent.getUserId(),
                        cartCheckoutEvent
                );
            }

            OrderPlacedEvent orderPlacedEvent = toOrderPlacedEvent(objectMapper, value);
            if (orderPlacedEvent != null) {
                return wrap(
                        objectMapper,
                        producer,
                        OrderMessagingConstants.ORDER_PLACED_RAW_EVENT_TYPE,
                        orderPlacedEvent.getOrderNumber(),
                        orderPlacedEvent
                );
            }

            return delegate.toEnvelope(value);
        };
    }

    private CartCheckoutEvent toCartCheckoutEvent(ObjectMapper objectMapper, Object value) {
        if (value instanceof CartCheckoutEvent event) {
            return event;
        }
        if (looksLikeEnvelope(value) || !looksLikeRawCartCheckout(value)) {
            return null;
        }
        return objectMapper.convertValue(value, CartCheckoutEvent.class);
    }

    private OrderPlacedEvent toOrderPlacedEvent(ObjectMapper objectMapper, Object value) {
        if (value instanceof OrderPlacedEvent event) {
            return event;
        }
        if (looksLikeEnvelope(value) || !looksLikeRawOrderPlaced(value)) {
            return null;
        }
        return objectMapper.convertValue(value, OrderPlacedEvent.class);
    }

    private boolean looksLikeEnvelope(Object value) {
        if (value instanceof EventEnvelope) {
            return true;
        }
        if (value instanceof Map<?, ?> map) {
            return map.containsKey("eventType") && map.containsKey("payload");
        }
        if (value instanceof JsonNode node) {
            return node.has("eventType") && node.has("payload");
        }
        return false;
    }

    private boolean looksLikeRawCartCheckout(Object value) {
        if (value instanceof Map<?, ?> map) {
            return map.containsKey("userId") && map.containsKey("items");
        }
        if (value instanceof JsonNode node) {
            return node.has("userId") && node.has("items");
        }
        return false;
    }

    private boolean looksLikeRawOrderPlaced(Object value) {
        if (value instanceof Map<?, ?> map) {
            return map.containsKey("orderNumber")
                    && map.containsKey("userId")
                    && map.containsKey("orderLineItemsDtoList");
        }
        if (value instanceof JsonNode node) {
            return node.has("orderNumber")
                    && node.has("userId")
                    && node.has("orderLineItemsDtoList");
        }
        return false;
    }

    private EventEnvelope wrap(
            ObjectMapper objectMapper,
            String producer,
            String eventType,
            String aggregateId,
            Object payload
    ) {
        return EnvelopeBuilder.wrap(
                objectMapper,
                eventType,
                1,
                aggregateId,
                aggregateId,
                null,
                null,
                producer,
                payload
        );
    }
}
