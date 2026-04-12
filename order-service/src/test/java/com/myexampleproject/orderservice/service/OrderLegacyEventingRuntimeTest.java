package com.myexampleproject.orderservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myexampleproject.common.dto.OrderLineItemRequest;
import com.myexampleproject.common.event.CartCheckoutEvent;
import com.myexampleproject.common.event.CartLineItem;
import com.myexampleproject.common.event.OrderPlacedEvent;
import com.myexampleproject.orderservice.config.OrderLegacyEventingConfiguration;
import com.myorg.lsf.eventing.autoconfig.LsfEnvelopeListener;
import com.myorg.lsf.eventing.autoconfig.LsfEventingAutoConfiguration;
import com.myorg.lsf.eventing.autoconfig.LsfEventingRedisAutoConfiguration;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class OrderLegacyEventingRuntimeTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    LsfEventingRedisAutoConfiguration.class,
                    LsfEventingAutoConfiguration.class
            ))
            .withUserConfiguration(OrderLegacyEventingTestConfig.class)
            .withPropertyValues(
                    "spring.application.name=order-service",
                    "lsf.eventing.listener.enabled=true",
                    "lsf.eventing.consume-topics[0]=payment-processed-envelope-topic",
                    "lsf.eventing.consume-topics[1]=payment-failed-envelope-topic",
                    "lsf.eventing.consume-topics[2]=cart-checkout-topic",
                    "lsf.eventing.consume-topics[3]=order-placed-topic",
                    "lsf.eventing.ignore-unknown-event-type=true",
                    "lsf.eventing.idempotency.enabled=true",
                    "lsf.eventing.idempotency.store=memory",
                    "lsf.kafka.consumer.group-id=order-updater-group"
            );

    @Test
    void shouldDispatchLegacyRawTopicsThroughEventingAdapter() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(LsfEnvelopeListener.class);

            LsfEnvelopeListener listener = context.getBean(LsfEnvelopeListener.class);
            OrderService orderService = context.getBean(OrderService.class);

            CartCheckoutEvent cartCheckoutEvent = new CartCheckoutEvent(
                    "user-7",
                    List.of(new CartLineItem("SKU-1", 2, new BigDecimal("10.00")))
            );
            listener.onMessage(cartCheckoutEvent);

            verify(orderService, times(1)).placeOrder(
                    argThat(request ->
                            request != null
                                    && request.getItems().size() == 1
                                    && "SKU-1".equals(request.getItems().get(0).getSkuCode())
                                    && request.getItems().get(0).getQuantity() == 2
                    ),
                    eq("user-7")
            );

            OrderPlacedEvent orderPlacedEvent = new OrderPlacedEvent(
                    "ORDER-1",
                    "user-7",
                    List.of(OrderLineItemRequest.builder().skuCode("SKU-9").quantity(3).build())
            );
            listener.onMessage(orderPlacedEvent);

            verify(orderService, times(1)).handleOrderPlacement(
                    any(),
                    argThat(event ->
                            event != null
                                    && "ORDER-1".equals(event.getOrderNumber())
                                    && "user-7".equals(event.getUserId())
                                    && event.getOrderLineItemsDtoList().size() == 1
                                    && "SKU-9".equals(event.getOrderLineItemsDtoList().get(0).getSkuCode())
                    )
            );
        });
    }

    @Test
    void shouldDispatchLegacyMapPayloadsThroughEventingAdapter() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(LsfEnvelopeListener.class);

            LsfEnvelopeListener listener = context.getBean(LsfEnvelopeListener.class);
            OrderService orderService = context.getBean(OrderService.class);
            ObjectMapper objectMapper = context.getBean(ObjectMapper.class);

            CartCheckoutEvent cartCheckoutEvent = new CartCheckoutEvent(
                    "user-9",
                    List.of(new CartLineItem("SKU-2", 1, new BigDecimal("25.00")))
            );
            listener.onMessage(objectMapper.convertValue(cartCheckoutEvent, Map.class));

            verify(orderService, times(1)).placeOrder(
                    argThat(request ->
                            request != null
                                    && request.getItems().size() == 1
                                    && "SKU-2".equals(request.getItems().get(0).getSkuCode())
                                    && request.getItems().get(0).getQuantity() == 1
                    ),
                    eq("user-9")
            );

            OrderPlacedEvent orderPlacedEvent = new OrderPlacedEvent(
                    "ORDER-MAP-1",
                    "user-9",
                    List.of(OrderLineItemRequest.builder().skuCode("SKU-5").quantity(4).build())
            );
            listener.onMessage(objectMapper.convertValue(orderPlacedEvent, Map.class));

            verify(orderService, times(1)).handleOrderPlacement(
                    any(),
                    argThat(event ->
                            event != null
                                    && "ORDER-MAP-1".equals(event.getOrderNumber())
                                    && "user-9".equals(event.getUserId())
                                    && event.getOrderLineItemsDtoList().size() == 1
                                    && "SKU-5".equals(event.getOrderLineItemsDtoList().get(0).getSkuCode())
                    )
            );
        });
    }

    @Configuration(proxyBeanMethods = false)
    @Import({OrderLegacyEventingConfiguration.class, OrderLegacyEventHandler.class})
    static class OrderLegacyEventingTestConfig {

        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper();
        }

        @Bean
        KafkaTemplate<String, Object> kafkaTemplate() {
            return mock(KafkaTemplate.class);
        }

        @Bean
        OrderService orderService() {
            return mock(OrderService.class);
        }
    }
}
