package com.myexampleproject.cartservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myexampleproject.cartservice.config.CartEventingConfiguration;
import com.myexampleproject.common.event.CartCheckoutEvent;
import com.myexampleproject.common.event.CartLineItem;
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
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class CartCheckoutEventingRuntimeTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    LsfEventingRedisAutoConfiguration.class,
                    LsfEventingAutoConfiguration.class
            ))
            .withUserConfiguration(CartCheckoutEventingTestConfig.class)
            .withPropertyValues(
                    "lsf.eventing.listener.enabled=true",
                    "lsf.eventing.consume-topics[0]=cart-checkout-topic",
                    "spring.application.name=cart-service",
                    "spring.kafka.consumer.group-id=cart-cleaner-group"
            );

    @Test
    void shouldDispatchRawCheckoutPayloadThroughLsfEventing() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(LsfEnvelopeListener.class);

            LsfEnvelopeListener listener = context.getBean(LsfEnvelopeListener.class);
            CartService cartService = context.getBean(CartService.class);
            CartCheckoutEvent event = new CartCheckoutEvent(
                    "user-42",
                    List.of(new CartLineItem("SKU-1", 1, new BigDecimal("10.00")))
            );

            listener.onMessage(event);

            verify(cartService, times(1)).cleanupAfterCheckout(argThat(payload ->
                    payload != null
                            && "user-42".equals(payload.getUserId())
                            && payload.getItems().size() == 1
                            && "SKU-1".equals(payload.getItems().get(0).getSkuCode())
            ));
        });
    }

    @Test
    void shouldDispatchMapCheckoutPayloadThroughLsfEventing() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(LsfEnvelopeListener.class);

            LsfEnvelopeListener listener = context.getBean(LsfEnvelopeListener.class);
            CartService cartService = context.getBean(CartService.class);
            ObjectMapper objectMapper = context.getBean(ObjectMapper.class);
            CartCheckoutEvent event = new CartCheckoutEvent(
                    "user-77",
                    List.of(new CartLineItem("SKU-9", 2, new BigDecimal("15.00")))
            );

            listener.onMessage(objectMapper.convertValue(event, Map.class));

            verify(cartService, times(1)).cleanupAfterCheckout(argThat(payload ->
                    payload != null
                            && "user-77".equals(payload.getUserId())
                            && payload.getItems().size() == 1
                            && "SKU-9".equals(payload.getItems().get(0).getSkuCode())
                            && payload.getItems().get(0).getQuantity() == 2
            ));
        });
    }

    @Configuration(proxyBeanMethods = false)
    @Import({CartEventingConfiguration.class, CartCheckoutCleanupEventHandler.class})
    static class CartCheckoutEventingTestConfig {

        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper();
        }

        @Bean
        KafkaTemplate<String, Object> kafkaTemplate() {
            return mock(KafkaTemplate.class);
        }

        @Bean
        CartService cartService() {
            return mock(CartService.class);
        }
    }
}
