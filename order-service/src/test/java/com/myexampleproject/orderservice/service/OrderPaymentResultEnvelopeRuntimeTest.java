package com.myexampleproject.orderservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myexampleproject.common.event.PaymentProcessedEvent;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class OrderPaymentResultEnvelopeRuntimeTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    LsfEventingRedisAutoConfiguration.class,
                    LsfEventingAutoConfiguration.class
            ))
            .withUserConfiguration(EnvelopeRuntimeTestConfig.class)
            .withPropertyValues(
                    "lsf.eventing.consume-topics[0]=payment-processed-envelope-topic",
                    "lsf.eventing.consume-topics[1]=payment-failed-envelope-topic",
                    "lsf.eventing.ignore-unknown-event-type=false",
                    "lsf.eventing.idempotency.enabled=true",
                    "lsf.eventing.idempotency.store=memory",
                    "lsf.kafka.consumer.group-id=order-updater-group",
                    "app.order.workflow.mode=legacy"
            );

    @Test
    void shouldDispatchProcessedEnvelopeAndSkipDuplicateEventId() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(LsfEnvelopeListener.class);

            LsfEnvelopeListener listener = context.getBean(LsfEnvelopeListener.class);
            OrderPaymentResultProcessor processor = context.getBean(OrderPaymentResultProcessor.class);

            EventEnvelope envelope = EventEnvelope.builder()
                    .eventId("evt-payment-processed-1")
                    .eventType("payment.processed.v1")
                    .aggregateId("ORDER-1")
                    .payload(context.getBean(ObjectMapper.class).valueToTree(
                            new PaymentProcessedEvent("ORDER-1", "PAY-1")
                    ))
                    .build();

            listener.onMessage(envelope);
            listener.onMessage(envelope);

            verify(processor, times(1)).handlePaymentSuccess(
                    argThat(event ->
                            event != null
                                    && "ORDER-1".equals(event.getOrderNumber())
                                    && "PAY-1".equals(event.getPaymentId())
                    ),
                    eq("lsf-envelope"),
                    eq("evt-payment-processed-1")
            );
        });
    }

    @Test
    void shouldDisableDirectPaymentResultHandlerInSagaMode() {
        runner.withPropertyValues("app.order.workflow.mode=lsf-saga")
                .run(context -> assertThat(context).doesNotHaveBean(OrderPaymentResultEventHandler.class));
    }

    @Configuration(proxyBeanMethods = false)
    @Import(OrderPaymentResultEventHandler.class)
    static class EnvelopeRuntimeTestConfig {

        @Bean
        ObjectMapper objectMapper() {
            return new ObjectMapper();
        }

        @Bean
        KafkaTemplate<String, Object> kafkaTemplate() {
            return mock(KafkaTemplate.class);
        }

        @Bean
        OrderPaymentResultProcessor orderPaymentResultProcessor() {
            return mock(OrderPaymentResultProcessor.class);
        }
    }
}
