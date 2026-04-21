package com.myexampleproject.paymentservice.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myexampleproject.common.dto.OrderLineItemRequest;
import com.myexampleproject.common.dto.PaymentMethod;
import com.myexampleproject.common.event.OrderValidatedEvent;
import com.myexampleproject.paymentservice.service.PaymentService;
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

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class PaymentOrderValidatedEnvelopeRuntimeTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    LsfEventingRedisAutoConfiguration.class,
                    LsfEventingAutoConfiguration.class
            ))
            .withUserConfiguration(EnvelopeRuntimeTestConfig.class)
            .withPropertyValues(
                    "lsf.eventing.consume-topics[0]=order-validated-envelope-topic",
                    "lsf.eventing.ignore-unknown-event-type=false",
                    "lsf.eventing.idempotency.enabled=true",
                    "lsf.eventing.idempotency.store=memory",
                    "lsf.kafka.consumer.group-id=payment-group"
            );

    @Test
    void shouldDispatchEnvelopeAndSkipDuplicateEventId() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(LsfEnvelopeListener.class);

            ObjectMapper mapper = context.getBean(ObjectMapper.class);
            LsfEnvelopeListener listener = context.getBean(LsfEnvelopeListener.class);
            PaymentService paymentService = context.getBean(PaymentService.class);

            OrderValidatedEvent payload = new OrderValidatedEvent(
                    "ORDER-1",
                    List.of(new OrderLineItemRequest("SKU-1", 1)),
                    PaymentMethod.MOCK_SUCCESS
            );
            EventEnvelope envelope = EventEnvelope.builder()
                    .eventId("evt-validated-1")
                    .eventType("order.validated.v1")
                    .aggregateId("ORDER-1")
                    .payload(mapper.valueToTree(payload))
                    .build();

            listener.onMessage(envelope);
            listener.onMessage(envelope);

            verify(paymentService, times(1)).processValidatedOrder(
                    argThat(event ->
                            event != null
                                    && "ORDER-1".equals(event.getOrderNumber())
                                    && event.getItems() != null
                                    && event.getItems().size() == 1
                                    && event.getPaymentMethod() == PaymentMethod.MOCK_SUCCESS
                    ),
                    eq("lsf-envelope"),
                    eq("evt-validated-1")
            );
        });
    }

    @Configuration(proxyBeanMethods = false)
    @Import(PaymentOrderValidatedEventHandler.class)
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
        PaymentService paymentService() {
            return mock(PaymentService.class);
        }
    }
}
