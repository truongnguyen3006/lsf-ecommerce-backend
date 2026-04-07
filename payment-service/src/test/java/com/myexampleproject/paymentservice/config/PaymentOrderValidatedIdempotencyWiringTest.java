package com.myexampleproject.paymentservice.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myexampleproject.paymentservice.consumer.PaymentOrderValidatedEventHandler;
import com.myexampleproject.paymentservice.service.PaymentService;
import com.myorg.lsf.eventing.autoconfig.LsfEventingAutoConfiguration;
import com.myorg.lsf.eventing.autoconfig.LsfEventingRedisAutoConfiguration;
import com.myorg.lsf.eventing.idempotency.IdempotencyStore;
import com.myorg.lsf.eventing.idempotency.RedisIdempotencyStore;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class PaymentOrderValidatedIdempotencyWiringTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    LsfEventingRedisAutoConfiguration.class,
                    LsfEventingAutoConfiguration.class
            ))
            .withUserConfiguration(IdempotencyWiringTestConfig.class)
            .withPropertyValues(
                    "lsf.eventing.consume-topics[0]=order-validated-envelope-topic",
                    "lsf.eventing.idempotency.enabled=true",
                    "lsf.eventing.idempotency.store=redis",
                    "lsf.eventing.idempotency.require-redis=true",
                    "lsf.kafka.consumer.group-id=payment-group",
                    "spring.data.redis.host=localhost",
                    "spring.data.redis.port=6379"
            );

    @Test
    void shouldWireRedisIdempotencyStoreWhenRedisModeIsEnabled() {
        runner.run(context -> {
            assertThat(context).hasSingleBean(IdempotencyStore.class);
            assertThat(context.getBean(IdempotencyStore.class)).isInstanceOf(RedisIdempotencyStore.class);
        });
    }

    @Configuration(proxyBeanMethods = false)
    @Import(PaymentOrderValidatedEventHandler.class)
    static class IdempotencyWiringTestConfig {

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
