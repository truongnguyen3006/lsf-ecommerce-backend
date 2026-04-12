package com.myexampleproject.productservice.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.time.Clock;
import java.time.Duration;

@Configuration
@EnableScheduling
@ConditionalOnProperty(prefix = "lsf.outbox", name = "enabled", havingValue = "true")
public class ProductOutboxSchedulingConfiguration {

    @Bean
    public Clock productOutboxClock() {
        return Clock.systemUTC();
    }

    @Bean
    public ProductOutboxSchedule productOutboxSchedule(
            @Value("${lsf.outbox.publisher.initial-delay:1s}") Duration initialDelay,
            @Value("${lsf.outbox.publisher.poll-interval:1s}") Duration pollInterval
    ) {
        return new ProductOutboxSchedule(initialDelay.toMillis(), pollInterval.toMillis());
    }
}
