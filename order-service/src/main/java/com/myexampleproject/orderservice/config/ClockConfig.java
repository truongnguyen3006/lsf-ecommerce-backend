package com.myexampleproject.orderservice.config;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.time.Clock;

@Configuration(proxyBeanMethods = false)
public class ClockConfig {

    @Bean
    @Primary
    public Clock orderServiceClock(@Qualifier("lsfSagaClock") Clock sagaClock) {
        return sagaClock;
    }
}
