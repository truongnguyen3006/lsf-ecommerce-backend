package com.myexampleproject.cartservice.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

@Configuration
public class CartAsyncConfig {

    @Bean(name = "cartCheckoutExecutor")
    public AsyncTaskExecutor cartCheckoutExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(32);
        executor.setMaxPoolSize(128);
        executor.setQueueCapacity(5000);
        executor.setThreadNamePrefix("cart-checkout-");
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        executor.initialize();
        return executor;
    }
}
