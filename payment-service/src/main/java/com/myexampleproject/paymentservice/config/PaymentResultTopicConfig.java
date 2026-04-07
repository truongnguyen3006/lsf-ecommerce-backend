package com.myexampleproject.paymentservice.config;

import com.myexampleproject.paymentservice.service.PaymentService;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class PaymentResultTopicConfig {

    @Bean
    public NewTopic paymentProcessedEnvelopeTopic() {
        return TopicBuilder.name(PaymentService.PAYMENT_PROCESSED_ENVELOPE_TOPIC)
                .partitions(10)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic paymentFailedEnvelopeTopic() {
        return TopicBuilder.name(PaymentService.PAYMENT_FAILED_ENVELOPE_TOPIC)
                .partitions(10)
                .replicas(1)
                .build();
    }
}
