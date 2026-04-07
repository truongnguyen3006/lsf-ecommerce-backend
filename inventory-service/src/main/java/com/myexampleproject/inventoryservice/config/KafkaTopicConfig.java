package com.myexampleproject.inventoryservice.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    private final int NUM_PARTITIONS = 10;
    private final short REPLICAS = 1;

    @Bean
    public NewTopic inventoryCheckRequestTopic() {
        return TopicBuilder.name("inventory-check-request-topic")
                .partitions(NUM_PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

    @Bean
    public NewTopic inventoryCheckResultTopic() {
        return TopicBuilder.name("inventory-check-result-topic")
                .partitions(NUM_PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

    @Bean
    public NewTopic inventoryStateChangelogTopic() {
        return TopicBuilder.name("inventory-state-changelog-topic")
                .partitions(NUM_PARTITIONS)
                .replicas(REPLICAS)
                .compact()
                .build();
    }

    @Bean
    public NewTopic productCreatedTopic() {
        return TopicBuilder.name("product-created-topic")
                .partitions(NUM_PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    public NewTopic orderProcessingTopic() {
        return TopicBuilder.name("order-processing-topic")
                .partitions(NUM_PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    public NewTopic orderValidatedTopic() {
        return TopicBuilder.name("order-validated-topic")
                .partitions(NUM_PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    public NewTopic orderFailedTopic() {
        return TopicBuilder.name("order-failed-topic")
                .partitions(NUM_PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    public NewTopic orderPlacedTopic() {
        return TopicBuilder.name("order-placed-topic")
                .partitions(NUM_PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    public NewTopic paymentProcessedTopic() {
        // Deprecated in Phase 2.9a. Kept for topic-lifecycle sign-off and potential off-repo consumers.
        return TopicBuilder.name("payment-processed-topic")
                .partitions(NUM_PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    public NewTopic paymentFailedTopic() {
        // Deprecated in Phase 2.9a. Kept for topic-lifecycle sign-off and potential off-repo consumers.
        return TopicBuilder.name("payment-failed-topic")
                .partitions(NUM_PARTITIONS).replicas(REPLICAS).build();
    }

    @Bean
    public NewTopic inventoryAdjustmentTopic() {
        return TopicBuilder.name("inventory-adjustment-topic")
                .partitions(NUM_PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

    @Bean
    public NewTopic cartCheckoutTopic() {
        return TopicBuilder.name("cart-checkout-topic")
                .partitions(NUM_PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

    @Bean
    public NewTopic inventoryEventsTopic() {
        return TopicBuilder.name("inventory-events")
                .partitions(1)
                .replicas((short)1)
                .build();
    }

    // Topics introduced for the LSF-based reservation lifecycle.
    //thêm 2 bean mới
//    @Bean
//    public NewTopic inventoryReservationConfirmTopic() {
//        return TopicBuilder.name("inventory-reservation-confirm-topic")
//                .partitions(NUM_PARTITIONS)
//                .replicas(REPLICAS)
//                .build();
//    }
//
//    @Bean
//    public NewTopic inventoryReservationReleaseTopic() {
//        return TopicBuilder.name("inventory-reservation-release-topic")
//                .partitions(NUM_PARTITIONS)
//                .replicas(REPLICAS)
//                .build();
//    }

    @Bean
    public NewTopic inventoryReservationConfirmTopic() {
        return TopicBuilder.name("inventory-reservation-confirm-envelope-topic")
                .partitions(NUM_PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

    @Bean
    public NewTopic inventoryReservationReleaseTopic() {
        return TopicBuilder.name("inventory-reservation-release-envelope-topic")
                .partitions(NUM_PARTITIONS)
                .replicas(REPLICAS)
                .build();
    }

}
