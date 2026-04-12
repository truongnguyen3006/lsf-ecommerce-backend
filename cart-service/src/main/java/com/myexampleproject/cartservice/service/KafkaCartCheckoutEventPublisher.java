package com.myexampleproject.cartservice.service;

import com.myexampleproject.common.event.CartCheckoutEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class KafkaCartCheckoutEventPublisher implements CartCheckoutEventPublisher {

    private final KafkaTemplate<String, CartCheckoutEvent> kafkaTemplate;

    @Override
    public void publish(CartCheckoutEvent event) {
        String userId = event.getUserId();
        kafkaTemplate.send(CartEventingConstants.CHECKOUT_TOPIC, userId, event)
                .whenComplete((metadata, exception) -> {
                    if (exception != null) {
                        log.error("Failed to send checkout event for user {}: {}", userId, exception.getMessage());
                        return;
                    }

                    log.info(
                            "Checkout event sent for user {} partition={}",
                            userId,
                            metadata.getRecordMetadata().partition()
                    );
                });
    }
}
