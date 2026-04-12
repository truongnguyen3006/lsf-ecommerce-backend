package com.myexampleproject.cartservice.service;

import com.myexampleproject.common.event.CartCheckoutEvent;
import com.myexampleproject.common.event.CartLineItem;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class KafkaCartCheckoutEventPublisherTest {

    @Mock
    private KafkaTemplate<String, CartCheckoutEvent> kafkaTemplate;

    @Test
    void shouldPublishCheckoutEventToLegacyTopic() {
        KafkaCartCheckoutEventPublisher publisher = new KafkaCartCheckoutEventPublisher(kafkaTemplate);
        CartCheckoutEvent event = new CartCheckoutEvent(
                "user-1",
                List.of(new CartLineItem("SKU-1", 2, BigDecimal.TEN))
        );

        when(kafkaTemplate.send(eq(CartEventingConstants.CHECKOUT_TOPIC), anyString(), eq(event)))
                .thenReturn(CompletableFuture.completedFuture(null));

        publisher.publish(event);

        verify(kafkaTemplate).send(CartEventingConstants.CHECKOUT_TOPIC, "user-1", event);
    }
}
