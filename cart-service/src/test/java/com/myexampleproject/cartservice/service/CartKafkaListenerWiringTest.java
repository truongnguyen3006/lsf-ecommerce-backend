package com.myexampleproject.cartservice.service;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.annotation.KafkaListener;

import java.lang.reflect.Method;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CartKafkaListenerWiringTest {

    @Test
    void productCacheListenerUsesDedicatedContainerFactory() throws NoSuchMethodException {
        Method method = CartService.class.getMethod("handleProductCacheUpdate", List.class);

        KafkaListener annotation = method.getAnnotation(KafkaListener.class);

        assertThat(annotation).isNotNull();
        assertThat(annotation.containerFactory()).isEqualTo("cartKafkaListenerContainerFactory");
    }
}
