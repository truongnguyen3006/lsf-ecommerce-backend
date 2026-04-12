package com.myexampleproject.productservice.config;

import com.myexampleproject.common.event.ProductCacheEvent;
import com.myexampleproject.common.event.ProductCreatedEvent;
import com.myexampleproject.productservice.model.Product;
import com.myexampleproject.productservice.model.ProductVariant;
import com.myexampleproject.productservice.repository.ProductRepository;
import com.myexampleproject.productservice.service.ProductEventPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ValueOperations;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProductSeederTest {

    @Mock
    private ProductRepository productRepository;

    @Mock
    private ProductEventPublisher productEventPublisher;

    @Mock
    private RedisTemplate<String, Object> redisTemplate;

    @Mock
    private ValueOperations<String, Object> valueOperations;

    private ProductSeeder productSeeder;

    @BeforeEach
    void setUp() {
        when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        productSeeder = new ProductSeeder(productRepository, productEventPublisher, redisTemplate);
    }

    @Test
    void runShouldPublishSeedEventsThroughPublisherAndMarkSeedFlag() throws Exception {
        Product product = Product.builder()
                .name("Air Max")
                .variants(List.of(
                        ProductVariant.builder()
                                .skuCode("AIR-MAX-RED-42")
                                .price(new BigDecimal("125.00"))
                                .imageUrl("variant-red.jpg")
                                .color("Red")
                                .size("42")
                                .build()
                ))
                .build();

        when(redisTemplate.hasKey("system:data:seeded")).thenReturn(false);
        when(productRepository.count()).thenReturn(1L);
        when(productRepository.findAll()).thenReturn(List.of(product));

        productSeeder.run();

        ArgumentCaptor<ProductCreatedEvent> createdCaptor = ArgumentCaptor.forClass(ProductCreatedEvent.class);
        ArgumentCaptor<ProductCacheEvent> cacheCaptor = ArgumentCaptor.forClass(ProductCacheEvent.class);

        verify(productEventPublisher).publishProductCreated(createdCaptor.capture());
        verify(productEventPublisher).publishProductCacheUpdate(cacheCaptor.capture());
        verify(valueOperations).set("system:data:seeded", "true", Duration.ofHours(24));

        assertEquals("AIR-MAX-RED-42", createdCaptor.getValue().getSkuCode());
        assertEquals(10000, createdCaptor.getValue().getInitialQuantity());
        assertEquals("AIR-MAX-RED-42", cacheCaptor.getValue().getSkuCode());
        assertEquals("Air Max", cacheCaptor.getValue().getName());
    }
}
