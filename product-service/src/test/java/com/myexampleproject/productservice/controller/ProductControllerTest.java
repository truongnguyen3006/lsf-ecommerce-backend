package com.myexampleproject.productservice.controller;

import com.myexampleproject.common.event.ProductCacheEvent;
import com.myexampleproject.common.event.ProductCreatedEvent;
import com.myexampleproject.productservice.model.Product;
import com.myexampleproject.productservice.model.ProductVariant;
import com.myexampleproject.productservice.repository.ProductRepository;
import com.myexampleproject.productservice.service.ProductEventPublisher;
import com.myexampleproject.productservice.service.ProductService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProductControllerTest {

    @Mock
    private ProductService productService;

    @Mock
    private ProductRepository productRepository;

    @Mock
    private ProductEventPublisher productEventPublisher;

    private ProductController productController;

    @BeforeEach
    void setUp() {
        productController = new ProductController(productService, productRepository, productEventPublisher);
    }

    @Test
    void warmProductCacheShouldPublishEventsForEachVariant() {
        Product product = Product.builder()
                .name("Air Max")
                .basePrice(new BigDecimal("120.00"))
                .imageUrl("parent.jpg")
                .variants(List.of(
                        ProductVariant.builder()
                                .skuCode("AIR-MAX-RED-42")
                                .color("Red")
                                .size("42")
                                .price(new BigDecimal("125.00"))
                                .imageUrl("variant-red.jpg")
                                .build()
                ))
                .build();

        when(productRepository.findAll()).thenReturn(List.of(product));

        String result = productController.warmProductCache();

        ArgumentCaptor<ProductCacheEvent> cacheCaptor = ArgumentCaptor.forClass(ProductCacheEvent.class);
        ArgumentCaptor<ProductCreatedEvent> createdCaptor = ArgumentCaptor.forClass(ProductCreatedEvent.class);

        verify(productService).clearProductListCache();
        verify(productEventPublisher).publishProductCacheUpdate(cacheCaptor.capture());
        verify(productEventPublisher).publishProductCreated(createdCaptor.capture());

        ProductCacheEvent cacheEvent = cacheCaptor.getValue();
        ProductCreatedEvent createdEvent = createdCaptor.getValue();

        assertTrue(result.contains("1"));
        assertEquals("AIR-MAX-RED-42", cacheEvent.getSkuCode());
        assertEquals("AIR-MAX-RED-42", createdEvent.getSkuCode());
        assertEquals(1000, createdEvent.getInitialQuantity());
    }

    private static void assertEquals(Object expected, Object actual) {
        org.junit.jupiter.api.Assertions.assertEquals(expected, actual);
    }
}
