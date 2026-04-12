package com.myexampleproject.productservice.service;

import com.myexampleproject.common.event.ProductCacheEvent;
import com.myexampleproject.common.event.ProductCreatedEvent;
import com.myexampleproject.productservice.dto.ProductRequest;
import com.myexampleproject.productservice.dto.ProductResponse;
import com.myexampleproject.productservice.dto.ProductVariantRequest;
import com.myexampleproject.productservice.model.Product;
import com.myexampleproject.productservice.model.ProductImage;
import com.myexampleproject.productservice.model.ProductVariant;
import com.myexampleproject.productservice.repository.ProductRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class ProductServiceTest {

    @Mock
    private ProductRepository productRepository;

    @Mock
    private ProductEventPublisher productEventPublisher;

    private ProductService productService;

    @BeforeEach
    void setUp() {
        productService = new ProductService(productRepository, productEventPublisher);
    }

    @Test
    void createProductShouldSaveAndPublishEventsThroughPublisher() {
        ProductRequest request = ProductRequest.builder()
                .name("Air Max")
                .description("Running shoe")
                .category("Shoes")
                .basePrice(new BigDecimal("120.00"))
                .imageUrl("parent.jpg")
                .variants(List.of(
                        ProductVariantRequest.builder()
                                .skuCode("AIR-MAX-RED-42")
                                .color("Red")
                                .size("42")
                                .price(new BigDecimal("125.00"))
                                .initialQuantity(15)
                                .imageUrl("variant-red.jpg")
                                .isActive(true)
                                .galleryImages(List.of("g1.jpg"))
                                .build()
                ))
                .build();

        when(productRepository.save(any(Product.class))).thenAnswer(invocation -> {
            Product product = invocation.getArgument(0);
            product.setId(1L);
            return product;
        });

        ProductResponse response = productService.createProduct(request);

        assertEquals("Air Max", response.getName());
        assertEquals(1, response.getVariants().size());

        ArgumentCaptor<ProductCreatedEvent> createdCaptor = ArgumentCaptor.forClass(ProductCreatedEvent.class);
        ArgumentCaptor<ProductCacheEvent> cacheCaptor = ArgumentCaptor.forClass(ProductCacheEvent.class);

        verify(productRepository).save(any(Product.class));
        verify(productEventPublisher).publishProductCreated(createdCaptor.capture());
        verify(productEventPublisher).publishProductCacheUpdate(cacheCaptor.capture());

        ProductCreatedEvent createdEvent = createdCaptor.getValue();
        assertEquals("AIR-MAX-RED-42", createdEvent.getSkuCode());
        assertEquals(15, createdEvent.getInitialQuantity());

        ProductCacheEvent cacheEvent = cacheCaptor.getValue();
        assertEquals("AIR-MAX-RED-42", cacheEvent.getSkuCode());
        assertEquals("Air Max", cacheEvent.getName());
        assertEquals(new BigDecimal("125.00"), cacheEvent.getPrice());
        assertEquals("variant-red.jpg", cacheEvent.getImageUrl());
        assertEquals("Red", cacheEvent.getColor());
        assertEquals("42", cacheEvent.getSize());
    }

    @Test
    void updateProductShouldSaveAndRepublishAllVariantEvents() {
        Product existingProduct = existingProduct();
        ProductRequest request = ProductRequest.builder()
                .name("Air Max 2026")
                .description("Updated")
                .category("Shoes")
                .basePrice(new BigDecimal("130.00"))
                .imageUrl("parent-updated.jpg")
                .variants(List.of(
                        ProductVariantRequest.builder()
                                .skuCode("AIR-MAX-RED-42")
                                .color("Blue")
                                .size("43")
                                .price(new BigDecimal("135.00"))
                                .imageUrl(null)
                                .isActive(true)
                                .galleryImages(List.of("updated-red.jpg"))
                                .build(),
                        ProductVariantRequest.builder()
                                .skuCode("AIR-MAX-BLK-44")
                                .color("Black")
                                .size("44")
                                .price(null)
                                .imageUrl("black.jpg")
                                .isActive(false)
                                .galleryImages(List.of("black-1.jpg"))
                                .build()
                ))
                .build();

        when(productRepository.findById(1L)).thenReturn(Optional.of(existingProduct));
        when(productRepository.save(any(Product.class))).thenAnswer(invocation -> invocation.getArgument(0));

        ProductResponse response = productService.updateProduct(1L, request);

        assertEquals("Air Max 2026", response.getName());
        assertEquals(2, response.getVariants().size());

        ArgumentCaptor<ProductCreatedEvent> createdCaptor = ArgumentCaptor.forClass(ProductCreatedEvent.class);
        ArgumentCaptor<ProductCacheEvent> cacheCaptor = ArgumentCaptor.forClass(ProductCacheEvent.class);

        verify(productRepository).save(existingProduct);
        verify(productEventPublisher, times(2)).publishProductCreated(createdCaptor.capture());
        verify(productEventPublisher, times(2)).publishProductCacheUpdate(cacheCaptor.capture());

        List<ProductCreatedEvent> createdEvents = createdCaptor.getAllValues();
        assertEquals(List.of("AIR-MAX-RED-42", "AIR-MAX-BLK-44"),
                createdEvents.stream().map(ProductCreatedEvent::getSkuCode).toList());
        assertEquals(List.of(0, 0),
                createdEvents.stream().map(ProductCreatedEvent::getInitialQuantity).toList());

        List<ProductCacheEvent> cacheEvents = cacheCaptor.getAllValues();
        assertEquals(List.of("AIR-MAX-RED-42", "AIR-MAX-BLK-44"),
                cacheEvents.stream().map(ProductCacheEvent::getSkuCode).toList());
        assertEquals(new BigDecimal("135.00"), cacheEvents.get(0).getPrice());
        assertEquals("parent-updated.jpg", cacheEvents.get(0).getImageUrl());
        assertEquals(new BigDecimal("130.00"), cacheEvents.get(1).getPrice());
        assertEquals("black.jpg", cacheEvents.get(1).getImageUrl());
    }

    private Product existingProduct() {
        Product product = Product.builder()
                .id(1L)
                .name("Air Max")
                .description("Original")
                .category("Shoes")
                .basePrice(new BigDecimal("120.00"))
                .imageUrl("parent.jpg")
                .variants(new ArrayList<>())
                .build();

        ProductVariant variant = ProductVariant.builder()
                .skuCode("AIR-MAX-RED-42")
                .color("Red")
                .size("42")
                .price(new BigDecimal("125.00"))
                .imageUrl("variant-red.jpg")
                .isActive(true)
                .product(product)
                .images(new ArrayList<>(List.of(ProductImage.builder().imageUrl("old.jpg").build())))
                .build();
        for (ProductImage image : variant.getImages()) {
            image.setVariant(variant);
        }
        product.getVariants().add(variant);
        assertNotNull(product.getVariants());
        return product;
    }
}
