package com.myexampleproject.productservice.service;

import com.myexampleproject.common.event.ProductCacheEvent;
import com.myexampleproject.common.event.ProductCreatedEvent;
import com.myexampleproject.productservice.dto.ProductRequest;
import com.myexampleproject.productservice.dto.ProductResponse;
import com.myexampleproject.productservice.dto.ProductVariantRequest;
import com.myexampleproject.productservice.dto.ProductVariantResponse;
import com.myexampleproject.productservice.model.Product;
import com.myexampleproject.productservice.model.ProductImage;
import com.myexampleproject.productservice.model.ProductVariant;
import com.myexampleproject.productservice.repository.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.cache.annotation.Caching;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProductService {

    private final ProductRepository productRepository;
    private final ProductEventPublisher productEventPublisher;

    @CacheEvict(cacheNames = "products_json_v5", allEntries = true)
    public void clearProductListCache() {
        log.info("Da xoa cache danh sach san pham (products_json_v5)");
    }

    @Transactional
    @CacheEvict(cacheNames = "products_json_v5", allEntries = true)
    public ProductResponse createProduct(ProductRequest request) {
        Product product = Product.builder()
                .name(request.getName())
                .description(request.getDescription())
                .basePrice(request.getBasePrice())
                .category(request.getCategory())
                .imageUrl(request.getImageUrl())
                .build();

        List<ProductVariant> variants = new ArrayList<>();
        if (request.getVariants() != null) {
            for (ProductVariantRequest variantRequest : request.getVariants()) {
                ProductVariant variant = ProductVariant.builder()
                        .skuCode(variantRequest.getSkuCode())
                        .color(variantRequest.getColor())
                        .size(variantRequest.getSize())
                        .price(variantRequest.getPrice() != null ? variantRequest.getPrice() : request.getBasePrice())
                        .imageUrl(variantRequest.getImageUrl() != null ? variantRequest.getImageUrl() : request.getImageUrl())
                        .isActive(variantRequest.getIsActive() != null ? variantRequest.getIsActive() : true)
                        .product(product)
                        .build();

                if (variantRequest.getGalleryImages() != null) {
                    List<ProductImage> imageEntities = variantRequest.getGalleryImages().stream()
                            .map(url -> ProductImage.builder().imageUrl(url).variant(variant).build())
                            .collect(Collectors.toList());
                    variant.setImages(imageEntities);
                }
                variants.add(variant);
            }
        }
        product.setVariants(variants);

        Product savedProduct = productRepository.save(product);
        publishCreateEvents(product, request);
        return mapToProductResponse(savedProduct);
    }

    @Transactional
    @CacheEvict(cacheNames = {"products_json_v5", "product_item_json_v5"}, allEntries = true)
    public ProductResponse updateProduct(Long id, ProductRequest request) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Product not found"));

        if (request.getName() != null) {
            product.setName(request.getName());
        }
        if (request.getDescription() != null) {
            product.setDescription(request.getDescription());
        }
        if (request.getBasePrice() != null) {
            product.setBasePrice(request.getBasePrice());
        }
        if (request.getCategory() != null) {
            product.setCategory(request.getCategory());
        }
        if (request.getImageUrl() != null) {
            product.setImageUrl(request.getImageUrl());
        }

        if (request.getVariants() == null) {
            Product savedProduct = productRepository.save(product);
            return mapToProductResponse(savedProduct);
        }

        List<ProductVariant> currentVariants = product.getVariants();
        if (currentVariants == null) {
            currentVariants = new ArrayList<>();
            product.setVariants(currentVariants);
        }

        List<ProductVariantRequest> incomingVariants = request.getVariants();
        Map<String, ProductVariantRequest> requestMap = incomingVariants.stream()
                .collect(Collectors.toMap(ProductVariantRequest::getSkuCode, variant -> variant));

        Iterator<ProductVariant> iterator = currentVariants.iterator();
        while (iterator.hasNext()) {
            ProductVariant existingVariant = iterator.next();
            String sku = existingVariant.getSkuCode();

            if (requestMap.containsKey(sku)) {
                ProductVariantRequest variantRequest = requestMap.get(sku);
                existingVariant.setColor(variantRequest.getColor());
                existingVariant.setSize(variantRequest.getSize());

                BigDecimal newPrice = variantRequest.getPrice();
                if (newPrice == null) {
                    newPrice = product.getBasePrice();
                }
                existingVariant.setPrice(newPrice);
                existingVariant.setImageUrl(
                        variantRequest.getImageUrl() != null ? variantRequest.getImageUrl() : product.getImageUrl()
                );
                existingVariant.setIsActive(variantRequest.getIsActive() != null ? variantRequest.getIsActive() : true);

                if (variantRequest.getGalleryImages() != null) {
                    if (existingVariant.getImages() != null) {
                        existingVariant.getImages().clear();
                    } else {
                        existingVariant.setImages(new ArrayList<>());
                    }

                    List<ProductImage> newImages = variantRequest.getGalleryImages().stream()
                            .map(url -> ProductImage.builder().imageUrl(url).variant(existingVariant).build())
                            .collect(Collectors.toList());
                    existingVariant.getImages().addAll(newImages);
                }
                requestMap.remove(sku);
            } else {
                iterator.remove();
            }
        }

        for (ProductVariantRequest newRequest : requestMap.values()) {
            BigDecimal variantPrice = newRequest.getPrice();
            if (variantPrice == null) {
                variantPrice = product.getBasePrice();
            }

            ProductVariant newVariant = ProductVariant.builder()
                    .skuCode(newRequest.getSkuCode())
                    .color(newRequest.getColor())
                    .size(newRequest.getSize())
                    .price(variantPrice)
                    .imageUrl(newRequest.getImageUrl() != null ? newRequest.getImageUrl() : product.getImageUrl())
                    .isActive(newRequest.getIsActive() != null ? newRequest.getIsActive() : true)
                    .product(product)
                    .build();

            if (newRequest.getGalleryImages() != null) {
                List<ProductImage> imageEntities = newRequest.getGalleryImages().stream()
                        .map(url -> ProductImage.builder().imageUrl(url).variant(newVariant).build())
                        .collect(Collectors.toList());
                newVariant.setImages(imageEntities);
            }
            currentVariants.add(newVariant);
        }

        Product savedProduct = productRepository.save(product);
        publishUpdateEvents(savedProduct);
        return mapToProductResponse(savedProduct);
    }

    private void publishCreateEvents(Product product, ProductRequest request) {
        if (request.getVariants() == null) {
            return;
        }

        for (ProductVariantRequest variantRequest : request.getVariants()) {
            ProductCreatedEvent inventoryEvent = ProductCreatedEvent.builder()
                    .skuCode(variantRequest.getSkuCode())
                    .initialQuantity(variantRequest.getInitialQuantity())
                    .build();
            productEventPublisher.publishProductCreated(inventoryEvent);

            ProductCacheEvent cacheEvent = ProductCacheEvent.builder()
                    .skuCode(variantRequest.getSkuCode())
                    .name(product.getName())
                    .price(variantRequest.getPrice() != null ? variantRequest.getPrice() : product.getBasePrice())
                    .imageUrl(variantRequest.getImageUrl())
                    .color(variantRequest.getColor())
                    .size(variantRequest.getSize())
                    .build();
            productEventPublisher.publishProductCacheUpdate(cacheEvent);
        }
    }

    private void publishUpdateEvents(Product product) {
        if (product.getVariants() == null) {
            return;
        }

        for (ProductVariant variant : product.getVariants()) {
            ProductCacheEvent cacheEvent = ProductCacheEvent.builder()
                    .skuCode(variant.getSkuCode())
                    .name(product.getName())
                    .price(variant.getPrice() != null ? variant.getPrice() : product.getBasePrice())
                    .imageUrl(variant.getImageUrl() != null ? variant.getImageUrl() : product.getImageUrl())
                    .color(variant.getColor())
                    .size(variant.getSize())
                    .build();
            productEventPublisher.publishProductCacheUpdate(cacheEvent);

            ProductCreatedEvent inventoryEvent = ProductCreatedEvent.builder()
                    .skuCode(variant.getSkuCode())
                    .initialQuantity(0)
                    .build();
            productEventPublisher.publishProductCreated(inventoryEvent);
        }
    }

    @Cacheable(cacheNames = "products_json_v5")
    public List<ProductResponse> getAllProducts() {
        List<Product> products = productRepository.findAll();
        return products.stream().map(this::mapToProductResponse).collect(Collectors.toList());
    }

    @Cacheable(cacheNames = "product_item_json_v5", key = "#id")
    public ProductResponse getProductById(Long id) {
        Product product = productRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Product not found"));
        return mapToProductResponse(product);
    }

    @Caching(evict = {
            @CacheEvict(cacheNames = "product_item_json_v5", key = "#id"),
            @CacheEvict(cacheNames = "products_json_v5", allEntries = true)
    })
    public void deleteProductById(Long id) {
        if (!productRepository.existsById(id)) {
            throw new RuntimeException("Product not found");
        }
        productRepository.deleteById(id);
    }

    private ProductResponse mapToProductResponse(Product product) {
        List<ProductVariantResponse> variantResponses = new ArrayList<>();
        if (product.getVariants() != null) {
            variantResponses = product.getVariants().stream()
                    .map(variant -> ProductVariantResponse.builder()
                            .skuCode(variant.getSkuCode())
                            .color(variant.getColor())
                            .size(variant.getSize())
                            .price(variant.getPrice())
                            .imageUrl(variant.getImageUrl())
                            .isActive(variant.getIsActive())
                            .galleryImages(variant.getImages().stream()
                                    .map(ProductImage::getImageUrl)
                                    .collect(Collectors.toList()))
                            .build())
                    .collect(Collectors.toList());
        }

        return ProductResponse.builder()
                .id(product.getId())
                .name(product.getName())
                .description(product.getDescription())
                .price(product.getBasePrice())
                .category(product.getCategory())
                .imageUrl(product.getImageUrl())
                .variants(variantResponses)
                .build();
    }
}
