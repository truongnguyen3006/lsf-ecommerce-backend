package com.myexampleproject.productservice.controller;

import com.myexampleproject.common.event.ProductCacheEvent;
import com.myexampleproject.common.event.ProductCreatedEvent;
import com.myexampleproject.productservice.dto.ProductRequest;
import com.myexampleproject.productservice.dto.ProductResponse;
import com.myexampleproject.productservice.model.Product;
import com.myexampleproject.productservice.model.ProductVariant;
import com.myexampleproject.productservice.repository.ProductRepository;
import com.myexampleproject.productservice.service.ProductEventPublisher;
import com.myexampleproject.productservice.service.ProductService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/product")
@RequiredArgsConstructor
@Slf4j
public class ProductController {

    private final ProductService productService;
    private final ProductRepository productRepository;
    private final ProductEventPublisher productEventPublisher;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public ProductResponse createProduct(@RequestBody ProductRequest productRequest) {
        return productService.createProduct(productRequest);
    }

    @PutMapping("/{id}")
    @ResponseStatus(HttpStatus.OK)
    public ProductResponse updateProduct(@PathVariable Long id, @RequestBody ProductRequest productRequest) {
        return productService.updateProduct(id, productRequest);
    }

    @GetMapping
    @ResponseStatus(HttpStatus.OK)
    public List<ProductResponse> getAllProducts() {
        return productService.getAllProducts();
    }

    @GetMapping("/{id}")
    public ProductResponse getProductById(@PathVariable Long id) {
        return productService.getProductById(id);
    }

    @DeleteMapping("/{id}")
    public void deleteProductById(@PathVariable Long id) {
        productService.deleteProductById(id);
    }

    @GetMapping("/admin/warm-cache")
    public String warmProductCache() {
        log.info("Starting FULL cache warm-up (Parent + Variants)...");

        productService.clearProductListCache();

        List<Product> allProducts = productRepository.findAll();
        int variantCount = 0;

        for (Product product : allProducts) {
            if (product.getVariants() == null || product.getVariants().isEmpty()) {
                continue;
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
                        .initialQuantity(1000)
                        .build();
                productEventPublisher.publishProductCreated(inventoryEvent);

                variantCount++;
            }
        }

        return "Warm-up hoan tat. Da gui event cho " + variantCount + " bien the (SKU).";
    }
}
