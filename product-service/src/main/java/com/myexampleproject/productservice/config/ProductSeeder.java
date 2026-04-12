package com.myexampleproject.productservice.config;

import com.myexampleproject.common.event.ProductCacheEvent;
import com.myexampleproject.common.event.ProductCreatedEvent;
import com.myexampleproject.productservice.model.Product;
import com.myexampleproject.productservice.model.ProductVariant;
import com.myexampleproject.productservice.repository.ProductRepository;
import com.myexampleproject.productservice.service.ProductEventPublisher;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
public class ProductSeeder implements CommandLineRunner {

    private static final String SEED_KEY = "system:data:seeded";

    private final ProductRepository productRepository;
    private final ProductEventPublisher productEventPublisher;
    private final RedisTemplate<String, Object> redisTemplate;

    @Override
    @Transactional
    public void run(String... args) {
        if (Boolean.TRUE.equals(redisTemplate.hasKey(SEED_KEY))) {
            log.info("Du lieu da duoc dong bo truoc do. Bo qua buoc seeding.");
            return;
        }

        long count = productRepository.count();
        if (count == 0) {
            log.info("Database MySQL trong. Vui long kiem tra file init.sql.");
            return;
        }

        log.info("Phat hien khoi dong lan dau. Bat dau dong bo {} san pham...", count);
        syncData();
    }

    private void syncData() {
        List<Product> products = productRepository.findAll();
        int variantCount = 0;

        for (Product product : products) {
            if (product.getVariants() == null) {
                continue;
            }

            for (ProductVariant variant : product.getVariants()) {
                String sku = variant.getSkuCode();

                ProductCreatedEvent inventoryEvent = ProductCreatedEvent.builder()
                        .skuCode(sku)
                        .initialQuantity(10000)
                        .build();
                productEventPublisher.publishProductCreated(inventoryEvent);

                ProductCacheEvent cacheEvent = ProductCacheEvent.builder()
                        .skuCode(sku)
                        .name(product.getName())
                        .price(variant.getPrice())
                        .imageUrl(variant.getImageUrl())
                        .color(variant.getColor())
                        .size(variant.getSize())
                        .build();
                productEventPublisher.publishProductCacheUpdate(cacheEvent);

                variantCount++;
            }
        }

        redisTemplate.opsForValue().set(SEED_KEY, "true", Duration.ofHours(24));
        log.info("Hoan tat seeding. Da ban event cho {} SKU.", variantCount);
    }
}
