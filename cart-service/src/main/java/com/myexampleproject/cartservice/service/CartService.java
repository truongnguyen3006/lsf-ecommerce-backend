package com.myexampleproject.cartservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myexampleproject.cartservice.model.CartEntity;
import com.myexampleproject.cartservice.model.CartItemEntity;
import com.myexampleproject.cartservice.repository.CartRepository;
import com.myexampleproject.common.dto.CartItemRequest;
import com.myexampleproject.common.dto.PaymentMethod;
import com.myexampleproject.common.event.CartCheckoutEvent;
import com.myexampleproject.common.event.CartLineItem;
import com.myexampleproject.common.event.ProductCacheEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
@Slf4j
public class CartService {

    private static final String REDIS_QTY_HASH_PREFIX = "cart:qty:";
    private static final String REDIS_DATA_HASH_PREFIX = "cart:data:";
    private static final Duration REDIS_TTL = Duration.ofHours(24);
    private static final String PRODUCT_CACHE_KEY = "products:cache";

    private final CartRepository cartRepository;
    private final RedisTemplate<String, Object> redisTemplate;
    private final CartCheckoutEventPublisher cartCheckoutEventPublisher;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "product-cache-update-topic",
            groupId = "cart-product-cacher",
            containerFactory = "cartKafkaListenerContainerFactory"
    )
    public void handleProductCacheUpdate(List<ConsumerRecord<String, Object>> records) {
        log.info("Receiving {} product cache updates for CartService...", records.size());

        for (ConsumerRecord<String, Object> record : records) {
            try {
                ProductCacheEvent event = objectMapper.convertValue(record.value(), ProductCacheEvent.class);
                redisTemplate.opsForHash().put(PRODUCT_CACHE_KEY, event.getSkuCode(), event);
                log.debug("CartService cached product info for SKU: {}", event.getSkuCode());
            } catch (Exception exception) {
                log.error("CART-CACHE: Failed to cache product {}: {}", record.key(), exception.getMessage());
            }
        }
    }

    public void addItem(String userId, CartItemRequest line) {
        ProductCacheEvent productInfo = getProductFromCache(line.getSkuCode());
        if (productInfo == null) {
            throw new RuntimeException("Product not found: " + line.getSkuCode());
        }

        String qtyKey = REDIS_QTY_HASH_PREFIX + userId;
        String dataKey = REDIS_DATA_HASH_PREFIX + userId;
        String sku = line.getSkuCode();
        Long quantity = (long) line.getQuantity();

        redisTemplate.opsForHash().increment(qtyKey, sku, quantity);
        redisTemplate.opsForHash().put(dataKey, sku, productInfo);
        redisTemplate.expire(qtyKey, REDIS_TTL);
        redisTemplate.expire(dataKey, REDIS_TTL);
    }

    public void removeItem(String userId, String sku) {
        String qtyKey = REDIS_QTY_HASH_PREFIX + userId;
        String dataKey = REDIS_DATA_HASH_PREFIX + userId;

        redisTemplate.opsForHash().delete(qtyKey, sku);
        redisTemplate.opsForHash().delete(dataKey, sku);
    }

    public CartEntity viewCart(String userId) {
        String qtyKey = REDIS_QTY_HASH_PREFIX + userId;
        String dataKey = REDIS_DATA_HASH_PREFIX + userId;

        Map<Object, Object> qtyMap = redisTemplate.opsForHash().entries(qtyKey);
        Map<Object, Object> dataMap = redisTemplate.opsForHash().entries(dataKey);

        if (qtyMap.isEmpty()) {
            return CartEntity.builder().userId(userId).items(new ArrayList<>()).build();
        }

        List<CartItemEntity> items = new ArrayList<>();
        for (Map.Entry<Object, Object> entry : qtyMap.entrySet()) {
            String sku = (String) entry.getKey();
            Integer quantity = ((Number) entry.getValue()).intValue();

            Object data = dataMap.get(sku);
            ProductCacheEvent productInfo = data == null
                    ? null
                    : objectMapper.convertValue(data, ProductCacheEvent.class);

            CartItemEntity item = CartItemEntity.builder()
                    .skuCode(sku)
                    .quantity(quantity)
                    .productName(productInfo != null ? productInfo.getName() : "San pham khong ro")
                    .price(productInfo != null ? productInfo.getPrice() : BigDecimal.ZERO)
                    .imageUrl(productInfo != null ? productInfo.getImageUrl() : null)
                    .build();
            items.add(item);
        }

        return CartEntity.builder().userId(userId).items(items).build();
    }

    public void checkout(String userId) {
        checkout(userId, PaymentMethod.defaultMethod());
    }

    public void checkout(String userId, PaymentMethod paymentMethod) {
        cartCheckoutEventPublisher.publish(buildCheckoutEvent(userId, paymentMethod));
    }

    @Transactional
    public void cleanupAfterCheckout(CartCheckoutEvent event) {
        String userId = event.getUserId();
        log.info("CLEANUP: Cleaning cart for user {}", userId);

        cartRepository.deleteById(userId);
        String qtyKey = REDIS_QTY_HASH_PREFIX + userId;
        String dataKey = REDIS_DATA_HASH_PREFIX + userId;
        redisTemplate.delete(List.of(qtyKey, dataKey));

        log.info("CLEANUP: Removed DB record and Redis hashes for user {}", userId);
    }

    private ProductCacheEvent getProductFromCache(String skuCode) {
        try {
            Object cachedData = redisTemplate.opsForHash().get(PRODUCT_CACHE_KEY, skuCode);
            if (cachedData == null) {
                return null;
            }
            return objectMapper.convertValue(cachedData, ProductCacheEvent.class);
        } catch (Exception exception) {
            log.error("Failed to read product cache: {}", exception.getMessage());
            return null;
        }
    }

    private CartCheckoutEvent buildCheckoutEvent(String userId, PaymentMethod paymentMethod) {
        CartEntity cart = viewCart(userId);
        if (cart == null || cart.getItems().isEmpty()) {
            throw new IllegalStateException("Cart empty");
        }

        List<CartLineItem> items = cart.getItems().stream()
                .map(item -> new CartLineItem(item.getSkuCode(), item.getQuantity(), item.getPrice()))
                .toList();

        return new CartCheckoutEvent(
                userId,
                items,
                paymentMethod == null ? PaymentMethod.defaultMethod() : paymentMethod
        );
    }
}
