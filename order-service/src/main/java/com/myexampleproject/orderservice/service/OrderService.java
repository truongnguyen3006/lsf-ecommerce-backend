package com.myexampleproject.orderservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myexampleproject.common.dto.OrderLineItemRequest;
import com.myexampleproject.common.dto.OrderLineItemsDto;
import com.myexampleproject.common.event.InventoryCheckRequest;
import com.myexampleproject.common.event.InventoryCheckResult;
import com.myexampleproject.common.event.OrderPlacedEvent;
import com.myexampleproject.common.event.OrderStatusEvent;
import com.myexampleproject.common.event.ProductCacheEvent;
import com.myexampleproject.orderservice.config.OrderWorkflowProperties;
import com.myexampleproject.orderservice.dto.OrderRequest;
import com.myexampleproject.orderservice.dto.OrderResponse;
import com.myexampleproject.orderservice.model.Order;
import com.myexampleproject.orderservice.model.OrderLineItems;
import com.myexampleproject.orderservice.repository.OrderRepository;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.outbox.OutboxWriter;
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
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private static final String SAGA_PREFIX = "saga:order:";
    private static final String PRODUCT_CACHE_KEY = "products:cache";

    private final OrderRepository orderRepository;
    private final ObjectMapper objectMapper;
    private final RedisTemplate<String, Object> redisTemplate;
    private final OutboxWriter outboxWriter;
    private final OrderOutboxEnvelopeFactory envelopeFactory;
    private final OrderWorkflowProperties workflowProperties;
    private final OrderSagaStateService orderSagaStateService;
    private final OrderWorkflowPublisher orderWorkflowPublisher;
    private final OrderSagaWorkflowService orderSagaWorkflowService;
    private final OrderSagaInventoryBridgeService orderSagaInventoryBridgeService;
    private final OrderSagaEvidenceMetrics evidenceMetrics;

    public String placeOrder(OrderRequest orderRequest, String userId) {
        String orderNumber = UUID.randomUUID().toString();
        log.info("Order {} received. Persisting pending order and starting workflow...", orderNumber);

        List<OrderLineItemRequest> items = orderRequest.getItems();
        OrderPlacedEvent placedEvent = new OrderPlacedEvent(orderNumber, userId, items);
        handleOrderPlacement(placedEvent);
        return orderNumber;
    }

    @KafkaListener(
            topics = "inventory-check-result-topic",
            groupId = "order-saga-group",
            containerFactory = "orderBatchKafkaListenerContainerFactory"
    )
    public void handleInventoryCheckResult(List<ConsumerRecord<String, Object>> records) {
        if (workflowProperties.isSagaMode()) {
            handleSagaInventoryCheckResults(records);
            return;
        }

        handleLegacyInventoryCheckResults(records);
    }

    private void handleLegacyInventoryCheckResults(List<ConsumerRecord<String, Object>> records) {
        log.info("LEGACY: Received batch of {} inventory results", records.size());

        for (ConsumerRecord<String, Object> record : records) {
            try {
                InventoryCheckResult result = objectMapper.convertValue(record.value(), InventoryCheckResult.class);
                String orderNumber = result.getOrderNumber();
                String sagaKey = SAGA_PREFIX + orderNumber;

                log.info(
                        "SAGA: Result for Order {}, SKU {}: Success={}",
                        orderNumber,
                        result.getItem().getSkuCode(),
                        result.isSuccess()
                );

                Long receivedCount = redisTemplate.opsForHash().increment(sagaKey, "receivedItems", 1);
                if (!result.isSuccess()) {
                    redisTemplate.opsForHash().put(sagaKey, "failed", true);
                    redisTemplate.opsForHash().put(sagaKey, "failureReason", result.getReason());
                    log.warn(
                            "SAGA: Marked order {} as failed because SKU {} was rejected. reason={}",
                            orderNumber,
                            result.getItem().getSkuCode(),
                            result.getReason()
                    );
                }

                Object totalObj = redisTemplate.opsForHash().get(sagaKey, "totalItems");
                if (totalObj == null) {
                    log.warn("SAGA: State missing for order {}. Waiting...", orderNumber);
                    continue;
                }

                int totalItems = parseIntegerSafely(totalObj);
                log.debug("SAGA: Order {} progress: {}/{}", orderNumber, receivedCount, totalItems);

                if (receivedCount == null || receivedCount < totalItems) {
                    continue;
                }

                boolean failed = parseBooleanSafely(redisTemplate.opsForHash().get(sagaKey, "failed"));
                if (failed) {
                    Object reasonObj = redisTemplate.opsForHash().get(sagaKey, "failureReason");
                    String reason = reasonObj != null ? String.valueOf(reasonObj) : "Inventory reservation failed";
                    boolean changed = orderSagaStateService.markFailedAndEnqueueRelease(
                            orderNumber,
                            "FAILED",
                            reason
                    );
                    if (!changed) {
                        log.warn(
                                "SAGA COMPLETE: Order {} already moved away from PENDING. reason={}",
                                orderNumber,
                                reason
                        );
                    }
                } else {
                    boolean changed = orderSagaStateService.markValidatedAndEnqueueStatus(orderNumber);
                    if (changed) {
                        log.info("SAGA COMPLETE: Order {} passed all inventory checks.", orderNumber);
                    }
                }

                redisTemplate.delete(sagaKey);
            } catch (Exception exception) {
                log.error("SAGA ERROR: Key: {}", record.key(), exception);
            }
        }
    }

    private void handleSagaInventoryCheckResults(List<ConsumerRecord<String, Object>> records) {
        log.info("LSF-SAGA: Received batch of {} inventory results", records.size());

        for (ConsumerRecord<String, Object> record : records) {
            try {
                InventoryCheckResult result = objectMapper.convertValue(record.value(), InventoryCheckResult.class);
                orderSagaInventoryBridgeService.handleInventoryCheckResult(
                        result,
                        workflowProperties.getSaga().getReplyTopic()
                );
            } catch (Exception exception) {
                log.error("LSF-SAGA inventory bridge error for key {}", record.key(), exception);
            }
        }
    }

    @KafkaListener(
            topics = "product-cache-update-topic",
            groupId = "order-product-cacher",
            containerFactory = "orderBatchKafkaListenerContainerFactory"
    )
    public void handleProductCacheUpdate(List<ConsumerRecord<String, Object>> records) {
        log.info("Receiving {} product cache updates...", records.size());

        for (ConsumerRecord<String, Object> record : records) {
            try {
                ProductCacheEvent event = objectMapper.convertValue(record.value(), ProductCacheEvent.class);
                redisTemplate.opsForHash().put(PRODUCT_CACHE_KEY, event.getSkuCode(), event);
                log.debug("Cached product info for SKU: {}", event.getSkuCode());
            } catch (Exception exception) {
                log.error("Failed to cache product {}: {}", record.key(), exception.getMessage());
            }
        }
    }

    @Transactional(readOnly = true)
    public OrderResponse getOrderDetails(String orderNumber) {
        log.info("Fetching order details for: {}", orderNumber);
        Order order = orderRepository.findByOrderNumber(orderNumber)
                .orElseThrow(() -> new RuntimeException("Order not found: " + orderNumber));
        return mapToOrderResponse(order);
    }

    public void handleOrderPlacement(OrderPlacedEvent event) {
        handleOrderPlacement(null, event);
    }

    public void handleOrderPlacement(EventEnvelope envelope, OrderPlacedEvent event) {
        evidenceMetrics.recordWorkflowModeStart(workflowProperties.getMode());
        if (workflowProperties.isSagaMode()) {
            handleSagaOrderPlacement(envelope, event);
            return;
        }

        handleLegacyOrderPlacement(event);
    }

    @Transactional
    protected void handleLegacyOrderPlacement(OrderPlacedEvent event) {
        Order order = persistPendingOrder(event);

        List<OrderLineItemRequest> items = event.getOrderLineItemsDtoList();
        String orderNumber = event.getOrderNumber();

        Map<String, Object> sagaState = Map.of(
                "totalItems", items.size(),
                "receivedItems", 0,
                "failed", false,
                "request", new OrderRequest(items)
        );
        redisTemplate.opsForHash().putAll(SAGA_PREFIX + orderNumber, sagaState);
        redisTemplate.expire(SAGA_PREFIX + orderNumber, Duration.ofMinutes(10));

        for (OrderLineItemRequest item : items) {
            orderWorkflowPublisher.publishInventoryCheckRequest(new InventoryCheckRequest(orderNumber, item));
        }

        log.info("SAGA started for persisted Order {}. Check requests sent.", orderNumber);
        appendOrderStatusOutbox(order.getOrderNumber(), order.getStatus());
    }

    @Transactional
    protected void handleSagaOrderPlacement(EventEnvelope envelope, OrderPlacedEvent event) {
        Order order = persistPendingOrder(event);
        appendOrderStatusOutbox(order.getOrderNumber(), order.getStatus());
        orderSagaWorkflowService.startOrderSaga(event, envelope);
        log.info("LSF-SAGA started for persisted order {}", event.getOrderNumber());
    }

    public List<OrderResponse> getAllOrders() {
        return orderRepository.findAll().stream()
                .map(this::mapToOrderResponse)
                .toList();
    }

    private Order persistPendingOrder(OrderPlacedEvent event) {
        log.info("Order validated: Saving Order {} to database...", event.getOrderNumber());

        Order order = new Order();
        order.setOrderNumber(event.getOrderNumber());
        order.setUserId(event.getUserId());
        order.setStatus("PENDING");

        List<OrderLineItemRequest> itemRequests = event.getOrderLineItemsDtoList();
        List<OrderLineItems> orderLineItemsEntities = new ArrayList<>();

        for (OrderLineItemRequest itemRequest : itemRequests) {
            Object cachedData = redisTemplate.opsForHash().get(PRODUCT_CACHE_KEY, itemRequest.getSkuCode());
            if (cachedData == null) {
                log.error("Product cache missing for SKU: {}", itemRequest.getSkuCode());
                throw new RuntimeException("Product not in cache: " + itemRequest.getSkuCode());
            }

            ProductCacheEvent productInfo = objectMapper.convertValue(cachedData, ProductCacheEvent.class);
            OrderLineItems entity = mapToDtoWithPrice(itemRequest, productInfo);
            entity.setOrder(order);
            orderLineItemsEntities.add(entity);
        }

        order.setOrderLineItemsList(orderLineItemsEntities);
        BigDecimal totalPrice = orderLineItemsEntities.stream()
                .map(item -> item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        order.setTotalPrice(totalPrice);

        orderRepository.save(order);
        log.info("Order validated: Order {} saved to database.", event.getOrderNumber());
        return order;
    }

    private boolean parseBooleanSafely(Object value) {
        if (value instanceof Boolean bool) {
            return bool;
        }
        if (value instanceof String stringValue) {
            return Boolean.parseBoolean(stringValue);
        }
        return false;
    }

    private int parseIntegerSafely(Object value) {
        if (value instanceof Integer integerValue) {
            return integerValue;
        }
        if (value instanceof Long longValue) {
            return longValue.intValue();
        }
        if (value instanceof String stringValue) {
            return Integer.parseInt(stringValue);
        }
        throw new IllegalArgumentException("Cannot cast " + value.getClass() + " to int");
    }

    private OrderResponse mapToOrderResponse(Order order) {
        return OrderResponse.builder()
                .id(order.getId())
                .orderNumber(order.getOrderNumber())
                .status(order.getStatus())
                .orderLineItemsList(order.getOrderLineItemsList().stream()
                        .map(this::mapToOrderLineItemsDto)
                        .toList())
                .totalPrice(order.getTotalPrice())
                .orderDate(order.getOrderDate())
                .build();
    }

    private OrderLineItemsDto mapToOrderLineItemsDto(OrderLineItems entity) {
        return OrderLineItemsDto.builder()
                .id(entity.getId())
                .skuCode(entity.getSkuCode())
                .price(entity.getPrice())
                .quantity(entity.getQuantity())
                .productName(entity.getProductName())
                .color(entity.getColor())
                .size(entity.getSize())
                .build();
    }

    private OrderLineItems mapToDtoWithPrice(OrderLineItemRequest itemRequest, ProductCacheEvent productInfo) {
        OrderLineItems orderLineItems = new OrderLineItems();
        orderLineItems.setQuantity(itemRequest.getQuantity());
        orderLineItems.setSkuCode(itemRequest.getSkuCode());
        orderLineItems.setPrice(productInfo.getPrice());
        orderLineItems.setProductName(productInfo.getName());
        orderLineItems.setColor(productInfo.getColor());
        orderLineItems.setSize(productInfo.getSize());
        return orderLineItems;
    }

    private void appendOrderStatusOutbox(String orderNumber, String status) {
        OrderStatusEvent event = new OrderStatusEvent(orderNumber, status);
        EventEnvelope envelope = envelopeFactory.wrap(
                "ecommerce.order.status.v1",
                orderNumber,
                orderNumber,
                event
        );
        outboxWriter.append(envelope, "order-status-envelope-topic", orderNumber);
    }
}
