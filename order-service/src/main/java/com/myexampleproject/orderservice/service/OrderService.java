package com.myexampleproject.orderservice.service;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myexampleproject.common.dto.OrderLineItemRequest;
import com.myexampleproject.common.event.*;
import com.myexampleproject.orderservice.config.CartMapper;
import com.myexampleproject.orderservice.dto.OrderResponse;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.outbox.OutboxWriter;
import jakarta.annotation.PostConstruct;
import jakarta.persistence.Convert;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import com.myexampleproject.common.event.InventoryCheckRequest;
import com.myexampleproject.common.event.InventoryCheckResult;
import org.springframework.data.redis.core.RedisTemplate; // <-- Bạn sẽ cần Redis
import java.time.Duration;
import java.util.Map;

import com.myexampleproject.common.dto.OrderLineItemsDto;
import com.myexampleproject.orderservice.dto.OrderRequest;
import com.myexampleproject.orderservice.model.Order;
import com.myexampleproject.orderservice.model.OrderLineItems;
import com.myexampleproject.orderservice.repository.OrderRepository;

import lombok.RequiredArgsConstructor;
import io.micrometer.core.instrument.Counter; // <-- THÊM IMPORT NÀY
import io.micrometer.core.instrument.MeterRegistry; // <-- THÊM IMPORT NÀY

import com.myorg.lsf.contracts.quota.ConfirmReservationCommand;
import com.myorg.lsf.contracts.quota.ReleaseReservationCommand;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private  Counter ordersCompletedCounter;
    private  Counter ordersFailedCounter;
    // Inject thêm cái này để dùng trong @PostConstruct
    private final MeterRegistry meterRegistry;

    private final OrderRepository orderRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    // THÊM: Cần Redis để quản lý state của Saga
    private final RedisTemplate<String, Object> redisTemplate;
    private static final String SAGA_PREFIX = "saga:order:";
    private static final String PRODUCT_CACHE_KEY = "products:cache";

    //inject OutboxWriter vào OrderService
    private final OutboxWriter outboxWriter;
    private final OrderOutboxEnvelopeFactory envelopeFactory;

    //new
    private final OrderSagaStateService orderSagaStateService;
    private final OrderPaymentResultProcessor orderPaymentResultProcessor;

    //Viết hàm này vì dùng @RequiredArgsConstructor với biến không có final , Counter
    @PostConstruct
    public void initMetrics() {
        this.ordersCompletedCounter = Counter.builder("orders_processed_total")
                .tag("status", "completed")
                .description("Total successful orders")
                .register(meterRegistry);

        this.ordersFailedCounter = Counter.builder("orders_processed_total")
                .tag("status", "failed")
                .description("Total failed orders")
                .register(meterRegistry);
    }

    // Entry point for asynchronous order processing.
    // The order request is accepted first; persistence and downstream workflow continue via Kafka.
    public String placeOrder(OrderRequest orderRequest, String userId) {
        String orderNumber = UUID.randomUUID().toString();
        log.info("Order {} received. Starting Inventory SAGA...", orderNumber);

        List<OrderLineItemRequest> items = orderRequest.getItems();

        OrderPlacedEvent placedEvent = new OrderPlacedEvent(orderNumber, userId, items);
        kafkaTemplate.send("order-placed-topic", orderNumber, placedEvent);
        return orderNumber;
    }

    // LSF integration note:
    // the saga waits for all inventory results before deciding success/failure,
    // avoiding early failure when an order contains multiple SKUs.
    @KafkaListener(
            topics = "inventory-check-result-topic",
            groupId = "order-saga-group",
            containerFactory = "orderBatchKafkaListenerContainerFactory"
    )
    public void handleInventoryCheckResult(List<ConsumerRecord<String, Object>> records) {
        log.info("SAGA: Received batch of {} inventory results", records.size());

        for (ConsumerRecord<String, Object> record : records) {
            try {
                InventoryCheckResult result = objectMapper.convertValue(record.value(), InventoryCheckResult.class);
                String orderNumber = result.getOrderNumber();
                String sagaKey = SAGA_PREFIX + orderNumber;

                log.info("SAGA: Result for Order {}, SKU {}: Success={}",
                        orderNumber, result.getItem().getSkuCode(), result.isSuccess());

                Long receivedCount = redisTemplate.opsForHash().increment(sagaKey, "receivedItems", 1);
                if (!result.isSuccess()) {
                    redisTemplate.opsForHash().put(sagaKey, "failed", true);
                    redisTemplate.opsForHash().put(sagaKey, "failureReason", result.getReason());
                    log.warn("SAGA: Marked order {} as failed because SKU {} was rejected. reason={}",
                            orderNumber, result.getItem().getSkuCode(), result.getReason());
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
                    kafkaTemplate.send("order-failed-topic", orderNumber, new OrderFailedEvent(orderNumber, reason));
                    log.warn("SAGA COMPLETE: Order {} failed after collecting all inventory results. reason={}",
                            orderNumber, reason);
                } else {
                    Object requestObj = redisTemplate.opsForHash().get(sagaKey, "request");
                    OrderRequest originalRequest = objectMapper.convertValue(requestObj, OrderRequest.class);
                    kafkaTemplate.send("order-validated-topic", orderNumber,
                            new OrderValidatedEvent(orderNumber, originalRequest.getItems()));
                    log.info("SAGA COMPLETE: Order {} passed all inventory checks.", orderNumber);
                }

                redisTemplate.delete(sagaKey);
            } catch (Exception e) {
                log.error("SAGA ERROR: Key: {}", record.key(), e);
            }
        }
    }

    //mới
    private boolean parseBooleanSafely(Object obj) {
        if (obj instanceof Boolean bool) {
            return bool;
        }
        if (obj instanceof String str) {
            return Boolean.parseBoolean(str);
        }
        return false;
    }

    // --- HELPER METHOD AN TOÀN ---
    private int parseIntegerSafely(Object obj) {
        if (obj instanceof Integer) {
            return (Integer) obj;
        } else if (obj instanceof Long) {
            return ((Long) obj).intValue();
        } else if (obj instanceof String) {
            return Integer.parseInt((String) obj);
        }
        throw new IllegalArgumentException("Cannot cast " + obj.getClass() + " to int");
    }

    // Dùng 1 group-id riêng cho việc xây dựng cache
    @KafkaListener(
            topics = "product-cache-update-topic",
            groupId = "order-product-cacher",
            containerFactory = "orderBatchKafkaListenerContainerFactory"
    )
    public void handleProductCacheUpdate(List<ConsumerRecord<String, Object>> records) {
        log.info("Receiving {} product cache updates...", records.size());
        for (ConsumerRecord<String, Object> record : records) {
            try {
                // Deserialize
                ProductCacheEvent event = objectMapper.convertValue(record.value(), ProductCacheEvent.class);
                String sku = event.getSkuCode();

                // Lưu vào REDIS HASH
                // Key: "products:cache"
                // HashKey: "SKU_CODE"
                // Value: Toàn bộ object 'event' (chứa giá + tên)
                redisTemplate.opsForHash().put(PRODUCT_CACHE_KEY, sku, event);

                log.debug("Cached product info for SKU: {}", sku);

            } catch (Exception e) {
                log.error("LỖI KHI CACHING PRODUCT {}: {}", record.key(), e.getMessage());
            }
        }
    }

    @Transactional(readOnly = true) // Giao dịch chỉ đọc, nhanh hơn
    public OrderResponse getOrderDetails(String orderNumber) {
        log.info("Fetching order details for: {}", orderNumber);

        // 1. Tìm Order trong CSDL
        Order order = orderRepository.findByOrderNumber(orderNumber)
                .orElseThrow(() -> new RuntimeException("Order not found: " + orderNumber));

        // 2. Map từ Entity (Order) sang DTO (OrderResponse)
        return mapToOrderResponse(order);
    }

    /**
     * Helper: Chuyển đổi Entity Order -> DTO OrderResponse.
     */
    private OrderResponse mapToOrderResponse(Order order) {
        return OrderResponse.builder()
                .id(order.getId())
                .orderNumber(order.getOrderNumber())
                .status(order.getStatus())
                .orderLineItemsList(order.getOrderLineItemsList()
                        .stream()
                        .map(this::mapToOrderLineItemsDto) // Tái sử dụng logic map
                        .toList())
                .totalPrice(order.getTotalPrice())
                .orderDate(order.getOrderDate())
                .build();
    }

    /**
     * Helper: Chuyển đổi Entity OrderLineItems -> DTO OrderLineItemsDto.
     * (Đây là logic ngược lại với hàm mapToDto bạn đã có)
     */
    // Hàm này được gọi trong getOrderDetails
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

    // ==========================================================
    // SỬA LỖI 1 TẠI ĐÂY
    // ==========================================================
    @KafkaListener(
            topics = "cart-checkout-topic",
            groupId = "order-updater-group",
            containerFactory = "orderBatchKafkaListenerContainerFactory"
    )
    public void handleCartCheckout(List<ConsumerRecord<String, Object>> records) {
        log.info("Received a batch of {} cart-checkout events", records.size());

        for (ConsumerRecord<String, Object> record : records) {
            try {
                Object payload = record.value();
                log.info("Processing cart checkout for user: {}", record.key());

                CartCheckoutEvent event = objectMapper.convertValue(payload, CartCheckoutEvent.class);

                // Convert CartCheckoutEvent -> OrderRequest
                OrderRequest req = CartMapper.fromCart(event);

                placeOrder(req, event.getUserId()); // Gọi trực tiếp

            } catch (Exception e) {
                log.error("LỖI KHI XỬ LÝ CartCheckoutEvent: {}. Sẽ KHÔNG retry.", record.key(), e);
            }
        }
    }


    @KafkaListener(
            topics = {
                    "order-placed-topic",
                    "order-failed-topic"
            },
            containerFactory = "orderBatchKafkaListenerContainerFactory"
    )
    public void handleOrderEvents(List<ConsumerRecord<String, Object>> records) {
        log.info("Received a batch of {} events", records.size());

        // Loop qua danh sách
        for (ConsumerRecord<String, Object> record : records) {
            String topic = record.topic();
            Object payload = record.value();
            log.debug("Processing event from topic [{}], key [{}]", topic, record.key());

            // Logic switch-case của bạn giữ nguyên
            try {
                switch (topic) {
                    case "order-placed-topic":
                        OrderPlacedEvent placedEvent = objectMapper.convertValue(payload, OrderPlacedEvent.class);
                        handleOrderPlacement(placedEvent); // Hàm private này giữ nguyên
                        break;

                    case "order-failed-topic":
                        OrderFailedEvent failedEvent = objectMapper.convertValue(payload, OrderFailedEvent.class);
                        handleOrderFailure(failedEvent); // Hàm private này giữ nguyên
                        break;

                    default:
                        log.warn("Received message on unhandled topic: {}", topic);
                }
            } catch (Exception e) {
                log.error("LỖI KHI XỬ LÝ MESSAGE: {}. Sẽ KHÔNG retry.", record.key(), e);
            }
        }
    }

    public <T> T toEvent(Object payload, Class<T> clazz) {
        return objectMapper.convertValue(payload, clazz);
    }

    // Consumer-project orchestration logic:
    // order data is still assembled here, while reservation itself is delegated to inventory + LSF quota.
    @Transactional
    protected void handleOrderPlacement(OrderPlacedEvent event) {
        log.info("Order validated: Saving Order {} to database...", event.getOrderNumber());

        Order order = new Order();
        order.setOrderNumber(event.getOrderNumber());
        order.setUserId(event.getUserId());
        order.setStatus("PENDING");

        List<OrderLineItemRequest> itemRequests = event.getOrderLineItemsDtoList();

        // Tạo List<OrderLineItems> (Entity) mới
        List<OrderLineItems> orderLineItemsEntities = new ArrayList<>();

        for (OrderLineItemRequest itemReq : itemRequests) {
            // 1. Lấy thông tin sản phẩm từ Cache
            Object cachedData = redisTemplate.opsForHash().get(PRODUCT_CACHE_KEY, itemReq.getSkuCode());

            if (cachedData == null) {
                // Lỗi nghiêm trọng: Sản phẩm không có trong cache
                // (Trong thực tế, bạn có thể gọi API dự phòng, hoặc FAILED đơn hàng)
                log.error("KHÔNG TÌM THẤY CACHE cho SKU: {}", itemReq.getSkuCode());
                // Tạm thời FAILED đơn hàng này
                throw new RuntimeException("Product not in cache: " + itemReq.getSkuCode());
            }

//             2. Convert cache (là ProductCacheEvent)
            ProductCacheEvent productInfo = objectMapper.convertValue(cachedData, ProductCacheEvent.class);

            // 3. Gọi hàm mapToDto (đã sửa) với giá
            OrderLineItems entity = mapToDtoWithPrice(itemReq, productInfo);

            // 4. Thiết lập quan hệ
            entity.setOrder(order);
            orderLineItemsEntities.add(entity);

            // --- LOGIC SỬA ĐỔI KẾT THÚC ---
        }

        order.setOrderLineItemsList(orderLineItemsEntities);

        BigDecimal totalPrice = orderLineItemsEntities.stream()
                // Nhân giá (price) với số lượng (quantity) của từng món
                .map(item -> item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
                // Cộng tất cả kết quả lại
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        order.setTotalPrice(totalPrice);


        orderRepository.save(order);
        log.info("Order validated: Order {} saved to database.", event.getOrderNumber());

        List<OrderLineItemRequest> items = event.getOrderLineItemsDtoList(); // Lấy từ event
        String orderNumber = event.getOrderNumber();

        // A. Lưu state vào Redis
        Map<String, Object> sagaState = Map.of(
                "totalItems", items.size(),
                "receivedItems", 0,
                "failed", false,
                "request", new OrderRequest(items) // Tái tạo lại object request để lưu
        );
        redisTemplate.opsForHash().putAll(SAGA_PREFIX + orderNumber, sagaState);
        redisTemplate.expire(SAGA_PREFIX + orderNumber, Duration.ofMinutes(10));

        // B. Gửi yêu cầu kiểm tra kho
        for (OrderLineItemRequest item : items) {
            InventoryCheckRequest checkRequest = new InventoryCheckRequest(orderNumber, item);
            kafkaTemplate.send("inventory-check-request-topic", item.getSkuCode(), checkRequest);
        }

        log.info("SAGA started for persisted Order {}. Check requests sent.", orderNumber);

        OrderStatusEvent statusEvent = new OrderStatusEvent(event.getOrderNumber(), "PENDING");
//        kafkaTemplate.send("order-status-topic", event.getOrderNumber(), statusEvent);
        appendOrderStatusOutbox(order.getOrderNumber(), order.getStatus());
    }

    // Hàm này được gọi trong handleOrderPlacement
    private OrderLineItems mapToDtoWithPrice(OrderLineItemRequest itemRequest, ProductCacheEvent productInfo) {
        OrderLineItems orderLineItems = new OrderLineItems();
        orderLineItems.setQuantity(itemRequest.getQuantity());
        orderLineItems.setSkuCode(itemRequest.getSkuCode());

        // Lấy từ Cache (ProductCacheEvent)
        orderLineItems.setPrice(productInfo.getPrice());
        orderLineItems.setProductName(productInfo.getName());

        // GÁN GIÁ TRỊ MỚI TỪ CACHE VÀO ENTITY
        orderLineItems.setColor(productInfo.getColor());
        orderLineItems.setSize(productInfo.getSize());

        return orderLineItems;
    }

//    @Transactional
//    protected void handleOrderFailure(OrderFailedEvent failedEvent) {
//        log.info("Using OrderFailedEvent class: {}", failedEvent.getClass().getName());
//        log.warn("INVENTORY FAILED: Received feedback for Order {}. Reason: {}",
//                failedEvent.getOrderNumber(), failedEvent.getReason());
//
//        Order order = orderRepository.findByOrderNumber(failedEvent.getOrderNumber())
//                .orElseThrow(() -> new RuntimeException("Order not found: " + failedEvent.getOrderNumber()));
//        if (order.getStatus().equals("PENDING")) {
//            order.setStatus("FAILED");
//            orderRepository.save(order);
//            log.warn("Order {} status updated to FAILED due to inventory issue.", order.getOrderNumber());
//            kafkaTemplate.send("order-status-topic", order.getOrderNumber(),
//                    new OrderStatusEvent(order.getOrderNumber(), order.getStatus()));
//            this.ordersFailedCounter.increment();
//        } else {
//            log.warn("Received failure event for order {} but status was not PENDING (Status: {}).",
//                    order.getOrderNumber(), order.getStatus());
//        }
//    }

    // Changed from direct inventory compensation to reservation release command.
    @Transactional
    protected void handleOrderFailure(OrderFailedEvent failedEvent) {
//        log.warn("INVENTORY FAILED: Received feedback for Order {}. Reason: {}",
//                failedEvent.getOrderNumber(), failedEvent.getReason());
//
//        Order order = orderRepository.findByOrderNumberWithItems(failedEvent.getOrderNumber())
//                .orElseThrow(() -> new RuntimeException("Order not found: " + failedEvent.getOrderNumber()));
//
//        if ("PENDING".equals(order.getStatus())) {
//            order.setStatus("FAILED");
//            orderRepository.save(order);
//            publishReleaseCommands(order, "INVENTORY_FAILED: " + failedEvent.getReason());
////            kafkaTemplate.send("order-status-topic", order.getOrderNumber(),
////                    new OrderStatusEvent(order.getOrderNumber(), order.getStatus()));
//            appendOrderStatusOutbox(order.getOrderNumber(), order.getStatus());
//            this.ordersFailedCounter.increment();
//            log.warn("Order {} status updated to FAILED due to inventory issue.", order.getOrderNumber());
//        } else {
//            log.warn("Received failure event for order {} but status was not PENDING (Status: {}).",
//                    order.getOrderNumber(), order.getStatus());
//        }
        boolean changed = orderSagaStateService.markFailedAndEnqueueRelease(
                failedEvent.getOrderNumber(),
                "FAILED",
                "inventory: " + failedEvent.getReason()
        );

        if (changed) {
            ordersFailedCounter.increment();
            log.warn("Order {} status updated to FAILED due to inventory issue.", failedEvent.getOrderNumber());
        }
    }

//    @Transactional
//    protected void handlePaymentSuccess(PaymentProcessedEvent paymentProcessedEvent) {
//        log.info("SUCCESS: Received PaymentProcessedEvent for Order {}. Payment ID: {}. Updating status...",
//                paymentProcessedEvent.getOrderNumber(), paymentProcessedEvent.getPaymentId());
//
//        // Không cần try-catch ở đây nữa vì đã có ở hàm listener chính
//        Order order = orderRepository.findByOrderNumber(paymentProcessedEvent.getOrderNumber())
//                .orElseThrow(() -> new RuntimeException("Order not found: " + paymentProcessedEvent.getOrderNumber()));
//
//        if ("PENDING".equals(order.getStatus()) || "VALIDATED".equals(order.getStatus())) {
//            order.setStatus("COMPLETED");
//            orderRepository.save(order);
//            log.info("Order {} status updated to COMPLETED.", order.getOrderNumber());
//            kafkaTemplate.send("order-status-topic", order.getOrderNumber(),
//                    new OrderStatusEvent(order.getOrderNumber(), order.getStatus()));
//            this.ordersCompletedCounter.increment();
//        } else {
//            log.warn("Received payment success for order {} but status was not PENDING (Status: {}).",
//                    order.getOrderNumber(), order.getStatus());
//        }
//    }
    // Payment success now confirms previously reserved quota instead of relying on early stock deduction.
    @Transactional
    protected void handlePaymentSuccess(PaymentProcessedEvent paymentProcessedEvent) {
        orderPaymentResultProcessor.handlePaymentSuccess(paymentProcessedEvent, "legacy-topic", null);
    }

//    @Transactional
//    protected void handlePaymentFailure(PaymentFailedEvent paymentFailedEvent) {
//        log.warn("FAILED: Received PaymentFailedEvent for Order {}. Reason: {}. Updating status...",
//                paymentFailedEvent.getOrderNumber(), paymentFailedEvent.getReason());
//        Order order = orderRepository.findByOrderNumberWithItems(paymentFailedEvent.getOrderNumber())
//                .orElseThrow(() -> new RuntimeException("Order not found: " + paymentFailedEvent.getOrderNumber()));
//        if ("PENDING".equals(order.getStatus()) || "VALIDATED".equals(order.getStatus())) {
//            order.setStatus("PAYMENT_FAILED");
//            orderRepository.save(order);
//            List<OrderLineItems> items = order.getOrderLineItemsList();
//            for (OrderLineItems item : items) {
//                InventoryAdjustmentEvent adjustmentEvent = InventoryAdjustmentEvent.builder()
//                        .skuCode(item.getSkuCode())
//                        .adjustmentQuantity(item.getQuantity()) // Số dương: Cộng lại vào kho
//                        .reason("COMPENSATION: Payment Failed for Order " + order.getOrderNumber())
//                        .build();
//                kafkaTemplate.send("inventory-adjustment-topic", item.getSkuCode(), adjustmentEvent);
//                log.info("COMPENSATION: Sent restock request for SKU {} (+{})", item.getSkuCode(), item.getQuantity());
//            }
//            log.warn("Order {} status updated to PAYMENT_FAILED.", order.getOrderNumber());
//            kafkaTemplate.send("order-status-topic", order.getOrderNumber(),
//                    new OrderStatusEvent(order.getOrderNumber(), order.getStatus()));
//        } else {
//            log.warn("Received payment failure for order {} but status was not PENDING (Status: {}).",
//                    order.getOrderNumber(), order.getStatus());
//        }
//    }

    // Payment failure now releases reservation through lsf-contracts command flow.
    @Transactional
    protected void handlePaymentFailure(PaymentFailedEvent paymentFailedEvent) {
        orderPaymentResultProcessor.handlePaymentFailure(paymentFailedEvent, "legacy-topic", null);
    }

    @KafkaListener(
            topics = "order-validated-topic",
            groupId = "order-group",
            containerFactory = "orderBatchKafkaListenerContainerFactory"
    )
    public void handleValidated(List<ConsumerRecord<String, Object>> records) {
        log.info("SAGA SUCCESS: Received batch of {} validated events", records.size());
        for (ConsumerRecord<String, Object> record : records) {
//            try {
//                Object payload = record.value();
//                OrderValidatedEvent event = objectMapper.convertValue(payload, OrderValidatedEvent.class);
//                log.info("SAGA SUCCESS: Order {} validated, updating status.", event.getOrderNumber());
//                Order order = orderRepository.findByOrderNumber(event.getOrderNumber())
//                        .orElseThrow(() -> new RuntimeException("Order not found: " + event.getOrderNumber()));
//                if ("PENDING".equals(order.getStatus())) {
//                    order.setStatus("VALIDATED");
//                    orderRepository.save(order);
//                    appendOrderStatusOutbox(order.getOrderNumber(), order.getStatus());
//                } else {
//                    log.warn("Received validated event for order {} but status was not PENDING (Status: {}).",
//                            order.getOrderNumber(), order.getStatus());
//                }
//            } catch (Exception e) {
//                log.error("Order placement failed OrderValidatedEvent: {}. Sẽ KHÔNG retry.", record.key(), e);
//            }
            try{
                Object payload = record.value();
                OrderValidatedEvent validatedEvent = objectMapper.convertValue(payload, OrderValidatedEvent.class);
                boolean changed = orderSagaStateService.markValidatedAndEnqueueStatus(validatedEvent.getOrderNumber());

                if (changed) {
                    log.info("Order {} moved to VALIDATED", validatedEvent.getOrderNumber());
                }
            }catch (Exception e){
                log.error("Order placement failed OrderValidatedEvent: {}. Sẽ KHÔNG retry.", record.key(), e);
            }
        }
    }

    public List<OrderResponse> getAllOrders() {
        List<Order> orders = orderRepository.findAll();
        return orders.stream()
                .map(this::mapToOrderResponse)
                .toList();
    }

    private OrderLineItems mapToDto(OrderLineItemsDto orderLineItemsDto) {
        OrderLineItems orderLineItems = new OrderLineItems();
        orderLineItems.setPrice(orderLineItemsDto.getPrice());
        orderLineItems.setQuantity(orderLineItemsDto.getQuantity());
        orderLineItems.setSkuCode(orderLineItemsDto.getSkuCode());
        return orderLineItems;
    }

    //mới
    // Publish standardized reservation confirmation commands defined by the LSF framework.
    private void publishConfirmCommands(Order order) {
        for (OrderLineItems item : order.getOrderLineItemsList()) {
            ConfirmReservationCommand command = ConfirmReservationCommand.builder()
                    .workflowId(order.getOrderNumber())
                    .resourceId(item.getSkuCode())
                    .quantity(item.getQuantity())
                    .build();
            kafkaTemplate.send("inventory-reservation-confirm-envelope-topic", item.getSkuCode(), command);
            log.info("Sent confirm reservation command for order={}, sku={}, qty={}",
                    order.getOrderNumber(), item.getSkuCode(), item.getQuantity());
        }
    }

    // Publish standardized reservation release commands defined by the LSF framework.
    private void publishReleaseCommands(Order order, String reason) {
        for (OrderLineItems item : order.getOrderLineItemsList()) {
            ReleaseReservationCommand command = ReleaseReservationCommand.builder()
                    .workflowId(order.getOrderNumber())
                    .resourceId(item.getSkuCode())
                    .quantity(item.getQuantity())
                    .reason(reason)
                    .build();
            kafkaTemplate.send("inventory-reservation-release-envelope-topic", item.getSkuCode(), command);
            log.info("Sent release reservation command for order={}, sku={}, qty={}, reason={}",
                    order.getOrderNumber(), item.getSkuCode(), item.getQuantity(), reason);
        }
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
