package com.myexampleproject.orderservice.service;

import com.myexampleproject.common.dto.OrderLineItemRequest;
import com.myexampleproject.common.event.OrderStatusEvent;
import com.myexampleproject.common.event.OrderValidatedEvent;
import com.myexampleproject.orderservice.model.Order;
import com.myexampleproject.orderservice.model.OrderLineItems;
import com.myexampleproject.orderservice.repository.OrderRepository;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.contracts.quota.ConfirmReservationCommand;
import com.myorg.lsf.contracts.quota.ReleaseReservationCommand;
import com.myorg.lsf.outbox.OutboxWriter;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class OrderSagaStateService {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaStateService.class);

    // Dùng đúng topic status mà branch hiện tại của bạn đang chạy ổn.
    // Nếu bạn đã sửa sang envelope-topic thì giữ dòng dưới.
    private static final String ORDER_STATUS_TOPIC = "order-status-envelope-topic";
    // Nếu project bạn đang dùng tên cũ thì đổi lại:
    // private static final String ORDER_STATUS_TOPIC = "order-status-topic";

    private static final String ORDER_VALIDATED_TOPIC = "order-validated-envelope-topic";
    private static final String INVENTORY_RESERVATION_CONFIRM_TOPIC = "inventory-reservation-confirm-envelope-topic";
    private static final String INVENTORY_RESERVATION_RELEASE_TOPIC = "inventory-reservation-release-envelope-topic";

    private final OrderRepository orderRepository;
    private final OutboxWriter outboxWriter;
    private final OrderOutboxEnvelopeFactory envelopeFactory;

    @Transactional
    public boolean markValidatedAndEnqueueStatus(String orderNumber) {
        Order order = loadOrderWithItems(orderNumber);

        if (!"PENDING".equals(order.getStatus())) {
            log.warn("Ignore VALIDATED transition for order {} because current status={}", orderNumber, order.getStatus());
            return false;
        }

        order.setStatus("VALIDATED");
        orderRepository.save(order);

        appendOrderStatusOutbox(order.getOrderNumber(), order.getStatus());
        appendOrderValidatedOutbox(order);
        log.info("Order {} updated to VALIDATED and status/validated events appended to outbox", orderNumber);
        return true;
    }

    @Transactional
    public boolean markValidatedAndEnqueueStatusOnly(String orderNumber) {
        Order order = loadOrderWithItems(orderNumber);

        if (!"PENDING".equals(order.getStatus())) {
            log.warn(
                    "Ignore VALIDATED(status-only) transition for order {} because current status={}",
                    orderNumber,
                    order.getStatus()
            );
            return false;
        }

        order.setStatus("VALIDATED");
        orderRepository.save(order);

        appendOrderStatusOutbox(order.getOrderNumber(), order.getStatus());
        log.info("Order {} updated to VALIDATED and status event appended to outbox", orderNumber);
        return true;
    }

    @Transactional
    public boolean markCompletedAndEnqueueConfirm(String orderNumber) {
        Order order = loadOrderWithItems(orderNumber);

        if (!"PENDING".equals(order.getStatus()) && !"VALIDATED".equals(order.getStatus())) {
            log.warn("Ignore COMPLETED transition for order {} because current status={}", orderNumber, order.getStatus());
            return false;
        }

        order.setStatus("COMPLETED");
        orderRepository.save(order);

        appendReservationConfirmOutbox(order);
        appendOrderStatusOutbox(order.getOrderNumber(), order.getStatus());

        log.info("Order {} updated to COMPLETED and confirm/status events appended to outbox", orderNumber);
        return true;
    }

    @Transactional
    public boolean markFailedAndEnqueueRelease(String orderNumber, String newStatus, String reason) {
        Order order = loadOrderWithItems(orderNumber);

        if (!"PENDING".equals(order.getStatus()) && !"VALIDATED".equals(order.getStatus())) {
            log.warn("Ignore failure transition for order {} because current status={}", orderNumber, order.getStatus());
            return false;
        }

        order.setStatus(newStatus);
        orderRepository.save(order);

        appendReservationReleaseOutbox(order, reason);
        appendOrderStatusOutbox(order.getOrderNumber(), order.getStatus());

        log.info("Order {} updated to {} and release/status events appended to outbox", orderNumber, newStatus);
        return true;
    }

    private Order loadOrderWithItems(String orderNumber) {
        return orderRepository.findByOrderNumberWithItems(orderNumber)
                .orElseThrow(() -> new RuntimeException("Order not found: " + orderNumber));
    }

    private void appendOrderStatusOutbox(String orderNumber, String status) {
        OrderStatusEvent payload = new OrderStatusEvent(orderNumber, status);

        EventEnvelope envelope = envelopeFactory.wrap(
                "ecommerce.order.status.v1",
                orderNumber,
                orderNumber,
                payload
        );

        outboxWriter.append(envelope, ORDER_STATUS_TOPIC, orderNumber);
    }

    private void appendOrderValidatedOutbox(Order order) {
        List<OrderLineItemRequest> items = order.getOrderLineItemsList().stream()
                .map(this::toOrderLineItemRequest)
                .toList();

        OrderValidatedEvent payload = new OrderValidatedEvent(order.getOrderNumber(), items);

        EventEnvelope envelope = envelopeFactory.wrap(
                "order.validated.v1",
                order.getOrderNumber(),
                order.getOrderNumber(),
                payload
        );

        outboxWriter.append(envelope, ORDER_VALIDATED_TOPIC, order.getOrderNumber());
    }

    private OrderLineItemRequest toOrderLineItemRequest(OrderLineItems item) {
        return OrderLineItemRequest.builder()
                .skuCode(item.getSkuCode())
                .quantity(item.getQuantity())
                .build();
    }

    private void appendReservationConfirmOutbox(Order order) {
        for (OrderLineItems item : order.getOrderLineItemsList()) {
            ConfirmReservationCommand payload = ConfirmReservationCommand.builder()
                    .workflowId(order.getOrderNumber())
                    .resourceId(item.getSkuCode())
                    .quantity(item.getQuantity())
                    .build();

            EventEnvelope envelope = envelopeFactory.wrap(
                    "inventory.reservation.confirm.v1",
                    order.getOrderNumber(),
                    order.getOrderNumber(),
                    payload
            );

            outboxWriter.append(
                    envelope,
                    INVENTORY_RESERVATION_CONFIRM_TOPIC,
                    item.getSkuCode()
            );

            log.info("Appended confirm outbox for order={}, sku={}, qty={}",
                    order.getOrderNumber(), item.getSkuCode(), item.getQuantity());
        }
    }

    private void appendReservationReleaseOutbox(Order order, String reason) {
        for (OrderLineItems item : order.getOrderLineItemsList()) {
            ReleaseReservationCommand payload = ReleaseReservationCommand.builder()
                    .workflowId(order.getOrderNumber())
                    .resourceId(item.getSkuCode())
                    .quantity(item.getQuantity())
                    .reason(reason)
                    .build();

            EventEnvelope envelope = envelopeFactory.wrap(
                    "inventory.reservation.release.v1",
                    order.getOrderNumber(),
                    order.getOrderNumber(),
                    payload
            );

            outboxWriter.append(
                    envelope,
                    INVENTORY_RESERVATION_RELEASE_TOPIC,
                    item.getSkuCode()
            );

            log.info("Appended release outbox for order={}, sku={}, qty={}, reason={}",
                    order.getOrderNumber(), item.getSkuCode(), item.getQuantity(), reason);
        }
    }
}
