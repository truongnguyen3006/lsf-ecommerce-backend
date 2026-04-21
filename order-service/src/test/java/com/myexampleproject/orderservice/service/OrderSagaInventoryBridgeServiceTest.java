package com.myexampleproject.orderservice.service;

import com.myexampleproject.common.dto.OrderLineItemRequest;
import com.myexampleproject.common.dto.PaymentMethod;
import com.myexampleproject.common.event.InventoryCheckRequest;
import com.myexampleproject.common.event.InventoryCheckResult;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.eventing.LsfPublisher;
import com.myorg.lsf.saga.SagaReplyFanInSession;
import com.myorg.lsf.saga.SagaReplyFanInSignal;
import com.myorg.lsf.saga.SagaReplyFanInSupport;
import com.myorg.lsf.saga.SagaReplyFanInUpdate;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class OrderSagaInventoryBridgeServiceTest {

    @Test
    void startInventoryValidationShouldIgnoreDuplicateStartForSameOrder() {
        OrderWorkflowPublisher workflowPublisher = mock(OrderWorkflowPublisher.class);
        LsfPublisher lsfPublisher = mock(LsfPublisher.class);
        OrderSagaStateService orderSagaStateService = mock(OrderSagaStateService.class);
        OrderSagaEvidenceMetrics evidenceMetrics = mock(OrderSagaEvidenceMetrics.class);
        TestOrderSagaInventorySessionStore store = new TestOrderSagaInventorySessionStore();
        OrderSagaInventoryBridgeService service = new OrderSagaInventoryBridgeService(
                workflowPublisher,
                store,
                lsfPublisher,
                orderSagaStateService,
                evidenceMetrics
        );

        EventEnvelope envelope = EventEnvelope.builder()
                .eventId("evt-1")
                .requestId("req-1")
                .correlationId("ORDER-1")
                .build();
        OrderInventoryValidationRequestedCommand command = new OrderInventoryValidationRequestedCommand(
                "ORDER-1",
                List.of(item("SKU-1"), item("SKU-2")),
                PaymentMethod.COD
        );

        service.startInventoryValidation(envelope, command, "order-saga-replies-topic");
        service.startInventoryValidation(envelope, command, "order-saga-replies-topic");

        verify(workflowPublisher, times(2)).publishInventoryCheckRequest(any(InventoryCheckRequest.class));
        verify(evidenceMetrics).recordAggregatorDuplicateStart();
        assertThat(store.pendingCount()).isEqualTo(1L);
    }

    @Test
    void handleInventoryCheckResultShouldWaitForAllItemsAndIgnoreLateDuplicateReply() {
        OrderWorkflowPublisher workflowPublisher = mock(OrderWorkflowPublisher.class);
        LsfPublisher lsfPublisher = mock(LsfPublisher.class);
        OrderSagaStateService orderSagaStateService = mock(OrderSagaStateService.class);
        OrderSagaEvidenceMetrics evidenceMetrics = mock(OrderSagaEvidenceMetrics.class);
        TestOrderSagaInventorySessionStore store = new TestOrderSagaInventorySessionStore();
        OrderSagaInventoryBridgeService service = new OrderSagaInventoryBridgeService(
                workflowPublisher,
                store,
                lsfPublisher,
                orderSagaStateService,
                evidenceMetrics
        );

        EventEnvelope envelope = EventEnvelope.builder()
                .eventId("evt-2")
                .requestId("req-2")
                .correlationId("ORDER-2")
                .build();

        service.startInventoryValidation(
                envelope,
                new OrderInventoryValidationRequestedCommand(
                        "ORDER-2",
                        List.of(item("SKU-1"), item("SKU-2")),
                        PaymentMethod.MOCK_SUCCESS
                ),
                "order-saga-replies-topic"
        );

        service.handleInventoryCheckResult(success("ORDER-2", "SKU-1"), "order-saga-replies-topic");

        verify(lsfPublisher, never()).publish(eq("order-saga-replies-topic"), any(), any(), any(), any(), any());
        assertThat(store.find("ORDER-2")).isPresent();
        assertThat(store.find("ORDER-2").orElseThrow().receivedReplies()).isEqualTo(1);

        service.handleInventoryCheckResult(success("ORDER-2", "SKU-2"), "order-saga-replies-topic");
        service.handleInventoryCheckResult(success("ORDER-2", "SKU-2"), "order-saga-replies-topic");

        verify(lsfPublisher, times(1)).publish(eq("order-saga-replies-topic"), eq("ORDER-2"), eq(OrderSagaMessagingConstants.INVENTORY_VALIDATED_EVENT_TYPE), eq("ORDER-2"), any(), any());
        verify(evidenceMetrics).recordAggregatorLateResultIgnored();
        assertThat(store.find("ORDER-2")).isEmpty();
    }

    @Test
    void handleInventoryCheckResultShouldPublishFailureReplyWhenAnyItemFails() {
        OrderWorkflowPublisher workflowPublisher = mock(OrderWorkflowPublisher.class);
        LsfPublisher lsfPublisher = mock(LsfPublisher.class);
        OrderSagaStateService orderSagaStateService = mock(OrderSagaStateService.class);
        OrderSagaEvidenceMetrics evidenceMetrics = mock(OrderSagaEvidenceMetrics.class);
        TestOrderSagaInventorySessionStore store = new TestOrderSagaInventorySessionStore();
        OrderSagaInventoryBridgeService service = new OrderSagaInventoryBridgeService(
                workflowPublisher,
                store,
                lsfPublisher,
                orderSagaStateService,
                evidenceMetrics
        );

        EventEnvelope envelope = EventEnvelope.builder()
                .eventId("evt-3")
                .requestId("req-3")
                .correlationId("ORDER-3")
                .build();

        service.startInventoryValidation(
                envelope,
                new OrderInventoryValidationRequestedCommand(
                        "ORDER-3",
                        List.of(item("SKU-1"), item("SKU-2")),
                        PaymentMethod.MOCK_SUCCESS
                ),
                "order-saga-replies-topic"
        );

        service.handleInventoryCheckResult(success("ORDER-3", "SKU-1"), "order-saga-replies-topic");
        service.handleInventoryCheckResult(failure("ORDER-3", "SKU-2", "Inventory rejected"), "order-saga-replies-topic");

        ArgumentCaptor<OrderInventoryValidationFailedReply> payloadCaptor = ArgumentCaptor.forClass(OrderInventoryValidationFailedReply.class);
        verify(lsfPublisher).publish(
                eq("order-saga-replies-topic"),
                eq("ORDER-3"),
                eq(OrderSagaMessagingConstants.INVENTORY_VALIDATION_FAILED_EVENT_TYPE),
                eq("ORDER-3"),
                payloadCaptor.capture(),
                any()
        );
        assertThat(payloadCaptor.getValue().reason()).contains("Inventory rejected");
    }

    private static OrderLineItemRequest item(String skuCode) {
        return OrderLineItemRequest.builder()
                .skuCode(skuCode)
                .quantity(1)
                .build();
    }

    private static InventoryCheckResult success(String orderNumber, String skuCode) {
        return new InventoryCheckResult(orderNumber, item(skuCode), true, null);
    }

    private static InventoryCheckResult failure(String orderNumber, String skuCode, String reason) {
        return new InventoryCheckResult(orderNumber, item(skuCode), false, reason);
    }

    static class TestOrderSagaInventorySessionStore implements OrderSagaInventorySessionStore {

        private static final Duration TTL = Duration.ofSeconds(10);

        private final List<SagaReplyFanInSession> sessions = new ArrayList<>();

        @Override
        public boolean start(String orderNumber, int totalItems, String commandEventId, String requestId) {
            if (find(orderNumber).isPresent()) {
                return false;
            }
            sessions.add(SagaReplyFanInSupport.start(orderNumber, totalItems, commandEventId, requestId, 1L, TTL));
            return true;
        }

        @Override
        public Optional<SagaReplyFanInUpdate> applyResult(InventoryCheckResult result) {
            Optional<SagaReplyFanInSession> existing = find(result.getOrderNumber());
            return existing.map(session -> {
                sessions.remove(session);
                SagaReplyFanInUpdate update = SagaReplyFanInSupport.apply(
                        session,
                        result.isSuccess()
                                ? SagaReplyFanInSignal.successful()
                                : SagaReplyFanInSignal.failure(result.getReason()),
                        session.updatedAtMs() + 1,
                        TTL
                );
                if (!update.shouldIgnore()) {
                    sessions.add(update.session());
                }
                return update;
            });
        }

        @Override
        public Optional<SagaReplyFanInSession> find(String orderNumber) {
            return sessions.stream().filter(session -> session.aggregateId().equals(orderNumber)).findFirst();
        }

        @Override
        public List<SagaReplyFanInSession> findPending(int limit) {
            return sessions.stream().limit(limit).toList();
        }

        @Override
        public long pendingCount() {
            return sessions.size();
        }

        @Override
        public int purgeExpired(long nowMs, int limit) {
            return 0;
        }

        @Override
        public void delete(String orderNumber) {
            find(orderNumber).ifPresent(sessions::remove);
        }
    }
}
