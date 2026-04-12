package com.myexampleproject.orderservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myexampleproject.common.dto.OrderLineItemRequest;
import com.myexampleproject.common.event.InventoryCheckResult;
import com.myexampleproject.common.event.OrderPlacedEvent;
import com.myexampleproject.common.event.OrderValidatedEvent;
import com.myexampleproject.common.event.PaymentFailedEvent;
import com.myexampleproject.common.event.PaymentProcessedEvent;
import com.myexampleproject.orderservice.config.OrderCheckoutSagaConfiguration;
import com.myexampleproject.orderservice.config.OrderWorkflowMode;
import com.myexampleproject.orderservice.config.OrderWorkflowProperties;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.eventing.LsfEventHandler;
import com.myorg.lsf.eventing.LsfPublishOptions;
import com.myorg.lsf.eventing.LsfPublisher;
import com.myorg.lsf.saga.LsfSagaOrchestrator;
import com.myorg.lsf.saga.LsfSagaProperties;
import com.myorg.lsf.saga.SagaReplyFanInOutcome;
import com.myorg.lsf.saga.SagaReplyFanInSession;
import com.myorg.lsf.saga.SagaReplyFanInSignal;
import com.myorg.lsf.saga.SagaReplyFanInSupport;
import com.myorg.lsf.saga.SagaReplyFanInUpdate;
import com.myorg.lsf.saga.SagaInstance;
import com.myorg.lsf.saga.SagaStatus;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ConcurrentMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@SpringBootTest(
        classes = OrderLsfSagaSpikeRuntimeTest.OrderLsfSagaSpikeTestApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
                "spring.application.name=order-service",
                "lsf.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
                "lsf.kafka.schema-registry-url=mock://order-saga-spike",
                "lsf.kafka.consumer.group-id=order-saga-spike-group",
                "lsf.kafka.consumer.batch=false",
                "lsf.kafka.consumer.concurrency=1",
                "lsf.kafka.consumer.auto-offset-reset=earliest",
                "lsf.kafka.consumer.json-value-type=com.myorg.lsf.contracts.core.envelope.EventEnvelope",
                "lsf.kafka.dlq.enabled=false",
                "lsf.eventing.producer-name=order-service",
                "lsf.eventing.listener.enabled=true",
                "lsf.eventing.idempotency.enabled=false",
                "lsf.eventing.consume-topics[0]=order-saga-internal-topic",
                "lsf.eventing.consume-topics[1]=order-saga-replies-topic",
                "lsf.eventing.consume-topics[2]=order-validated-envelope-topic",
                "lsf.eventing.consume-topics[3]=payment-processed-envelope-topic",
                "lsf.eventing.consume-topics[4]=payment-failed-envelope-topic",
                "lsf.saga.enabled=true",
                "lsf.saga.store=jdbc",
                "lsf.saga.transport.mode=direct",
                "lsf.saga.timeout-scanner.enabled=false",
                "lsf.saga.jdbc.initialize-schema=always",
                "spring.flyway.enabled=false",
                "lsf.outbox.enabled=false",
                "eureka.client.enabled=false",
                "spring.autoconfigure.exclude=com.myorg.lsf.outbox.mysql.LsfOutboxMySqlAutoConfiguration,com.myorg.lsf.outbox.admin.LsfOutboxAdminAutoConfiguration",
                "app.order.workflow.saga.definition-name=order-checkout-saga",
                "app.order.workflow.saga.internal-topic=order-saga-internal-topic",
                "app.order.workflow.saga.reply-topic=order-saga-replies-topic",
                "app.order.workflow.saga.inventory-step-timeout=500ms",
                "app.order.workflow.saga.payment-step-timeout=500ms",
                "app.order.workflow.saga.compensation-timeout=500ms",
                "app.order.workflow.saga.aggregator-ttl=2s"
        }
)
@EmbeddedKafka(
        partitions = 1,
        topics = {
                "order-saga-internal-topic",
                "order-saga-replies-topic",
                "order-validated-envelope-topic",
                "payment-processed-envelope-topic",
                "payment-failed-envelope-topic"
        }
)
class OrderLsfSagaSpikeRuntimeTest {

    @Autowired
    private OrderSagaWorkflowService workflowService;

    @Autowired
    private LsfSagaOrchestrator orchestrator;

    @Autowired
    private SimulatedOrderWorkflowPublisher workflowPublisher;

    @Autowired
    private OrderWorkflowProperties workflowProperties;

    @MockBean
    private OrderSagaStateService orderSagaStateService;

    @BeforeEach
    void setUp() {
        workflowPublisher.clear();
        workflowPublisher.resumeInventoryReplies();
    }

    @Test
    void shouldCompleteSagaOnInventoryAndPaymentSuccess() {
        String orderNumber = "order-success-" + UUID.randomUUID();
        when(orderSagaStateService.markValidatedAndEnqueueStatusOnly(orderNumber)).thenReturn(true);
        when(orderSagaStateService.markCompletedAndEnqueueConfirm(orderNumber)).thenReturn(true);

        assertThat(workflowProperties.getMode()).isEqualTo(OrderWorkflowMode.LSF_SAGA);

        workflowService.startOrderSaga(
                orderPlaced(orderNumber, List.of(item("SKU-1", 1))),
                sourceEnvelope(orderNumber)
        );

        SagaInstance completed = awaitSagaStatus(orderNumber, SagaStatus.COMPLETED, Duration.ofSeconds(15));

        assertThat(completed.getStatus()).isEqualTo(SagaStatus.COMPLETED);
        assertThat(workflowPublisher.recordedRequests()).hasSize(1);
        assertThat(workflowPublisher.recordedRequests().getFirst().getOrderNumber()).isEqualTo(orderNumber);
        verify(orderSagaStateService).markValidatedAndEnqueueStatusOnly(orderNumber);
        verify(orderSagaStateService).markCompletedAndEnqueueConfirm(orderNumber);
        verify(orderSagaStateService, never()).markFailedAndEnqueueRelease(eq(orderNumber), contains("FAILED"), contains(""));
    }

    @Test
    void shouldFailSagaWhenInventoryValidationFails() {
        String orderNumber = "order-inventory-failure-" + UUID.randomUUID();
        when(orderSagaStateService.markFailedAndEnqueueRelease(eq(orderNumber), eq("FAILED"), contains("Inventory rejected"))).thenReturn(true);

        workflowService.startOrderSaga(
                orderPlaced(orderNumber, List.of(item("FAIL-INV-1", 1))),
                sourceEnvelope(orderNumber)
        );

        SagaInstance failed = awaitSagaStatus(orderNumber, SagaStatus.FAILED, Duration.ofSeconds(15));

        assertThat(failed.getStatus()).isEqualTo(SagaStatus.FAILED);
        verify(orderSagaStateService).markFailedAndEnqueueRelease(
                eq(orderNumber),
                eq("FAILED"),
                contains("Inventory rejected")
        );
        verify(orderSagaStateService, never()).markValidatedAndEnqueueStatusOnly(orderNumber);
        verify(orderSagaStateService, never()).markCompletedAndEnqueueConfirm(orderNumber);
    }

    @Test
    void shouldCompensateInventoryWhenPaymentFails() {
        String orderNumber = "order-payment-failure-" + UUID.randomUUID();
        when(orderSagaStateService.markValidatedAndEnqueueStatusOnly(orderNumber)).thenReturn(true);
        when(orderSagaStateService.markFailedAndEnqueueRelease(eq(orderNumber), eq("PAYMENT_FAILED"), contains("payment:"))).thenReturn(true);

        workflowService.startOrderSaga(
                orderPlaced(orderNumber, List.of(item("SKU-1", 1), item("SKU-2", 1))),
                sourceEnvelope(orderNumber)
        );

        SagaInstance compensated = awaitSagaStatus(orderNumber, SagaStatus.COMPENSATED, Duration.ofSeconds(15));

        assertThat(compensated.getStatus()).isEqualTo(SagaStatus.COMPENSATED);
        verify(orderSagaStateService).markValidatedAndEnqueueStatusOnly(orderNumber);
        verify(orderSagaStateService).markFailedAndEnqueueRelease(
                eq(orderNumber),
                eq("PAYMENT_FAILED"),
                contains("payment:")
        );
        verify(orderSagaStateService, never()).markCompletedAndEnqueueConfirm(orderNumber);
    }

    @Test
    void shouldResumePersistedWaitingSagaWhenDelayedInventoryReplyArrives() {
        String orderNumber = "order-delayed-reply-" + UUID.randomUUID();
        when(orderSagaStateService.markValidatedAndEnqueueStatusOnly(orderNumber)).thenReturn(true);
        when(orderSagaStateService.markCompletedAndEnqueueConfirm(orderNumber)).thenReturn(true);
        workflowPublisher.pauseInventoryReplies();

        workflowService.startOrderSaga(
                orderPlaced(orderNumber, List.of(item("SKU-DELAYED", 1))),
                sourceEnvelope(orderNumber)
        );

        SagaInstance waiting = awaitSagaStatus(orderNumber, SagaStatus.WAITING, Duration.ofSeconds(5));

        assertThat(waiting.getCurrentStep()).isEqualTo("inventoryValidation");
        awaitPendingReplies(1, Duration.ofSeconds(5));

        workflowPublisher.flushInventoryReplies();

        SagaInstance completed = awaitSagaStatus(orderNumber, SagaStatus.COMPLETED, Duration.ofSeconds(15));

        assertThat(completed.getStatus()).isEqualTo(SagaStatus.COMPLETED);
        verify(orderSagaStateService).markValidatedAndEnqueueStatusOnly(orderNumber);
        verify(orderSagaStateService).markCompletedAndEnqueueConfirm(orderNumber);
    }

    @Test
    void shouldCompensateWhenPaymentReplyTimesOut() throws InterruptedException {
        String orderNumber = "order-payment-timeout-" + UUID.randomUUID();
        when(orderSagaStateService.markValidatedAndEnqueueStatusOnly(orderNumber)).thenReturn(true);
        when(orderSagaStateService.markFailedAndEnqueueRelease(eq(orderNumber), eq("PAYMENT_FAILED"), contains("payment timeout"))).thenReturn(true);

        workflowService.startOrderSaga(
                orderPlaced(orderNumber, List.of(item("NO-PAY-1", 1))),
                sourceEnvelope(orderNumber)
        );

        Thread.sleep(900L);
        orchestrator.triggerTimeouts();

        SagaInstance compensated = awaitSagaStatus(orderNumber, SagaStatus.COMPENSATED, Duration.ofSeconds(15));

        assertThat(compensated.getFailureReason()).contains("paymentProcessing");
        verify(orderSagaStateService).markValidatedAndEnqueueStatusOnly(orderNumber);
        verify(orderSagaStateService).markFailedAndEnqueueRelease(
                eq(orderNumber),
                eq("PAYMENT_FAILED"),
                contains("payment timeout")
        );
    }

    private SagaInstance awaitSagaStatus(String sagaId, SagaStatus expected, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        SagaInstance lastSnapshot = null;
        while (System.nanoTime() < deadline) {
            lastSnapshot = orchestrator.findById(sagaId).orElse(null);
            if (lastSnapshot != null && lastSnapshot.getStatus() == expected) {
                return lastSnapshot;
            }
            if (lastSnapshot != null && lastSnapshot.getStatus().isTerminal() && lastSnapshot.getStatus() != expected) {
                break;
            }
            try {
                Thread.sleep(100L);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting for saga status " + expected);
            }
        }
        fail("Expected saga %s to reach %s but was %s".formatted(
                sagaId,
                expected,
                lastSnapshot == null ? "<missing>" : lastSnapshot.getStatus()
        ));
        return null;
    }

    private void awaitPendingReplies(int expected, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            if (workflowPublisher.pendingReplies().size() == expected) {
                return;
            }
            try {
                Thread.sleep(50L);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                fail("Interrupted while waiting for pending replies " + expected);
            }
        }
        fail("Expected pending replies %s but was %s".formatted(expected, workflowPublisher.pendingReplies().size()));
    }

    private OrderPlacedEvent orderPlaced(String orderNumber, List<OrderLineItemRequest> items) {
        return new OrderPlacedEvent(orderNumber, "user-1", items);
    }

    private EventEnvelope sourceEnvelope(String orderNumber) {
        return EventEnvelope.builder()
                .eventId("evt-" + orderNumber)
                .eventType("order.placed.raw.v1")
                .aggregateId(orderNumber)
                .correlationId("incoming-" + orderNumber)
                .requestId("req-" + orderNumber)
                .producer("test")
                .build();
    }

    private static OrderLineItemRequest item(String sku, int quantity) {
        return OrderLineItemRequest.builder()
                .skuCode(sku)
                .quantity(quantity)
                .build();
    }

    @EnableKafka
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @Import({
            OrderWorkflowProperties.class,
            OrderCheckoutSagaConfiguration.class,
            OrderSagaInventoryEventHandler.class,
            OrderSagaInventoryBridgeService.class,
            OrderSagaWorkflowService.class,
            PaymentReplySimulationHandler.class
    })
    static class OrderLsfSagaSpikeTestApplication {

        @Bean
        @Primary
        ObjectMapper objectMapper() {
            return new ObjectMapper().findAndRegisterModules();
        }

        @Bean
        @Primary
        Clock clock() {
            return Clock.systemUTC();
        }

        @Bean(destroyMethod = "shutdown")
        DataSource dataSource() {
            return new EmbeddedDatabaseBuilder()
                    .generateUniqueName(true)
                    .setType(EmbeddedDatabaseType.H2)
                    .build();
        }

        @Bean
        JdbcTemplate jdbcTemplate(DataSource dataSource) {
            return new JdbcTemplate(dataSource);
        }

        @Bean
        PlatformTransactionManager transactionManager(DataSource dataSource) {
            return new DataSourceTransactionManager(dataSource);
        }

        @Bean
        OrderSagaInventorySessionStore orderSagaInventorySessionStore() {
            return new InMemoryOrderSagaInventorySessionStore();
        }

        @Bean
        SimulatedOrderWorkflowPublisher orderWorkflowPublisher(
                ObjectProvider<OrderSagaInventoryBridgeService> bridgeProvider,
                OrderWorkflowProperties workflowProperties
        ) {
            return new SimulatedOrderWorkflowPublisher(bridgeProvider, workflowProperties);
        }

        @Bean
        @Primary
        SimpleMeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }

        @Bean
        OrderSagaEvidenceMetrics orderSagaEvidenceMetrics(
                SimpleMeterRegistry meterRegistry,
                JdbcTemplate jdbcTemplate,
                OrderSagaInventorySessionStore sessionStore,
                LsfSagaProperties sagaProperties
        ) {
            return new OrderSagaEvidenceMetrics(
                    meterRegistry,
                    jdbcTemplate,
                    sessionStore,
                    sagaProperties
            );
        }
    }

    @Component
    public static class PaymentReplySimulationHandler {

        private final LsfPublisher publisher;

        PaymentReplySimulationHandler(LsfPublisher publisher) {
            this.publisher = publisher;
        }

        @LsfEventHandler(value = "order.validated.v1", payload = OrderValidatedEvent.class)
        public void onValidated(EventEnvelope envelope, OrderValidatedEvent payload) {
            boolean skipReply = payload.getItems().stream()
                    .map(OrderLineItemRequest::getSkuCode)
                    .anyMatch(sku -> sku.startsWith("NO-PAY"));

            if (skipReply) {
                return;
            }

            int totalQuantity = payload.getItems().stream().mapToInt(OrderLineItemRequest::getQuantity).sum();
            boolean failPayment = totalQuantity == 2;

            if (failPayment) {
                publisher.publish(
                        "payment-failed-envelope-topic",
                        payload.getOrderNumber(),
                        "payment.failed.v1",
                        payload.getOrderNumber(),
                        new PaymentFailedEvent(payload.getOrderNumber(), "declined"),
                        LsfPublishOptions.builder()
                                .correlationId(payload.getOrderNumber())
                                .causationId(envelope.getEventId())
                                .requestId(envelope.getRequestId())
                                .build()
                );
                return;
            }

            publisher.publish(
                    "payment-processed-envelope-topic",
                    payload.getOrderNumber(),
                    "payment.processed.v1",
                    payload.getOrderNumber(),
                    new PaymentProcessedEvent(payload.getOrderNumber(), "PAY-" + payload.getOrderNumber()),
                    LsfPublishOptions.builder()
                            .correlationId(payload.getOrderNumber())
                            .causationId(envelope.getEventId())
                            .requestId(envelope.getRequestId())
                            .build()
            );
        }
    }

    static class SimulatedOrderWorkflowPublisher implements OrderWorkflowPublisher {

        private final ObjectProvider<OrderSagaInventoryBridgeService> bridgeProvider;
        private final OrderWorkflowProperties workflowProperties;
        private final List<InventoryCheckResult> recordedRequests = new CopyOnWriteArrayList<>();
        private final Deque<InventoryCheckResult> pendingReplies = new ArrayDeque<>();
        private volatile boolean inventoryRepliesPaused;

        SimulatedOrderWorkflowPublisher(
                ObjectProvider<OrderSagaInventoryBridgeService> bridgeProvider,
                OrderWorkflowProperties workflowProperties
        ) {
            this.bridgeProvider = bridgeProvider;
            this.workflowProperties = workflowProperties;
        }

        @Override
        public void publishOrderPlaced(OrderPlacedEvent event) {
            throw new UnsupportedOperationException("Not needed for saga runtime spike test");
        }

        @Override
        public void publishInventoryCheckRequest(com.myexampleproject.common.event.InventoryCheckRequest event) {
            boolean success = !event.getItem().getSkuCode().startsWith("FAIL-INV");
            InventoryCheckResult result = new InventoryCheckResult(
                    event.getOrderNumber(),
                    event.getItem(),
                    success,
                    success ? null : "Inventory rejected for " + event.getItem().getSkuCode()
            );
            recordedRequests.add(result);
            if (inventoryRepliesPaused) {
                pendingReplies.addLast(result);
                return;
            }
            dispatchResult(result);
        }

        List<InventoryCheckResult> recordedRequests() {
            return recordedRequests;
        }

        List<InventoryCheckResult> pendingReplies() {
            return List.copyOf(pendingReplies);
        }

        void pauseInventoryReplies() {
            inventoryRepliesPaused = true;
        }

        void resumeInventoryReplies() {
            inventoryRepliesPaused = false;
        }

        void flushInventoryReplies() {
            inventoryRepliesPaused = false;
            while (!pendingReplies.isEmpty()) {
                dispatchResult(pendingReplies.removeFirst());
            }
        }

        void clear() {
            recordedRequests.clear();
            pendingReplies.clear();
            inventoryRepliesPaused = false;
        }

        private void dispatchResult(InventoryCheckResult result) {
            bridgeProvider.getObject().handleInventoryCheckResult(
                    result,
                    workflowProperties.getSaga().getReplyTopic()
            );
        }
    }

    static class InMemoryOrderSagaInventorySessionStore implements OrderSagaInventorySessionStore {

        private static final Duration TTL = Duration.ofSeconds(2);

        private final ConcurrentMap<String, SagaReplyFanInSession> sessions = new ConcurrentHashMap<>();

        @Override
        public boolean start(String orderNumber, int totalItems, String commandEventId, String requestId) {
            long nowMs = System.currentTimeMillis();
            SagaReplyFanInSession existing = sessions.get(orderNumber);
            if (existing != null && !existing.isExpired(nowMs)) {
                return false;
            }

            sessions.put(orderNumber, SagaReplyFanInSupport.start(orderNumber, totalItems, commandEventId, requestId, nowMs, TTL));
            return true;
        }

        @Override
        public Optional<SagaReplyFanInUpdate> applyResult(InventoryCheckResult result) {
            SagaReplyFanInSession existing = sessions.get(result.getOrderNumber());
            if (existing == null) {
                return Optional.empty();
            }

            long nowMs = System.currentTimeMillis();
            SagaReplyFanInUpdate update = SagaReplyFanInSupport.apply(
                    existing,
                    result.isSuccess()
                            ? SagaReplyFanInSignal.successful()
                            : SagaReplyFanInSignal.failure(result.getReason()),
                    nowMs,
                    TTL
            );
            if (update.outcome() == SagaReplyFanInOutcome.EXPIRED) {
                sessions.remove(result.getOrderNumber());
            } else if (!update.shouldIgnore()) {
                sessions.put(result.getOrderNumber(), update.session());
            }
            return Optional.of(update);
        }

        @Override
        public Optional<SagaReplyFanInSession> find(String orderNumber) {
            return Optional.ofNullable(sessions.get(orderNumber));
        }

        @Override
        public List<SagaReplyFanInSession> findPending(int limit) {
            return sessions.values().stream().limit(Math.max(1, limit)).toList();
        }

        @Override
        public long pendingCount() {
            return sessions.size();
        }

        @Override
        public int purgeExpired(long nowMs, int limit) {
            List<String> expired = sessions.values().stream()
                    .filter(session -> session.isExpired(nowMs))
                    .limit(Math.max(1, limit))
                    .map(SagaReplyFanInSession::aggregateId)
                    .toList();
            expired.forEach(sessions::remove);
            return expired.size();
        }

        @Override
        public void delete(String orderNumber) {
            sessions.remove(orderNumber);
        }
    }
}
