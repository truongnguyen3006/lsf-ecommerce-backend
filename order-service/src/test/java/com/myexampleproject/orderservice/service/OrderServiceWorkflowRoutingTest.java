package com.myexampleproject.orderservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myexampleproject.common.dto.OrderLineItemRequest;
import com.myexampleproject.common.dto.PaymentMethod;
import com.myexampleproject.common.event.OrderPlacedEvent;
import com.myexampleproject.orderservice.config.OrderWorkflowMode;
import com.myexampleproject.orderservice.config.OrderWorkflowProperties;
import com.myexampleproject.orderservice.dto.OrderRequest;
import com.myexampleproject.orderservice.repository.OrderRepository;
import com.myorg.lsf.outbox.OutboxWriter;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

class OrderServiceWorkflowRoutingTest {

    @Test
    void shouldRouteOrderPlacementToSagaPathByDefault() {
        OrderWorkflowProperties workflowProperties = new OrderWorkflowProperties();
        OrderSagaEvidenceMetrics evidenceMetrics = mock(OrderSagaEvidenceMetrics.class);
        OrderService service = spy(service(workflowProperties, evidenceMetrics));
        OrderPlacedEvent event = sampleEvent("ORDER-SAGA");

        doNothing().when(service).handleSagaOrderPlacement(any(), eq(event));
        doNothing().when(service).handleLegacyOrderPlacement(any());

        service.handleOrderPlacement(null, event);

        verify(evidenceMetrics).recordWorkflowModeStart(OrderWorkflowMode.LSF_SAGA);
        verify(service).handleSagaOrderPlacement(null, event);
        verify(service, never()).handleLegacyOrderPlacement(any());
    }

    @Test
    void shouldKeepLegacyRollbackPathAvailable() {
        OrderWorkflowProperties workflowProperties = new OrderWorkflowProperties();
        workflowProperties.setMode(OrderWorkflowMode.LEGACY);

        OrderSagaEvidenceMetrics evidenceMetrics = mock(OrderSagaEvidenceMetrics.class);
        OrderService service = spy(service(workflowProperties, evidenceMetrics));
        OrderPlacedEvent event = sampleEvent("ORDER-LEGACY");

        doNothing().when(service).handleSagaOrderPlacement(any(), any());
        doNothing().when(service).handleLegacyOrderPlacement(eq(event));

        service.handleOrderPlacement(null, event);

        verify(evidenceMetrics).recordWorkflowModeStart(OrderWorkflowMode.LEGACY);
        verify(service).handleLegacyOrderPlacement(event);
        verify(service, never()).handleSagaOrderPlacement(any(), any());
    }

    @Test
    void placeOrderShouldStartLocalWorkflowInsteadOfPublishingSelfLoopEvent() {
        OrderWorkflowProperties workflowProperties = new OrderWorkflowProperties();
        OrderSagaEvidenceMetrics evidenceMetrics = mock(OrderSagaEvidenceMetrics.class);
        OrderWorkflowPublisher workflowPublisher = mock(OrderWorkflowPublisher.class);
        OrderService service = spy(service(workflowProperties, evidenceMetrics, workflowPublisher));

        doNothing().when(service).handleOrderPlacement(any(OrderPlacedEvent.class));

        service.placeOrder(OrderRequest.builder()
                .items(List.of(
                        OrderLineItemRequest.builder().skuCode("SKU-1").quantity(1).build()
                ))
                .paymentMethod(PaymentMethod.MOCK_SUCCESS)
                .build(), "user-1");

        verify(service).handleOrderPlacement(any(OrderPlacedEvent.class));
        verify(workflowPublisher, never()).publishOrderPlaced(any());
    }

    private OrderService service(
            OrderWorkflowProperties workflowProperties,
            OrderSagaEvidenceMetrics evidenceMetrics
    ) {
        return service(workflowProperties, evidenceMetrics, mock(OrderWorkflowPublisher.class));
    }

    private OrderService service(
            OrderWorkflowProperties workflowProperties,
            OrderSagaEvidenceMetrics evidenceMetrics,
            OrderWorkflowPublisher workflowPublisher
    ) {
        return new OrderService(
                mock(OrderRepository.class),
                new ObjectMapper(),
                mock(RedisTemplate.class),
                mock(OutboxWriter.class),
                mock(OrderOutboxEnvelopeFactory.class),
                workflowProperties,
                mock(OrderSagaStateService.class),
                workflowPublisher,
                mock(OrderSagaWorkflowService.class),
                mock(OrderSagaInventoryBridgeService.class),
                evidenceMetrics
        );
    }

    private OrderPlacedEvent sampleEvent(String orderNumber) {
        return new OrderPlacedEvent(
                orderNumber,
                "user-1",
                List.of(OrderLineItemRequest.builder().skuCode("SKU-1").quantity(1).build()),
                PaymentMethod.MOCK_SUCCESS
        );
    }
}
