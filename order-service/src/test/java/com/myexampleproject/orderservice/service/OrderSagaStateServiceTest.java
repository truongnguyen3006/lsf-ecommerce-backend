package com.myexampleproject.orderservice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myexampleproject.orderservice.model.Order;
import com.myexampleproject.orderservice.model.OrderLineItems;
import com.myexampleproject.orderservice.repository.OrderRepository;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.outbox.OutboxWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderSagaStateServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private OutboxWriter outboxWriter;

    @Test
    void shouldAppendStatusAndValidatedEnvelopeWhenOrderBecomesValidated() {
        OrderOutboxEnvelopeFactory envelopeFactory = new OrderOutboxEnvelopeFactory(new ObjectMapper());
        OrderSagaStateService service = new OrderSagaStateService(orderRepository, outboxWriter, envelopeFactory);
        Order order = orderWithSingleItem("ORDER-1", "PENDING");

        when(orderRepository.findByOrderNumberWithItems("ORDER-1")).thenReturn(Optional.of(order));
        when(outboxWriter.append(any(EventEnvelope.class), anyString(), anyString())).thenReturn(1L);

        boolean changed = service.markValidatedAndEnqueueStatus("ORDER-1");

        assertTrue(changed);
        assertEquals("VALIDATED", order.getStatus());

        ArgumentCaptor<EventEnvelope> envelopeCaptor = ArgumentCaptor.forClass(EventEnvelope.class);
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);

        verify(orderRepository).save(order);
        verify(outboxWriter, times(2)).append(envelopeCaptor.capture(), topicCaptor.capture(), keyCaptor.capture());

        List<String> topics = topicCaptor.getAllValues();
        List<EventEnvelope> envelopes = envelopeCaptor.getAllValues();

        assertEquals(List.of("order-status-envelope-topic", "order-validated-envelope-topic"), topics);
        assertEquals("ecommerce.order.status.v1", envelopes.get(0).getEventType());
        assertEquals("order.validated.v1", envelopes.get(1).getEventType());
        assertEquals(List.of("ORDER-1", "ORDER-1"), keyCaptor.getAllValues());
        assertEquals("ORDER-1", envelopes.get(1).getPayload().get("orderNumber").asText());
        assertEquals("SKU-1", envelopes.get(1).getPayload().get("items").get(0).get("skuCode").asText());
        assertEquals(1, envelopes.get(1).getPayload().get("items").get(0).get("quantity").asInt());
    }

    @Test
    void shouldAppendConfirmAndStatusEnvelopeWhenPaymentSucceeds() {
        OrderOutboxEnvelopeFactory envelopeFactory = new OrderOutboxEnvelopeFactory(new ObjectMapper());
        OrderSagaStateService service = new OrderSagaStateService(orderRepository, outboxWriter, envelopeFactory);
        Order order = orderWithSingleItem("ORDER-2", "VALIDATED");

        when(orderRepository.findByOrderNumberWithItems("ORDER-2")).thenReturn(Optional.of(order));
        when(outboxWriter.append(any(EventEnvelope.class), anyString(), anyString())).thenReturn(1L);

        boolean changed = service.markCompletedAndEnqueueConfirm("ORDER-2");

        assertTrue(changed);
        assertEquals("COMPLETED", order.getStatus());

        ArgumentCaptor<EventEnvelope> envelopeCaptor = ArgumentCaptor.forClass(EventEnvelope.class);
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);

        verify(orderRepository).save(order);
        verify(outboxWriter, times(2)).append(envelopeCaptor.capture(), topicCaptor.capture(), keyCaptor.capture());

        List<String> topics = topicCaptor.getAllValues();
        List<EventEnvelope> envelopes = envelopeCaptor.getAllValues();

        assertEquals(List.of("inventory-reservation-confirm-envelope-topic", "order-status-envelope-topic"), topics);
        assertEquals("inventory.reservation.confirm.v1", envelopes.get(0).getEventType());
        assertEquals("ecommerce.order.status.v1", envelopes.get(1).getEventType());
        assertEquals(List.of("SKU-1", "ORDER-2"), keyCaptor.getAllValues());
        assertEquals("ORDER-2", envelopes.get(0).getPayload().get("workflowId").asText());
        assertEquals("SKU-1", envelopes.get(0).getPayload().get("resourceId").asText());
        assertEquals(1, envelopes.get(0).getPayload().get("quantity").asInt());
    }

    @Test
    void shouldAppendOnlyStatusEnvelopeWhenSagaMarksOrderValidated() {
        OrderOutboxEnvelopeFactory envelopeFactory = new OrderOutboxEnvelopeFactory(new ObjectMapper());
        OrderSagaStateService service = new OrderSagaStateService(orderRepository, outboxWriter, envelopeFactory);
        Order order = orderWithSingleItem("ORDER-SAGA-1", "PENDING");

        when(orderRepository.findByOrderNumberWithItems("ORDER-SAGA-1")).thenReturn(Optional.of(order));
        when(outboxWriter.append(any(EventEnvelope.class), anyString(), anyString())).thenReturn(1L);

        boolean changed = service.markValidatedAndEnqueueStatusOnly("ORDER-SAGA-1");

        assertTrue(changed);
        assertEquals("VALIDATED", order.getStatus());

        ArgumentCaptor<EventEnvelope> envelopeCaptor = ArgumentCaptor.forClass(EventEnvelope.class);
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);

        verify(orderRepository).save(order);
        verify(outboxWriter, times(1)).append(envelopeCaptor.capture(), topicCaptor.capture(), keyCaptor.capture());

        assertEquals("order-status-envelope-topic", topicCaptor.getValue());
        assertEquals("ecommerce.order.status.v1", envelopeCaptor.getValue().getEventType());
        assertEquals("ORDER-SAGA-1", keyCaptor.getValue());
    }

    @Test
    void shouldAppendReleaseAndStatusEnvelopeWhenPaymentFails() {
        OrderOutboxEnvelopeFactory envelopeFactory = new OrderOutboxEnvelopeFactory(new ObjectMapper());
        OrderSagaStateService service = new OrderSagaStateService(orderRepository, outboxWriter, envelopeFactory);
        Order order = orderWithSingleItem("ORDER-3", "VALIDATED");

        when(orderRepository.findByOrderNumberWithItems("ORDER-3")).thenReturn(Optional.of(order));
        when(outboxWriter.append(any(EventEnvelope.class), anyString(), anyString())).thenReturn(1L);

        boolean changed = service.markFailedAndEnqueueRelease("ORDER-3", "PAYMENT_FAILED", "payment: declined");

        assertTrue(changed);
        assertEquals("PAYMENT_FAILED", order.getStatus());

        ArgumentCaptor<EventEnvelope> envelopeCaptor = ArgumentCaptor.forClass(EventEnvelope.class);
        ArgumentCaptor<String> topicCaptor = ArgumentCaptor.forClass(String.class);
        ArgumentCaptor<String> keyCaptor = ArgumentCaptor.forClass(String.class);

        verify(orderRepository).save(order);
        verify(outboxWriter, times(2)).append(envelopeCaptor.capture(), topicCaptor.capture(), keyCaptor.capture());

        List<String> topics = topicCaptor.getAllValues();
        List<EventEnvelope> envelopes = envelopeCaptor.getAllValues();

        assertEquals(List.of("inventory-reservation-release-envelope-topic", "order-status-envelope-topic"), topics);
        assertEquals("inventory.reservation.release.v1", envelopes.get(0).getEventType());
        assertEquals("ecommerce.order.status.v1", envelopes.get(1).getEventType());
        assertEquals(List.of("SKU-1", "ORDER-3"), keyCaptor.getAllValues());
        assertEquals("ORDER-3", envelopes.get(0).getPayload().get("workflowId").asText());
        assertEquals("payment: declined", envelopes.get(0).getPayload().get("reason").asText());
    }

    private Order orderWithSingleItem(String orderNumber, String status) {
        Order order = new Order();
        order.setOrderNumber(orderNumber);
        order.setStatus(status);

        OrderLineItems item = new OrderLineItems();
        item.setSkuCode("SKU-1");
        item.setQuantity(1);
        item.setPrice(BigDecimal.TEN);
        item.setOrder(order);
        order.setOrderLineItemsList(List.of(item));
        return order;
    }
}
