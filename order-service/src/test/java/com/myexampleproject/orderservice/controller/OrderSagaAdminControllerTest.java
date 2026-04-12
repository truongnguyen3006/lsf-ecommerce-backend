package com.myexampleproject.orderservice.controller;

import com.myexampleproject.orderservice.service.OrderSagaAdminService;
import com.myexampleproject.orderservice.service.OrderSagaAdminSnapshot;
import com.myexampleproject.orderservice.service.OrderSagaAggregatorSnapshot;
import com.myexampleproject.orderservice.service.OrderSagaInstanceView;
import com.myexampleproject.orderservice.service.OrderSagaRuntimeCounters;
import com.myexampleproject.orderservice.service.OrderSagaTimeoutSummary;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class OrderSagaAdminControllerTest {

    @Test
    void snapshotExposesWorkflowModeAndRecentSagaInstances() {
        OrderSagaAdminService service = mock(OrderSagaAdminService.class);
        OrderSagaAdminController controller = new OrderSagaAdminController(service);
        OrderSagaAdminSnapshot snapshot = new OrderSagaAdminSnapshot(
                "LSF_SAGA",
                "LSF_SAGA",
                "LEGACY",
                true,
                true,
                "JDBC",
                "DIRECT",
                Map.of("COMPLETED", 2L),
                new OrderSagaTimeoutSummary(0L, 0L, 0L),
                new OrderSagaAggregatorSnapshot(0L, 0L, 0L, 0L, List.of()),
                new OrderSagaRuntimeCounters(1L, 0L, 0L, 0L, 0L, 0L, 0L, 0L, 0L),
                List.of(new OrderSagaInstanceView(
                        "ORDER-1",
                        "order-checkout-saga",
                        "COMPLETED",
                        "FORWARD",
                        null,
                        "ORDER-1",
                        null,
                        null,
                        10L,
                        20L
                )),
                List.of(),
                List.of(),
                List.of()
        );
        when(service.snapshot()).thenReturn(snapshot);

        OrderSagaAdminSnapshot payload = controller.snapshot();

        assertThat(payload.workflowMode()).isEqualTo("LSF_SAGA");
        assertThat(payload.defaultWorkflowMode()).isEqualTo("LSF_SAGA");
        assertThat(payload.rollbackWorkflowMode()).isEqualTo("LEGACY");
        assertThat(payload.rollbackAvailable()).isTrue();
        assertThat(payload.sagaEnabled()).isTrue();
        assertThat(payload.transportMode()).isEqualTo("DIRECT");
        assertThat(payload.summary()).containsEntry("COMPLETED", 2L);
        assertThat(payload.runtimeCounters().legacyStartsSinceBoot()).isEqualTo(1L);
        assertThat(payload.recentInstances()).hasSize(1);
        assertThat(payload.recentInstances().getFirst().sagaId()).isEqualTo("ORDER-1");
    }
}
