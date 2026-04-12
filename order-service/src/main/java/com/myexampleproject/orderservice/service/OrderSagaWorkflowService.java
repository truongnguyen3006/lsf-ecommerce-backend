package com.myexampleproject.orderservice.service;

import com.myexampleproject.common.event.OrderPlacedEvent;
import com.myexampleproject.orderservice.config.OrderWorkflowProperties;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.saga.LsfSagaOrchestrator;
import com.myorg.lsf.saga.SagaStartOptions;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
@RequiredArgsConstructor
public class OrderSagaWorkflowService {

    private final LsfSagaOrchestrator orchestrator;
    private final OrderWorkflowProperties workflowProperties;
    private final OrderSagaEvidenceMetrics evidenceMetrics;

    public void startOrderSaga(OrderPlacedEvent event, EventEnvelope sourceEnvelope) {
        String orderNumber = event.getOrderNumber();
        OrderCheckoutSagaState initialState = OrderCheckoutSagaState.initial(
                orderNumber,
                event.getOrderLineItemsDtoList()
        );

        SagaStartOptions.SagaStartOptionsBuilder builder = SagaStartOptions.builder()
                .correlationId(resolveCorrelationId(orderNumber, sourceEnvelope));

        if (sourceEnvelope != null && StringUtils.hasText(sourceEnvelope.getRequestId())) {
            builder.requestId(sourceEnvelope.getRequestId());
        }
        if (sourceEnvelope != null && StringUtils.hasText(sourceEnvelope.getEventId())) {
            builder.causationId(sourceEnvelope.getEventId());
        }

        evidenceMetrics.recordSagaStarted();
        orchestrator.start(
                workflowProperties.getSaga().getDefinitionName(),
                orderNumber,
                initialState,
                builder.build()
        );
    }

    private String resolveCorrelationId(String orderNumber, EventEnvelope sourceEnvelope) {
        return orderNumber;
    }
}
