package com.myexampleproject.orderservice.service;

import com.myexampleproject.orderservice.config.OrderWorkflowProperties;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.eventing.LsfEventHandler;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OrderSagaInventoryEventHandler {

    private final OrderSagaInventoryBridgeService bridgeService;
    private final OrderWorkflowProperties workflowProperties;

    @LsfEventHandler(
            value = OrderSagaMessagingConstants.INVENTORY_VALIDATION_REQUESTED_EVENT_TYPE,
            payload = OrderInventoryValidationRequestedCommand.class
    )
    public void handleInventoryValidation(
            EventEnvelope envelope,
            OrderInventoryValidationRequestedCommand payload
    ) {
        bridgeService.startInventoryValidation(
                envelope,
                payload,
                workflowProperties.getSaga().getReplyTopic()
        );
    }

    @LsfEventHandler(
            value = OrderSagaMessagingConstants.INVENTORY_RELEASE_REQUESTED_EVENT_TYPE,
            payload = OrderInventoryReleaseRequestedCommand.class
    )
    public void handleInventoryRelease(
            EventEnvelope envelope,
            OrderInventoryReleaseRequestedCommand payload
    ) {
        bridgeService.applyInventoryRelease(
                envelope,
                payload,
                workflowProperties.getSaga().getReplyTopic()
        );
    }
}
