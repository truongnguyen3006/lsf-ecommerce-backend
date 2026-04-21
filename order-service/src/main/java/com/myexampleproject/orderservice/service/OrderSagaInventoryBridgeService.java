package com.myexampleproject.orderservice.service;

import com.myexampleproject.common.dto.OrderLineItemRequest;
import com.myexampleproject.common.event.InventoryCheckRequest;
import com.myexampleproject.common.event.InventoryCheckResult;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.eventing.LsfPublishOptions;
import com.myorg.lsf.eventing.LsfPublisher;
import com.myorg.lsf.saga.SagaReplyFanInOutcome;
import com.myorg.lsf.saga.SagaReplyFanInSession;
import com.myorg.lsf.saga.SagaReplyFanInUpdate;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderSagaInventoryBridgeService {

    private final OrderWorkflowPublisher orderWorkflowPublisher;
    private final OrderSagaInventorySessionStore sessionStore;
    private final LsfPublisher lsfPublisher;
    private final OrderSagaStateService orderSagaStateService;
    private final OrderSagaEvidenceMetrics evidenceMetrics;

    public void startInventoryValidation(
            EventEnvelope envelope,
            OrderInventoryValidationRequestedCommand command,
            String replyTopic
    ) {
        if (command.items() == null || command.items().isEmpty()) {
            publishValidationSuccess(command.orderNumber(), 0, envelope, replyTopic);
            return;
        }

        boolean started = sessionStore.start(
                command.orderNumber(),
                command.items().size(),
                envelope.getEventId(),
                envelope.getRequestId()
        );

        if (!started) {
            evidenceMetrics.recordAggregatorDuplicateStart();
            log.info(
                    "Ignoring duplicate inventory validation bridge start for order {}.",
                    command.orderNumber()
            );
            return;
        }

        for (OrderLineItemRequest item : command.items()) {
            orderWorkflowPublisher.publishInventoryCheckRequest(
                    new InventoryCheckRequest(command.orderNumber(), item, command.paymentMethod())
            );
        }

        log.info(
                "Started saga inventory validation bridge for order {} with {} items.",
                command.orderNumber(),
                command.items().size()
        );
    }

    public void handleInventoryCheckResult(InventoryCheckResult result, String replyTopic) {
        boolean matched = sessionStore.applyResult(result)
                .map(update -> handleFanInUpdate(result.getOrderNumber(), update, replyTopic))
                .orElse(false);

        if (!matched) {
            log.debug(
                    "Ignoring inventory result for order {} because no saga inventory bridge session exists.",
                    result.getOrderNumber()
            );
            evidenceMetrics.recordAggregatorLateResultIgnored();
        }
    }

    public void applyInventoryRelease(
            EventEnvelope envelope,
            OrderInventoryReleaseRequestedCommand command,
            String replyTopic
    ) {
        orderSagaStateService.markFailedAndEnqueueRelease(
                command.orderNumber(),
                command.targetStatus(),
                command.reason()
        );

        lsfPublisher.publish(
                replyTopic,
                command.orderNumber(),
                OrderSagaMessagingConstants.INVENTORY_RELEASE_COMPLETED_EVENT_TYPE,
                command.orderNumber(),
                new OrderInventoryReleaseCompletedReply(command.orderNumber(), command.targetStatus()),
                LsfPublishOptions.builder()
                        .correlationId(resolveCorrelationId(command.orderNumber(), envelope))
                        .causationId(envelope.getEventId())
                        .requestId(envelope.getRequestId())
                        .build()
        );
    }

    private boolean handleFanInUpdate(
            String orderNumber,
            SagaReplyFanInUpdate update,
            String replyTopic
    ) {
        if (update.outcome() == SagaReplyFanInOutcome.EXPIRED
                || update.outcome() == SagaReplyFanInOutcome.IGNORED_ALREADY_COMPLETE) {
            return false;
        }

        if (update.outcome() == SagaReplyFanInOutcome.IN_PROGRESS) {
            return true;
        }

        SagaReplyFanInSession session = update.session();
        if (update.outcome() == SagaReplyFanInOutcome.COMPLETED_FAILURE) {
            publishValidationFailure(
                    orderNumber,
                    session.failureReason(),
                    session.commandEventId(),
                    session.requestId(),
                    replyTopic
            );
        } else {
            publishValidationSuccess(
                    orderNumber,
                    session.expectedReplies(),
                    session.commandEventId(),
                    session.requestId(),
                    replyTopic
            );
        }

        sessionStore.delete(orderNumber);
        return true;
    }

    private void publishValidationSuccess(String orderNumber, int checkedItems, EventEnvelope envelope, String replyTopic) {
        publishValidationSuccess(
                orderNumber,
                checkedItems,
                envelope.getEventId(),
                envelope.getRequestId(),
                replyTopic
        );
    }

    private void publishValidationSuccess(
            String orderNumber,
            int checkedItems,
            String causationId,
            String requestId,
            String replyTopic
    ) {
        lsfPublisher.publish(
                replyTopic,
                orderNumber,
                OrderSagaMessagingConstants.INVENTORY_VALIDATED_EVENT_TYPE,
                orderNumber,
                new OrderInventoryValidationSucceededReply(orderNumber, checkedItems),
                LsfPublishOptions.builder()
                        .correlationId(orderNumber)
                        .causationId(causationId)
                        .requestId(requestId)
                        .build()
        );
    }

    private void publishValidationFailure(
            String orderNumber,
            String reason,
            String causationId,
            String requestId,
            String replyTopic
    ) {
        lsfPublisher.publish(
                replyTopic,
                orderNumber,
                OrderSagaMessagingConstants.INVENTORY_VALIDATION_FAILED_EVENT_TYPE,
                orderNumber,
                new OrderInventoryValidationFailedReply(orderNumber, reason),
                LsfPublishOptions.builder()
                        .correlationId(orderNumber)
                        .causationId(causationId)
                        .requestId(requestId)
                        .build()
        );
    }

    private String resolveCorrelationId(String orderNumber, EventEnvelope envelope) {
        return envelope.getCorrelationId() == null || envelope.getCorrelationId().isBlank()
                ? orderNumber
                : envelope.getCorrelationId();
    }
}
