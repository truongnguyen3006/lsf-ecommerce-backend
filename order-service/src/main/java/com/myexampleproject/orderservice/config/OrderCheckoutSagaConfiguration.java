package com.myexampleproject.orderservice.config;

import com.myexampleproject.common.event.OrderValidatedEvent;
import com.myexampleproject.common.event.PaymentFailedEvent;
import com.myexampleproject.common.event.PaymentProcessedEvent;
import com.myexampleproject.orderservice.service.OrderCheckoutSagaState;
import com.myexampleproject.orderservice.service.OrderInventoryReleaseCompletedReply;
import com.myexampleproject.orderservice.service.OrderInventoryReleaseRequestedCommand;
import com.myexampleproject.orderservice.service.OrderInventoryValidationFailedReply;
import com.myexampleproject.orderservice.service.OrderInventoryValidationRequestedCommand;
import com.myexampleproject.orderservice.service.OrderInventoryValidationSucceededReply;
import com.myexampleproject.orderservice.service.OrderSagaEvidenceMetrics;
import com.myexampleproject.orderservice.service.OrderSagaMessagingConstants;
import com.myexampleproject.orderservice.service.OrderSagaStateService;
import com.myorg.lsf.saga.SagaDefinition;
import com.myorg.lsf.saga.SagaFailureMode;
import com.myorg.lsf.saga.SagaReplyDecision;
import com.myorg.lsf.saga.SagaStep;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration(proxyBeanMethods = false)
public class OrderCheckoutSagaConfiguration {

    @Bean
    public SagaDefinition<OrderCheckoutSagaState> orderCheckoutSagaDefinition(
            OrderWorkflowProperties workflowProperties,
            OrderSagaStateService orderSagaStateService,
            OrderSagaEvidenceMetrics evidenceMetrics
    ) {
        return SagaDefinition.<OrderCheckoutSagaState>builder(
                        workflowProperties.getSaga().getDefinitionName(),
                        OrderCheckoutSagaState.class
                )
                .step(SagaStep.<OrderCheckoutSagaState>builder("inventoryValidation")
                        .command(context -> context.command(
                                workflowProperties.getSaga().getInternalTopic(),
                                context.state().orderNumber(),
                                OrderSagaMessagingConstants.INVENTORY_VALIDATION_REQUESTED_EVENT_TYPE,
                                context.state().orderNumber(),
                                new OrderInventoryValidationRequestedCommand(
                                        context.state().orderNumber(),
                                        context.state().items()
                                )
                        ))
                        .onReply(
                                OrderSagaMessagingConstants.INVENTORY_VALIDATED_EVENT_TYPE,
                                OrderInventoryValidationSucceededReply.class,
                                (context, envelope, payload) -> {
                                    orderSagaStateService.markValidatedAndEnqueueStatusOnly(
                                            context.state().orderNumber()
                                    );
                                    return SagaReplyDecision.success(
                                            context.state().withInventoryValidated(),
                                            "inventory validated"
                                    );
                                }
                        )
                        .onReply(
                                OrderSagaMessagingConstants.INVENTORY_VALIDATION_FAILED_EVENT_TYPE,
                                OrderInventoryValidationFailedReply.class,
                                (context, envelope, payload) -> {
                                    orderSagaStateService.markFailedAndEnqueueRelease(
                                            context.state().orderNumber(),
                                            "FAILED",
                                            payload.reason()
                                    );
                                    evidenceMetrics.recordSagaFailed();
                                    return SagaReplyDecision.failure(
                                            context.state().withInventoryFailure(payload.reason()),
                                            payload.reason()
                                    );
                                }
                        )
                        .compensation(compensation -> compensation
                                .command(context -> context.command(
                                        workflowProperties.getSaga().getInternalTopic(),
                                        context.state().orderNumber(),
                                        OrderSagaMessagingConstants.INVENTORY_RELEASE_REQUESTED_EVENT_TYPE,
                                        context.state().orderNumber(),
                                        new OrderInventoryReleaseRequestedCommand(
                                                context.state().orderNumber(),
                                                context.state().compensationStatusOr("PAYMENT_FAILED"),
                                                context.state().compensationReasonOr("payment timeout")
                                        )
                                ))
                                .timeout(workflowProperties.getSaga().getCompensationTimeout())
                                .onReply(
                                        OrderSagaMessagingConstants.INVENTORY_RELEASE_COMPLETED_EVENT_TYPE,
                                        OrderInventoryReleaseCompletedReply.class,
                                        (context, envelope, payload) -> {
                                            evidenceMetrics.recordSagaCompensated();
                                            return SagaReplyDecision.success(
                                                    context.state().withCompensationCompleted(),
                                                    payload.status()
                                            );
                                        }
                                )
                        )
                        .timeout(workflowProperties.getSaga().getInventoryStepTimeout())
                        .failureMode(SagaFailureMode.COMPENSATE)
                        .build())
                .step(SagaStep.<OrderCheckoutSagaState>builder("paymentProcessing")
                        .command(context -> context.command(
                                "order-validated-envelope-topic",
                                context.state().orderNumber(),
                                "order.validated.v1",
                                context.state().orderNumber(),
                                new OrderValidatedEvent(
                                        context.state().orderNumber(),
                                        context.state().items()
                                )
                        ))
                        .onReply(
                                "payment.processed.v1",
                                PaymentProcessedEvent.class,
                                (context, envelope, payload) -> {
                                    orderSagaStateService.markCompletedAndEnqueueConfirm(
                                            context.state().orderNumber()
                                    );
                                    evidenceMetrics.recordSagaCompleted();
                                    return SagaReplyDecision.success(
                                            context.state().withPaymentSucceeded(),
                                            "payment processed"
                                    );
                                }
                        )
                        .onReply(
                                "payment.failed.v1",
                                PaymentFailedEvent.class,
                                (context, envelope, payload) -> SagaReplyDecision.failure(
                                        context.state().withPaymentFailure("payment: " + payload.getReason()),
                                        payload.getReason()
                                )
                        )
                        .timeout(workflowProperties.getSaga().getPaymentStepTimeout())
                        .failureMode(SagaFailureMode.COMPENSATE)
                        .build())
                .build();
    }
}
