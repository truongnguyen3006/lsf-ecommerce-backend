package com.myexampleproject.orderservice.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
@ConfigurationProperties(prefix = "app.order.workflow")
@Data
public class OrderWorkflowProperties {

    public static final OrderWorkflowMode DEFAULT_MODE = OrderWorkflowMode.LSF_SAGA;
    public static final OrderWorkflowMode ROLLBACK_MODE = OrderWorkflowMode.LEGACY;

    private OrderWorkflowMode mode = DEFAULT_MODE;

    private final Saga saga = new Saga();

    public boolean isSagaMode() {
        return mode == OrderWorkflowMode.LSF_SAGA;
    }

    public OrderWorkflowMode getDefaultMode() {
        return DEFAULT_MODE;
    }

    public OrderWorkflowMode getRollbackMode() {
        return ROLLBACK_MODE;
    }

    public boolean isRollbackAvailable() {
        return true;
    }

    @Data
    public static class Saga {
        private String definitionName = "order-checkout-saga";
        private String internalTopic = "order-saga-internal-topic";
        private String replyTopic = "order-saga-replies-topic";
        private int recentLimit = 10;
        private int pendingAggregatorLimit = 10;
        private int aggregatorCleanupBatch = 100;
        private Duration aggregatorTtl = Duration.ofMinutes(10);
        private Duration inventoryStepTimeout = Duration.ofSeconds(30);
        private Duration paymentStepTimeout = Duration.ofSeconds(45);
        private Duration compensationTimeout = Duration.ofSeconds(30);
    }
}
