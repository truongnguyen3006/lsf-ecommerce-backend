package com.myexampleproject.paymentservice.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class PaymentOrderValidatedConsumerModeGuard {

    public PaymentOrderValidatedConsumerModeGuard(
            @Value("${app.payment.order-validated-envelope-listener.enabled:true}") boolean envelopeEnabled,
            @Value("${app.payment.legacy-order-validated-listener.enabled:false}") boolean legacyEnabled
    ) {
        if (envelopeEnabled == legacyEnabled) {
            throw new IllegalStateException(
                    "Exactly one order-validated consumer path must be enabled in payment-service. " +
                            "Set app.payment.order-validated-envelope-listener.enabled and " +
                            "app.payment.legacy-order-validated-listener.enabled to opposite values."
            );
        }
    }
}
