package com.myexampleproject.paymentservice.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PaymentOrderValidatedConsumerModeGuardTest {

    @Test
    void shouldAllowEnvelopeOnlyMode() {
        assertDoesNotThrow(() -> new PaymentOrderValidatedConsumerModeGuard(true, false));
    }

    @Test
    void shouldAllowLegacyOnlyMode() {
        assertDoesNotThrow(() -> new PaymentOrderValidatedConsumerModeGuard(false, true));
    }

    @Test
    void shouldRejectWhenBothModesAreEnabled() {
        assertThrows(
                IllegalStateException.class,
                () -> new PaymentOrderValidatedConsumerModeGuard(true, true)
        );
    }

    @Test
    void shouldRejectWhenBothModesAreDisabled() {
        assertThrows(
                IllegalStateException.class,
                () -> new PaymentOrderValidatedConsumerModeGuard(false, false)
        );
    }
}
