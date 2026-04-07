package com.myexampleproject.paymentservice.config;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class PaymentResultRuntimeDefaultsTest {

    @Test
    void shouldUseEnvelopeNativePaymentResultRuntime() throws Exception {
        Properties properties = load("application.properties");

        assertThat(properties.getProperty("app.payment.order-validated-envelope-listener.enabled")).isEqualTo("true");
        assertThat(properties.getProperty("app.payment.legacy-order-validated-listener.enabled")).isEqualTo("false");
        assertThat(properties.getProperty("app.payment.result-legacy-publish.enabled")).isNull();
        assertThat(properties.getProperty("app.payment.result-envelope-publish.enabled")).isNull();
        assertThat(properties.getProperty("app.payment.result-cutover.envelope-only-required")).isNull();
    }

    @Test
    void shouldRemoveLegacyPaymentResultProfiles() {
        assertThat(Files.exists(Path.of("src/main/resources/application-payment-result-rollback.properties"))).isFalse();
        assertThat(Files.exists(Path.of("src/main/resources/application-cutover-envelope-only.properties"))).isFalse();
    }

    private Properties load(String resourceName) throws Exception {
        Properties properties = new Properties();
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream(resourceName)) {
            if (stream == null) {
                throw new IllegalStateException("Missing resource " + resourceName);
            }
            properties.load(stream);
        }
        return properties;
    }
}
