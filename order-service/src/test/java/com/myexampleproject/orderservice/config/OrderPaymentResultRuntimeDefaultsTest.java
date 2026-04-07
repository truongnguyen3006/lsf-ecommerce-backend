package com.myexampleproject.orderservice.config;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class OrderPaymentResultRuntimeDefaultsTest {

    @Test
    void shouldUseEnvelopeNativeDefaultsForPaymentResultCorridor() throws Exception {
        Properties properties = load("application.properties");

        assertThat(properties.getProperty("lsf.eventing.listener.enabled")).isEqualTo("true");
        assertThat(properties.getProperty("lsf.eventing.consume-topics[0]")).isEqualTo("payment-processed-envelope-topic");
        assertThat(properties.getProperty("lsf.eventing.consume-topics[1]")).isEqualTo("payment-failed-envelope-topic");
        assertThat(properties.getProperty("app.order.payment-result-legacy-listener.enabled")).isNull();
        assertThat(properties.getProperty("app.order.payment-result-joiner.source")).isNull();
        assertThat(properties.getProperty("app.order.payment-result-cutover.envelope-only-required")).isNull();
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
