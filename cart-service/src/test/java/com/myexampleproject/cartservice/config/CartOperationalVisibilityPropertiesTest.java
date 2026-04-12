package com.myexampleproject.cartservice.config;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class CartOperationalVisibilityPropertiesTest {

    @Test
    void applicationPropertiesEnableOperationalVisibility() throws IOException {
        Properties properties = loadProperties();

        assertThat(properties.getProperty("management.endpoints.web.exposure.include"))
                .isEqualTo("health,info,metrics,prometheus,beans,conditions");
        assertThat(properties.getProperty("management.endpoint.metrics.enabled")).isEqualTo("true");
        assertThat(properties.getProperty("management.prometheus.metrics.export.enabled")).isEqualTo("true");
        assertThat(properties.getProperty("management.tracing.sampling.probability")).isEqualTo("1.0");
        assertThat(properties.getProperty("spring.kafka.consumer.group-id")).isEqualTo("cart-cleaner-group");
        assertThat(properties.getProperty("lsf.eventing.listener.enabled")).isEqualTo("true");
        assertThat(properties.getProperty("lsf.eventing.consume-topics[0]")).isEqualTo("cart-checkout-topic");
        assertThat(properties.getProperty("lsf.observability.enabled")).isEqualTo("true");
        assertThat(properties.getProperty("lsf.observability.tag-event-id")).isEqualTo("false");
    }

    private Properties loadProperties() throws IOException {
        Properties properties = new Properties();
        try (InputStream stream = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            assertThat(stream).isNotNull();
            properties.load(new InputStreamReader(stream, StandardCharsets.UTF_8));
        }
        return properties;
    }
}
