package com.myexampleproject.orderservice.config;

import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class OrderWorkflowModePropertiesTest {

    @Test
    void shouldKeepLsfSagaAsDefaultWorkflowModeAfterCutover() throws Exception {
        Properties properties = load("application.properties");

        assertThat(properties.getProperty("app.order.workflow.mode")).isEqualTo("lsf-saga");
        assertThat(properties.getProperty("lsf.saga.enabled")).isEqualTo("true");
        assertThat(properties.getProperty("lsf.saga.store")).isEqualTo("jdbc");
        assertThat(properties.getProperty("lsf.saga.transport.mode")).isEqualTo("direct");
    }

    @Test
    void shouldExposeLegacyAsRollbackModeInCodeDefaults() {
        OrderWorkflowProperties properties = new OrderWorkflowProperties();

        assertThat(properties.getMode()).isEqualTo(OrderWorkflowMode.LSF_SAGA);
        assertThat(properties.getDefaultMode()).isEqualTo(OrderWorkflowMode.LSF_SAGA);
        assertThat(properties.getRollbackMode()).isEqualTo(OrderWorkflowMode.LEGACY);
        assertThat(properties.isRollbackAvailable()).isTrue();
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
