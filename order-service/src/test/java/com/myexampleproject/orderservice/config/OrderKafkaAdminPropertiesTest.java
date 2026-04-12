package com.myexampleproject.orderservice.config;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class OrderKafkaAdminPropertiesTest {

    @Test
    void applicationPropertiesEnableKafkaAdminSurface() throws IOException {
        Properties properties = loadProperties();

        assertThat(properties.getProperty("lsf.kafka.admin.enabled")).isEqualTo("true");
        assertThat(properties.getProperty("lsf.kafka.admin.base-path")).isEqualTo("/admin/kafka");
        assertThat(properties.getProperty("lsf.kafka.admin.allow-replay")).isEqualTo("true");
        assertThat(properties.getProperty("lsf.kafka.admin.max-limit")).isEqualTo("200");
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
