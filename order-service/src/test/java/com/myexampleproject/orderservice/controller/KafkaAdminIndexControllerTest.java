package com.myexampleproject.orderservice.controller;

import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaAdminIndexControllerTest {

    private final KafkaAdminIndexController controller = new KafkaAdminIndexController();

    @Test
    void indexExposesKafkaAdminLinks() {
        Map<String, Object> payload = controller.index();

        assertThat(payload.get("service")).isEqualTo("order-service");
        assertThat(payload.get("basePath")).isEqualTo("/admin/kafka");
        assertThat(payload).containsKey("routes");
        assertThat(payload.get("routes")).isInstanceOf(Map.class);

        @SuppressWarnings("unchecked")
        Map<String, String> routes = (Map<String, String>) payload.get("routes");
        assertThat(routes.get("dlqTopics")).isEqualTo("/admin/kafka/dlq/topics");
        assertThat(routes.get("replay")).isEqualTo("/admin/kafka/dlq/replay");
    }
}
