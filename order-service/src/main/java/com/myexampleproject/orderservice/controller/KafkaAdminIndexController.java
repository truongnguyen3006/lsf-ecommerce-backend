package com.myexampleproject.orderservice.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/admin/kafka")
public class KafkaAdminIndexController {

    @GetMapping
    public Map<String, Object> index() {
        return Map.of(
                "service", "order-service",
                "basePath", "/admin/kafka",
                "routes", Map.of(
                        "dlqTopics", "/admin/kafka/dlq/topics",
                        "dlqRecords", "/admin/kafka/dlq/records?topic=<topic>",
                        "dlqRecord", "/admin/kafka/dlq/records/{topic}/{partition}/{offset}",
                        "replay", "/admin/kafka/dlq/replay"
                )
        );
    }
}
