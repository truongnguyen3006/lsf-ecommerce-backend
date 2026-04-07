package com.myexampleproject.paymentservice.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myexampleproject.common.event.OrderValidatedEvent;
import com.myexampleproject.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@RequiredArgsConstructor
@Slf4j
@ConditionalOnProperty(
        prefix = "app.payment.legacy-order-validated-listener",
        name = "enabled",
        havingValue = "true"
)
public class PaymentLegacyOrderValidatedListener {

    private final PaymentService paymentService;
    private final ObjectMapper objectMapper;

    @KafkaListener(
            topics = "order-validated-topic",
            groupId = "${lsf.kafka.consumer.group-id:payment-group}",
            autoStartup = "${app.payment.legacy-order-validated-listener.enabled:false}"
    )
    public void handleOrderValidation(Object payload) {
        if (payload == null) {
            return;
        }

        if (payload instanceof ConsumerRecord<?, ?> record) {
            processLegacyRecord(record.key(), record.value());
            return;
        }

        if (payload instanceof ConsumerRecords<?, ?> records) {
            records.forEach(record -> processLegacyRecord(record.key(), record.value()));
            return;
        }

        if (payload instanceof List<?> list) {
            for (Object item : list) {
                if (item instanceof ConsumerRecord<?, ?> record) {
                    processLegacyRecord(record.key(), record.value());
                } else {
                    processLegacyRecord(null, item);
                }
            }
            return;
        }

        processLegacyRecord(null, payload);
    }

    private void processLegacyRecord(Object key, Object value) {
        try {
            OrderValidatedEvent event = objectMapper.convertValue(value, OrderValidatedEvent.class);
            paymentService.processValidatedOrder(event, "legacy-topic", null);
        } catch (Exception e) {
            log.error("Legacy payment processing failed for key {}: {}", key, e.getMessage(), e);
        }
    }
}
