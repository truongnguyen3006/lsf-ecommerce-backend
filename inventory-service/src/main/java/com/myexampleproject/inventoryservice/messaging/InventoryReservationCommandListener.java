package com.myexampleproject.inventoryservice.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myexampleproject.inventoryservice.service.InventoryQuotaService;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.contracts.quota.ConfirmReservationCommand;
import com.myorg.lsf.contracts.quota.ReleaseReservationCommand;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.logging.Logger;

@Slf4j
@Component
@ConditionalOnProperty(
        name = "app.inventory.legacy-reservation-listener.enabled",
        havingValue = "true"
)
@RequiredArgsConstructor
// Consumer-side handler for reservation lifecycle commands defined in lsf-contracts.
// Order service publishes confirm/release commands; inventory service owns the resource state.
public class InventoryReservationCommandListener {
    private final InventoryQuotaService inventoryQuotaService;
    private final ObjectMapper objectMapper;
    @KafkaListener(topics = {
            "inventory-reservation-confirm-envelope-topic",
            "inventory-reservation-release-envelope-topic"
    })
    public void handleReservationCommand(ConsumerRecord<String, Object> record) {
        try {
            EventEnvelope envelope = (EventEnvelope) record.value();
            log.info(
                    "RESERVATION COMMAND RECEIVED -> topic={}, key={}, eventId={}, eventType={}",
                    record.topic(),
                    record.key(),
                    envelope.getEventId(),
                    envelope.getEventType()
            );
            switch (record.topic()) {
                case "inventory-reservation-confirm-envelope-topic" -> {
                    ConfirmReservationCommand cmd = objectMapper.convertValue(
                            envelope.getPayload(),
                            ConfirmReservationCommand.class
                    );
                    inventoryQuotaService.confirm(cmd.getWorkflowId(), cmd.getResourceId());
                }
                case "inventory-reservation-release-envelope-topic" -> {
                    ReleaseReservationCommand cmd = objectMapper.convertValue(
                            envelope.getPayload(),
                            ReleaseReservationCommand.class
                    );
                    inventoryQuotaService.release(
                            cmd.getWorkflowId(),
                            cmd.getResourceId(),
                            cmd.getReason()
                    );
                }
            }
        } catch (Exception e) {
            log.error("Failed to process reservation command", e);
        }
    }

}