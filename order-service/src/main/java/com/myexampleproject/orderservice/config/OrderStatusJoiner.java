package com.myexampleproject.orderservice.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myexampleproject.common.event.OrderStatusEvent;
import com.myexampleproject.common.event.PaymentProcessedEvent;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.util.Map;

@Configuration
@EnableKafkaStreams
@Slf4j
@RequiredArgsConstructor
public class OrderStatusJoiner {

    private final ObjectMapper objectMapper;

    @Value("${lsf.kafka.schema-registry-url}")
    private String schemaRegistryUrl;

    private <T> Serde<T> jsonSerde(Class<T> clazz) {
        Serde<T> serde = new KafkaJsonSchemaSerde<>(clazz);
        serde.configure(Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl), false);
        return serde;
    }

    @Bean
    public KStream<String, PaymentProcessedEvent> joinPaymentAndOrderStatus(StreamsBuilder builder) {
        Serde<PaymentProcessedEvent> paymentSerde = jsonSerde(PaymentProcessedEvent.class);
        Serde<EventEnvelope> envelopeSerde = jsonSerde(EventEnvelope.class);

        KTable<String, EventEnvelope> rawStatusTable = builder.table(
                "order-status-envelope-topic",
                Consumed.with(Serdes.String(), envelopeSerde)
        );

        KTable<String, OrderStatusEvent> orderStatusTable = rawStatusTable.mapValues(envelope -> {
            if (envelope == null || envelope.getPayload() == null) {
                return null;
            }

            try {
                return objectMapper.convertValue(envelope.getPayload(), OrderStatusEvent.class);
            } catch (Exception e) {
                log.error("Failed to unwrap OrderStatusEvent from EventEnvelope. envelope={}", envelope, e);
                return null;
            }
        });

        KStream<String, PaymentProcessedEvent> paymentStream = paymentProcessedEnvelopeStream(builder, envelopeSerde);

        KStream<String, PaymentProcessedEvent> validPayments = paymentStream.join(
                orderStatusTable,
                (payment, status) -> {
                    if (status == null) {
                        log.warn("Skip PaymentProcessedEvent: order {} is missing from order-status-envelope-topic",
                                payment.getOrderNumber());
                        return null;
                    }

                    String currentStatus = status.getStatus();
                    if (!"PENDING".equals(currentStatus) && !"VALIDATED".equals(currentStatus)) {
                        log.warn(
                                "Skip PaymentProcessedEvent: order {} is not in an acceptable state (status={})",
                                payment.getOrderNumber(),
                                currentStatus
                        );
                        return null;
                    }

                    log.info("Valid PaymentProcessedEvent for order {}", payment.getOrderNumber());
                    return payment;
                },
                Joined.with(Serdes.String(), paymentSerde, null)
        ).filter((key, value) -> value != null);

        validPayments.to("payment-validated-topic", Produced.with(Serdes.String(), paymentSerde));
        return validPayments;
    }

    private KStream<String, PaymentProcessedEvent> paymentProcessedEnvelopeStream(
            StreamsBuilder builder,
            Serde<EventEnvelope> envelopeSerde
    ) {
        log.info("OrderStatusJoiner is reading envelope payment result topic payment-processed-envelope-topic");
        return builder.stream(
                        "payment-processed-envelope-topic",
                        Consumed.with(Serdes.String(), envelopeSerde)
                )
                .mapValues(this::unwrapPaymentProcessedEnvelope)
                .filter((key, value) -> value != null);
    }

    private PaymentProcessedEvent unwrapPaymentProcessedEnvelope(EventEnvelope envelope) {
        if (envelope == null || envelope.getPayload() == null) {
            return null;
        }

        try {
            return objectMapper.convertValue(envelope.getPayload(), PaymentProcessedEvent.class);
        } catch (Exception e) {
            log.error("Failed to unwrap PaymentProcessedEvent from EventEnvelope. envelope={}", envelope, e);
            return null;
        }
    }
}
