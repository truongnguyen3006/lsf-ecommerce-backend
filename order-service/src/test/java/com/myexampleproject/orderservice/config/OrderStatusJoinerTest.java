package com.myexampleproject.orderservice.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.myexampleproject.common.event.OrderStatusEvent;
import com.myexampleproject.common.event.PaymentProcessedEvent;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.json.KafkaJsonSchemaSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

class OrderStatusJoinerTest {

    private static final String SCHEMA_REGISTRY_URL = "mock://order-status-joiner-test";

    @Test
    void shouldJoinPaymentResultsFromEnvelopeTopic() throws Exception {
        try (Serde<EventEnvelope> envelopeSerde = jsonSerde(EventEnvelope.class);
             Serde<PaymentProcessedEvent> paymentSerde = jsonSerde(PaymentProcessedEvent.class);
             TopologyTestDriver driver = topologyDriver()) {

            TestInputTopic<String, EventEnvelope> statusTopic = driver.createInputTopic(
                    "order-status-envelope-topic",
                    new StringSerializer(),
                    envelopeSerde.serializer()
            );
            TestInputTopic<String, EventEnvelope> paymentEnvelopeTopic = driver.createInputTopic(
                    "payment-processed-envelope-topic",
                    new StringSerializer(),
                    envelopeSerde.serializer()
            );
            TestOutputTopic<String, PaymentProcessedEvent> outputTopic = driver.createOutputTopic(
                    "payment-validated-topic",
                    new StringDeserializer(),
                    paymentSerde.deserializer()
            );

            statusTopic.pipeInput("ORDER-1", envelope("ecommerce.order.status.v1", "ORDER-1",
                    new OrderStatusEvent("ORDER-1", "VALIDATED")));
            paymentEnvelopeTopic.pipeInput("ORDER-1", envelope("payment.processed.v1", "ORDER-1",
                    new PaymentProcessedEvent("ORDER-1", "PAY-1")));

            assertThat(outputTopic.readValue()).isEqualTo(new PaymentProcessedEvent("ORDER-1", "PAY-1"));
        }
    }

    private TopologyTestDriver topologyDriver() throws Exception {
        OrderStatusJoiner joiner = new OrderStatusJoiner(new ObjectMapper());
        ReflectionTestUtils.setField(joiner, "schemaRegistryUrl", SCHEMA_REGISTRY_URL);

        StreamsBuilder builder = new StreamsBuilder();
        joiner.joinPaymentAndOrderStatus(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-status-joiner-test-envelope");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        return new TopologyTestDriver(builder.build(), props);
    }

    private EventEnvelope envelope(String eventType, String orderNumber, Object payload) {
        return EventEnvelope.builder()
                .eventType(eventType)
                .aggregateId(orderNumber)
                .correlationId(orderNumber)
                .payload(new ObjectMapper().valueToTree(payload))
                .build();
    }

    private static <T> Serde<T> jsonSerde(Class<T> clazz) {
        KafkaJsonSchemaSerde<T> serde = new KafkaJsonSchemaSerde<>(clazz);
        serde.configure(Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL), false);
        return serde;
    }
}
