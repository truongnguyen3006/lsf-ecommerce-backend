package com.myexampleproject.notificationservice.config;

import com.myexampleproject.common.event.OrderPlacedEvent;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.kafka.SerdeFactory;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    private final KafkaProperties springKafkaProperties;

    public KafkaConsumerConfig(KafkaProperties springKafkaProperties) {
        this.springKafkaProperties = springKafkaProperties;
    }

    private Map<String, Object> schemaAwareProps() {
        Map<String, Object> props = new HashMap<>(springKafkaProperties.buildConsumerProperties(null));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class);
        props.put("use.latest.version", true);
        props.put("oneof.for.nullables", false);
        props.put("json.ignore.unknown", true);
        return props;
    }

    private Map<String, Object> rawStringProps() {
        Map<String, Object> props = new HashMap<>(springKafkaProperties.buildConsumerProperties(null));
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return props;
    }

    @Bean
    public ConsumerFactory<String, OrderPlacedEvent> orderPlacedConsumerFactory() {
        Map<String, Object> props = schemaAwareProps();
        props.put("json.value.type", OrderPlacedEvent.class.getName());
        return new DefaultKafkaConsumerFactory<>(props);
    }

//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, OrderPlacedEvent> orderPlacedKafkaListenerContainerFactory() {
//        return singleRecordListenerContainerFactory(orderPlacedConsumerFactory());
//    }

    @Bean
    public ConsumerFactory<String, String> orderFailedRawConsumerFactory() {
        return new DefaultKafkaConsumerFactory<>(rawStringProps());
    }

//    @Bean
//    public ConcurrentKafkaListenerContainerFactory<String, String> orderFailedRawKafkaListenerContainerFactory() {
//        return singleRecordListenerContainerFactory(orderFailedRawConsumerFactory());
//    }

    /**
     * Generic container factory used by lsf-eventing-starter.
     */
    @Bean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            com.myorg.lsf.kafka.KafkaProperties lsfKafkaProperties,
            SerdeFactory serdeFactory,
            CommonErrorHandler commonErrorHandler
    ) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, lsfKafkaProperties.getBootstrapServers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, lsfKafkaProperties.getSchemaRegistryUrl());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, serdeFactory.valueDeserializerClass());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, lsfKafkaProperties.getConsumer().getAutoOffsetReset());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, lsfKafkaProperties.getConsumer().getMaxPollRecords());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, lsfKafkaProperties.getConsumer().getGroupId());
        props.put("json.value.type", EventEnvelope.class.getName());
        props.put("use.latest.version", true);
        props.put("oneof.for.nullables", false);
        props.put("json.ignore.unknown", true);

        ConsumerFactory<String, Object> consumerFactory = new DefaultKafkaConsumerFactory<>(props);
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(lsfKafkaProperties.getConsumer().isBatch());
        factory.setConcurrency(lsfKafkaProperties.getConsumer().getConcurrency());
        factory.getContainerProperties().setAckMode(
                lsfKafkaProperties.getConsumer().isBatch()
                        ? ContainerProperties.AckMode.BATCH
                        : ContainerProperties.AckMode.RECORD
        );
        factory.setCommonErrorHandler(commonErrorHandler);
        return factory;
    }

    private <T> ConcurrentKafkaListenerContainerFactory<String, T> singleRecordListenerContainerFactory(
            ConsumerFactory<String, T> consumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<String, T> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        return factory;
    }
}
