package com.myexampleproject.orderservice.config;

import com.myorg.lsf.kafka.KafkaProperties;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.util.ReflectionUtils;

import java.lang.reflect.Method;

@Configuration
@Slf4j
public class OrderKafkaListenerModeConfig {

    @Bean(name = "kafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> orderEnvelopeKafkaListenerContainerFactory(
            KafkaProperties props,
            ConsumerFactory<String, Object> consumerFactory,
            CommonErrorHandler errorHandler
    ) {
        return createFactory(props, consumerFactory, errorHandler, false);
    }

    @Bean(name = "orderBatchKafkaListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, Object> orderBatchKafkaListenerContainerFactory(
            KafkaProperties props,
            ConsumerFactory<String, Object> consumerFactory,
            CommonErrorHandler errorHandler
    ) {
        return createFactory(props, consumerFactory, errorHandler, props.getConsumer().isBatch());
    }

    private ConcurrentKafkaListenerContainerFactory<String, Object> createFactory(
            KafkaProperties props,
            ConsumerFactory<String, Object> consumerFactory,
            CommonErrorHandler errorHandler,
            boolean batch
    ) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setBatchListener(batch);
        factory.setConcurrency(props.getConsumer().getConcurrency());
        factory.getContainerProperties().setAckMode(
                batch ? ContainerProperties.AckMode.BATCH : ContainerProperties.AckMode.RECORD
        );
        factory.setCommonErrorHandler(errorHandler);

        if (props.getObservability().isObservationEnabled()) {
            if (batch && props.getObservability().isWarnOnBatch()) {
                log.warn("orderBatchKafkaListenerContainerFactory is running in batch mode. " +
                        "Observation spans per record are not available for these legacy listeners.");
            }
            invokeIfPresent(factory.getContainerProperties(), "setObservationEnabled", true);
        }

        return factory;
    }

    private static void invokeIfPresent(Object target, String methodName, boolean arg) {
        Method method = ReflectionUtils.findMethod(target.getClass(), methodName, boolean.class);
        if (method == null) {
            return;
        }
        try {
            ReflectionUtils.makeAccessible(method);
            method.invoke(target, arg);
        } catch (Exception ex) {
            log.warn("Failed to call {}.{}({})", target.getClass().getSimpleName(), methodName, arg, ex);
        }
    }
}
