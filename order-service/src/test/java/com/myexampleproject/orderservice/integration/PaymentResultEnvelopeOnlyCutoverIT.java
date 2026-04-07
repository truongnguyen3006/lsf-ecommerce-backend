package com.myexampleproject.orderservice.integration;

import com.myexampleproject.common.event.PaymentProcessedEvent;
import com.myexampleproject.notificationservice.service.LsfPaymentResultEventHandler;
import com.myexampleproject.notificationservice.service.NotificationPaymentResultDispatcher;
import com.myexampleproject.orderservice.config.OrderKafkaListenerModeConfig;
import com.myexampleproject.orderservice.model.Order;
import com.myexampleproject.orderservice.model.OrderLineItems;
import com.myexampleproject.orderservice.repository.OrderRepository;
import com.myexampleproject.orderservice.service.OrderOutboxEnvelopeFactory;
import com.myexampleproject.orderservice.service.OrderPaymentResultEventHandler;
import com.myexampleproject.orderservice.service.OrderPaymentResultProcessor;
import com.myexampleproject.orderservice.service.OrderSagaStateService;
import com.myexampleproject.paymentservice.config.PaymentOrderValidatedConsumerModeGuard;
import com.myexampleproject.paymentservice.consumer.PaymentLegacyOrderValidatedListener;
import com.myexampleproject.paymentservice.consumer.PaymentOrderValidatedEventHandler;
import com.myexampleproject.paymentservice.service.PaymentService;
import com.myorg.lsf.contracts.core.envelope.EventEnvelope;
import com.myorg.lsf.outbox.mysql.JdbcOutboxRepository;
import com.myorg.lsf.outbox.mysql.OutboxPublisher;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaDeserializer;
import io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer;
import io.micrometer.core.instrument.MeterRegistry;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.data.jpa.JpaRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration;
import org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.boot.autoconfigure.security.oauth2.resource.servlet.OAuth2ResourceServerAutoConfiguration;
import org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import javax.sql.DataSource;
import java.math.BigDecimal;
import java.sql.DriverManager;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@Testcontainers(disabledWithoutDocker = true)
class PaymentResultEnvelopeOnlyCutoverIT {

    private static final String SCHEMA_REGISTRY_URL = "mock://phase27-payment-result-cutover";
    private static final String ORDER_VALIDATED_ENVELOPE_TOPIC = "order-validated-envelope-topic";
    private static final String ORDER_STATUS_ENVELOPE_TOPIC = "order-status-envelope-topic";
    private static final String PAYMENT_PROCESSED_TOPIC = "payment-processed-topic";
    private static final String PAYMENT_FAILED_TOPIC = "payment-failed-topic";
    private static final String PAYMENT_PROCESSED_ENVELOPE_TOPIC = "payment-processed-envelope-topic";
    private static final String PAYMENT_FAILED_ENVELOPE_TOPIC = "payment-failed-envelope-topic";
    private static final String INVENTORY_RESERVATION_CONFIRM_ENVELOPE_TOPIC =
            "inventory-reservation-confirm-envelope-topic";

    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0.36")
            .withDatabaseName("phase27_payment_result_cutover_it")
            .withUsername("test")
            .withPassword("test");

    private static final KafkaContainer KAFKA = new KafkaContainer(
            DockerImageName.parse("apache/kafka-native:3.8.0")
    );

    private static ConfigurableApplicationContext orderContext;
    private static ConfigurableApplicationContext paymentContext;
    private static ConfigurableApplicationContext notificationContext;

    @BeforeAll
    static void setUp() throws Exception {
        MYSQL.start();
        KAFKA.start();
        waitForMysqlReady();
        createTopics();

        paymentContext = new SpringApplicationBuilder(PaymentCutoverTestApplication.class)
                .web(WebApplicationType.NONE)
                .run(propertyPairs(
                        commonKafkaProperties(),
                        Map.ofEntries(
                                // Cross-service IT runs inside order-service test JVM, so we pin the corridor mode
                                // explicitly instead of relying on classpath application.properties precedence.
                                Map.entry("spring.application.name", "payment-service"),
                                Map.entry("eureka.client.enabled", "false"),
                                Map.entry("spring.cloud.discovery.enabled", "false"),
                                Map.entry("lsf.kafka.consumer.group-id", "payment-phase27-it"),
                                Map.entry("lsf.kafka.consumer.auto-offset-reset", "earliest"),
                                Map.entry("lsf.kafka.consumer.batch", "false"),
                                Map.entry("app.payment.order-validated-envelope-listener.enabled", "true"),
                                Map.entry("app.payment.legacy-order-validated-listener.enabled", "false"),
                                Map.entry("app.payment.order-validated.idempotency.store", "memory"),
                                Map.entry("app.payment.order-validated.idempotency.require-redis", "false"),
                                Map.entry("lsf.outbox.enabled", "false"),
                                Map.entry("lsf.outbox.admin.enabled", "false"),
                                Map.entry("lsf.outbox.publisher.enabled", "false"),
                                Map.entry("lsf.eventing.listener.enabled", "true"),
                                Map.entry("lsf.eventing.consume-topics[0]", ORDER_VALIDATED_ENVELOPE_TOPIC),
                                Map.entry("lsf.eventing.idempotency.enabled", "true"),
                                Map.entry("lsf.eventing.idempotency.store", "memory"),
                                Map.entry("lsf.eventing.idempotency.require-redis", "false"),
                                Map.entry("lsf.eventing.idempotency.key-prefix", "lsf:payment:idemp:{groupId}:")
                        )
                ));

        orderContext = new SpringApplicationBuilder(OrderCutoverTestApplication.class)
                .web(WebApplicationType.NONE)
                .run(propertyPairs(
                        commonKafkaProperties(),
                        Map.ofEntries(
                                // Cross-service IT runs inside order-service test JVM, so we pin the corridor mode
                                // explicitly instead of relying on classpath application.properties precedence.
                                Map.entry("spring.application.name", "order-service"),
                                Map.entry("eureka.client.enabled", "false"),
                                Map.entry("spring.cloud.discovery.enabled", "false"),
                                Map.entry("spring.datasource.url", MYSQL.getJdbcUrl()),
                                Map.entry("spring.datasource.username", MYSQL.getUsername()),
                                Map.entry("spring.datasource.password", MYSQL.getPassword()),
                                Map.entry("spring.flyway.enabled", "true"),
                                Map.entry("spring.jpa.hibernate.ddl-auto", "validate"),
                                Map.entry("lsf.kafka.consumer.group-id", "order-phase27-it"),
                                Map.entry("lsf.kafka.consumer.auto-offset-reset", "earliest"),
                                Map.entry("app.order.payment-result.idempotency.store", "memory"),
                                Map.entry("app.order.payment-result.idempotency.require-redis", "false"),
                                Map.entry("lsf.outbox.enabled", "true"),
                                Map.entry("lsf.outbox.table", "lsf_outbox"),
                                Map.entry("lsf.outbox.admin.enabled", "false"),
                                Map.entry("lsf.outbox.publisher.enabled", "true"),
                                Map.entry("lsf.outbox.publisher.scheduling-enabled", "false"),
                                Map.entry("lsf.outbox.publisher.batch-size", "10"),
                                Map.entry("lsf.outbox.publisher.send-timeout", "5s"),
                                Map.entry("lsf.outbox.publisher.claim-strategy", "SKIP_LOCKED"),
                                Map.entry("lsf.eventing.listener.enabled", "true"),
                                Map.entry("lsf.eventing.consume-topics[0]", PAYMENT_PROCESSED_ENVELOPE_TOPIC),
                                Map.entry("lsf.eventing.consume-topics[1]", PAYMENT_FAILED_ENVELOPE_TOPIC),
                                Map.entry("lsf.eventing.ignore-unknown-event-type", "true"),
                                Map.entry("lsf.eventing.idempotency.enabled", "true"),
                                Map.entry("lsf.eventing.idempotency.store", "memory"),
                                Map.entry("lsf.eventing.idempotency.require-redis", "false"),
                                Map.entry("lsf.eventing.idempotency.key-prefix",
                                        "lsf:order:payment-result:idemp:{groupId}:")
                        )
                ));

        notificationContext = new SpringApplicationBuilder(NotificationCutoverTestApplication.class)
                .web(WebApplicationType.NONE)
                .run(propertyPairs(
                        commonKafkaProperties(),
                        Map.ofEntries(
                                // Cross-service IT runs inside order-service test JVM, so we pin the corridor mode
                                // explicitly instead of relying on classpath application.properties precedence.
                                Map.entry("spring.application.name", "notification-service"),
                                Map.entry("eureka.client.enabled", "false"),
                                Map.entry("spring.cloud.discovery.enabled", "false"),
                                Map.entry("lsf.kafka.consumer.group-id", "notification-phase27-it"),
                                Map.entry("lsf.kafka.consumer.auto-offset-reset", "earliest"),
                                Map.entry("lsf.kafka.consumer.batch", "false"),
                                Map.entry("lsf.eventing.listener.enabled", "true"),
                                Map.entry("lsf.eventing.consume-topics[0]", PAYMENT_PROCESSED_ENVELOPE_TOPIC),
                                Map.entry("lsf.eventing.consume-topics[1]", PAYMENT_FAILED_ENVELOPE_TOPIC),
                                Map.entry("lsf.eventing.ignore-unknown-event-type", "true"),
                                Map.entry("lsf.eventing.idempotency.enabled", "true"),
                                Map.entry("lsf.eventing.idempotency.store", "memory"),
                                Map.entry("lsf.eventing.idempotency.key-prefix",
                                        "lsf:notification:idemp:{groupId}:")
                        )
                ));

        waitForSingleListener(paymentContext, ORDER_VALIDATED_ENVELOPE_TOPIC);
        waitForSingleListener(orderContext, PAYMENT_PROCESSED_ENVELOPE_TOPIC, PAYMENT_FAILED_ENVELOPE_TOPIC);
        waitForSingleListener(notificationContext, PAYMENT_PROCESSED_ENVELOPE_TOPIC, PAYMENT_FAILED_ENVELOPE_TOPIC);
    }

    @AfterAll
    static void tearDown() {
        if (notificationContext != null) {
            notificationContext.close();
        }
        if (orderContext != null) {
            orderContext.close();
        }
        if (paymentContext != null) {
            paymentContext.close();
        }
        KAFKA.stop();
        MYSQL.stop();
    }

    @Test
    void shouldRunPaymentResultCorridorInEnvelopeOnlyMode() throws Exception {
        assertThat(paymentContext.getBeansOfType(PaymentLegacyOrderValidatedListener.class)).isEmpty();

        OrderRepository orderRepository = orderContext.getBean(OrderRepository.class);
        OrderSagaStateService sagaStateService = orderContext.getBean(OrderSagaStateService.class);
        JdbcOutboxRepository outboxRepository = orderContext.getBean(JdbcOutboxRepository.class);
        OutboxPublisher outboxPublisher = orderContext.getBean(OutboxPublisher.class);
        com.fasterxml.jackson.databind.ObjectMapper objectMapper =
                orderContext.getBean(com.fasterxml.jackson.databind.ObjectMapper.class);
        MeterRegistry paymentMeterRegistry = paymentContext.getBean(MeterRegistry.class);
        SimpMessagingTemplate messagingTemplate = notificationContext.getBean(SimpMessagingTemplate.class);

        String orderNumber = "phase27-" + UUID.randomUUID();
        orderRepository.saveAndFlush(pendingOrder(orderNumber));

        try (Consumer<String, EventEnvelope> validatedEnvelopeConsumer =
                     newConsumer("phase27-order-validated-audit", EventEnvelope.class, ORDER_VALIDATED_ENVELOPE_TOPIC);
             Consumer<String, PaymentProcessedEvent> processedLegacyConsumer =
                     newConsumer("phase27-payment-processed-legacy-audit", PaymentProcessedEvent.class,
                             PAYMENT_PROCESSED_TOPIC);
             Consumer<String, EventEnvelope> failedLegacyConsumer =
                     newConsumer("phase27-payment-failed-legacy-audit", EventEnvelope.class, PAYMENT_FAILED_TOPIC);
             Consumer<String, EventEnvelope> processedEnvelopeConsumer =
                     newConsumer("phase27-payment-processed-envelope-audit", EventEnvelope.class,
                             PAYMENT_PROCESSED_ENVELOPE_TOPIC);
             Consumer<String, EventEnvelope> confirmEnvelopeConsumer =
                     newConsumer("phase27-confirm-audit", EventEnvelope.class,
                             INVENTORY_RESERVATION_CONFIRM_ENVELOPE_TOPIC);
             Consumer<String, EventEnvelope> failedEnvelopeConsumer =
                     newConsumer("phase27-payment-failed-envelope-audit", EventEnvelope.class,
                             PAYMENT_FAILED_ENVELOPE_TOPIC);
             Producer<String, Object> producer = newProducer()) {

            assertThat(sagaStateService.markValidatedAndEnqueueStatus(orderNumber)).isTrue();
            outboxPublisher.runOnce();

            ConsumerRecord<String, EventEnvelope> validatedEnvelopeRecord =
                    pollSingleRecord(validatedEnvelopeConsumer, ORDER_VALIDATED_ENVELOPE_TOPIC, Duration.ofSeconds(20));
            EventEnvelope validatedEnvelope = validatedEnvelopeRecord.value();

            assertThat(validatedEnvelope.getEventType()).isEqualTo("order.validated.v1");
            assertThat(validatedEnvelope.getAggregateId()).isEqualTo(orderNumber);
            assertThat(outboxRepository.statusByEventId(validatedEnvelope.getEventId())).isEqualTo("SENT");

            ConsumerRecord<String, EventEnvelope> processedEnvelopeRecord =
                    pollSingleRecord(processedEnvelopeConsumer, PAYMENT_PROCESSED_ENVELOPE_TOPIC,
                            Duration.ofSeconds(20));
            EventEnvelope processedEnvelope = processedEnvelopeRecord.value();
            PaymentProcessedEvent processedPayload =
                    objectMapper.convertValue(processedEnvelope.getPayload(), PaymentProcessedEvent.class);

            assertThat(processedEnvelope.getEventType()).isEqualTo("payment.processed.v1");
            assertThat(processedEnvelope.getAggregateId()).isEqualTo(orderNumber);
            assertThat(processedEnvelope.getCorrelationId()).isEqualTo(orderNumber);
            assertThat(processedEnvelope.getCausationId()).isEqualTo(validatedEnvelope.getEventId());
            assertThat(processedPayload.getOrderNumber()).isEqualTo(orderNumber);
            assertThat(processedPayload.getPaymentId()).isNotBlank();

            waitForOrderStatus(orderRepository, orderNumber, "COMPLETED", Duration.ofSeconds(20));
            outboxPublisher.runOnce();

            ConsumerRecord<String, EventEnvelope> confirmEnvelopeRecord =
                    pollSingleRecord(confirmEnvelopeConsumer, INVENTORY_RESERVATION_CONFIRM_ENVELOPE_TOPIC,
                            Duration.ofSeconds(20));
            EventEnvelope confirmEnvelope = confirmEnvelopeRecord.value();

            assertThat(confirmEnvelope.getEventType()).isEqualTo("inventory.reservation.confirm.v1");
            assertThat(confirmEnvelope.getAggregateId()).isEqualTo(orderNumber);
            assertThat(confirmEnvelope.getPayload().get("workflowId").asText()).isEqualTo(orderNumber);

            ArgumentCaptor<Object> payloadCaptor = ArgumentCaptor.forClass(Object.class);
            verify(messagingTemplate, timeout(20_000)).convertAndSend(
                    eq("/topic/order/" + orderNumber),
                    payloadCaptor.capture()
            );
            assertThat(payloadCaptor.getValue()).isInstanceOf(Map.class);
            assertThat(((Map<?, ?>) payloadCaptor.getValue()).get("status")).isEqualTo("COMPLETED");

            assertThat(countRecords(processedLegacyConsumer, PAYMENT_PROCESSED_TOPIC, Duration.ofSeconds(4))).isZero();
            assertThat(countRecords(failedLegacyConsumer, PAYMENT_FAILED_TOPIC, Duration.ofSeconds(4))).isZero();
            assertThat(countRecords(failedEnvelopeConsumer, PAYMENT_FAILED_ENVELOPE_TOPIC, Duration.ofSeconds(2))).isZero();
            assertThat(paymentMeterRegistry.find("payment_result_publish_total")
                    .tags("path", "legacy", "result", "processed")
                    .counter()).isNull();
            assertThat(paymentMeterRegistry.get("payment_result_publish_total")
                    .tag("path", "envelope")
                    .tag("result", "processed")
                    .counter()
                    .count()).isEqualTo(1.0);

            producer.send(new ProducerRecord<>(
                    ORDER_VALIDATED_ENVELOPE_TOPIC,
                    orderNumber,
                    validatedEnvelope
            )).get(10, TimeUnit.SECONDS);

            assertThat(countRecords(processedLegacyConsumer, PAYMENT_PROCESSED_TOPIC, Duration.ofSeconds(4))).isZero();
            assertThat(countRecords(failedLegacyConsumer, PAYMENT_FAILED_TOPIC, Duration.ofSeconds(4))).isZero();
            assertThat(countRecords(processedEnvelopeConsumer, PAYMENT_PROCESSED_ENVELOPE_TOPIC,
                    Duration.ofSeconds(4))).isZero();
        }
    }

    private static Map<String, String> commonKafkaProperties() {
        return Map.of(
                "lsf.kafka.bootstrap-servers", KAFKA.getBootstrapServers(),
                "lsf.kafka.schema-registry-url", SCHEMA_REGISTRY_URL,
                "spring.kafka.bootstrap-servers", KAFKA.getBootstrapServers(),
                "spring.kafka.properties.schema.registry.url", SCHEMA_REGISTRY_URL
        );
    }

    @SafeVarargs
    private static String[] propertyPairs(Map<String, String>... propertySets) {
        List<String> pairs = new ArrayList<>();
        for (Map<String, String> propertySet : propertySets) {
            propertySet.forEach((key, value) -> pairs.add("--" + key + "=" + value));
        }
        return pairs.toArray(String[]::new);
    }

    private static void createTopics() throws Exception {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());

        try (AdminClient adminClient = AdminClient.create(props)) {
            adminClient.createTopics(List.of(
                    new NewTopic(ORDER_VALIDATED_ENVELOPE_TOPIC, 10, (short) 1),
                    new NewTopic(ORDER_STATUS_ENVELOPE_TOPIC, 10, (short) 1),
                    new NewTopic(PAYMENT_PROCESSED_TOPIC, 10, (short) 1),
                    new NewTopic(PAYMENT_FAILED_TOPIC, 10, (short) 1),
                    new NewTopic(PAYMENT_PROCESSED_ENVELOPE_TOPIC, 10, (short) 1),
                    new NewTopic(PAYMENT_FAILED_ENVELOPE_TOPIC, 10, (short) 1),
                    new NewTopic(INVENTORY_RESERVATION_CONFIRM_ENVELOPE_TOPIC, 10, (short) 1)
            )).all().get(20, TimeUnit.SECONDS);
        }
    }

    private static void waitForMysqlReady() {
        Instant deadline = Instant.now().plusSeconds(30);
        while (Instant.now().isBefore(deadline)) {
            try (var ignored = DriverManager.getConnection(
                    MYSQL.getJdbcUrl(),
                    MYSQL.getUsername(),
                    MYSQL.getPassword()
            )) {
                return;
            } catch (Exception ex) {
                sleep(Duration.ofMillis(500));
            }
        }
        fail("MySQL Testcontainer did not become ready in time.");
    }

    private static void waitForSingleListener(ConfigurableApplicationContext context, String... expectedTopics) {
        KafkaListenerEndpointRegistry registry = context.getBean(KafkaListenerEndpointRegistry.class);
        Instant deadline = Instant.now().plusSeconds(20);
        while (Instant.now().isBefore(deadline)) {
            Collection<MessageListenerContainer> containers = registry.getListenerContainers();
            if (containers.size() == 1) {
                MessageListenerContainer container = containers.iterator().next();
                if (container.isRunning()) {
                    assertThat(container.getContainerProperties().getTopics())
                            .containsExactlyInAnyOrder(expectedTopics);
                    return;
                }
            }
            sleep(Duration.ofMillis(250));
        }
        fail("Kafka listener did not become ready in time for topics " + List.of(expectedTopics));
    }

    private static void waitForOrderStatus(
            OrderRepository orderRepository,
            String orderNumber,
            String expectedStatus,
            Duration timeout
    ) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            Optional<Order> maybeOrder = orderRepository.findByOrderNumber(orderNumber);
            if (maybeOrder.isPresent() && expectedStatus.equals(maybeOrder.get().getStatus())) {
                return;
            }
            sleep(Duration.ofMillis(250));
        }
        fail("Order " + orderNumber + " did not reach status " + expectedStatus + " within " + timeout + ".");
    }

    private static <T> Consumer<String, T> newConsumer(String groupId, Class<T> valueType, String topic) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaJsonSchemaDeserializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        props.put("json.value.type", valueType.getName());

        KafkaConsumer<String, T> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(topic));
        return consumer;
    }

    private static Producer<String, Object> newProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaJsonSchemaSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        return new KafkaProducer<>(props);
    }

    private static <T> ConsumerRecord<String, T> pollSingleRecord(
            Consumer<String, T> consumer,
            String topic,
            Duration timeout
    ) {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            for (ConsumerRecord<String, T> record : consumer.poll(Duration.ofMillis(500)).records(topic)) {
                return record;
            }
        }
        fail("Did not receive record on topic " + topic + " within " + timeout + ".");
        return null;
    }

    private static long countRecords(Consumer<String, ?> consumer, String topic, Duration timeout) {
        Instant deadline = Instant.now().plus(timeout);
        long count = 0;
        while (Instant.now().isBefore(deadline)) {
            for (ConsumerRecord<String, ?> ignored : consumer.poll(Duration.ofMillis(250)).records(topic)) {
                count++;
            }
        }
        return count;
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration.toMillis());
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for Kafka state.", ex);
        }
    }

    private static Order pendingOrder(String orderNumber) {
        Order order = new Order();
        order.setUserId("phase27-user");
        order.setOrderNumber(orderNumber);
        order.setStatus("PENDING");
        order.setTotalPrice(new BigDecimal("10.00"));

        OrderLineItems lineItem = new OrderLineItems();
        lineItem.setSkuCode("SKU-PHASE27");
        lineItem.setQuantity(1);
        lineItem.setPrice(new BigDecimal("10.00"));
        lineItem.setProductName("Phase 2.7 Test Item");
        lineItem.setColor("black");
        lineItem.setSize("M");
        lineItem.setOrder(order);

        order.setOrderLineItemsList(new ArrayList<>(List.of(lineItem)));
        return order;
    }

    @SpringBootConfiguration
    @EnableKafka
    @EnableAutoConfiguration(exclude = {
            SecurityAutoConfiguration.class,
            OAuth2ResourceServerAutoConfiguration.class,
            RedisAutoConfiguration.class,
            RedisRepositoriesAutoConfiguration.class
    })
    @EnableJpaRepositories(basePackageClasses = OrderRepository.class)
    @EntityScan(basePackageClasses = Order.class)
    @Import({
            OrderKafkaListenerModeConfig.class,
            OrderSagaStateService.class,
            OrderOutboxEnvelopeFactory.class,
            OrderPaymentResultProcessor.class,
            OrderPaymentResultEventHandler.class
    })
    static class OrderCutoverTestApplication {

        @Bean
        JdbcTemplate jdbcTemplate(DataSource dataSource) {
            return new JdbcTemplate(dataSource);
        }
    }

    @SpringBootConfiguration
    @EnableKafka
    @EnableAutoConfiguration(
            exclude = {
                    DataSourceAutoConfiguration.class,
                    DataSourceTransactionManagerAutoConfiguration.class,
                    HibernateJpaAutoConfiguration.class,
                    JpaRepositoriesAutoConfiguration.class,
                    SecurityAutoConfiguration.class,
                    OAuth2ResourceServerAutoConfiguration.class,
                    RedisAutoConfiguration.class,
                    RedisRepositoriesAutoConfiguration.class
            },
            excludeName = {
                    "com.myorg.lsf.outbox.mysql.LsfOutboxMySqlAutoConfiguration",
                    "com.myorg.lsf.outbox.admin.LsfOutboxAdminAutoConfiguration"
            }
    )
    @Import({
            PaymentService.class,
            PaymentOrderValidatedEventHandler.class,
            PaymentLegacyOrderValidatedListener.class,
            PaymentOrderValidatedConsumerModeGuard.class
    })
    static class PaymentCutoverTestApplication {
    }

    @SpringBootConfiguration
    @EnableKafka
    @EnableAutoConfiguration(
            exclude = {
                    DataSourceAutoConfiguration.class,
                    DataSourceTransactionManagerAutoConfiguration.class,
                    HibernateJpaAutoConfiguration.class,
                    JpaRepositoriesAutoConfiguration.class,
                    SecurityAutoConfiguration.class,
                    OAuth2ResourceServerAutoConfiguration.class,
                    RedisAutoConfiguration.class,
                    RedisRepositoriesAutoConfiguration.class
            },
            excludeName = {
                    "org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration",
                    "com.myorg.lsf.outbox.mysql.LsfOutboxMySqlAutoConfiguration",
                    "com.myorg.lsf.outbox.admin.LsfOutboxAdminAutoConfiguration"
            }
    )
    @Import({
            NotificationPaymentResultDispatcher.class,
            LsfPaymentResultEventHandler.class
    })
    static class NotificationCutoverTestApplication {

        @Bean
        SimpMessagingTemplate simpMessagingTemplate() {
            return mock(SimpMessagingTemplate.class);
        }
    }
}
