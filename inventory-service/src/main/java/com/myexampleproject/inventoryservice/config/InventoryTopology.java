package com.myexampleproject.inventoryservice.config;

import com.myexampleproject.common.event.InventoryAdjustmentEvent;
import com.myexampleproject.common.event.InventoryCheckRequest;
import com.myexampleproject.common.event.InventoryCheckResult;
import com.myexampleproject.common.event.ProductCreatedEvent;
import com.myexampleproject.inventoryservice.service.InventoryQuotaService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
@Component
@RequiredArgsConstructor
public class InventoryTopology {

    public static final String INVENTORY_STORE = "inventory-store";

    private final SerdeConfig serdeConfig;
    private final MeterRegistry meterRegistry;
    private final InventoryQuotaService inventoryQuotaService;
    private final Map<String, AtomicInteger> stockGauges = new ConcurrentHashMap<>();

    private void updateStockMetric(String sku, int newStock) {
        try {
            AtomicInteger gauge = stockGauges.computeIfAbsent(sku, k ->
                    meterRegistry.gauge("inventory_stock_level", Tags.of("sku", k), new AtomicInteger(newStock)));
            if (gauge != null) {
                gauge.set(newStock);
            }
        } catch (Exception e) {
            log.warn("Lỗi cập nhật metrics cho SKU {}: {}", sku, e.getMessage());
        }
    }

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        var stringSerde = Serdes.String();
        var intSerde = Serdes.Integer();

        var productSerde = serdeConfig.jsonSchemaSerde(ProductCreatedEvent.class);
        var adjustSerde = serdeConfig.jsonSchemaSerde(InventoryAdjustmentEvent.class);
        var checkRequestSerde = serdeConfig.jsonSchemaSerde(InventoryCheckRequest.class);
        var checkResultSerde = serdeConfig.jsonSchemaSerde(InventoryCheckResult.class);

        KStream<String, Integer> productStream = builder
                .stream("product-created-topic", Consumed.with(stringSerde, productSerde))
                .map((key, event) -> KeyValue.pair(event.getSkuCode(), Math.max(0, event.getInitialQuantity())))
                .repartition(Repartitioned.with(stringSerde, intSerde).withName("product-repartition-by-sku"));

        KStream<String, Integer> adjustStream = builder
                .stream("inventory-adjustment-topic", Consumed.with(stringSerde, adjustSerde))
                .mapValues(InventoryAdjustmentEvent::getAdjustmentQuantity)
                .repartition(Repartitioned.with(stringSerde, intSerde).withName("adjust-repartition-by-sku"));

        // This state store still tracks physical/baseline stock.
        // Temporary reservation state is handled by LSF quota, not by direct deduction here.
        KStream<String, Integer> inventoryChanges = productStream.merge(adjustStream);
        inventoryChanges
                .groupByKey(Grouped.with(stringSerde, intSerde))
                .aggregate(
                        () -> 0,
                        (sku, change, currentStock) -> {
                            long newStock = (long) currentStock + change;
                            int finalStock = (int) Math.max(0, Math.min(newStock, Integer.MAX_VALUE));
                            updateStockMetric(sku, finalStock);
                            log.info("AGGREGATE STOCK -> {} ({} + {}) = {}", sku, currentStock, change, finalStock);
                            return finalStock;
                        },
                        Materialized.<String, Integer, KeyValueStore<Bytes, byte[]>>as(INVENTORY_STORE)
                                .withKeySerde(stringSerde)
                                .withValueSerde(intSerde)
                );
        // LSF integration point:
        // inventory check no longer deducts stock directly in the state store.
        // Instead, it delegates resource holding to the LSF quota framework.
        builder.stream("inventory-check-request-topic", Consumed.with(stringSerde, checkRequestSerde))
                .transform(
                        () -> new Transformer<String, InventoryCheckRequest, KeyValue<String, InventoryCheckResult>>() {
                            private KeyValueStore<String, ValueAndTimestamp<Integer>> store;

                            @Override
                            public void init(ProcessorContext context) {
                                this.store = context.getStateStore(INVENTORY_STORE);
                            }

                            @Override
                            public KeyValue<String, InventoryCheckResult> transform(String skuCode, InventoryCheckRequest request) {
                                ValueAndTimestamp<Integer> stockWithTimestamp = store.get(skuCode);
                                int currentStock = stockWithTimestamp != null && stockWithTimestamp.value() != null
                                        ? stockWithTimestamp.value()
                                        : 0;
                                // Reserve against quota using the current physical stock as the effective limit.
                                InventoryCheckResult result = inventoryQuotaService.reserve(
                                        request.getOrderNumber(),
                                        request.getItem(),
                                        currentStock,
                                        request.getPaymentMethod()
                                );

                                return KeyValue.pair(request.getOrderNumber(), result);
                            }

                            @Override
                            public void close() {
                            }
                        },
                        INVENTORY_STORE
                )
                .to("inventory-check-result-topic", Produced.with(stringSerde, checkResultSerde));

        log.info("=== INVENTORY TOPOLOGY WITH QUOTA RESERVE LOADED OK ===");
    }
}
