package com.myexampleproject.orderservice.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class OrderDataIntegrityGuard implements ApplicationRunner {

    private static final String DELETE_ORPHAN_LINE_ITEMS_SQL = """
            DELETE li
            FROM t_orders_line_items li
            LEFT JOIN t_orders o ON o.id = li.order_id
            WHERE li.order_id IS NOT NULL
              AND o.id IS NULL
            """;

    private static final String MAX_ORDER_ID_SQL = """
            SELECT COALESCE(MAX(id), 0)
            FROM t_orders
            """;

    private static final String MAX_REFERENCED_ORDER_ID_SQL = """
            SELECT COALESCE(MAX(order_id), 0)
            FROM t_orders_line_items
            WHERE order_id IS NOT NULL
            """;

    private static final String SUSPICIOUS_ORDER_COUNT_SQL = """
            SELECT COUNT(*)
            FROM (
                SELECT o.id
                FROM t_orders o
                JOIN t_orders_line_items li ON li.order_id = o.id
                GROUP BY o.id, o.total_price
                HAVING ABS(COALESCE(SUM(li.price * li.quantity), 0) - COALESCE(o.total_price, 0)) > 0.009
            ) suspicious_orders
            """;

    private final JdbcTemplate jdbcTemplate;
    private final boolean cleanupOrphanLineItems;
    private final boolean alignOrderAutoIncrement;
    private final boolean warnOnSuspiciousOrders;

    public OrderDataIntegrityGuard(
            JdbcTemplate jdbcTemplate,
            @Value("${app.order.integrity.cleanup-orphan-line-items:true}") boolean cleanupOrphanLineItems,
            @Value("${app.order.integrity.align-order-auto-increment:true}") boolean alignOrderAutoIncrement,
            @Value("${app.order.integrity.warn-on-suspicious-orders:true}") boolean warnOnSuspiciousOrders
    ) {
        this.jdbcTemplate = jdbcTemplate;
        this.cleanupOrphanLineItems = cleanupOrphanLineItems;
        this.alignOrderAutoIncrement = alignOrderAutoIncrement;
        this.warnOnSuspiciousOrders = warnOnSuspiciousOrders;
    }

    @Override
    public void run(ApplicationArguments args) {
        if (cleanupOrphanLineItems) {
            cleanupOrphanLineItems();
        }

        if (alignOrderAutoIncrement) {
            alignOrderAutoIncrement();
        }

        if (warnOnSuspiciousOrders) {
            warnOnSuspiciousOrders();
        }
    }

    void cleanupOrphanLineItems() {
        int removed = jdbcTemplate.update(DELETE_ORPHAN_LINE_ITEMS_SQL);
        if (removed > 0) {
            log.warn(
                    "Removed {} orphan order line items left behind from old demo/reset runs.",
                    removed
            );
        } else {
            log.info("No orphan order line items found.");
        }
    }

    void alignOrderAutoIncrement() {
        long maxOrderId = queryLong(MAX_ORDER_ID_SQL);
        long maxReferencedOrderId = queryLong(MAX_REFERENCED_ORDER_ID_SQL);
        long nextOrderId = Math.max(maxOrderId, maxReferencedOrderId) + 1;

        jdbcTemplate.execute("ALTER TABLE t_orders AUTO_INCREMENT = " + nextOrderId);
        log.info(
                "Aligned t_orders AUTO_INCREMENT to {} to avoid reusing stale order_id values.",
                nextOrderId
        );
    }

    void warnOnSuspiciousOrders() {
        long suspiciousOrders = queryLong(SUSPICIOUS_ORDER_COUNT_SQL);
        if (suspiciousOrders > 0) {
            log.warn(
                    "{} existing orders have line items that no longer match total_price. " +
                            "These are likely old demo/reset collisions and should be purged before reusing them in the admin demo.",
                    suspiciousOrders
            );
        }
    }

    private long queryLong(String sql) {
        Long value = jdbcTemplate.queryForObject(sql, Long.class);
        return value == null ? 0L : value;
    }
}
