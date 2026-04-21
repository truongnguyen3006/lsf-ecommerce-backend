SET @payment_method_column_exists := (
    SELECT COUNT(*)
    FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE()
      AND TABLE_NAME = 't_orders'
      AND COLUMN_NAME = 'payment_method'
);

SET @payment_method_add_sql := IF(
    @payment_method_column_exists = 0,
    'ALTER TABLE t_orders ADD COLUMN payment_method VARCHAR(32) DEFAULT ''MOCK_SUCCESS''',
    'SELECT 1'
);

PREPARE payment_method_add_stmt FROM @payment_method_add_sql;
EXECUTE payment_method_add_stmt;
DEALLOCATE PREPARE payment_method_add_stmt;

UPDATE t_orders
SET payment_method = 'MOCK_SUCCESS'
WHERE payment_method IS NULL;

ALTER TABLE t_orders
    MODIFY COLUMN payment_method VARCHAR(32) NOT NULL DEFAULT 'MOCK_SUCCESS';
