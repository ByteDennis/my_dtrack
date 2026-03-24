-- dialect: pyspark
-- # PySpark / Hive SQL Test File
-- Test: completions for Spark functions (explode, collect_list, get_json_object)
-- Test: hover over from_json, datediff, concat_ws
-- Test: parameter hints — type collect_list( and see (expr)
-- Test: go-to-definition — Ctrl+click "orders" to jump to CREATE TABLE
-- Test: snippets — type "lateral" for LATERAL VIEW explode

CREATE TABLE orders (
  order_id BIGINT,
  customer_id BIGINT,
  items ARRAY<STRUCT<product_id: BIGINT, qty: INT, price: DOUBLE>>,
  tags ARRAY<STRING>,
  metadata STRING, -- JSON
  order_date DATE,
  status STRING
);

CREATE VIEW order_items AS
SELECT
  order_id,
  customer_id,
  item.product_id,
  item.qty,
  item.price
FROM orders
LATERAL VIEW explode(items) t AS item;

-- Try hovering over these functions:
SELECT
  customer_id,
  collect_list(order_id) AS all_orders,
  collect_set(status) AS unique_statuses,
  size(tags) AS tag_count,
  array_contains(tags, 'priority') AS is_priority,
  get_json_object(metadata, '$.channel') AS channel,
  concat_ws(', ', tags) AS tag_string,
  datediff(current_date(), order_date) AS days_since_order,
  date_format(order_date, 'yyyy-MM') AS order_month,
  from_unixtime(unix_timestamp(), 'yyyy-MM-dd HH:mm:ss') AS now_str
FROM orders
WHERE status IN ('shipped', 'delivered')
GROUP BY customer_id, tags, metadata, order_date;

-- Try completions:
-- named_  → should suggest named_struct
-- percen  → should suggest percentile_approx
-- from_j  → should suggest from_json
-- RLIKE   → Hive/Spark keyword

-- Lateral view snippet test — type "lateral" and tab:
