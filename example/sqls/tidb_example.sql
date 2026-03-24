-- dialect: tidb
-- # TiDB (MySQL) Test File
-- Test: completions for MySQL functions (IFNULL, GROUP_CONCAT, JSON_EXTRACT)
-- Test: hover over DATE_FORMAT, TIMESTAMPDIFF, REGEXP_REPLACE
-- Test: parameter hints — type IFNULL( and see (expr1, expr2)
-- Test: go-to-definition — Ctrl+click "products" to jump to CREATE TABLE
-- Test: snippets — type "upsert" for INSERT ... ON DUPLICATE KEY UPDATE

CREATE TABLE products (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  name VARCHAR(255) NOT NULL,
  category VARCHAR(100),
  price DECIMAL(10,2),
  stock INT DEFAULT 0,
  attrs JSON,
  created_at DATETIME DEFAULT NOW(),
  updated_at DATETIME DEFAULT NOW()
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE product_tags (
  product_id BIGINT,
  tag VARCHAR(50),
  PRIMARY KEY (product_id, tag),
  FOREIGN KEY (product_id) REFERENCES products(id)
);

-- Try hovering over these functions:
SELECT
  p.id,
  p.name,
  IFNULL(p.category, 'Uncategorized') AS category,
  IF(p.stock > 0, 'In Stock', 'Out of Stock') AS availability,
  JSON_EXTRACT(p.attrs, '$.color') AS color,
  JSON_UNQUOTE(JSON_EXTRACT(p.attrs, '$.brand')) AS brand,
  GROUP_CONCAT(pt.tag ORDER BY pt.tag SEPARATOR ', ') AS tags,
  DATE_FORMAT(p.created_at, '%Y-%m-%d') AS created,
  TIMESTAMPDIFF(DAY, p.created_at, NOW()) AS age_days,
  CONCAT_WS(' | ', p.name, p.category) AS display
FROM products p
LEFT JOIN product_tags pt ON pt.product_id = p.id
WHERE p.price BETWEEN 10.00 AND 100.00
  AND REGEXP_LIKE(p.name, '^[A-Z]')
GROUP BY p.id
HAVING COUNT(pt.tag) >= 2
ORDER BY p.created_at DESC
LIMIT 50;

-- Try completions:
-- STR_TO  → should suggest STR_TO_DATE
-- FIND_IN → should suggest FIND_IN_SET
-- UUID    → should suggest UUID()
-- JSON_S  → should suggest JSON_SET

-- Upsert snippet test — type "upsert" and tab:
