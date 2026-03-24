-- dialect: athena
-- # Athena (Trino) Test File
-- Test: completions for Trino functions (approx_distinct, json_extract_scalar, element_at)
-- Test: hover over date_trunc, regexp_extract, array_agg
-- Test: parameter hints — type approx_distinct( and see (expr, [accuracy])
-- Test: go-to-definition — Ctrl+click "events" to jump to CREATE TABLE

CREATE TABLE events (
  event_id VARCHAR,
  user_id VARCHAR,
  event_type VARCHAR,
  properties JSON,
  event_time TIMESTAMP,
  tags ARRAY(VARCHAR)
);

CREATE VIEW daily_summary AS
SELECT
  date_trunc('day', event_time) AS event_day,
  event_type,
  COUNT(*) AS cnt
FROM events
GROUP BY 1, 2;

-- Try hovering over these functions:
SELECT
  approx_distinct(user_id) AS unique_users,
  json_extract_scalar(properties, '$.page') AS page,
  element_at(tags, 1) AS first_tag,
  array_join(tags, ', ') AS all_tags,
  cardinality(tags) AS tag_count,
  date_diff('hour', event_time, NOW()) AS hours_ago,
  regexp_extract(json_extract_scalar(properties, '$.url'), '//([^/]+)', 1) AS domain
FROM events
WHERE event_type IN ('pageview', 'click')
  AND event_time > date_add('day', -7, NOW())
GROUP BY 1, 2, 3, 4, 5, 6, 7;

-- Try completions:
-- TRY_  → should suggest TRY_CAST
-- split  → should suggest split, split_part
-- seque  → should suggest sequence
-- CROSS JOIN UNNEST( → Athena-specific pattern
SELECT t.tag
FROM events
CROSS JOIN UNNEST(tags) AS t(tag);
