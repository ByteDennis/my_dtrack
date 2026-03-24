WITH cust_log AS (
SELECT * FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY cust_id ORDER BY log_ts DESC) AS rn FROM analytics_db.cust_log) WHERE rn = 1
)
SELECT log_ts AS date_value, COUNT(*) AS row_count
FROM cust_log
WHERE log_ts <= TIMESTAMP '2026-03-17 00:00:00'
GROUP BY log_ts
ORDER BY date_value;
