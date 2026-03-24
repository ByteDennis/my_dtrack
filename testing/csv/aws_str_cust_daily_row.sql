SELECT rpt_dt AS date_value, COUNT(*) AS row_count
FROM analytics_db.cust_daily
WHERE rpt_dt <= '2026-03-17'
GROUP BY rpt_dt
ORDER BY date_value;
