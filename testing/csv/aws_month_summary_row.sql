SELECT month_flag AS date_value, COUNT(*) AS row_count
FROM warehouse_db.month_summary
WHERE month_flag <= 20260317
GROUP BY month_flag
ORDER BY date_value;
