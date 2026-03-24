SELECT snap_dt AS date_value, COUNT(*) AS row_count
FROM analytics_db.acct_snap
WHERE snap_dt <= '20260317'
GROUP BY snap_dt
ORDER BY date_value;
