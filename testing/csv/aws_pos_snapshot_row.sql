WITH pos_snapshot AS (
SELECT * FROM warehouse_db.pos_snapshot WHERE int_date = (SELECT MAX(int_date) FROM warehouse_db.pos_snapshot)
)
SELECT int_date AS date_value, COUNT(*) AS row_count
FROM pos_snapshot
WHERE int_date <= 20260317
GROUP BY int_date
ORDER BY date_value;
