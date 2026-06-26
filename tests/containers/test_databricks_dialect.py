from dtrack.platforms.base import build_col_sql, quote_ident
from dtrack.platforms.databricks import _build_row_sql_databricks


# >>> register a Spark temp view dt with the sample rows <<< #
def _seed(spark):
    df = spark.createDataFrame(
        [("2024-01-01", 10), ("2024-01-01", 20), ("2024-01-02", 30)],
        ["rpt_dt", "amt"])
    df.createOrReplaceTempView("dt")
    return df


# >>> dtrack's Databricks/Spark-SQL row SQL executes on real Spark and counts per date <<< #
def test_databricks_row_sql_counts(spark):
    _seed(spark)
    cfg = {"name": "dt", "table": "dt", "source": "databricks",
           "conn_macro": "", "date_col": "rpt_dt", "date_type": "string_dash"}
    rows = spark.sql(_build_row_sql_databricks(cfg)).collect()
    counts = {r["dt"]: r["row_count"] for r in rows}
    assert counts == {"2024-01-01": 2, "2024-01-02": 1}


# >>> dtrack's Databricks col-stats SQL (via _col_sql_dialect_ops) executes on real Spark <<< #
def test_databricks_col_sql_executes(spark):
    _seed(spark)
    cfg = {"name": "dt", "table": "dt", "source": "databricks",
           "conn_macro": "", "date_col": "rpt_dt", "date_type": "string_dash"}
    full_table = quote_ident("dt", "databricks")
    sql, is_numeric = build_col_sql("databricks", cfg, "amt", "BIGINT", full_table,
                                    vintage="all")
    assert is_numeric
    rows = spark.sql(sql).collect()
    assert len(rows) == 1
