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
    rec = rows[0].asDict()
    assert int(rec["n_total"]) == 3
    assert int(rec["n_missing"]) == 0
    assert int(rec["n_unique"]) == 3
    assert float(rec["mean"]) == 20.0
    assert float(rec["min_val"]) == 10.0
    assert float(rec["max_val"]) == 30.0


# >>> STRICT (lower/trim) collapses case+whitespace drift on real Spark <<< #
def test_databricks_strict_normalization(spark):
    df = spark.createDataFrame(
        [("2024-01-01", "USD"), ("2024-01-01", "usd"), ("2024-01-01", "USD ")],
        ["rpt_dt", "ccy"])
    df.createOrReplaceTempView("dt_strict")
    cfg = {"name": "dt_strict", "table": "dt_strict", "source": "databricks",
           "conn_macro": "", "date_col": "rpt_dt", "date_type": "string_dash"}
    full_table = quote_ident("dt_strict", "databricks")

    def n_unique(strict):
        sql, _ = build_col_sql("databricks", cfg, "ccy", "STRING", full_table,
                               vintage="all", strict=strict)
        return int(spark.sql(sql).collect()[0].asDict()["n_unique"])

    assert n_unique(False) == 3
    assert n_unique(True) == 1
