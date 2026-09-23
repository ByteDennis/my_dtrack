import os

import pytest

pytestmark = pytest.mark.live


def _env(*names):
    missing = [name for name in names if not os.getenv(name)]
    if missing:
        pytest.skip(f"set {', '.join(missing)} to run this live test")


# >>> Oracle: the left-side preflight and an exact partition count against a real table <<< #
def test_oracle_left_preflight_and_partition_counts():
    _env(
        "ORACLE_USER",
        "ORACLE_PASSWORD",
        "ORACLE_DSN",
        "DTRACK_LIVE_ORACLE_TABLE",
        "DTRACK_LIVE_ORACLE_DATE_COL",
    )
    from dtrack_left.sources import OracleSource

    source = OracleSource(
        {
            "source": "oracle",
            "table": os.environ["DTRACK_LIVE_ORACLE_TABLE"],
            "date_col": os.environ["DTRACK_LIVE_ORACLE_DATE_COL"],
            "date_type": os.getenv("DTRACK_LIVE_ORACLE_DATE_TYPE", "date"),
        }
    )
    health = source.preflight()
    assert health["ok"], health
    counts = source.partition_counts(os.getenv("DTRACK_LIVE_FROM"), os.getenv("DTRACK_LIVE_TO"))
    assert counts and all(isinstance(value, int) for value in counts.values())


# >>> Databricks: SparkTarget statistics on a real cluster must match the pandas rule on the same rows <<< #
def test_spark_target_statistics_match_pandas_on_a_real_table():
    _env("DTRACK_LIVE_SPARK_TABLE", "DTRACK_LIVE_SPARK_DATE_COL")
    pyspark = pytest.importorskip("pyspark")
    from dtrack_right.statistics import compute_csv_statistics
    from dtrack_right.targets import SparkTarget

    spark = pyspark.sql.SparkSession.builder.getOrCreate()
    config = {
        "source": "databricks",
        "table": os.environ["DTRACK_LIVE_SPARK_TABLE"],
        "date_col": os.environ["DTRACK_LIVE_SPARK_DATE_COL"],
    }
    target = SparkTarget(spark, config)
    assert target.preflight()["ok"]
    counts = target.partition_counts(os.getenv("DTRACK_LIVE_FROM"), os.getenv("DTRACK_LIVE_TO"))
    day = min(counts)
    columns = [c for c in target.column_types if c != config["date_col"]][:5]
    manifest = {
        "pair": "live",
        "columns": columns,
        "col_map": {c: c for c in columns},
        "right_key_cols": [],
        "key_families": [],
        "sample": {"mode": "all"},
        "strict": False,
    }
    spark_stats = target.window_statistics(manifest, {"bucket": day, "partitions": [day]})
    frame = spark.sql(
        f"SELECT * FROM {target.relation()} WHERE {target.config['date_col']} = '{day}'"
    ).toPandas()
    path = "/tmp/dtrack_live_window.csv"
    frame.astype(str).to_csv(path, index=False)
    pandas_stats = compute_csv_statistics(path, columns)
    for column in columns:
        for field in ("n_total", "n_missing", "n_unique", "hash_sum"):
            assert str(spark_stats[column][field]) == str(pandas_stats[column][field]), (
                column,
                field,
            )
