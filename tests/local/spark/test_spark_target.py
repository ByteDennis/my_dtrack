import pandas as pd
import pytest
from support import assert_stats_agree, synthetic_data

from dtrack_left.backends.sources import FileSource
from dtrack_left.common.hashing import normalize_value, sample_csv
from dtrack_left.common.stats import compute_csv_statistics
from dtrack_local.pipeline import WarehouseTarget
from dtrack_right.backends.targets import SparkTarget

pytestmark = pytest.mark.spark

SCHEMA = "business_date STRING, order_id BIGINT, amount DECIMAL(10,2), status STRING, region STRING"
COLUMNS = ["order_id", "amount", "status", "region"]


@pytest.fixture(scope="module")
def spark():
    pyspark = pytest.importorskip("pyspark")
    session = (
        pyspark.sql.SparkSession.builder.master("local[2]")
        .appName("dtrack-spark-target")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture(scope="module")
def orders(spark, tmp_path_factory):
    root = tmp_path_factory.mktemp("synthetic")
    synthetic_data(root)
    path = root / "right/orders.csv"
    spark.read.csv(str(path), header=True, schema=SCHEMA).createOrReplaceTempView("orders")
    return path


def _manifest(sample, strict=False, key_tuples=None):
    return {
        "pair": "orders",
        "columns": ["ORDER_ID", "AMOUNT", "STATUS", "REGION"],
        "col_map": {
            "ORDER_ID": "order_id",
            "AMOUNT": "amount",
            "STATUS": "status",
            "REGION": "region",
        },
        "right_key_cols": ["order_id"],
        "key_families": ["numeric"],
        "sample": sample,
        "strict": strict,
    }


def _pandas_reference(path, partitions, sample, strict, tmp_path, key_tuples=None):
    frame = pd.read_csv(path, dtype=object)
    window = frame[frame["business_date"].isin(partitions)]
    csv_path = tmp_path / "window.csv"
    window.to_csv(csv_path, index=False)
    sampled = sample_csv(
        csv_path,
        key_columns=["order_id"],
        mode=sample["mode"],
        rate=sample.get("rate"),
        count=sample.get("count"),
        families=["numeric"],
        key_tuples=key_tuples,
    )
    return compute_csv_statistics(sampled, COLUMNS, strict=strict)


# >>> exact per-day counts through spark.sql equal the CSV truth <<< #
def test_partition_counts_match_the_file(spark, orders):
    target = SparkTarget(
        spark,
        {
            "source": "databricks",
            "table": "orders",
            "date_col": "business_date",
            "date_type": "string",
        },
    )
    health = target.preflight()
    assert health["ok"] and health["columns"] == sorted(COLUMNS + ["business_date"])
    counts = target.partition_counts("2026-01-01", "2026-02-28")
    truth = pd.read_csv(orders, dtype=object)["business_date"].value_counts().to_dict()
    assert counts == {k: int(v) for k, v in truth.items()}
    assert counts["2026-01-15"] == 27 and "2026-02-10" not in counts


@pytest.mark.parametrize(
    ("date_type", "raw_days"),
    [
        ("string", ["20260101", "20260102"]),
        ("sas", [" 01JAN2026", "02JAN2026 "]),
        ("sas", ["24107", "24108.75"]),
        ("sas", ["01JAN2026:09:45:35", "02JAN2026:00:00:00"]),
    ],
)
def test_partition_counts_support_compact_and_sas_dates(spark, date_type, raw_days):
    spark.createDataFrame(
        [(raw_days[0],), (raw_days[1],)], "raw_day STRING"
    ).createOrReplaceTempView("date_formats")
    target = SparkTarget(
        spark,
        {
            "source": "databricks",
            "table": "date_formats",
            "date_col": "raw_day",
            "date_type": date_type,
        },
    )
    assert target.partition_counts("2026-01-01", "2026-01-02") == {
        "2026-01-01": 1,
        "2026-01-02": 1,
    }


def test_sas_fractional_days_truncate_toward_zero_in_spark(spark):
    spark.createDataFrame(
        [("-1.75",), ("24108.75",)], "raw_day STRING"
    ).createOrReplaceTempView("sas_fractional_days")
    target = SparkTarget(
        spark,
        {
            "source": "databricks",
            "table": "sas_fractional_days",
            "date_col": "raw_day",
            "date_type": "sas",
        },
    )
    assert target.partition_counts("1959-12-31", "2026-01-02") == {
        "1959-12-31": 1,
        "2026-01-02": 1,
    }


# >>> the hash-bucket predicate in SQL selects exactly the rows pandas selects, and every statistic agrees <<< #
@pytest.mark.parametrize("strict", [False, True])
def test_window_statistics_agree_with_pandas_on_a_fraction_sample(spark, orders, tmp_path, strict):
    partitions = [f"2026-01-{d:02d}" for d in range(1, 31) if d not in (15,)]
    sample = {"mode": "fraction", "rate": 0.25, "threshold": round(0.25 * 2**32)}
    target = SparkTarget(
        spark,
        {
            "source": "databricks",
            "table": "orders",
            "date_col": "business_date",
            "date_type": "string",
        },
    )
    spark_stats = target.window_statistics(
        _manifest(sample, strict), {"bucket": "2026-01-01", "partitions": partitions}
    )
    pandas_stats = _pandas_reference(orders, partitions, sample, strict, tmp_path)
    assert_stats_agree(COLUMNS, spark_stats, pandas_stats)


# >>> fixed-N: the key list from keys.json selects the same rows in SQL as in pandas <<< #
def test_window_statistics_agree_with_pandas_on_fixed_keys(spark, orders, tmp_path):
    partitions = [f"2026-02-{d:02d}" for d in range(1, 29) if d != 10]
    keys = [[str(order_id)] for order_id in (100961, 100962, 101000, 101500, 101777)]
    sample = {"mode": "fixed", "count": 5}
    target = SparkTarget(
        spark,
        {
            "source": "databricks",
            "table": "orders",
            "date_col": "business_date",
            "date_type": "string",
        },
    )
    spark_stats = target.window_statistics(
        _manifest(sample), {"bucket": "2026-02-01", "partitions": partitions, "key_tuples": keys}
    )
    pandas_stats = _pandas_reference(orders, partitions, sample, False, tmp_path, key_tuples=keys)
    assert spark_stats["order_id"]["n_total"] == pandas_stats["order_id"]["n_total"] == 4
    assert_stats_agree(COLUMNS, spark_stats, pandas_stats)


# >>> the local Databricks query and Left source select the exact same normalised record keys <<< #
def test_local_databricks_query_selects_the_same_fraction_keys_as_left(spark, tmp_path):
    left = tmp_path / "left.csv"
    pd.DataFrame(
        {
            "day": ["2026-01-01"] * 12,
            "left_id": [f"{key:04d}.0" for key in range(1, 13)],
            "value": list(range(12)),
        }
    ).to_csv(left, index=False)
    spark.createDataFrame(
        [("2026-01-01", key, key - 1) for key in range(1, 13)],
        "day STRING, right_id BIGINT, value BIGINT",
    ).createOrReplaceTempView("local_sample_parity")
    sample = {"mode": "fraction", "rate": 0.5, "threshold": round(0.5 * 2**32)}
    left_path = FileSource(
        {
            "source": "csv",
            "path": str(left),
            "date_col": "day",
            "date_type": "string",
        }
    ).pull_window(
        ["2026-01-01"],
        ["value"],
        tmp_path / "left-selected.csv",
        key_columns=["left_id"],
        sample_plan=sample,
        key_families=["numeric"],
        side="left",
    )
    manifest = {
        "pair": "parity",
        "columns": ["value"],
        "col_map": {"value": "value"},
        "right_key_cols": ["right_id"],
        "key_families": ["numeric"],
        "sample": sample,
    }
    query, _columns = WarehouseTarget(
        None,
        {
                "source": "databricks",
                "table": "local_sample_parity",
                "date_col": "day",
                "date_type": {"type": "string", "format": "dash"},
        },
        tmp_path,
    ).window_sql(manifest, {"bucket": "2026-01-01", "partitions": ["2026-01-01"]})
    right_keys = {
        normalize_value(row.right_id, "numeric", "right") for row in spark.sql(query).collect()
    }
    left_keys = {
        normalize_value(value, "numeric", "left")
        for value in pd.read_csv(left_path, dtype=object)["left_id"]
    }
    assert left_keys == right_keys
    assert 0 < len(left_keys) < 12
    assert "md5(" in query
    assert " AS STRING) AS " not in query
