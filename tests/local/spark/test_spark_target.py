import pandas as pd
import pytest
from support import assert_stats_agree, synthetic_data

from dtrack_right.backends.targets import SparkTarget
from dtrack_right.common.hashing import sample_csv
from dtrack_right.common.stats import compute_csv_statistics

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
            "date_type": "string_dash",
        },
    )
    health = target.preflight()
    assert health["ok"] and health["columns"] == sorted(COLUMNS + ["business_date"])
    counts = target.partition_counts("2026-01-01", "2026-02-28")
    truth = pd.read_csv(orders, dtype=object)["business_date"].value_counts().to_dict()
    assert counts == {k: int(v) for k, v in truth.items()}
    assert counts["2026-01-15"] == 27 and "2026-02-10" not in counts


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
            "date_type": "string_dash",
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
            "date_type": "string_dash",
        },
    )
    spark_stats = target.window_statistics(
        _manifest(sample), {"bucket": "2026-02-01", "partitions": partitions, "key_tuples": keys}
    )
    pandas_stats = _pandas_reference(orders, partitions, sample, False, tmp_path, key_tuples=keys)
    assert spark_stats["order_id"]["n_total"] == pandas_stats["order_id"]["n_total"] == 4
    assert_stats_agree(COLUMNS, spark_stats, pandas_stats)
