import pandas as pd
import pytest
from support import assert_stats_agree

from dtrack_right.backends.targets import SparkTarget
from dtrack_right.common.hashing import sample_csv
from dtrack_right.common.stats import compute_csv_statistics

pytestmark = pytest.mark.live
COLUMNS = ["order_id", "amount", "status", "region"]


def _target(spark, right_tables):
    catalog, schema, table = right_tables["orders"].split(".")
    config = {"source": "databricks", "catalog": catalog, "schema": schema, "table": table}
    return SparkTarget(spark, config | {"date_col": "business_date", "date_type": "string_dash"})


def _manifest(sample):
    return {
        "pair": "orders",
        "columns": ["ORDER_ID", "AMOUNT", "STATUS", "REGION"],
        "col_map": dict(zip(["ORDER_ID", "AMOUNT", "STATUS", "REGION"], COLUMNS, strict=True)),
        "right_key_cols": ["order_id"],
        "key_families": ["numeric"],
        "sample": sample,
        "strict": False,
    }


def _pandas(synthetic, partitions, sample, tmp_path, key_tuples=None):
    frame = pd.read_csv(synthetic.right / "orders.csv", dtype=object)
    frame[frame["business_date"].isin(partitions)].to_csv(tmp_path / "w.csv", index=False)
    sampled = sample_csv(
        tmp_path / "w.csv",
        key_columns=["order_id"],
        mode=sample["mode"],
        rate=sample.get("rate"),
        count=sample.get("count"),
        families=["numeric"],
        key_tuples=key_tuples,
    )
    return compute_csv_statistics(sampled, COLUMNS)


# >>> step 4: Databricks Connect reaches the warehouse and DESCRIBE/COUNT see the uploaded table <<< #
def test_spark_target_counts_a_real_unity_catalog_table(spark, right_tables, synthetic):
    target = _target(spark, right_tables)
    health = target.preflight()
    assert health["ok"], health
    assert target.column_types["amount"].startswith("decimal")
    counts = target.partition_counts("2026-01-01", "2026-02-28")
    truth = pd.read_csv(synthetic.right / "orders.csv", dtype=object)["business_date"]
    assert counts == {k: int(v) for k, v in truth.value_counts().items()}


# >>> step 5: the one-query statistics on the cluster equal the pandas rule on the same sampled rows <<< #
def test_spark_target_statistics_match_pandas_on_the_cluster(
    spark, right_tables, synthetic, tmp_path
):
    partitions = [f"2026-01-{d:02d}" for d in range(1, 31) if d != 15]
    sample = {"mode": "fraction", "rate": 0.25, "threshold": round(0.25 * 2**32)}
    got = _target(spark, right_tables).window_statistics(
        _manifest(sample), {"bucket": "2026-01-01", "partitions": partitions}
    )
    assert_stats_agree(COLUMNS, got, _pandas(synthetic, partitions, sample, tmp_path))
    keys = [["100961"], ["100962"], ["101000"], ["101500"], ["101777"]]
    february = [f"2026-02-{d:02d}" for d in range(1, 29) if d != 10]
    fixed = {"mode": "fixed", "count": 5}
    got = _target(spark, right_tables).window_statistics(
        _manifest(fixed), {"bucket": "2026-02-01", "partitions": february, "key_tuples": keys}
    )
    want = _pandas(synthetic, february, fixed, tmp_path, key_tuples=keys)
    assert_stats_agree(COLUMNS, got, want)


# >>> step 5b: data outside a table; a query over the table, and dd/MM/yyyy files patched through {src} <<< #
def test_query_and_files_kinds_count_like_the_table(
    spark, right_tables, synthetic, workspace, need, tag, keep, tmp_path
):
    import io

    truth = pd.read_csv(synthetic.right / "orders.csv", dtype=object)
    expected = {k: int(v) for k, v in truth["business_date"].value_counts().items()}
    table = ".".join(f"`{part}`" for part in right_tables["orders"].split("."))
    query = SparkTarget(spark, {"kind": "query", "query": f"SELECT * FROM {table}"})
    query.config |= {"date_col": "business_date", "date_type": "string_dash"}
    assert query.preflight()["ok"]
    assert query.partition_counts("2026-01-01", "2026-02-28") == expected

    folder = f"{need('DTRACK_LIVE_VOLUME_WORK').rstrip('/')}/inputs/{tag}"
    raw = truth.assign(d_str=pd.to_datetime(truth.pop("business_date")).dt.strftime("%d/%m/%Y"))
    workspace.files.upload(f"{folder}/orders.csv", io.BytesIO(raw.to_csv(index=False).encode()))
    try:
        files = SparkTarget(
            spark,
            {
                "kind": "files",
                "path": f"{folder}/",
                "format": "csv",
                "options": {"header": "true"},
                "schema_hints": "d_str STRING, amount DECIMAL(18,2)",
                "query": "SELECT * EXCEPT (d_str), to_date(d_str, 'dd/MM/yyyy') AS business_date"
                " FROM {src}",
                "date_col": "business_date",
                "date_type": "date",
            },
        )
        health = files.preflight()
        assert health["ok"], health
        assert files.column_types["amount"].startswith("decimal")
        assert files.partition_counts("2026-01-01", "2026-02-28") == expected
    finally:
        if not keep:
            workspace.files.delete(f"{folder}/orders.csv")
            workspace.files.delete_directory(folder)


# >>> step 5c: every column read as text on the cluster; col_type_overrides make both sides numeric again <<< #
def test_col_type_overrides_line_up_text_files_with_the_left_statistics(
    spark, synthetic, workspace, need, tag, keep, tmp_path
):
    import io

    from dtrack_right.checks.col_check import compare_statistics

    truth = pd.read_csv(synthetic.right / "orders.csv", dtype=object)
    folder = f"{need('DTRACK_LIVE_VOLUME_WORK').rstrip('/')}/inputs/{tag}-types"
    workspace.files.upload(f"{folder}/orders.csv", io.BytesIO(truth.to_csv(index=False).encode()))
    overrides = {"ORDER_ID": "numeric", "AMOUNT": "numeric"}
    try:
        target = SparkTarget(
            spark,
            {
                "kind": "files",
                "path": f"{folder}/",
                "format": "csv",
                "options": {"header": "true", "inferColumnTypes": "false"},
                "date_col": "business_date",
                "date_type": "string_dash",
            },
        )
        assert target.preflight()["ok"]
        assert target.column_types["amount"] == "string"
        partitions = [f"2026-01-{d:02d}" for d in range(1, 31) if d != 15]
        manifest = _manifest({"mode": "all"}) | {"col_type_overrides": overrides}
        got = target.window_statistics(manifest, {"bucket": "2026-01-01", "partitions": partitions})
        truth[truth["business_date"].isin(partitions)].to_csv(tmp_path / "w.csv", index=False)
        col_types = {manifest["col_map"][k]: v for k, v in overrides.items()}
        want = compute_csv_statistics(tmp_path / "w.csv", COLUMNS, col_types=col_types)
        for column in COLUMNS:
            result = compare_statistics(
                pair="orders", window="w", column=column, left=want[column], right=got[column]
            )
            assert result["verdict"] == "match", (column, result)
        assert got["amount"]["col_type"] == got["order_id"]["col_type"] == "numeric"
    finally:
        if not keep:
            workspace.files.delete(f"{folder}/orders.csv")
            workspace.files.delete_directory(folder)
