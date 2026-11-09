import csv
import re
from datetime import date
from types import SimpleNamespace

import pandas as pd
import pytest
from support import FakeFrame, FakeRow, FakeSpark

from pair_recon_right.engine.backends import targets
from pair_recon_right.engine.backends.frame_stats import frame_statistics, hash16
from pair_recon_right.engine.backends.targets import CsvTarget, SparkTarget
from pair_recon_right.engine.common.model.hashing import collapse_decimal, collapse_decimal_sql

ORDERS = {"source": "databricks", "catalog": "main", "schema": "sales", "table": "orders"}
ORDERS |= {"date_col": "business_date", "date_type": "date"}
DESCRIBE = [
    ("business_date", "date", None),
    ("order_id", "bigint", None),
    ("amount", "decimal(10,2)", None),
    ("status", "string", None),
    ("", "", ""),
    ("# Partition Information", "", ""),
]


@pytest.fixture(autouse=True)
def _spark_aggregates(monkeypatch):
    monkeypatch.setattr(targets, "STATS_DRIVER_ROWS", 0)


def _spark_rule(sql_text):
    literals = [item.replace("\\\\", "\\") for item in re.findall(r"'((?:[^']|'')*)'", sql_text)]
    guard, pattern, replacement, tail = literals[:4]

    def apply(text):
        if not re.search(guard, text):
            return text
        text = re.sub(pattern, replacement.replace("$1", r"\1"), text)
        return re.sub(tail, "", text)

    return apply


@pytest.mark.parametrize("text", ["2.50", "2.0", "-0.10", "10", "007", "1e3", "abc", "0.000"])
def test_decimal(text):
    sql = collapse_decimal_sql("x")
    assert sql.startswith("CASE WHEN x rlike ")
    assert _spark_rule(sql)(text) == collapse_decimal(text)


def test_relations():
    assert SparkTarget(None, ORDERS).relation() == "`main`.`sales`.`orders`"
    assert SparkTarget(None, {"query": "SELECT 1"}).relation() == "(SELECT 1) pair_recon_source"
    volume = SparkTarget(None, {"kind": "volume_raw", "path": "/Volumes/c/s/v/o'k/"})
    assert volume.relation() == "read_files('/Volumes/c/s/v/o''k/', format => 'parquet')"
    csv = SparkTarget(None, {"kind": "volume_raw", "path": "/V/x", "format": "csv"})
    assert csv.relation().endswith("format => 'csv')")
    legacy = SparkTarget(None, {"conn_macro": "cat.sch", "table": "t"})
    assert legacy.relation() == "`cat`.`sch`.`t`"
    assert (
        SparkTarget(None, {**ORDERS, "conn_macro": "x.y"}).relation() == "`main`.`sales`.`orders`"
    )


def test_query_windows():
    target = SparkTarget(
        None,
        {
            **ORDERS,
            "query": "SELECT * FROM raw r WHERE {{query_windows:driver}}",
            "query_windows": {"driver": {"expression": "r.business_date", "date_type": "date"}},
        },
    )
    bounded = target.relation(from_date="2026-01-01", to_date="2026-01-31")
    assert "r.business_date >= DATE '2026-01-01'" in bounded
    assert "r.business_date < DATE '2026-02-01'" in bounded
    exact = target.relation(partitions=["2026-01-01", "2026-01-03"])
    assert "r.business_date >= DATE '2026-01-01'" in exact
    assert "r.business_date < DATE '2026-01-02'" in exact
    assert "r.business_date >= DATE '2026-01-03'" in exact
    assert "r.business_date < DATE '2026-01-04'" in exact
    assert "{{" not in bounded + exact
    monthly = SparkTarget(
        None,
        {**target.config, "partition": "month"},
    ).relation(partitions=["2026-01-01"])
    assert "r.business_date >= DATE '2026-01-01'" in monthly
    assert "r.business_date < DATE '2026-02-01'" in monthly


def test_file():
    config = {
        "kind": "files",
        "path": "/Volumes/c/s/work/inputs/orders/",
        "format": "csv",
        "options": {"header": "true", "delimiter": "|"},
        "schema_hints": "amount DECIMAL(18,2)",
    }
    reader = (
        "read_files('/Volumes/c/s/work/inputs/orders/', format => 'csv', header => 'true', "
        "delimiter => '|', schemaHints => 'amount DECIMAL(18,2)')"
    )
    assert SparkTarget(None, config).relation() == reader
    patch = "SELECT *, to_date(d, 'dd/MM/yyyy') AS dt FROM {src}"
    patched = SparkTarget(None, {**config, "query": patch}).relation()
    assert patched == f"(SELECT *, to_date(d, 'dd/MM/yyyy') AS dt FROM {reader}) pair_recon_source"


def test_csv(tmp_path):
    path = tmp_path / "right.csv"
    path.write_text("order_id,amount,status\n1,2.5,好\n2,7.0,✓\n", encoding="utf-8")
    target = CsvTarget(
        {"source": "csv", "path": str(path), "partition": "all", "pre_sampled": True}
    )
    assert target.health_check()["ok"]
    assert target.partition_counts() == {"all": 2}
    manifest = _manifest({"mode": "all"})
    manifest["partition"] = "all"
    manifest["statistics"] = ["n_unique", "hash_sum"]
    stats = target.window_statistics(manifest, {"partitions": ["all"]})
    assert stats["status"]["n_unique"] == 2
    assert set(stats["amount"]) == {"col_type", "n_total", "n_unique", "hash_sum"}


def test_health_check():
    spark = FakeSpark(lambda sql: DESCRIBE)
    target = SparkTarget(spark, ORDERS)
    health = target.health_check()
    assert health["ok"] and health["columns"] == ["amount", "business_date", "order_id", "status"]
    assert spark.statements == ["DESCRIBE QUERY SELECT * FROM `main`.`sales`.`orders`"]
    assert target.column_types["amount"] == "decimal(10,2)"
    missing = SparkTarget(
        FakeSpark(lambda sql: DESCRIBE), {**ORDERS, "date_col": "dt"}
    ).health_check()
    assert missing["ok"] is False and "'dt'" in missing["error"]
    assert missing["error"].startswith("ValueError: ")
    assert missing["relation"] == "`main`.`sales`.`orders`"


COMPACT = {"type": "string", "format": "compact"}


@pytest.mark.parametrize(
    ("date_type", "column_type", "error"),
    [
        (COMPACT, "string", None),
        (COMPACT, "bigint", None),
        ("date", "timestamp", None),
        ("date", "date", None),
        ({"type": "number", "format": "month"}, "int", None),
        (COMPACT, "timestamp", "date_col 'business_date' is timestamp"),
        ({"type": "sas", "format": "datetime"}, "timestamp", "needs a text or numeric column"),
        ("date", "string", "date_col 'business_date' is string"),
        ("date", "int", "needs a date or timestamp column"),
    ],
)
def test_date_type_fits_column(date_type, column_type, error):
    describe = [("business_date", column_type, None)] + DESCRIBE[1:]
    target = SparkTarget(FakeSpark(lambda sql: describe), {**ORDERS, "date_type": date_type})
    health = target.health_check()
    if error is None:
        assert health["ok"]
        return
    assert health["ok"] is False
    assert error in health["error"] and "date type" in health["error"]
    assert "LIMIT 16" not in "".join(target.spark.statements)


def test_auto_date_type_probe_skips_the_column_check():
    describe = [("business_date", "timestamp", None)] + DESCRIBE[1:]

    def respond(sql):
        if sql.startswith("DESCRIBE"):
            return describe
        return [(date(2026, 8, 1),)] if "LIMIT 16" in sql else []

    target = SparkTarget(FakeSpark(respond), {**ORDERS, "date_type": None})
    assert target.health_check()["ok"]
    assert target.config["date_type"] == "date"


def test_numeric_keys_hash_as_plain_decimal_text():
    spark = FakeSpark(lambda sql: DESCRIBE if sql.startswith("DESCRIBE") else [(7,)])
    target = SparkTarget(spark, {**ORDERS, "partition": "all"})
    assert target.health_check()["ok"]
    sample = {"mode": "fraction", "threshold": 1000}
    target.partition_counts(
        sample=sample, key_columns=["amount", "order_id"], key_families=["string", "string"]
    )
    sql = spark.statements[-1]
    assert "regexp_replace(regexp_replace(CAST(`amount` AS STRING)" in sql
    assert "CAST(`order_id` AS STRING)" in sql and "DECIMAL(38,15)" not in sql
    manifest = _manifest(sample, keys=("amount",), families=("string",))
    assert "regexp_replace(regexp_replace(CAST(`amount` AS STRING)" in target._sample_predicate(
        manifest, {}
    )
    assert "regexp_replace(regexp_replace(CAST(`amount`" not in SparkTarget(
        None, ORDERS
    )._sample_predicate(manifest, {})


def test_window_count_uses_the_statistics_predicate():
    spark = FakeSpark(lambda sql: DESCRIBE if sql.startswith("DESCRIBE") else [(42,)])
    target = SparkTarget(spark, ORDERS)
    manifest = _manifest({"mode": "fraction", "threshold": 99})
    window = {"bucket": "2026-01-01", "partitions": ["2026-01-01", "2026-01-02"]}
    assert target.window_count(manifest, window) == 42
    sql = spark.statements[-1]
    assert sql.startswith("SELECT count(1) FROM `main`.`sales`.`orders` WHERE ")
    assert "`business_date` >= DATE '2026-01-01' AND `business_date` < DATE '2026-01-03'" in sql
    assert "< 99" in sql and "count(DISTINCT" not in sql


def test_key_family_date_type_fits_column():
    spark = FakeSpark(lambda sql: DESCRIBE if sql.startswith("DESCRIBE") else [(7,)])
    target = SparkTarget(spark, {**ORDERS, "partition": "all"})
    assert target.health_check()["ok"]
    compact = {"type": "date", "left_date_type": "date", "right_date_type": COMPACT}
    native = {"type": "date", "left_date_type": COMPACT, "right_date_type": "date"}
    sample = {"mode": "fraction", "threshold": 1000}
    assert target.partition_counts(sample=sample, key_columns=["status"], key_families=[compact])
    with pytest.raises(ValueError, match="key column 'status' is string"):
        target.partition_counts(sample=sample, key_columns=["status"], key_families=[native])
    manifest = _manifest(sample, keys=("business_date",), families=(compact,))
    with pytest.raises(ValueError, match="key column 'business_date' is date"):
        target._sample_predicate(manifest, {})
    manifest = _manifest(sample, keys=("business_date",), families=(native,))
    assert "`business_date`" in target._sample_predicate(manifest, {})
    assert SparkTarget(None, ORDERS)._sample_predicate(_manifest(sample, families=(native,)), {})


def test_health_check_logs_context_and_spark_failure(caplog):
    def explode(sql):
        raise RuntimeError("Unable to fetch tables of db sales")

    caplog.set_level("INFO", logger="pair_recon.right.target")
    health = SparkTarget(FakeSpark(explode), {**ORDERS, "catalog": None}).health_check()
    assert health == {
        "ok": False,
        "source": "databricks",
        "relation": "`sales`.`orders`",
        "error": "RuntimeError: Unable to fetch tables of db sales",
    }
    messages = [record.getMessage() for record in caplog.records]
    assert any(m.startswith("health check relation=`sales`.`orders` kind=table") for m in messages)
    assert any(m.startswith("spark.sql health_check failed after") for m in messages)
    assert any("Unable to fetch tables" in (record.exc_text or "") for record in caplog.records)


def test_sas():
    def respond(sql):
        if sql.startswith("DESCRIBE"):
            return [("business_date", "string", None)] + DESCRIBE[1:]
        if "LIMIT 16" in sql:
            return [("01AUG2026:09:45:35",), ("02AUG2026:00:00:00",)]
        return [("2026-08-01", 1)]

    spark = FakeSpark(respond)
    target = SparkTarget(spark, {**ORDERS, "date_type": "sas"})
    assert target.health_check()["ok"]
    assert target.config["date_type"] == {"type": "sas", "format": "datetime"}
    assert target.partition_counts() == {"2026-08-01": 1}
    assert len([sql for sql in spark.statements if "LIMIT 16" in sql]) == 1


def test_partition():
    spark = FakeSpark(lambda sql: [("2026-01-01", 3), ("2026-01-02", 4)])
    target = SparkTarget(spark, {**ORDERS, "where": "region = 'EU'"})
    assert target.partition_counts("2026-01-01", "2026-01-31") == {
        "2026-01-01": 3,
        "2026-01-02": 4,
    }
    assert spark.statements == [
        (
            "SELECT to_date(`business_date`), COUNT(*) FROM `main`.`sales`.`orders` "
            "WHERE (region = 'EU') AND "
            "(`business_date` >= DATE '2026-01-01' AND `business_date` < DATE '2026-02-01') "
            "GROUP BY to_date(`business_date`)"
        )
    ]


def test_partition_counts_can_limit_a_retry_to_failed_partitions():
    spark = FakeSpark(lambda sql: [("2026-01-02", 4)])
    target = SparkTarget(spark, ORDERS)

    assert target.partition_counts(partitions=["2026-01-02"]) == {"2026-01-02": 4}
    assert "`business_date` >= DATE '2026-01-02'" in spark.statements[0]
    assert "`business_date` < DATE '2026-01-03'" in spark.statements[0]


def test_month_partition_aggregates_daily_query_results():
    spark = FakeSpark(lambda sql: [("2026-01-01", 3), ("2026-01-31", 4), ("2026-02-01", 5)])
    target = SparkTarget(spark, {**ORDERS, "partition": "month"})
    assert target.partition_counts() == {"2026-01-01": 7, "2026-02-01": 5}
    assert "GROUP BY to_date(`business_date`)" in spark.statements[0]


def test_all():
    describe = [row for row in DESCRIBE if row[0] != "business_date"]

    def respond(sql):
        if sql.startswith("DESCRIBE"):
            return describe
        if "COUNT(*)" in sql:
            return [(7,)]
        return [FakeRow(_stats_row())]

    spark = FakeSpark(respond)
    config = {
        "source": "databricks",
        "catalog": "main",
        "schema": "sales",
        "table": "orders",
        "partition": "all",
    }
    target = SparkTarget(spark, config)
    assert target.health_check()["ok"]
    assert target.partition_counts() == {"all": 7}
    count_sql = spark.statements[-1]
    target.partition_counts(
        sample={"mode": "fraction", "threshold": 1000},
        key_columns=["order_id"],
        key_families=["numeric"],
    )
    assert "< 1000" in spark.statements[-1]
    manifest = _manifest({"mode": "fraction", "threshold": 1000})
    manifest["partition"] = "all"
    target.window_statistics(manifest, {"bucket": "all", "partitions": ["all"]})
    stats_sql = spark.statements[-1]
    assert count_sql == "SELECT COUNT(*) FROM `main`.`sales`.`orders` WHERE 1=1"
    assert "business_date" not in stats_sql
    assert "< 1000" in spark.statements[-2]
    assert "FROM `pair_recon_" in stats_sql and "WHERE 1=1" in stats_sql


def test_sql():
    events = []

    class Spark:
        def sql(self, sql):
            events.append(("execute", sql))
            return SimpleNamespace(collect=list)

    audit = SimpleNamespace(record=lambda sql, **meta: events.append(("audit", sql, meta)))
    SparkTarget(Spark(), ORDERS).set_sql_audit(audit)._rows("SELECT * FROM huge_table")
    assert events == [
        ("audit", "SELECT * FROM huge_table", {"backend": "spark", "operation": "query"}),
        ("execute", "SELECT * FROM huge_table"),
    ]


def _manifest(sample, keys=("order_id",), families=("numeric",)):
    return {
        "pair": "orders",
        "columns": ["ORDER_ID", "AMOUNT", "STATUS"],
        "col_map": {"ORDER_ID": "order_id", "AMOUNT": "amount", "STATUS": "status"},
        "right_key_cols": list(keys),
        "key_families": list(families),
        "sample": sample,
    }


@pytest.mark.parametrize(
    ("sample", "window", "expected"),
    [
        ({"mode": "all"}, {}, None),
        ({"mode": "fraction", "threshold": 1000}, {}, "conv(substr(md5(concat_ws('|', "),
        ({"mode": "fixed"}, {"key_tuples": [["7"], ["o'k"]]}, "IN ('7', 'o\\'k')"),
        ({"mode": "fixed"}, {"key_tuples": []}, "1=0"),
    ],
)
def test_sample(sample, window, expected):
    predicate = SparkTarget(None, ORDERS)._sample_predicate(_manifest(sample), window)
    if expected is None:
        assert predicate is None
    else:
        assert expected in predicate
    assert SparkTarget(None, ORDERS)._sample_predicate(_manifest(sample, keys=()), window) is None


@pytest.mark.parametrize("selector", ["order_id", "ORDER_ID", "missing_gid"])
def test_modular_sampling_checks_the_mapped_right_column_before_querying(selector):
    spark = FakeSpark(lambda sql: DESCRIBE if sql.startswith("DESCRIBE") else [])
    target = SparkTarget(spark, ORDERS)
    manifest = _manifest(
        {
            "mode": "fraction",
            "rate": 0.01,
            "modular": {
                "left_column": "LEFT_GID",
                "right_column": selector,
                "run_date": "20261109",
            },
        }
    )
    window = {"bucket": "2024-12-01", "partitions": ["2024-12-01"], "sample_seed": 123}
    if selector == "missing_gid":
        with pytest.raises(targets.MissingSamplingColumnError) as error:
            target.window_count(manifest, window)
        assert "orders 2024-12-01" in str(error.value)
        assert "LEFT_GID" in str(error.value) and "missing_gid" in str(error.value)
        assert all(sql.startswith("DESCRIBE") for sql in spark.statements)
    else:
        assert target.health_check()["ok"]
        predicate = target._sample_predicate(manifest, window)
        assert selector in predicate


def _stats_row():
    row = {"n_total": 4}
    values = [
        (0, 3, "1000", None, None, "1", "9", None, None),
        (1, 2, "2000", 7.5, 1.25, "2.5", "9.75", None, None),
        (0, 2, "3000", None, None, "a", "bb", 1.5, 2),
    ]
    for i, (missing, unique, hashed, mean, std, low, high, avg, top) in enumerate(values):
        row |= {
            f"n_missing_{i}": missing,
            f"n_unique_{i}": unique,
            f"hash_sum_{i}": hashed,
            f"mean_{i}": mean,
            f"std_{i}": std,
            f"min_{i}": low,
            f"max_{i}": high,
            f"length_avg_{i}": avg,
            f"length_max_{i}": top,
        }
    return row


def test_statistics():
    spark = FakeSpark(
        lambda sql: DESCRIBE if sql.startswith("DESCRIBE") else [FakeRow(_stats_row())]
    )
    target = SparkTarget(spark, ORDERS)
    window = {"bucket": "2026-01-01", "partitions": ["2026-01-01", "2026-01-02"]}
    stats = target.window_statistics(_manifest({"mode": "fraction", "threshold": 99}), window)
    assert len(spark.statements) == 3
    materialize, query = spark.statements[1:]
    assert (
        "`business_date` >= DATE '2026-01-01' AND `business_date` < DATE '2026-01-03'"
        in materialize
    )
    assert "< 99" in materialize and "APPROX" not in query.upper()
    assert "FROM `pair_recon_" in query and "WHERE 1=1" in query
    assert "AS min_0" in query
    assert "cast(min(cast(`amount` as double)) as string)" in query
    assert stats["order_id"]["col_type"] == "categorical"
    assert stats["order_id"]["mean"] is None and stats["order_id"]["top_10"] == ""
    assert stats["amount"] == {
        "col_type": "numeric",
        "n_total": 4,
        "n_missing": 1,
        "n_unique": 2,
        "hash_sum": "2000",
        "mean": 7.5,
        "std": 1.25,
        "min_val": "2.5",
        "max_val": "9.75",
        "length_avg": None,
        "length_max": None,
        "top_10": "",
    }
    assert stats["status"]["col_type"] == "categorical" and stats["status"]["length_max"] == 2
    selected_manifest = _manifest({"mode": "all"})
    selected_manifest["statistics"] = ["n_unique", "hash_sum"]
    selected = target.window_statistics(selected_manifest, window)
    selected_query = spark.statements[-1]
    assert "count(DISTINCT" in selected_query and "sha2(" in selected_query
    assert all(
        fragment not in selected_query
        for fragment in ("count_if(", "avg(", "stddev_samp(", "min(", "max(")
    )
    assert selected["amount"] == {
        "col_type": "numeric",
        "n_total": 4,
        "n_unique": 2,
        "hash_sum": "2000",
    }


def test_window_sample_keeps_bounded_rows_in_memory():
    spark = FakeSpark(
        lambda sql: (
            DESCRIBE if sql.startswith("DESCRIBE") else [(7, date(2026, 1, 1), 12.5, "open")]
        )
    )
    manifest = {
        **_manifest({"mode": "all"}),
        "partition": "day",
        "key_cols": ["ORDER_ID"],
        "left": {"date_col": "ORDER_DT"},
        "right": {"date_col": "business_date"},
    }
    sample = SparkTarget(spark, ORDERS).window_sample(
        manifest,
        {
            "bucket": "2026-01-01",
            "partitions": ["2026-01-01"],
            "diagnostic_key_tuples": [["7"]],
        },
        25,
    )

    assert sample["limit"] == 25
    assert sample["rows"] == [[7, date(2026, 1, 1), 12.5, "open"]]
    assert [row["right_column"] for row in sample["pairs"]] == [
        "order_id",
        "business_date",
        "amount",
        "status",
    ]
    assert "key" in sample["pairs"][0]["roles"]
    assert "LIMIT 25" in spark.statements[-1]
    assert "ORDER BY" in spark.statements[-1] and "'7'" in spark.statements[-1]
    assert "WHERE" in spark.statements[-1]


@pytest.mark.parametrize(
    ("overrides", "fragments", "types"),
    [
        pytest.param(
            {"STATUS": "numeric", "AMOUNT": "categorical"},
            (
                "avg(try_cast(",
                "AS mean_2",
                "NULL AS mean_1",
                "AS length_avg_1",
            ),
            {"status": "numeric", "amount": "categorical", "order_id": "categorical"},
            id="mixed",
        ),
        pytest.param(
            {"ORDER_ID": "numeric"},
            ("avg(cast(`order_id` as double)) AS mean_0",),
            {"order_id": "numeric"},
            id="integral",
        ),
    ],
)
def test_overrides(overrides, fragments, types):
    spark = FakeSpark(
        lambda sql: DESCRIBE if sql.startswith("DESCRIBE") else [FakeRow(_stats_row())]
    )
    manifest = _manifest({"mode": "all"})
    manifest["col_type_overrides"] = overrides
    stats = SparkTarget(spark, ORDERS).window_statistics(manifest, {"partitions": ["2026-01-01"]})
    query = spark.statements[-1]
    assert all(fragment in query for fragment in fragments)
    assert all(stats[column]["col_type"] == kind for column, kind in types.items())


def test_date():
    describe = [row if row[0] != "status" else ("status", "timestamp", None) for row in DESCRIBE]
    spark = FakeSpark(
        lambda sql: describe if sql.startswith("DESCRIBE") else [FakeRow(_stats_row())]
    )
    manifest = _manifest({"mode": "all"})
    manifest["column_types"] = {
        "STATUS": {
            "type": "date",
            "left_date_type": {"type": "sas", "format": "datetime"},
            "right_date_type": "date",
        }
    }
    SparkTarget(spark, ORDERS).window_statistics(manifest, {"partitions": ["2026-01-01"]})
    query = spark.statements[-1]
    assert "date_format(to_date(`status`), 'yyyy-MM-dd')" in query
    assert "NULL AS mean_2" in query
    text_spark = FakeSpark(lambda sql: DESCRIBE if sql.startswith("DESCRIBE") else [])
    with pytest.raises(ValueError, match="column 'status' is string; date type 'date'"):
        SparkTarget(text_spark, ORDERS).window_statistics(manifest, {"partitions": ["2026-01-01"]})


def test_system_normalization_rules():
    spark = FakeSpark(
        lambda sql: DESCRIBE if sql.startswith("DESCRIBE") else [FakeRow(_stats_row())]
    )
    target = SparkTarget(spark, ORDERS)
    target.window_statistics(_manifest({"mode": "all"}), {"partitions": ["2026-01-01"]})
    assert "lower(regexp_replace(trim(cast(`status` as string))" in spark.statements[-1]
    assert "[0-9]{4}-[0-9]{2}-[0-9]{2}" in spark.statements[-1]


def test_fixed():
    keys, families = ("card", "amt", "eff_dt"), ("string", "numeric", "date")
    manifest = _manifest({"mode": "fixed"}, keys=keys, families=families)
    window = {"key_tuples": [[" ABC ", "1.50", "2026-01-31 00:00:00"], ["O'K", "007", "20260131"]]}
    predicate = SparkTarget(None, ORDERS)._sample_predicate(manifest, window)
    assert predicate.endswith("IN ('abc|1.5|2026-01-31', 'o\\'k|7|2026-01-31')")


def test_statistics_batches_wide_tables(monkeypatch):
    monkeypatch.setattr(targets, "STATS_COLUMNS", 2)
    spark = FakeSpark(
        lambda sql: DESCRIBE if sql.startswith("DESCRIBE") else [FakeRow(_stats_row())]
    )
    window = {"bucket": "2026-01-01", "partitions": ["2026-01-01"]}
    seen = []
    stats = SparkTarget(spark, ORDERS).window_statistics(
        _manifest({"mode": "all"}), window, progress=lambda done, total: seen.append((done, total))
    )
    materialize, *queries = spark.statements[1:]
    assert materialize.startswith("SELECT * FROM `main`.`sales`.`orders` WHERE ")
    assert len(queries) == 2 and seen == [(2, 3), (3, 3)]
    assert all(query.startswith("SELECT count(1) AS n_total, ") for query in queries)
    assert "AS n_unique_1" in queries[0] and "AS n_unique_2" not in queries[0]
    assert "AS n_unique_2" in queries[1] and "AS n_unique_0" not in queries[1]
    assert stats["amount"]["hash_sum"] == "2000" and stats["status"]["length_max"] == 2


def test_spark_batches_reuse_one_cached_sample_without_transferring_rows_to_pandas(monkeypatch):
    def respond(sql):
        if sql.startswith("DESCRIBE"):
            return DESCRIBE
        if sql.startswith("SELECT * FROM"):
            return [FakeRow({"order_id": i}) for i in range(4)]
        if sql.startswith("SELECT count(1) FROM"):
            return [(4,)]
        return [FakeRow(_stats_row())]

    def no_pandas(_frame):
        pytest.fail("Spark statistics must collect only the aggregate row")

    monkeypatch.setattr(FakeFrame, "toPandas", no_pandas)
    monkeypatch.setattr(targets, "STATS_COLUMNS", 2)
    spark = FakeSpark(respond)
    target = SparkTarget(spark, ORDERS)
    manifest = _manifest({"mode": "fraction", "threshold": 99})
    window = {"bucket": "2026-01-01", "partitions": ["2026-01-01"]}
    first = target.window_statistics(manifest, window)
    assert target.window_statistics(manifest, window) == first
    assert target.window_count(manifest, window) == 4
    materializations = [sql for sql in spark.statements if sql.startswith("SELECT * FROM")]
    assert len(materializations) == 1 and "< 99" in materializations[0]
    queries = [sql for sql in spark.statements if sql.startswith("SELECT count(1)")]
    assert len(queries) == 5
    assert all("FROM `pair_recon_" in sql and "WHERE 1=1" in sql for sql in queries)
    target.release_window(window)
    assert target._views == {} and len(spark.catalog.dropped) == 1


@pytest.mark.parametrize("fail", [False, True])
def test_query_stage_reports_start_and_always_clears(fail):
    def respond(_sql):
        if fail:
            raise RuntimeError("query failed")
        return [(1,)]

    events = []
    target = SparkTarget(FakeSpark(respond), ORDERS)
    target.set_stage_reporter(lambda *event: events.append(event))
    if fail:
        with pytest.raises(RuntimeError, match="query failed"):
            target._collect("SELECT 1", "window_statistics", log_context="batch=1/2")
    else:
        assert target._collect("SELECT 1", "window_statistics", log_context="batch=1/2") == [(1,)]
    assert [event[1] for event in events] == ["window_statistics", None]
    assert events[0][0] == events[1][0] and events[0][2] == "batch=1/2"


DRIVER_ROWS = {
    "t0": ["1", "2", "9", "9"],
    "t1": ["2.5", "9.75", None, "9.75"],
    "v1": [2.5, 9.75, None, 9.75],
    "t2": ["a", "bb", None, "a"],
}


def _driver_spark(count=4):
    def respond(sql):
        if sql.startswith("DESCRIBE"):
            return DESCRIBE
        if sql.startswith("SELECT count(1) FROM"):
            return [(count,)]
        names = re.findall(r" AS ([tv][0-9]+)", sql)
        return [FakeRow({name: DRIVER_ROWS[name][i] for name in names}) for i in range(4)]

    return FakeSpark(respond)


def test_statistics_reduce_in_pandas_below_the_driver_row_cap(monkeypatch, caplog):
    monkeypatch.setattr(targets, "STATS_DRIVER_ROWS", 1000)
    monkeypatch.setattr(targets, "STATS_DRIVER_CELLS", 8)
    monkeypatch.setattr(targets, "STATS_COLUMNS", 1)
    spark = _driver_spark()
    seen = []
    with caplog.at_level("INFO", logger="pair_recon.right.target"):
        stats = SparkTarget(spark, ORDERS).window_statistics(
            _manifest({"mode": "all"}),
            {"bucket": "2026-01-01", "partitions": ["2026-01-01"]},
            progress=lambda done, total: seen.append((done, total)),
        )
    materialize, *queries = spark.statements[1:]
    assert materialize.startswith("SELECT * FROM `main`.`sales`.`orders` WHERE ")
    assert all("FROM `pair_recon_" in query and "WHERE 1=1" in query for query in queries)
    assert len(queries) == 2 and seen == [(2, 3), (3, 3)]
    assert "pair=orders vintage=2026-01-01 batch=1/2" in caplog.text
    wide = _driver_spark()
    monkeypatch.setattr(targets, "STATS_DRIVER_CELLS", 50000000)
    SparkTarget(wide, ORDERS).window_statistics(
        _manifest({"mode": "all"}), {"partitions": ["2026-01-01"]}
    )
    assert len(wide.statements) == 3 and " AS t2 FROM" in wide.statements[-1]
    assert "pair=orders vintage=2026-01-01 batch=2/2" in caplog.text
    assert queries[0].startswith("SELECT ") and "cast(`order_id` as string)" in queries[0]
    assert " AS t0, " in queries[0] and " AS t1, " in queries[0] and " AS t2" not in queries[0]
    assert " AS v1 FROM" in queries[0] and " AS v0" not in queries[0]
    assert "cast(`status` as string)" in queries[1] and " AS t2 FROM" in queries[1]
    assert " AS t1" not in queries[1] and " AS v2" not in queries[1]
    assert all("count(DISTINCT" not in query and "sha2(" not in query for query in queries)
    assert stats["order_id"] == {
        "col_type": "categorical",
        "n_total": 4,
        "n_missing": 0,
        "n_unique": 3,
        "hash_sum": str(sum(hash16(value) for value in DRIVER_ROWS["t0"])),
        "mean": None,
        "std": None,
        "min_val": "1",
        "max_val": "9",
        "length_avg": 1.0,
        "length_max": 1,
        "top_10": "",
    }
    amount = stats["amount"]
    assert amount["col_type"] == "numeric" and amount["n_missing"] == 1
    assert amount["n_unique"] == 2 and amount["min_val"] == "2.5" and amount["max_val"] == "9.75"
    assert amount["hash_sum"] == str(hash16("2.5") + 2 * hash16("9.75") + hash16("<<NULL>>"))
    assert amount["mean"] == pytest.approx(22 / 3) and amount["std"] == pytest.approx(4.186, 1e-3)
    assert amount["length_avg"] is None and amount["length_max"] is None
    assert stats["status"]["length_avg"] == pytest.approx(4 / 3)
    assert stats["status"]["length_max"] == 2 and stats["status"]["n_unique"] == 2
    manifest = _manifest({"mode": "all"})
    manifest["statistics"] = ["n_unique", "hash_sum"]
    selected = SparkTarget(spark, ORDERS).window_statistics(
        manifest, {"partitions": ["2026-01-01"]}
    )
    assert " AS v1" not in spark.statements[-1]
    assert selected["amount"] == {
        "col_type": "numeric",
        "n_total": 4,
        "n_unique": 2,
        "hash_sum": amount["hash_sum"],
    }


def test_statistics_fall_back_to_spark_above_the_driver_row_cap(monkeypatch):
    monkeypatch.setattr(targets, "STATS_DRIVER_ROWS", 3)
    spark = FakeSpark(
        lambda sql: (
            DESCRIBE
            if sql.startswith("DESCRIBE")
            else [FakeRow({})] * 4
            if sql.startswith("SELECT * FROM")
            else [FakeRow(_stats_row())]
        )
    )
    stats = SparkTarget(spark, ORDERS).window_statistics(
        _manifest({"mode": "all"}), {"partitions": ["2026-01-01"]}
    )
    assert spark.statements[2].startswith("SELECT count(1) AS n_total, count_if(")
    assert "FROM `pair_recon_" in spark.statements[2] and "WHERE 1=1" in spark.statements[2]
    assert spark.catalog.dropped == []
    assert stats["amount"]["hash_sum"] == "2000"


def test_optional_pandas_timing_separates_wait_fetch_and_reduction(monkeypatch, caplog):
    clock, released = [0.0], []
    original_pandas, original_stats = FakeFrame.toPandas, targets.frame_statistics

    class Slots:
        def acquire(self):
            clock[0] += 10

        def release(self):
            released.append(True)

    def fetch(frame):
        clock[0] += 5
        return original_pandas(frame)

    def reduce(*args):
        clock[0] += 3
        return original_stats(*args)

    monkeypatch.setattr(targets.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(targets, "_driver_slots", Slots())
    monkeypatch.setattr(FakeFrame, "toPandas", fetch)
    monkeypatch.setattr(targets, "frame_statistics", reduce)
    target = SparkTarget(_driver_spark(), ORDERS)
    target.column_types = {name: kind for name, kind, _ in DESCRIBE if name}
    plans = target._column_plans(_manifest({"mode": "all"}))
    with caplog.at_level("INFO", logger="pair_recon.right.target"):
        target._driver_statistics(
            plans, ["n_unique", "hash_sum"], "example", "1=1", log_context="batch=1/1"
        )
    assert "acquired (10.000s)" in caplog.text
    assert "to pandas (5.000s)" in caplog.text
    assert "done (3.000s)" in caplog.text
    assert released == [True]


def test_driver_statistics_match_the_left_duckdb_statistics(tmp_path):
    from pair_recon_left.common.model.manifest import DEFAULT_STATISTICS
    from pair_recon_left.common.runtime.stats import compute_csv_statistics

    amount = ["2.5", "9.75", None, "9.75", "3.125"]
    status = ["a", "bb", None, "a", "好"]
    code = ["007", "12", "12", None, "5"]
    path = tmp_path / "left.csv"
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["amount", "status", "code"])
        writer.writerows(zip(amount, status, code))
    selected = list(DEFAULT_STATISTICS)
    left = compute_csv_statistics(path, ["amount", "status", "code"], statistics=selected)
    frame = pd.DataFrame(
        {
            "t0": amount,
            "v0": [None if value is None else float(value) for value in amount],
            "t1": status,
            "t2": code,
        }
    )
    plans = [
        {"index": 0, "column": "amount", "numeric": True, "integral": False},
        {"index": 1, "column": "status", "numeric": False, "integral": False},
        {"index": 2, "column": "code", "numeric": False, "integral": True},
    ]
    right = frame_statistics(frame, plans, selected)
    assert set(left) == set(right)
    for column in left:
        assert set(left[column]) == set(right[column]), column
        for name, value in left[column].items():
            if isinstance(value, float):
                assert right[column][name] == pytest.approx(value), (column, name)
            else:
                assert right[column][name] == value, (column, name)


def test_window_rows_projects_normalized_key_raw_and_normalized_values():
    spark = FakeSpark(
        lambda sql: DESCRIBE if sql.startswith("DESCRIBE") else [("7|2026-01-01", 7, "2.50", "2.5")]
    )
    manifest = _manifest({"mode": "fraction", "threshold": 99})
    rows = SparkTarget(spark, ORDERS).window_rows(
        manifest, {"bucket": "2026-01-01", "partitions": ["2026-01-01"]}, ["AMOUNT"]
    )
    sql = spark.statements[-1]
    assert sql.startswith("SELECT concat_ws('|', ") and " AS _key, `order_id` AS k0, " in sql
    assert "`amount` AS v0, `__pr1_" in sql and " AS n0 FROM (SELECT *, " in sql
    assert "SELECT * FROM `main`.`sales`.`orders` WHERE " in sql and sql.endswith("WHERE 1=1")
    assert "< 99" in sql and "LIMIT" not in sql and " IN (" not in sql
    assert list(rows.pop("rows")) == [["7|2026-01-01", 7, "2.50", "2.5"]]
    assert rows == {
        "pairs": [{"left_column": "AMOUNT", "right_column": "amount"}],
        "key_columns": ["order_id"],
    }


def test_numeric_column_plan_uses_canonical_decimal_text_for_databricks():
    target = SparkTarget(None, ORDERS)
    target.column_types = {"amount": "string"}
    manifest = _manifest({"mode": "all"})
    manifest["col_type_overrides"] = {"AMOUNT": "numeric"}

    plan = next(plan for plan in target._column_plans(manifest) if plan["left"] == "AMOUNT")

    assert plan["numeric"] is True
    assert plan["text"].startswith("`__pr1_") and "try_cast" in " ".join(plan["layers"])
    assert "DECIMAL(38,18)" in " ".join(plan["layers"])
