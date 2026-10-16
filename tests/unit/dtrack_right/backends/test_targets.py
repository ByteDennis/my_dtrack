import re
from types import SimpleNamespace

import pytest
from support import FakeRow, FakeSpark

from dtrack_right.backends import targets
from dtrack_right.backends.targets import SparkTarget
from dtrack_right.common.hashing import collapse_decimal

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


# >>> read the regexes out of the generated Spark SQL and apply them with Python's re <<< #
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
def test_decimal_rule(text):
    sql = targets._collapse_decimal_sql("x")
    assert sql.startswith("CASE WHEN x rlike ")
    assert _spark_rule(sql)(text) == collapse_decimal(text)


def test_relations():
    assert SparkTarget(None, ORDERS).relation() == "`main`.`sales`.`orders`"
    assert SparkTarget(None, {"query": "SELECT 1"}).relation() == "(SELECT 1) dtrack_source"
    volume = SparkTarget(None, {"kind": "volume_raw", "path": "/Volumes/c/s/v/o'k/"})
    assert volume.relation() == "read_files('/Volumes/c/s/v/o''k/', format => 'parquet')"
    csv = SparkTarget(None, {"kind": "volume_raw", "path": "/V/x", "format": "csv"})
    assert csv.relation().endswith("format => 'csv')")
    legacy = SparkTarget(None, {"conn_macro": "cat.sch", "table": "t"})
    assert legacy.relation() == "`cat`.`sch`.`t`"
    assert (
        SparkTarget(None, {**ORDERS, "conn_macro": "x.y"}).relation() == "`main`.`sales`.`orders`"
    )


# >>> files that are not a table: read_files with options and schema hints, patched through {src} <<< #
def test_file_relation():
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
    assert patched == f"(SELECT *, to_date(d, 'dd/MM/yyyy') AS dt FROM {reader}) dtrack_source"


def test_preflight():
    spark = FakeSpark(lambda sql: DESCRIBE)
    target = SparkTarget(spark, ORDERS)
    health = target.preflight()
    assert health["ok"] and health["columns"] == ["amount", "business_date", "order_id", "status"]
    assert spark.statements == ["DESCRIBE QUERY SELECT * FROM `main`.`sales`.`orders`"]
    assert target.column_types["amount"] == "decimal(10,2)"
    missing = SparkTarget(FakeSpark(lambda sql: DESCRIBE), {**ORDERS, "date_col": "dt"}).preflight()
    assert missing["ok"] is False and "'dt'" in missing["error"]


def test_sas_probe():
    def respond(sql):
        if sql.startswith("DESCRIBE"):
            return DESCRIBE
        if "LIMIT 16" in sql:
            return [("01AUG2026:09:45:35",), ("02AUG2026:00:00:00",)]
        return [("2026-08-01", 1)]

    spark = FakeSpark(respond)
    target = SparkTarget(spark, {**ORDERS, "date_type": "sas"})
    assert target.preflight()["ok"]
    assert target.config["date_type"] == {"type": "sas", "format": "datetime"}
    assert target.partition_counts() == {"2026-08-01": 1}
    assert len([sql for sql in spark.statements if "LIMIT 16" in sql]) == 1


def test_partition_counts():
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


def test_all_partition():
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
        "partition_mode": "all",
    }
    target = SparkTarget(spark, config)
    assert target.preflight()["ok"]
    assert target.partition_counts() == {"all": 7}
    manifest = _manifest({"mode": "fraction", "threshold": 1000})
    manifest["partition_mode"] = "all"
    target.window_statistics(manifest, {"bucket": "all", "partitions": ["all"]})
    count_sql, stats_sql = spark.statements[-2:]
    assert count_sql == "SELECT COUNT(*) FROM `main`.`sales`.`orders` WHERE 1=1"
    assert "business_date" not in stats_sql
    assert "< 1000" in stats_sql


def test_sql_audit():
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


def _manifest(sample, strict=False, keys=("order_id",), families=("numeric",)):
    return {
        "pair": "orders",
        "columns": ["ORDER_ID", "AMOUNT", "STATUS"],
        "col_map": {"ORDER_ID": "order_id", "AMOUNT": "amount", "STATUS": "status"},
        "right_key_cols": list(keys),
        "key_families": list(families),
        "sample": sample,
        "strict": strict,
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
def test_sample_modes(sample, window, expected):
    predicate = SparkTarget(None, ORDERS)._sample_predicate(_manifest(sample), window)
    if expected is None:
        assert predicate is None
    else:
        assert expected in predicate
    assert SparkTarget(None, ORDERS)._sample_predicate(_manifest(sample, keys=()), window) is None


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
    assert len(spark.statements) == 2
    query = spark.statements[1]
    assert "`business_date` >= DATE '2026-01-01' AND `business_date` < DATE '2026-01-03'" in query
    assert "< 99" in query and "APPROX" not in query.upper()
    assert "min(trim(cast(`order_id` as string)))" in query
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


@pytest.mark.parametrize(
    ("overrides", "fragments", "types"),
    [
        pytest.param(
            {"STATUS": "numeric", "AMOUNT": "categorical"},
            (
                "avg(try_cast(CASE WHEN trim(cast(`status` as string))",
                "NULL AS mean_1",
                "avg(length(trim(cast(`amount` as string))))",
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
    query = spark.statements[1]
    assert all(fragment in query for fragment in fragments)
    assert all(stats[column]["col_type"] == kind for column, kind in types.items())


def test_date_column():
    spark = FakeSpark(
        lambda sql: DESCRIBE if sql.startswith("DESCRIBE") else [FakeRow(_stats_row())]
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
    query = spark.statements[1]
    assert "date_format(to_date(`status`), 'yyyy-MM-dd')" in query
    assert "NULL AS mean_2" in query


def test_strict_text():
    spark = FakeSpark(
        lambda sql: DESCRIBE if sql.startswith("DESCRIBE") else [FakeRow(_stats_row())]
    )
    target = SparkTarget(spark, ORDERS)
    target.window_statistics(
        _manifest({"mode": "all"}, strict=True), {"partitions": ["2026-01-01"]}
    )
    assert "count(DISTINCT lower(CASE WHEN trim(cast(`status` as string))" in spark.statements[1]


def test_fixed_normalization():
    keys, families = ("card", "amt", "eff_dt"), ("string", "numeric", "date")
    manifest = _manifest({"mode": "fixed"}, keys=keys, families=families)
    window = {"key_tuples": [[" ABC ", "1.50", "2026-01-31 00:00:00"], ["O'K", "007", "20260131"]]}
    predicate = SparkTarget(None, ORDERS)._sample_predicate(manifest, window)
    assert predicate.endswith("IN ('abc|1.5|2026-01-31', 'o\\'k|7|2026-01-31')")
