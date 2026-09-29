import re

import pytest
from support import FakeRow, FakeSpark, pairs_config

from dtrack_right.backends import targets
from dtrack_right.backends.targets import CsvAnchorTarget, SparkTarget
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
def test_spark_collapse_decimal_sql_applies_the_same_rule_as_python(text):
    sql = targets._collapse_decimal_sql("x")
    assert sql.startswith("CASE WHEN x rlike ")
    assert _spark_rule(sql)(text) == collapse_decimal(text)


@pytest.mark.parametrize(
    ("family", "fragment"),
    [
        ("numeric", "coalesce(nullif(CASE WHEN trim(cast(`k` as string))"),
        ("date", "coalesce(date_format(cast(`k` as date), 'yyyy-MM-dd'), '<<NULL>>')"),
        ("string", "coalesce(nullif(lower(trim(cast(`k` as string))), ''), '<<NULL>>')"),
    ],
)
def test_norm_sql_mirrors_normalize_value_per_family(family, fragment):
    assert targets._norm_sql("k", family).startswith(fragment)


def test_relation_supports_tables_queries_and_raw_volume_files():
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
def test_files_kind_reads_a_volume_folder_and_applies_the_src_patch():
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


def test_preflight_describes_the_relation_and_needs_the_date_column():
    spark = FakeSpark(lambda sql: DESCRIBE)
    target = SparkTarget(spark, ORDERS)
    health = target.preflight()
    assert health["ok"] and health["columns"] == ["amount", "business_date", "order_id", "status"]
    assert spark.statements == ["DESCRIBE QUERY SELECT * FROM `main`.`sales`.`orders`"]
    assert target.column_types["amount"] == "decimal(10,2)"
    missing = SparkTarget(FakeSpark(lambda sql: DESCRIBE), {**ORDERS, "date_col": "dt"}).preflight()
    assert missing["ok"] is False and "'dt'" in missing["error"]


def test_partition_counts_run_one_grouped_query_with_bounds_and_where():
    spark = FakeSpark(lambda sql: [("2026-01-01", 3), ("2026-01-02", 4)])
    target = SparkTarget(spark, {**ORDERS, "where": "region = 'EU'"})
    assert target.partition_counts("2026-01-01", "2026-01-31") == {
        "2026-01-01": 3,
        "2026-01-02": 4,
    }
    assert spark.statements == [
        (
            "SELECT `business_date`, COUNT(*) FROM `main`.`sales`.`orders` WHERE (region = 'EU') AND "
            "(`business_date` >= DATE '2026-01-01' AND `business_date` <= DATE '2026-01-31') "
            "GROUP BY `business_date`"
        )
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
        ({"mode": "fixed"}, {"key_tuples": [["7"], ["o'k"]]}, "IN ('7', 'o''k')"),
        ({"mode": "fixed"}, {"key_tuples": []}, "1=0"),
    ],
)
def test_sample_predicate_per_mode(sample, window, expected):
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


def test_window_statistics_is_one_query_mapped_back_per_column():
    spark = FakeSpark(
        lambda sql: DESCRIBE if sql.startswith("DESCRIBE") else [FakeRow(_stats_row())]
    )
    target = SparkTarget(spark, ORDERS)
    window = {"bucket": "2026-01-01", "partitions": ["2026-01-01", "2026-01-02"]}
    stats = target.window_statistics(_manifest({"mode": "fraction", "threshold": 99}), window)
    assert len(spark.statements) == 2
    query = spark.statements[1]
    assert "`business_date` BETWEEN DATE '2026-01-01' AND DATE '2026-01-02'" in query
    assert "< 99" in query and "APPROX" not in query.upper()
    assert "cast(min(`order_id`) as string)" in query
    assert "cast(min(cast(`amount` as double)) as string)" in query
    assert stats["order_id"]["col_type"] == "numeric" and stats["order_id"]["mean"] is None
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


def test_overrides_force_a_string_column_numeric_and_a_decimal_categorical():
    spark = FakeSpark(
        lambda sql: DESCRIBE if sql.startswith("DESCRIBE") else [FakeRow(_stats_row())]
    )
    manifest = _manifest({"mode": "all"})
    manifest["col_type_overrides"] = {"STATUS": "numeric", "AMOUNT": "categorical"}
    stats = SparkTarget(spark, ORDERS).window_statistics(manifest, {"partitions": ["2026-01-01"]})
    query = spark.statements[1]
    assert "avg(try_cast(CASE WHEN trim(cast(`status` as string))" in query
    assert "NULL AS mean_1" in query and "avg(length(trim(cast(`amount` as string))))" in query
    assert stats["status"]["col_type"] == "numeric" and stats["amount"]["col_type"] == "categorical"
    assert stats["order_id"]["col_type"] == "numeric"


def test_strict_mode_lowercases_and_collapses_text_columns():
    spark = FakeSpark(
        lambda sql: DESCRIBE if sql.startswith("DESCRIBE") else [FakeRow(_stats_row())]
    )
    target = SparkTarget(spark, ORDERS)
    target.window_statistics(
        _manifest({"mode": "all"}, strict=True), {"partitions": ["2026-01-01"]}
    )
    assert "count(DISTINCT lower(CASE WHEN trim(cast(`status` as string))" in spark.statements[1]


def test_csv_anchor_target_counts_and_summarises_the_right_csv(synthetic, tmp_path):
    config = pairs_config(synthetic.left)["pairs"]["orders"]["right"]
    target = CsvAnchorTarget(config, synthetic.right, tmp_path)
    assert target.preflight()["ok"]
    counts = target.partition_counts("2026-01-01", "2026-01-31")
    assert counts["2026-01-15"] == 27 and max(counts) <= "2026-01-31"
    manifest = _manifest({"mode": "fraction", "rate": 0.25}, keys=("order_id",))
    manifest["columns"], manifest["col_map"] = ["AMOUNT"], {"AMOUNT": "amount"}
    stats = target.window_statistics(
        manifest, {"bucket": "2026-01-01", "partitions": ["2026-01-02"]}
    )
    assert 0 < stats["amount"]["n_total"] < 30
    manifest["col_type_overrides"] = {"AMOUNT": "categorical"}
    stats = target.window_statistics(
        manifest, {"bucket": "2026-01-01", "partitions": ["2026-01-02"]}
    )
    assert stats["amount"]["col_type"] == "categorical"


def test_csv_anchor_target_preflight_reports_problems(tmp_path):
    missing = CsvAnchorTarget({"table": "t", "date_col": "d"}, tmp_path, tmp_path)
    assert missing.preflight()["ok"] is False
    (tmp_path / "t.csv").write_text("x\n1\n")
    assert "missing column d" in missing.preflight()["error"]
