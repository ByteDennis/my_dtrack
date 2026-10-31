import duckdb
import pytest

from pair_recon_left.common.model.hashing import FAMILIES, cast_text_sql, hash32, key_family
from pair_recon_left.common.sql.sampling import (
    SamplingSqlNotSupported,
    composite_sql,
    hash32_sql,
    hash_hex_sql,
    modular_sample_condition,
    normalize_sql,
    sample_predicate_sql,
)

VALUES = [
    None,
    "",
    "   ",
    "007",
    "+007.50",
    "-007.50",
    "1e3",
    " AbC ",
    "CAFÉ",
    "123-45-6789",
    "20260131",
    "2026-01-31 00:00:00",
    "1/5/2026",
    "02JAN2026",
    "not a date",
]


@pytest.mark.parametrize(
    "spec",
    sorted(FAMILIES),
    ids=str,
)
def test_duckdb(spec):
    family = key_family(spec)
    expression = normalize_sql("value", family, "duckdb", "left")
    rows = duckdb.sql("SELECT unnest(?) AS value", params=[VALUES])
    got = [row[0] for row in rows.query("rows", f"SELECT {expression} FROM rows").fetchall()]
    assert got == [family.normalize(value, "left") for value in VALUES]


def test_hash():
    family = key_family("numeric")
    composite = composite_sql(["value"], [family], "duckdb", "left")
    expression = hash32_sql(composite, "duckdb")
    values = ["007", "2.50", "-0.10", "1e3"]
    rows = duckdb.sql("SELECT unnest(?) AS value", params=[values])
    got = [row[0] for row in rows.query("rows", f"SELECT {expression} FROM rows").fetchall()]
    assert got == [hash32(family.normalize(value)) for value in values]


@pytest.mark.parametrize(
    ("dialect", "hash_fragment", "bucket_fragment"),
    [
        ("oracle", "STANDARD_HASH", "TO_NUMBER"),
        ("athena", "TO_HEX(MD5(TO_UTF8", "from_base"),
        ("spark", "md5(", "conv(substr"),
        ("duckdb", "md5(", "'0x' || SUBSTR"),
    ],
)
def test_backend(dialect, hash_fragment, bucket_fragment):
    assert hash_fragment in hash_hex_sql("normalized_key", dialect)
    assert bucket_fragment in hash32_sql("normalized_key", dialect)


def test_predicate():
    fraction = sample_predicate_sql(
        {"mode": "fraction", "threshold": 99}, ["id"], ["numeric"], "duckdb", "left"
    )
    fixed = sample_predicate_sql(
        {"mode": "fixed"},
        ["id"],
        ["numeric"],
        "duckdb",
        "right",
        [["007"], ["2.0"]],
    )
    assert "md5(" in fraction and fraction.endswith("< 99")
    assert "IN ('7', '2')" in fixed


def test_modular_predicate_uses_numeric_selector_without_hashing():
    plan = {
        "mode": "fraction",
        "rate": 0.25,
        "modular_column": "customer_id",
        "modular_seed": 123,
    }
    predicate = sample_predicate_sql(
        plan, ["record_id"], ["string"], "duckdb", "left"
    )

    assert predicate == modular_sample_condition("customer_id", 0.25, 123, "duckdb")
    assert '"customer_id"' in predicate
    assert "md5" not in predicate.casefold()

    selected = duckdb.sql(
        f"SELECT customer_id FROM range(0, 1000) rows(customer_id) WHERE {predicate}"
    ).fetchall()
    assert 240 <= len(selected) <= 260


def test_custom():
    class CustomFamily(type(key_family("string"))):
        name = "custom-test"

        def normalize_left(self, text):
            return text.removeprefix("X")

    family = CustomFamily()
    with pytest.raises(SamplingSqlNotSupported, match="needs duckdb SQL"):
        normalize_sql("value", family, "duckdb", "left")


def test_oracle():
    predicate = sample_predicate_sql(
        {"mode": "fixed"},
        ["id"],
        ["numeric"],
        "oracle",
        "left",
        [[str(value)] for value in range(1001)],
    )
    assert predicate.count(" IN (") == 2 and " OR " in predicate


def test_oracle2():
    numeric = normalize_sql("n", key_family("numeric"), "oracle", "left", "DB_TYPE_NUMBER")
    date = normalize_sql("d", key_family("date"), "oracle", "left", "DB_TYPE_DATE")
    timestamp = normalize_sql("ts", key_family("date"), "oracle", "left", "DB_TYPE_TIMESTAMP")
    assert "'TM9'" in numeric and "NLS_NUMERIC_CHARACTERS=''.,''" in numeric
    assert "TO_CHAR(d, 'YYYY-MM-DD HH24:MI:SS')" in date
    assert "'YYYY-MM-DD HH24:MI:SS'" in date
    assert "TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS.FF6')" in timestamp
    assert "'YYYY-MM-DD HH24:MI:SS.FF'" in timestamp


def test_timezone():
    with pytest.raises(SamplingSqlNotSupported, match="timezone-bearing Oracle keys"):
        normalize_sql("ts", key_family("date"), "oracle", "left", "DB_TYPE_TIMESTAMP_TZ")


def test_date():
    family = key_family(
        {
            "family": "date_type",
            "left_date_type": {"type": "sas", "format": "days"},
            "right_date_type": {"type": "string", "format": "dash"},
        }
    )
    left = normalize_sql("DT", family, "oracle", "left")
    right = normalize_sql("`dt`", family, "spark", "right")
    assert "1960-01-01" in left and "YYYY-MM-DD" in left
    assert "date_format" in right and "yyyy-MM-dd" in right


@pytest.mark.parametrize(
    ("dialect", "data_type", "expected"),
    [
        ("spark", None, "CAST(x AS STRING)"),
        ("spark", "bigint", "CAST(x AS STRING)"),
        ("spark", "string", "CAST(x AS STRING)"),
        ("spark", "decimal(18,2)", "regexp_replace(regexp_replace(CAST(x AS STRING)"),
        ("spark", "double", "CAST(try_cast(x AS DECIMAL(38,15)) AS STRING)"),
        ("hadoop", "double", "CAST(CAST(x AS DECIMAL(38,15)) AS STRING)"),
        ("hadoop", "decimal(10,0)", "regexp_replace(regexp_replace(CAST(x AS STRING)"),
    ],
)
def test_spark_numeric_key_text_matches_oracle_tm9(dialect, data_type, expected):
    sql = cast_text_sql("x", dialect, data_type)
    assert expected in sql
    if expected == "CAST(x AS STRING)":
        assert sql == expected


def test_column_type_sql_keeps_null_and_normalizes_values():
    family = key_family("numeric")
    expression = family.normalize_sql("value", "duckdb", "left")
    rows = duckdb.sql("SELECT unnest(?) AS value", params=[[None, "007", "2.50", ""]])
    got = [row[0] for row in rows.query("rows", f"SELECT {expression} FROM rows").fetchall()]
    assert got == [None, "7", "2.5", ""]
