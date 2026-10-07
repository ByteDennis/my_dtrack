import duckdb
import pytest

from dtrack_left.common.hashing import FAMILIES, hash32, key_family
from dtrack_left.common.sampling import (
    SamplingSqlNotSupported,
    composite_sql,
    hash32_sql,
    hash_hex_sql,
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
    [*sorted(FAMILIES), ["trim", "upper", "strip_prefix:ID-"], ["lpad:6"]],
    ids=str,
)
def test_duckdb_normalisation_matches_python_for_every_family(spec):
    family = key_family(spec)
    expression = normalize_sql("value", family, "duckdb", "left")
    rows = duckdb.sql("SELECT unnest(?) AS value", params=[VALUES])
    got = [row[0] for row in rows.query("rows", f"SELECT {expression} FROM rows").fetchall()]
    assert got == [family.normalize(value, "left") for value in VALUES]


def test_duckdb_hash_bucket_matches_python_reference():
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
def test_each_backend_compiles_its_own_md5_expression(dialect, hash_fragment, bucket_fragment):
    assert hash_fragment in hash_hex_sql("normalized_key", dialect)
    assert bucket_fragment in hash32_sql("normalized_key", dialect)


def test_fraction_and_fixed_predicates_share_one_normalisation_contract():
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


def test_non_spark_custom_family_fails_closed_instead_of_silently_drifting():
    class CustomFamily(type(key_family("string"))):
        name = "custom-test"

        def left_transform(self, text):
            return text.removeprefix("X")

    family = CustomFamily()
    with pytest.raises(SamplingSqlNotSupported, match="needs duckdb SQL"):
        normalize_sql("value", family, "duckdb", "left")


def test_oracle_fixed_predicate_chunks_more_than_1000_keys():
    predicate = sample_predicate_sql(
        {"mode": "fixed"},
        ["id"],
        ["numeric"],
        "oracle",
        "left",
        [[str(value)] for value in range(1001)],
    )
    assert predicate.count(" IN (") == 2 and " OR " in predicate


def test_oracle_native_types_use_explicit_nls_independent_formats():
    numeric = normalize_sql("n", key_family("numeric"), "oracle", "left", "DB_TYPE_NUMBER")
    date = normalize_sql("d", key_family("date"), "oracle", "left", "DB_TYPE_DATE")
    timestamp = normalize_sql("ts", key_family("date"), "oracle", "left", "DB_TYPE_TIMESTAMP")
    assert "'TM9'" in numeric and "NLS_NUMERIC_CHARACTERS=''.,''" in numeric
    assert "TO_CHAR(d, 'YYYY-MM-DD HH24:MI:SS')" in date
    assert "'YYYY-MM-DD HH24:MI:SS'" in date
    assert "TO_CHAR(ts, 'YYYY-MM-DD HH24:MI:SS.FF6')" in timestamp
    assert "'YYYY-MM-DD HH24:MI:SS.FF'" in timestamp


def test_oracle_timezone_keys_require_an_explicit_canonical_expression():
    with pytest.raises(SamplingSqlNotSupported, match="timezone-bearing Oracle keys"):
        normalize_sql("ts", key_family("date"), "oracle", "left", "DB_TYPE_TIMESTAMP_TZ")


def test_date_type_family_generates_dialect_sql_for_the_hash_key():
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
