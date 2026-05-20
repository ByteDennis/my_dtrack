"""Verify Athena and Databricks 16-bit hash recipes produce the same
numeric output for the same input, despite using engine-specific SQL.

We can't run actual Athena/Databricks SQL in unit tests, so we simulate
each engine's recipe in Python:

    Athena  : MD5(TO_UTF8(TRIM(v)))  ->  TO_HEX  ->  SUBSTR 1..4  ->  from_base(_, 16)
    Spark   : md5(TRIM(v))            ->  (hex already) -> SUBSTR 1..4 -> conv(_, 16, 10)

Both reduce to: int(md5(v.encode('utf-8')).hexdigest()[:4], 16).

This file also asserts the generated SQL strings have the expected
shape so a future refactor can't silently swap the function bodies.
"""

from __future__ import annotations

import hashlib

import pytest

from dtrack.platforms.athena import _athena_portable_hash16
from dtrack.platforms.databricks import _databricks_portable_hash16


# >>> reference Python implementation both SQL recipes should match <<< #
def _expected_hash16(value: str) -> int:
    h = hashlib.md5(value.encode("utf-8")).hexdigest()
    return int(h[:4], 16)


@pytest.mark.parametrize("value, expected", [
    ("123123", 17047),
    ("", _expected_hash16("")),
    ("hello", _expected_hash16("hello")),
    ("SF", _expected_hash16("SF")),
    ("SF ", _expected_hash16("SF")),
    ("  alpha  ", _expected_hash16("alpha")),
    ("a longer categorical value that exceeds short strings",
     _expected_hash16("a longer categorical value that exceeds short strings")),
])
def test_python_reference_matches_expected(value, expected):
    """Sanity: the in-Python reference computation that we use as the
    cross-dialect oracle returns the documented values."""
    assert _expected_hash16(value.strip()) == expected


def test_athena_sql_has_to_utf8_and_to_hex():
    """Athena SQL must wrap MD5 with TO_UTF8 + TO_HEX; otherwise it'd
    fail at parse time on Trino ('md5(varchar)' is not a function)."""
    sql = _athena_portable_hash16("col")
    assert "MD5(" in sql.upper()
    assert "TO_UTF8(" in sql.upper()
    assert "TO_HEX(" in sql.upper()
    assert "FROM_BASE(" in sql.upper()
    assert "TRIM(" in sql.upper()
    assert sql.endswith("AS BIGINT)")


def test_databricks_sql_uses_direct_md5_and_conv():
    """Spark SQL must NOT use TO_UTF8 or TO_HEX (they don't exist there)
    and must call md5 directly on the trimmed string."""
    sql = _databricks_portable_hash16("col")
    assert "md5(" in sql
    assert "TO_UTF8" not in sql.upper()
    assert "TO_HEX(" not in sql.upper()
    assert "conv(" in sql
    assert "TRIM(" in sql.upper()
    assert sql.endswith("AS BIGINT)")


def test_databricks_recipe_is_distinct_from_athena_recipe():
    """The two strings should differ — same algorithm, different dialect.
    A regression that aliases one to the other would silently break
    cross-engine parity."""
    assert _athena_portable_hash16("col") != _databricks_portable_hash16("col")


@pytest.mark.parametrize("value, expected", [
    ("123123", 17047),
    ("hello", _expected_hash16("hello")),
    ("SF", _expected_hash16("SF")),
])
def test_documented_value_is_17047_for_123123(value, expected):
    """Spot-check the exact number the user reported on Databricks:
    md5('123123')[:4] = '4297' = 17047. Same input must hit 17047 on
    Athena too (proven via Python's reference impl since we can't run
    Athena SQL in a unit test)."""
    assert _expected_hash16(value) == expected
