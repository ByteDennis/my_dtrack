"""Source->target migration-parity matrix on real engines.

LEFT (source) in {oracle, hadoop, csv} x RIGHT (target) in {databricks, athena,
csv}. Each pair seeds the source with a clean dataset and the target with the
SAME logical rows RE-PRESENTED (migration drift: case + trailing space). Then:

  no-STRICT  -> row counts + numeric stats match; the string drift surfaces as
                hash + n_unique DIFFs (dtrack is sensitive).
  STRICT=1   -> ALL match; the presentation drift is reconciled.
  negative   -> one REAL value changed on the target is flagged even under STRICT.
"""

import pytest

from migration import (
    OracleSide, HiveSide, TrinoSide, SparkSide, DuckdbSide, run_parity,
)

# clean source rows
LEFT = [("2024-01-01", 7, "USD"), ("2024-01-01", 11, "USD"),
        ("2024-01-02", 3, "EUR")]
# migrated target: same values, re-presented (case + trailing space)
RIGHT = [("2024-01-01", 7, "usd "), ("2024-01-01", 11, "USD"),
         ("2024-01-02", 3, "eur")]
# negative control: one REAL amt value changed (3 -> 999)
RIGHT_BAD = [("2024-01-01", 7, "usd "), ("2024-01-01", 11, "USD"),
             ("2024-01-02", 999, "eur")]


# >>> assert numeric parity + string-drift behaviour for one compare run <<< #
def _assert(row_cmp, col_cmp, strict, negative=False):
    assert row_cmp["mismatched"] == []
    assert row_cmp["only_left"] == [] and row_cmp["only_right"] == []
    amt = col_cmp["amt"][0]
    ccy = col_cmp["ccy"][0]
    if negative:
        # a genuine value change surfaces in the numeric stats even under STRICT
        assert (amt["min_match"] is False or amt["max_match"] is False), amt
        return
    # numeric column reconciles regardless of STRICT (numeric equality)
    assert amt["n_unique_diff"] == 0
    assert amt["min_match"] is True and amt["max_match"] is True
    if strict:
        assert ccy["hash_sum_match"] is True, ccy
        assert ccy["n_unique_diff"] == 0, ccy
    else:
        assert ccy["hash_sum_match"] is False, ccy
        assert ccy["n_unique_diff"] != 0, ccy


# >>> the full no-strict / strict / negative-control sweep for one pair <<< #
def _sweep(tmp_path, left, right):
    rc, cc = run_parity(tmp_path, left, right, LEFT, RIGHT, strict=False)
    _assert(rc, cc, strict=False)
    rc, cc = run_parity(tmp_path, left, right, LEFT, RIGHT, strict=True)
    _assert(rc, cc, strict=True)
    rc, cc = run_parity(tmp_path, left, right, LEFT, RIGHT_BAD, strict=True)
    _assert(rc, cc, strict=True, negative=True)


# ---- csv source (in-process DuckDB) -------------------------------------

def test_csv_to_csv(tmp_path):
    _sweep(tmp_path, DuckdbSide(str(tmp_path)), DuckdbSide(str(tmp_path)))


def test_csv_to_databricks(tmp_path, spark):
    _sweep(tmp_path, DuckdbSide(str(tmp_path)), SparkSide(spark))


def test_csv_to_athena(tmp_path, trino_cursor):
    _sweep(tmp_path, DuckdbSide(str(tmp_path)), TrinoSide(trino_cursor))


# ---- oracle source ------------------------------------------------------

def test_oracle_to_csv(tmp_path, oracle_cursor):
    _sweep(tmp_path, OracleSide(oracle_cursor), DuckdbSide(str(tmp_path)))


def test_oracle_to_databricks(tmp_path, oracle_cursor, spark):
    _sweep(tmp_path, OracleSide(oracle_cursor), SparkSide(spark))


def test_oracle_to_athena(tmp_path, oracle_cursor, trino_cursor):
    _sweep(tmp_path, OracleSide(oracle_cursor), TrinoSide(trino_cursor))


# ---- hadoop/hive source -------------------------------------------------

def test_hadoop_to_csv(tmp_path, hive_cursor):
    _sweep(tmp_path, HiveSide(hive_cursor), DuckdbSide(str(tmp_path)))


def test_hadoop_to_databricks(tmp_path, hive_cursor, spark):
    _sweep(tmp_path, HiveSide(hive_cursor), SparkSide(spark))


def test_hadoop_to_athena(tmp_path, hive_cursor, trino_cursor):
    _sweep(tmp_path, HiveSide(hive_cursor), TrinoSide(trino_cursor))
