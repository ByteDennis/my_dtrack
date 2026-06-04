"""End-to-end CSV↔CSV reconciliation through real DuckDB extraction.

No external engine: a `source=csv` pair reads raw data CSVs via DuckDB, the
gen-then-run path writes extract_{row,col}.sql, runs them, and emits the same
canonical `{qname}_row.csv` / `{qname}_col.csv` every platform produces. Those
load into sqlite and feed compare_row_counts / compare_column_stats.

This is the engine-free way to exercise the whole pipeline.
"""

import contextlib
import io
import os

import pytest

from dtrack.platforms.base import qualified_name
from dtrack.platforms.csv_duckdb import (
    _write_combined_row_sql_csv,
    _write_combined_col_sql_csv,
    run_csv_sql_file,
)
from dtrack.constants import ROW_CSV_HEADERS, COL_CSV_HEADERS
from dtrack.loader import load_row_counts, load_precomputed_col_stats
from dtrack.compare import compare_row_counts, compare_column_stats

from helpers import write_data_csv, csv_table


pytestmark = pytest.mark.skipif(
    __import__("importlib").util.find_spec("duckdb") is None,
    reason="duckdb not installed",
)


def _quiet(fn, *a, **k):
    with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
        return fn(*a, **k)


@pytest.fixture
def two_tables(sandbox):
    """Left vs right CSVs: identical on 2026-01-01, right has an extra row on
    2026-01-02 (so row count mismatches and amt stats differ that day)."""
    d = sandbox["dir"]
    left = write_data_csv(
        d / "left.csv", ["trade_date", "ccy", "amt"],
        [["2026-01-01", "USD", 10], ["2026-01-01", "EUR", 20],
         ["2026-01-02", "USD", 30]],
    )
    right = write_data_csv(
        d / "right.csv", ["trade_date", "ccy", "amt"],
        [["2026-01-01", "USD", 10], ["2026-01-01", "EUR", 20],
         ["2026-01-02", "USD", 30], ["2026-01-02", "GBP", 5]],
    )
    L = csv_table("left", left)
    R = csv_table("right", right)
    return L, R


def test_row_extraction_emits_canonical_csv(sandbox, two_tables):
    L, R = two_tables
    out = sandbox["out"]
    path = _write_combined_row_sql_csv([L, R], out)
    assert os.path.basename(path) == "extract_row.sql"

    results = _quiet(run_csv_sql_file, path, out)
    assert all(r.get("ok") for r in results), results

    qL = qualified_name(L)
    row_csv = os.path.join(out, f"{qL}_row.csv")
    assert os.path.exists(row_csv)
    lines = open(row_csv).read().strip().splitlines()
    assert lines[0].split(",") == ROW_CSV_HEADERS
    # 2 rows on 01-01, 1 row on 01-02
    body = dict(line.split(",") for line in lines[1:])
    assert body["2026-01-01"] == "2"
    assert body["2026-01-02"] == "1"


def test_col_extraction_emits_14_field_csv(sandbox, two_tables):
    L, _ = two_tables
    out = sandbox["out"]
    path = _write_combined_col_sql_csv([L], out)
    assert os.path.basename(path) == "extract_col.sql"
    _quiet(run_csv_sql_file, path, out)

    qL = qualified_name(L)
    rows = list(_read_dicts(os.path.join(out, f"{qL}_col.csv")))
    assert {r["column_name"] for r in rows} == {"ccy", "amt"}
    for r in rows:
        assert list(r.keys()) == COL_CSV_HEADERS
    amt = next(r for r in rows if r["column_name"] == "amt")
    assert amt["n_total"] == "3"
    assert amt["n_unique"] == "3"
    assert float(amt["min_val"]) == 10.0
    assert float(amt["max_val"]) == 30.0


def test_full_pipeline_row_compare(sandbox, two_tables):
    L, R = two_tables
    out, db = sandbox["out"], sandbox["db"]
    _quiet(run_csv_sql_file, _write_combined_row_sql_csv([L, R], out), out)
    qL, qR = qualified_name(L), qualified_name(R)

    # NB: the canonical row CSV header is `dt`; let the loader auto-detect it
    # (don't pass the source date_col, which is `trade_date`).
    _quiet(load_row_counts, db, os.path.join(out, f"{qL}_row.csv"), qL, source="csv")
    _quiet(load_row_counts, db, os.path.join(out, f"{qR}_row.csv"), qR, source="csv")

    rc = compare_row_counts(db, qL, qR)
    assert ("2026-01-01", 2) in rc["matching"]
    assert ("2026-01-02", 1, 2) in rc["mismatched"]
    assert rc["summary"]["total_left"] == 3
    assert rc["summary"]["total_right"] == 4


def test_full_pipeline_col_compare(sandbox, two_tables):
    L, R = two_tables
    out, db = sandbox["out"], sandbox["db"]
    _quiet(run_csv_sql_file, _write_combined_col_sql_csv([L, R], out), out)
    qL, qR = qualified_name(L), qualified_name(R)
    _quiet(load_precomputed_col_stats, db, os.path.join(out, f"{qL}_col.csv"), qL,
           source="csv", vintage="all")
    _quiet(load_precomputed_col_stats, db, os.path.join(out, f"{qR}_col.csv"), qR,
           source="csv", vintage="all")

    cc = compare_column_stats(db, qL, qR, level="fine")
    assert set(cc.keys()) == {"amt", "ccy"}
    amt = cc["amt"][0]
    # right has 4 rows vs left's 3 -> n_total differs; min differs (5 vs 10)
    assert amt["n_total_left"] == 3 and amt["n_total_right"] == 4
    assert amt["n_total_diff"] == 1
    assert amt["max_match"] is True       # both max 30
    assert amt["min_match"] is False      # 10 vs 5


def test_identical_tables_reconcile_clean(sandbox):
    """When both sides point at the same data, nothing mismatches."""
    d, out, db = sandbox["dir"], sandbox["out"], sandbox["db"]
    data = [["2026-03-01", "USD", 100], ["2026-03-02", "EUR", 200]]
    p = write_data_csv(d / "same.csv", ["trade_date", "ccy", "amt"], data)
    L = csv_table("a", p)
    R = csv_table("b", p)
    _quiet(run_csv_sql_file, _write_combined_row_sql_csv([L, R], out), out)
    qL, qR = qualified_name(L), qualified_name(R)
    _quiet(load_row_counts, db, os.path.join(out, f"{qL}_row.csv"), qL, source="csv")
    _quiet(load_row_counts, db, os.path.join(out, f"{qR}_row.csv"), qR, source="csv")
    rc = compare_row_counts(db, qL, qR)
    assert rc["mismatched"] == []
    assert rc["only_left"] == [] and rc["only_right"] == []
    assert len(rc["matching"]) == 2


def _read_dicts(path):
    import csv as _csv
    with open(path) as f:
        yield from _csv.DictReader(f)
