"""Simulate the extract CSVs as produced by different engines, then reconcile.

Every engine's extractor writes the SAME canonical schema (ROW_CSV_HEADERS /
COL_CSV_HEADERS); only the raw `dt` formatting differs (Athena ISO, Hadoop
YYYYMMDD, Oracle DD-MON-YY, Databricks ISO). We write those output CSVs
directly — no engine needed — and confirm the loader canonicalises the dates
so a pair whose two sides came from DIFFERENT engines still reconciles.

This is the "csv files obtained from various database engines" check.
"""

import os

import pytest

from dtrack.db import init_database, get_row_counts
from dtrack.loader import load_row_counts, load_precomputed_col_stats
from dtrack.compare import compare_row_counts, compare_column_stats

from helpers import (
    as_engine_date, write_row_output_csv, write_col_output_csv, col_row,
)

ENGINES = ["athena", "hadoop", "oracle", "databricks"]


@pytest.mark.parametrize("engine", ENGINES)
def test_loader_canonicalises_every_engine_date_format(sandbox, engine):
    """Each engine's dt format loads as the same canonical YYYY-MM-DD key."""
    db = sandbox["db"]
    dates = ["2026-01-01", "2026-01-02", "2026-02-15"]
    rows = [(as_engine_date(d, engine), i + 1) for i, d in enumerate(dates)]
    path = write_row_output_csv(os.path.join(sandbox["out"], f"{engine}_row.csv"), rows)

    load_row_counts(db, path, f"t_{engine}", source="csv")
    stored = dict(get_row_counts(db, f"t_{engine}"))
    assert set(stored.keys()) == set(dates), (
        f"{engine}: dates not canonicalised -> {sorted(stored.keys())}"
    )


@pytest.mark.parametrize("left_engine,right_engine", [
    ("oracle", "athena"),
    ("hadoop", "databricks"),
    ("oracle", "hadoop"),
])
def test_cross_engine_row_reconciliation(sandbox, left_engine, right_engine):
    """Two sides extracted by DIFFERENT engines, identical data -> all match."""
    db = sandbox["db"]
    dates = ["2026-01-01", "2026-01-02"]
    counts = [11, 22]
    lp = write_row_output_csv(
        os.path.join(sandbox["out"], "L_row.csv"),
        [(as_engine_date(d, left_engine), c) for d, c in zip(dates, counts)])
    rp = write_row_output_csv(
        os.path.join(sandbox["out"], "R_row.csv"),
        [(as_engine_date(d, right_engine), c) for d, c in zip(dates, counts)])

    load_row_counts(db, lp, "left_side", source="csv")
    load_row_counts(db, rp, "right_side", source="csv")

    rc = compare_row_counts(db, "left_side", "right_side")
    assert rc["only_left"] == [] and rc["only_right"] == []
    assert rc["mismatched"] == []
    assert sorted(rc["matching"]) == [("2026-01-01", 11), ("2026-01-02", 22)]


def test_cross_engine_row_mismatch_surfaces(sandbox):
    """Same dates across engines but a differing count is flagged, not hidden
    by date-format noise."""
    db = sandbox["db"]
    lp = write_row_output_csv(
        os.path.join(sandbox["out"], "L_row.csv"),
        [(as_engine_date("2026-05-01", "oracle"), 100),
         (as_engine_date("2026-05-02", "oracle"), 200)])
    rp = write_row_output_csv(
        os.path.join(sandbox["out"], "R_row.csv"),
        [(as_engine_date("2026-05-01", "hadoop"), 100),
         (as_engine_date("2026-05-02", "hadoop"), 999)])  # differs
    load_row_counts(db, lp, "L", source="csv")
    load_row_counts(db, rp, "R", source="csv")
    rc = compare_row_counts(db, "L", "R")
    assert rc["matching"] == [("2026-05-01", 100)]
    assert rc["mismatched"] == [("2026-05-02", 200, 999)]


def test_cross_engine_col_reconciliation(sandbox):
    """A categorical + numeric column extracted by Oracle (left) vs Athena
    (right) compare on canonical dt keys; a unique-count drift is flagged."""
    db = sandbox["db"]
    dt_o = as_engine_date("2026-01-01", "oracle")
    dt_a = as_engine_date("2026-01-01", "athena")

    lp = write_col_output_csv(os.path.join(sandbox["out"], "L_col.csv"), [
        col_row("amt", dt=dt_o, col_type="numeric", n_total=100, n_unique=90,
                min_val="1", max_val="500"),
        col_row("ccy", dt=dt_o, col_type="categorical", n_total=100, n_unique=3,
                min_val="EUR=10", max_val="USD=60"),
    ])
    rp = write_col_output_csv(os.path.join(sandbox["out"], "R_col.csv"), [
        col_row("amt", dt=dt_a, col_type="numeric", n_total=100, n_unique=90,
                min_val="1", max_val="500"),
        col_row("ccy", dt=dt_a, col_type="categorical", n_total=100, n_unique=4,
                min_val="EUR=10", max_val="USD=60"),  # n_unique drift 3->4
    ])
    load_precomputed_col_stats(db, lp, "L", source="csv", vintage="day")
    load_precomputed_col_stats(db, rp, "R", source="csv", vintage="day")

    cc = compare_column_stats(db, "L", "R", level="fine")
    assert set(cc.keys()) == {"amt", "ccy"}
    # amt agrees on the same canonical date bucket
    amt = cc["amt"][0]
    assert amt["dt"] == "2026-01-01"
    assert amt["n_total_diff"] == 0 and amt["n_unique_diff"] == 0
    # ccy unique drift surfaces
    ccy = cc["ccy"][0]
    assert ccy["n_unique_left"] == 3 and ccy["n_unique_right"] == 4
    assert ccy["n_unique_diff"] == 1
