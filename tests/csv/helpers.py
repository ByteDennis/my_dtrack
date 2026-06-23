"""Helpers for the CSV-source reconciliation tests.

Two kinds of CSV files show up in these tests:

1. **Raw source data** — the actual table data a `source=csv` pair points at
   via `conn_macro`. DuckDB reads these during extraction. `write_data_csv`.

2. **Canonical extractor output** — the per-table `{qname}_row.csv` /
   `{qname}_col.csv` that EVERY platform's extractor emits in the same schema
   (ROW_CSV_HEADERS / COL_CSV_HEADERS). To simulate "a CSV obtained from
   Oracle / Hadoop / Databricks / Athena" we just write these output files
   directly with engine-flavoured `dt` formatting — no engine required.
   `write_row_output_csv` / `write_col_output_csv` + `as_engine_date`.
"""

from __future__ import annotations

import csv
import datetime as _dt
from pathlib import Path

from dtrack.constants import COL_CSV_HEADERS, ROW_CSV_HEADERS


# ---------------------------------------------------------------------------
# Raw source data CSVs (read by DuckDB during csv extraction)
# ---------------------------------------------------------------------------

def write_data_csv(path, header, rows) -> str:
    """Write a raw data CSV (the file a source=csv table reads via conn_macro)."""
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for r in rows:
            w.writerow(r)
    return str(path)


def csv_table(name, csv_path, *, date_col="trade_date", date_type="string_dash",
              columns=None, where="", vintage="all", side="left"):
    """Build a `source=csv` table config dict the csv_duckdb writers accept."""
    return {
        "name": name,
        "table": name,
        "source": "csv",
        "conn_macro": str(csv_path),
        "date_col": date_col,
        "date_type": date_type,
        "where": where,
        "vintage": vintage,
        "_side": side,
        "columns": columns or {"trade_date": "DATE", "ccy": "VARCHAR", "amt": "DOUBLE"},
    }


# ---------------------------------------------------------------------------
# Engine-flavoured date rendering for simulated extractor output
# ---------------------------------------------------------------------------

_MONTHS = ["JAN", "FEB", "MAR", "APR", "MAY", "JUN",
           "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"]


def as_engine_date(iso: str, engine: str) -> str:
    """Render an ISO `YYYY-MM-DD` date the way each engine's extract CSV would.

    The loader canonicalises all of these back to YYYY-MM-DD, which is the
    behaviour the cross-engine tests pin.

        athena / databricks -> '2026-01-02'   (ISO, unchanged)
        hadoop              -> '20260102'      (string_compact YYYYMMDD)
        oracle              -> '02-JAN-26'     (DD-MON-YY)
    """
    d = _dt.date.fromisoformat(iso)
    e = engine.lower()
    if e in ("athena", "aws", "databricks"):
        return iso
    if e == "hadoop":
        return f"{d.year:04d}{d.month:02d}{d.day:02d}"
    if e == "oracle":
        return f"{d.day:02d}-{_MONTHS[d.month - 1]}-{d.year % 100:02d}"
    raise ValueError(f"unknown engine: {engine}")


# ---------------------------------------------------------------------------
# Canonical extractor-output CSVs (what load_* consumes)
# ---------------------------------------------------------------------------

def write_row_output_csv(path, dt_count_pairs) -> str:
    """Write a canonical `{qname}_row.csv` (header = ROW_CSV_HEADERS)."""
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(ROW_CSV_HEADERS)
        for dt, count in dt_count_pairs:
            w.writerow([dt, count])
    return str(path)


def col_row(column_name, *, dt="all", col_type="numeric", n_total=0, n_missing=0,
            n_unique=0, mean="", std="", min_val="", max_val="", top_10="",
            length_avg="", length_max="", hash_sum="") -> dict:
    """One row of a canonical col CSV, keyed by COL_CSV_HEADERS."""
    return {
        "dt": dt, "column_name": column_name, "col_type": col_type,
        "n_total": n_total, "n_missing": n_missing, "n_unique": n_unique,
        "mean": mean, "std": std, "min_val": min_val, "max_val": max_val,
        "top_10": top_10, "length_avg": length_avg, "length_max": length_max,
        "hash_sum": hash_sum,
    }


def write_col_output_csv(path, rows) -> str:
    """Write a canonical `{qname}_col.csv` (header = COL_CSV_HEADERS)."""
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COL_CSV_HEADERS)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in COL_CSV_HEADERS})
    return str(path)


# ---------------------------------------------------------------------------
# Config builder for the HTTP /api/generate flow
# ---------------------------------------------------------------------------

def unified_config_csv_pair(left_csv, right_csv, *, sas_outdir, aws_outdir,
                            pair_name="csv_pair"):
    """Minimal unified config with a single csv↔csv pair (left + right)."""
    cols = {"trade_date": "DATE", "ccy": "VARCHAR", "amt": "DOUBLE"}
    side = lambda name, path: {
        "name": name, "table": name, "source": "csv", "conn_macro": str(path),
        "date_col": "trade_date", "date_type": "string_dash",
        "columns": cols,
    }
    return {
        "metadata": {"title": "csv-suite"},
        "settings": {"sas_outdir": sas_outdir, "aws_outdir": aws_outdir,
                     "from_date": "", "to_date": ""},
        "pairs": {
            pair_name: {
                "description": "csv vs csv",
                "left": side("left", left_csv),
                "right": side("right", right_csv),
                "mode": "incremental", "fromDate": "", "toDate": "",
            }
        },
    }
