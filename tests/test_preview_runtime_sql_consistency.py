"""Pin the invariant: preview SQL == saved .sql block == runtime-executed SQL.

For every platform that has a `_build_row_sql_*` helper, we verify:

  1. The preview path (`web.app._build_preview_sql`) produces the same SQL
     string the platform builder produces directly.
  2. The combined writer (`_write_combined_row_sql_*`) embeds the same SQL
     verbatim (modulo a trailing semicolon and the per-block header comment).

This is the test the user asked for: "sql in the saved .sql is the same to
preview queries, and is the same to what's executed".
"""

import os
import tempfile

import pytest

from dtrack.web.app import _build_preview_sql


# ---------------------------------------------------------------------------
# Shared fixture: a synthetic table cfg with all the fields each builder needs.
# Date bounds get rendered via _preview_date_bounds (web.app); the writers and
# runtime builder both read from `_date_bounds`.
# ---------------------------------------------------------------------------

def _tbl(source, **overrides):
    base = {
        "source": source,
        "name":   "trades",
        "table":  "trades",
        "date_col": "trade_date",
        "date_type": "date",
        "conn_macro": {
            "aws":        "analytics_db",
            "hadoop":     "hive_prod",
            "databricks": "main.warehouse",
            "csv":        "./csv/",
            "oracle":     "pb23",
        }[source],
        "where": "",
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# (1) preview == direct builder call
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("source", ["aws", "hadoop", "databricks", "csv"])
def test_preview_matches_builder(source):
    tbl = _tbl(source)
    preview = _build_preview_sql(
        pair_name="p", side="left", cfg=tbl,
        from_date="2026-01-01", to_date="2026-04-30",
    )

    # Build the same tbl_cfg shape the preview composes, then ask the
    # platform builder directly.
    from dtrack.web.app import _preview_date_bounds
    tbl2 = dict(tbl)
    tbl2["_date_bounds"] = _preview_date_bounds(
        tbl["date_col"], tbl["date_type"],
        "2026-01-01", "2026-04-30", source,
    )

    if source == "aws":
        from dtrack.platforms.athena import _build_row_sql_athena as builder
    elif source == "hadoop":
        from dtrack.platforms.hadoop import _build_row_sql_hadoop as builder
    elif source == "databricks":
        from dtrack.platforms.databricks import _build_row_sql_databricks as builder
    elif source == "csv":
        from dtrack.platforms.csv_duckdb import _build_row_sql_csv as builder
    else:
        pytest.skip(f"no direct builder for {source}")

    direct = builder(tbl2)
    # The preview can append an `-- max_workers=N` hint for AWS; strip that
    # before comparing — it's commentary, not behavior.
    preview_sql = preview["sql"].rstrip()
    preview_sql = "\n".join(
        line for line in preview_sql.splitlines()
        if not line.strip().startswith("-- max_workers")
    ).rstrip()
    assert preview_sql == direct.rstrip(), (
        f"{source}: preview SQL differs from direct builder.\n"
        f"--- preview ---\n{preview_sql}\n"
        f"--- direct  ---\n{direct.rstrip()}"
    )


# ---------------------------------------------------------------------------
# (2) saved extract_row.sql embeds the SAME SQL the builder produced
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("source", ["aws", "hadoop", "databricks", "csv"])
def test_combined_writer_embeds_builder_output(source):
    tbl = _tbl(source)
    # Render the date bounds the same way /api/generate does.
    if source == "aws":
        # Athena's generator computes bounds elsewhere; for the test we pass
        # bounds in via _date_bounds so the writer just appends them.
        from dtrack.web.app import _preview_date_bounds
        tbl["_date_bounds"] = _preview_date_bounds(
            tbl["date_col"], tbl["date_type"],
            "2026-01-01", "2026-04-30", "aws",
        )
        from dtrack.platforms.athena import _write_combined_sql, _build_row_sql_athena
        with tempfile.TemporaryDirectory() as tmp:
            _write_combined_sql([tbl], tmp, "row", db_path=None)
            saved = open(os.path.join(tmp, "extract_row.sql"), "r").read()
        expected = _build_row_sql_athena(tbl)

    elif source == "hadoop":
        from dtrack.web.app import _preview_date_bounds
        tbl["_date_bounds"] = _preview_date_bounds(
            tbl["date_col"], tbl["date_type"],
            "2026-01-01", "2026-04-30", "hadoop",
        )
        from dtrack.platforms.hadoop import _write_combined_row_sql_hadoop, _build_row_sql_hadoop
        with tempfile.TemporaryDirectory() as tmp:
            _write_combined_row_sql_hadoop([tbl], tmp)
            saved = open(os.path.join(tmp, "extract_row.sql"), "r").read()
        expected = _build_row_sql_hadoop(tbl)

    elif source == "databricks":
        from dtrack.web.app import _preview_date_bounds
        tbl["_date_bounds"] = _preview_date_bounds(
            tbl["date_col"], tbl["date_type"],
            "2026-01-01", "2026-04-30", "databricks",
        )
        from dtrack.platforms.databricks import (
            _write_combined_row_sql_databricks, _build_row_sql_databricks,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _write_combined_row_sql_databricks([tbl], tmp)
            saved = open(os.path.join(tmp, "extract_row.sql"), "r").read()
        expected = _build_row_sql_databricks(tbl)

    elif source == "csv":
        from dtrack.web.app import _preview_date_bounds
        tbl["_date_bounds"] = _preview_date_bounds(
            tbl["date_col"], tbl["date_type"],
            "2026-01-01", "2026-04-30", "csv",
        )
        from dtrack.platforms.csv_duckdb import (
            _write_combined_row_sql_csv, _build_row_sql_csv,
        )
        with tempfile.TemporaryDirectory() as tmp:
            _write_combined_row_sql_csv([tbl], tmp)
            saved = open(os.path.join(tmp, "extract_row.sql"), "r").read()
        expected = _build_row_sql_csv(tbl)

    # The builder's SQL must appear verbatim inside the saved file. We don't
    # require an exact-file match because the writer adds header comments
    # and trailing semicolons, but the *body* must be byte-identical.
    assert expected.strip() in saved, (
        f"{source}: builder SQL not embedded verbatim in saved file.\n"
        f"--- expected ---\n{expected}\n"
        f"--- saved ---\n{saved}"
    )


# ---------------------------------------------------------------------------
# (3) Aggregate filename: every platform produces ONE extract_row.sql (not
#     per-pair / per-table). Databricks regression — used to write per-table.
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("source", ["aws", "hadoop", "databricks", "csv"])
def test_writer_produces_single_aggregate_file(source):
    tbl_a = _tbl(source, name="trades", table="trades")
    tbl_b = _tbl(source, name="orders", table="orders")
    if source == "aws":
        from dtrack.platforms.athena import _write_combined_sql as W
        write = lambda tbls, d: W(tbls, d, "row", db_path=None)
    elif source == "hadoop":
        from dtrack.platforms.hadoop import _write_combined_row_sql_hadoop as W
        write = lambda tbls, d: W(tbls, d)
    elif source == "databricks":
        from dtrack.platforms.databricks import _write_combined_row_sql_databricks as W
        write = lambda tbls, d: W(tbls, d)
    elif source == "csv":
        from dtrack.platforms.csv_duckdb import _write_combined_row_sql_csv as W
        write = lambda tbls, d: W(tbls, d)

    with tempfile.TemporaryDirectory() as tmp:
        write([tbl_a, tbl_b], tmp)
        files = sorted(os.listdir(tmp))
    sql_files = [f for f in files if f.endswith(".sql")]
    assert sql_files == ["extract_row.sql"], (
        f"{source}: expected exactly one aggregate extract_row.sql, "
        f"got {sql_files}"
    )
