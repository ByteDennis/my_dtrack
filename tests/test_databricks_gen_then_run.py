"""Databricks gen-then-run: the col path now writes an aggregate extract_col.sql
(block format with /* @conn_macro=... */ markers) that the runner parses and
executes, exactly like Athena / Hadoop / Oracle. These tests pin:

  1. row SQL aliases the date column as `dt` (so base's default row writer,
     which reads r['dt'], picks it up).
  2. col SQL emits every COL_CSV_HEADERS column as an alias, so the runner can
     map result rows straight onto the CSV header with no Python post-process.
  3. the combined col file parses back into one block per (qname, column) with
     the conn_macro recovered from the marker.
"""

import os
import tempfile

from dtrack.constants import COL_CSV_HEADERS
from dtrack.platforms.base import parse_extract_sql_file
from dtrack.platforms.databricks import (
    _build_row_sql_databricks,
    _build_col_sql_databricks,
    _write_combined_col_sql_databricks,
    _write_combined_row_sql_databricks,
)


def _tbl(**ov):
    base = {
        "name": "trades", "table": "trades", "source": "databricks",
        "date_col": "trade_date", "date_type": "date",
        "conn_macro": "main.warehouse", "where": "",
        "columns": {"trade_date": "DATE", "amt": "DOUBLE", "ccy": "STRING"},
        "vintage": "all",
    }
    base.update(ov)
    return base


def test_row_sql_aliases_dt_not_date_value():
    sql = _build_row_sql_databricks(_tbl())
    assert " AS dt," in sql
    assert "date_value" not in sql
    assert "main.warehouse.trades" in sql


def test_col_sql_emits_every_header_alias():
    for dtype in ("DOUBLE", "STRING"):
        sql = _build_col_sql_databricks("amt", dtype, "main.warehouse.trades",
                                        "1=1", "all")
        for h in COL_CSV_HEADERS:
            assert f" AS {h}" in sql, f"{dtype}: missing alias {h!r}\n{sql}"
        # literal label + name columns, not bound to a real column expr
        assert "'all' AS dt" in sql
        assert "'amt' AS column_name" in sql


def test_numeric_vs_categorical_col_type_literal():
    num = _build_col_sql_databricks("amt", "DOUBLE", "t", "1=1", "all")
    cat = _build_col_sql_databricks("ccy", "STRING", "t", "1=1", "all")
    assert "'numeric' AS col_type" in num
    assert "'categorical' AS col_type" in cat
    # categorical computes length stats; numeric leaves them NULL
    assert "LENGTH(" in cat
    assert "LENGTH(" not in num


def test_combined_col_file_parses_into_per_column_blocks():
    with tempfile.TemporaryDirectory() as tmp:
        path = _write_combined_col_sql_databricks([_tbl()], tmp)
        assert os.path.basename(path) == "extract_col.sql"
        blocks = parse_extract_sql_file(path)
    from dtrack.platforms.base import qualified_name
    qn = qualified_name(_tbl())
    names = sorted(b["name"] for b in blocks)
    # one block per non-date column
    assert names == [f"{qn}/amt", f"{qn}/ccy"]
    for b in blocks:
        assert b["conn_macro"] == "main.warehouse"
        assert "main.warehouse.trades" in b["sql"]


def test_combined_row_and_col_are_single_aggregate_files():
    with tempfile.TemporaryDirectory() as tmp:
        rp = _write_combined_row_sql_databricks([_tbl(), _tbl(name="o", table="orders")], tmp)
        cp = _write_combined_col_sql_databricks([_tbl(), _tbl(name="o", table="orders")], tmp)
        files = sorted(f for f in os.listdir(tmp) if f.endswith(".sql"))
    assert os.path.basename(rp) == "extract_row.sql"
    assert os.path.basename(cp) == "extract_col.sql"
    assert files == ["extract_col.sql", "extract_row.sql"]
