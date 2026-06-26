import os

import pytest

from dtrack.platforms.base import build_col_sql, qualified_name
from dtrack.platforms.csv_duckdb import (
    _csv_from_clause,
    _resolve_csv_types,
    discover_csv_columns,
    _discover_and_write_csv_columns,
)

pytestmark = pytest.mark.skipif(
    __import__("importlib").util.find_spec("duckdb") is None,
    reason="duckdb not installed",
)


# >>> run a query on a throwaway in-memory DuckDB, return (cols, rows) <<< #
def _run(sql):
    import duckdb
    con = duckdb.connect(database=":memory:")
    try:
        cur = con.execute(sql)
        cols = [d[0] for d in cur.description]
        return cols, cur.fetchall()
    finally:
        con.close()


# >>> write raw CSV text (so single-quote wrapping stays literal) <<< #
def _write_raw(path, text):
    with open(path, "w", newline="") as f:
        f.write(text)
    return str(path)


# >>> a single-quoted field holding a comma must stay one column <<< #
def test_single_quote_keeps_comma_field_intact(tmp_path):
    p = _write_raw(
        tmp_path / "q.csv",
        "trade_date,note,amt\n"
        "2026-01-01,'hello, world',11.40\n"
        "2026-01-02,'a,b,c',7\n",
    )
    cols = discover_csv_columns(p, quote="'")
    assert [c.lower() for c in cols] == ["trade_date", "note", "amt"]

    tbl = {"conn_macro": p, "quote": "'",
           "_read_types": {"trade_date": "DATE", "note": "VARCHAR", "amt": "DOUBLE"}}
    frm = _csv_from_clause(tbl)
    assert "quote=''''" in frm
    _, rows = _run(f"SELECT note FROM {frm} ORDER BY trade_date")
    assert [r[0] for r in rows] == ["hello, world", "a,b,c"]


# >>> a decimal column typed numeric lets avg() run (no Binder Error) <<< #
def test_decimal_column_typed_numeric_avg_works(tmp_path):
    p = _write_raw(
        tmp_path / "n.csv",
        "trade_date,amt\n"
        "2026-01-01,11.40\n"
        "2026-01-02,7\n",
    )
    tbl = {"conn_macro": p,
           "_read_types": {"trade_date": "DATE", "amt": "DOUBLE"}}
    frm = _csv_from_clause(tbl)
    _, rows = _run(f"SELECT avg(amt) FROM {frm}")
    assert rows[0][0] == pytest.approx((11.40 + 7) / 2)


# >>> build_col_sql('duckdb') over a typed CSV returns correct stat values <<< #
def test_duckdb_col_sql_stat_values(tmp_path):
    p = _write_raw(
        tmp_path / "c.csv",
        "trade_date,amt\n"
        "2026-01-01,11.40\n"
        "2026-01-02,7\n"
        "2026-01-03,11.40\n",
    )
    tbl = {"conn_macro": p, "date_col": "trade_date", "where": "",
           "_read_types": {"trade_date": "DATE", "amt": "DOUBLE"}}
    frm = _csv_from_clause(tbl)
    sql, is_numeric = build_col_sql("duckdb", tbl, "amt", "DOUBLE", frm,
                                    dt_label="all")
    assert is_numeric
    cols, rows = _run(sql)
    rec = dict(zip([c.lower() for c in cols], rows[0]))
    assert int(rec["n_total"]) == 3
    assert int(rec["n_unique"]) == 2
    assert float(rec["min_val"]) == pytest.approx(7.0)
    assert float(rec["max_val"]) == pytest.approx(11.40)
    assert float(rec["mean"]) == pytest.approx((11.40 + 7 + 11.40) / 3)


# >>> declared types (sidecar/config) win over DuckDB inference <<< #
def test_resolve_csv_types_declared_wins_over_inference():
    inferred = {"code": "BIGINT", "amt": "DOUBLE"}
    declared = {"code": "VARCHAR"}
    effective, overrides = _resolve_csv_types(inferred, declared)
    assert effective["code"] == "VARCHAR"
    assert overrides["code"] == "VARCHAR"
    assert effective["amt"] == "DOUBLE"


# >>> sidecar wins over inference and is never overwritten <<< #
def test_sidecar_authoritative_and_not_overwritten(tmp_path, capsys):
    data = _write_raw(
        tmp_path / "codes.csv",
        "trade_date,code\n"
        "2026-01-01,007\n"
        "2026-01-02,008\n",
    )
    tbl = {"name": "codes", "table": "codes", "source": "csv",
           "conn_macro": data, "date_col": "trade_date"}
    outdir = tmp_path / "out"
    outdir.mkdir()
    qname = qualified_name(tbl)
    sidecar = outdir / f"{qname}_columns.csv"
    sidecar.write_text("column_name,data_type\ntrade_date,DATE\ncode,VARCHAR\n")
    before = sidecar.read_text()

    _discover_and_write_csv_columns([tbl], str(outdir), db_path=None)

    assert tbl["_read_types"]["code"] == "VARCHAR"
    assert sidecar.read_text() == before


# >>> config columns win over the sidecar (config > sidecar > inference) <<< #
def test_config_columns_win_over_sidecar(tmp_path):
    data = _write_raw(
        tmp_path / "codes2.csv",
        "trade_date,code\n"
        "2026-01-01,007\n",
    )
    tbl = {"name": "codes2", "table": "codes2", "source": "csv",
           "conn_macro": data, "date_col": "trade_date",
           "columns": {"code": "INTEGER"}}
    outdir = tmp_path / "out"
    outdir.mkdir()
    qname = qualified_name(tbl)
    (outdir / f"{qname}_columns.csv").write_text(
        "column_name,data_type\ncode,VARCHAR\n")

    _discover_and_write_csv_columns([tbl], str(outdir), db_path=None)

    assert tbl["_read_types"]["code"] == "BIGINT"
