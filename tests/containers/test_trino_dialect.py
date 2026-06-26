import os
import tempfile

from dtrack.platforms.athena import _build_row_sql_athena, _write_combined_sql
from dtrack.platforms.base import build_col_sql, parse_extract_sql_file


# >>> drive a Trino DDL/DML statement to completion <<< #
def _run(cur, sql):
    cur.execute(sql)
    cur.fetchall()


# >>> dtrack's Athena/Trino row SQL executes on real Trino and counts per date <<< #
def test_trino_row_sql_counts(trino_cursor):
    cur = trino_cursor
    _run(cur, "DROP TABLE IF EXISTS memory.default.dt_row")
    _run(cur, "CREATE TABLE memory.default.dt_row (rpt_dt varchar, amt integer)")
    _run(cur, "INSERT INTO memory.default.dt_row VALUES "
              "('2024-01-01', 10), ('2024-01-01', 20), ('2024-01-02', 30)")

    cfg = {"name": "dt_row", "table": "dt_row", "source": "aws",
           "conn_macro": "memory.default", "date_col": "rpt_dt",
           "date_type": "string_dash"}
    cur.execute(_build_row_sql_athena(cfg))
    counts = {r[0]: r[1] for r in cur.fetchall()}
    assert counts == {"2024-01-01": 2, "2024-01-02": 1}


# >>> map a Trino col-stats result row to a name->value dict <<< #
def _col_row(cur, sql):
    cur.execute(sql)
    cols = [d[0].lower() for d in cur.description]
    rows = cur.fetchall()
    assert len(rows) == 1
    return dict(zip(cols, rows[0]))


# >>> build_col_sql('athena') numeric stats execute on real Trino with right values <<< #
def test_trino_col_sql_numeric_values(trino_cursor):
    cur = trino_cursor
    _run(cur, "DROP TABLE IF EXISTS memory.default.dt_col")
    _run(cur, "CREATE TABLE memory.default.dt_col (rpt_dt varchar, amt integer, ccy varchar)")
    _run(cur, "INSERT INTO memory.default.dt_col VALUES "
              "('2024-01-01', 10, 'USD'), ('2024-01-01', 20, 'USD'), "
              "('2024-01-02', 30, 'EUR')")

    cfg = {"name": "dt_col", "table": "dt_col", "source": "aws",
           "conn_macro": "memory.default", "date_col": "rpt_dt",
           "date_type": "string_dash"}
    sql, is_numeric = build_col_sql("athena", cfg, "amt", "integer",
                                    "memory.default.dt_col", vintage="all")
    assert is_numeric
    rec = _col_row(cur, sql)
    assert int(rec["n_total"]) == 3
    assert int(rec["n_missing"]) == 0
    assert int(rec["n_unique"]) == 3
    assert float(rec["mean"]) == 20.0
    assert float(rec["min_val"]) == 10.0
    assert float(rec["max_val"]) == 30.0


# >>> build_col_sql('athena') categorical stats (hash16/top_10) run on real Trino <<< #
def test_trino_col_sql_categorical_values(trino_cursor):
    cur = trino_cursor
    _run(cur, "DROP TABLE IF EXISTS memory.default.dt_cat")
    _run(cur, "CREATE TABLE memory.default.dt_cat (rpt_dt varchar, ccy varchar)")
    _run(cur, "INSERT INTO memory.default.dt_cat VALUES "
              "('2024-01-01', 'USD'), ('2024-01-01', 'USD'), "
              "('2024-01-02', 'EUR')")

    cfg = {"name": "dt_cat", "table": "dt_cat", "source": "aws",
           "conn_macro": "memory.default", "date_col": "rpt_dt",
           "date_type": "string_dash"}
    sql, is_numeric = build_col_sql("athena", cfg, "ccy", "VARCHAR",
                                    "memory.default.dt_cat", vintage="all")
    assert not is_numeric
    rec = _col_row(cur, sql)
    assert int(rec["n_total"]) == 3
    assert int(rec["n_unique"]) == 2
    assert "USD" in rec["top_10"] and "EUR" in rec["top_10"]
    assert float(rec["hash_sum"]) != 0.0
    assert int(float(rec["length_max"])) == 3


# >>> athena col gen (_write_combined_sql -> build_col_sql) emits Trino-executable SQL <<< #
def test_trino_gen_col_sql_executes(trino_cursor):
    cur = trino_cursor
    _run(cur, "DROP TABLE IF EXISTS memory.default.dt_col")
    _run(cur, "CREATE TABLE memory.default.dt_col (rpt_dt varchar, amt integer, ccy varchar)")
    _run(cur, "INSERT INTO memory.default.dt_col VALUES "
              "('2024-01-01', 10, 'USD'), ('2024-01-01', 20, 'USD'), "
              "('2024-01-02', 30, 'EUR')")

    tbl = {"name": "dt_col", "table": "dt_col", "source": "aws",
           "conn_macro": "memory.default", "date_col": "rpt_dt",
           "date_type": "string_dash", "vintage": "all",
           "columns": {"rpt_dt": "varchar", "amt": "integer", "ccy": "varchar"}}
    outdir = tempfile.mkdtemp()
    _write_combined_sql([tbl], outdir, "col", db_path=None, preset="detailed")
    parsed = parse_extract_sql_file(os.path.join(outdir, "extract_col.sql"))
    stmts = [(p.get("sql") if isinstance(p, dict) else p) for p in parsed]
    stmts = [s for s in stmts if s and "AS column_name" in s]
    assert len(stmts) == 2

    seen = {}
    for s in stmts:
        cur.execute(s)
        cols = [d[0].lower() for d in cur.description]
        rec = dict(zip(cols, cur.fetchall()[0]))
        seen[rec["column_name"]] = rec
    assert int(seen["amt"]["n_unique"]) == 3
    assert int(seen["ccy"]["n_unique"]) == 2
    assert float(seen["ccy"]["hash_sum"]) != 0.0
