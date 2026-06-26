from dtrack.platforms.base import build_col_sql, resolve_table
from dtrack.platforms.oracle import _build_row_sql_oracle


# >>> dtrack's Oracle row SQL executes on real Oracle and counts per date <<< #
def test_oracle_row_sql_counts(oracle_cursor):
    cur = oracle_cursor
    cur.execute("CREATE TABLE dt_row (rpt_dt DATE, amt NUMBER)")
    cur.executemany(
        "INSERT INTO dt_row (rpt_dt, amt) VALUES (TO_DATE(:1,'YYYY-MM-DD'), :2)",
        [("2024-01-01", 10), ("2024-01-01", 20), ("2024-01-02", 30)])
    cur.connection.commit()

    cfg = {"name": "dt_row", "table": "dt_row", "source": "oracle",
           "date_col": "rpt_dt", "date_type": "date"}
    cur.execute(_build_row_sql_oracle(cfg))
    counts = {r[0].strftime("%Y-%m-%d"): r[1] for r in cur.fetchall()}
    assert counts == {"2024-01-01": 2, "2024-01-02": 1}


# >>> dtrack's Oracle col-stats SQL (via _col_sql_dialect_ops) executes on real Oracle <<< #
def test_oracle_col_sql_executes(oracle_cursor):
    cur = oracle_cursor
    cur.execute("CREATE TABLE dt_col (rpt_dt DATE, amt NUMBER)")
    cur.executemany(
        "INSERT INTO dt_col (rpt_dt, amt) VALUES (TO_DATE(:1,'YYYY-MM-DD'), :2)",
        [("2024-01-01", 10), ("2024-01-01", 20), ("2024-01-02", 30)])
    cur.connection.commit()

    cfg = {"name": "dt_col", "table": "dt_col", "source": "oracle",
           "date_col": "rpt_dt", "date_type": "date"}
    full_table, cte = resolve_table(cfg, "dt_col")
    sql, is_numeric = build_col_sql("oracle", cfg, "amt", "NUMBER", full_table,
                                    cte_prefix=cte, vintage="all")
    assert is_numeric
    cur.execute(sql)
    cols = [d[0].lower() for d in cur.description]
    rows = cur.fetchall()
    assert len(rows) == 1
    rec = dict(zip(cols, rows[0]))
    assert int(rec["n_total"]) == 3
    assert int(rec["n_missing"]) == 0
    assert int(rec["n_unique"]) == 3
    assert float(rec["mean"]) == 20.0
    assert float(rec["min_val"]) == 10.0
    assert float(rec["max_val"]) == 30.0


# >>> STRICT (LOWER/TRIM/COMPOSE) collapses case+whitespace drift on real Oracle <<< #
def test_oracle_strict_normalization(oracle_cursor):
    cur = oracle_cursor
    cur.execute("CREATE TABLE dt_strict (rpt_dt DATE, ccy VARCHAR2(20))")
    cur.executemany(
        "INSERT INTO dt_strict (rpt_dt, ccy) VALUES (TO_DATE(:1,'YYYY-MM-DD'), :2)",
        [("2024-01-01", "USD"), ("2024-01-01", "usd"), ("2024-01-01", "USD ")])
    cur.connection.commit()

    cfg = {"name": "dt_strict", "table": "dt_strict", "source": "oracle",
           "date_col": "rpt_dt", "date_type": "date"}
    full_table, cte = resolve_table(cfg, "dt_strict")

    def n_unique(strict):
        sql, _ = build_col_sql("oracle", cfg, "ccy", "VARCHAR2", full_table,
                               cte_prefix=cte, vintage="all", strict=strict)
        cur.execute(sql)
        cols = [d[0].lower() for d in cur.description]
        return int(dict(zip(cols, cur.fetchall()[0]))["n_unique"])

    assert n_unique(False) == 3
    assert n_unique(True) == 1
