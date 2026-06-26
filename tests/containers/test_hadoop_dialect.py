from dtrack.platforms.base import build_col_sql, quote_ident
from dtrack.platforms.hadoop import _build_row_sql_hadoop


# >>> create + populate a Hive table for the sample rows <<< #
def _seed(cur, name):
    cur.execute(f"DROP TABLE IF EXISTS {name}")
    cur.execute(f"CREATE TABLE {name} (rpt_dt STRING, amt BIGINT)")
    cur.execute(f"INSERT INTO {name} VALUES "
                "('2024-01-01', 10), ('2024-01-01', 20), ('2024-01-02', 30)")


# >>> dtrack's Hadoop/Hive row SQL executes on real HiveServer2 and counts per date <<< #
def test_hadoop_row_sql_counts(hive_cursor):
    cur = hive_cursor
    _seed(cur, "dt_row")
    cfg = {"name": "dt_row", "table": "dt_row", "source": "hadoop",
           "conn_macro": "", "date_col": "rpt_dt", "date_type": "string_dash"}
    cur.execute(_build_row_sql_hadoop(cfg))
    counts = {r[0]: r[1] for r in cur.fetchall()}
    assert counts == {"2024-01-01": 2, "2024-01-02": 1}


# >>> dtrack's Hadoop col-stats SQL (via _col_sql_dialect_ops) executes on real Hive <<< #
def test_hadoop_col_sql_executes(hive_cursor):
    cur = hive_cursor
    _seed(cur, "dt_col")
    cfg = {"name": "dt_col", "table": "dt_col", "source": "hadoop",
           "conn_macro": "", "date_col": "rpt_dt", "date_type": "string_dash"}
    full_table = quote_ident("dt_col", "hadoop")
    sql, is_numeric = build_col_sql("hadoop", cfg, "amt", "BIGINT", full_table,
                                    vintage="all")
    assert is_numeric
    cur.execute(sql)
    cols = [d[0].split('.')[-1].lower() for d in cur.description]
    rows = cur.fetchall()
    assert len(rows) == 1
    rec = dict(zip(cols, rows[0]))
    assert int(rec["n_total"]) == 3
    assert int(rec["n_missing"]) == 0
    assert int(rec["n_unique"]) == 3
    assert float(rec["mean"]) == 20.0
    assert float(rec["min_val"]) == 10.0
    assert float(rec["max_val"]) == 30.0


# >>> STRICT (lower/trim) collapses case+whitespace drift on real Hive <<< #
def test_hadoop_strict_normalization(hive_cursor):
    cur = hive_cursor
    cur.execute("DROP TABLE IF EXISTS dt_strict")
    cur.execute("CREATE TABLE dt_strict (rpt_dt STRING, ccy STRING)")
    cur.execute("INSERT INTO dt_strict VALUES "
                "('2024-01-01','USD'), ('2024-01-01','usd'), ('2024-01-01','USD ')")
    cfg = {"name": "dt_strict", "table": "dt_strict", "source": "hadoop",
           "conn_macro": "", "date_col": "rpt_dt", "date_type": "string_dash"}
    full_table = quote_ident("dt_strict", "hadoop")

    def n_unique(strict):
        sql, _ = build_col_sql("hadoop", cfg, "ccy", "STRING", full_table,
                               vintage="all", strict=strict)
        cur.execute(sql)
        cols = [d[0].split('.')[-1].lower() for d in cur.description]
        return int(dict(zip(cols, cur.fetchall()[0]))["n_unique"])

    assert n_unique(False) == 3
    assert n_unique(True) == 1
