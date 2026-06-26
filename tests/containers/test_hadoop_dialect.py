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
    rows = cur.fetchall()
    assert len(rows) == 1
