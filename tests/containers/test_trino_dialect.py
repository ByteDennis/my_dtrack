from dtrack.platforms.athena import _build_row_sql_athena


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
