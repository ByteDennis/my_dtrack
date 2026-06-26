import pytest

from dtrack.platforms.base import build_col_sql, _resolve_strict

pytestmark = pytest.mark.skipif(
    __import__("importlib").util.find_spec("duckdb") is None,
    reason="duckdb not installed",
)


# >>> full stats row from build_col_sql('duckdb') for one col/preset <<< #
def _stats(con, table, dtype, strict, preset="detailed"):
    sql, _ = build_col_sql("duckdb", {"date_col": "rpt_dt"}, "v", dtype, table,
                           dt_label="all", preset=preset, strict=strict)
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return dict(zip(cols, cur.fetchall()[0]))


# >>> case/whitespace drift reconciles under STRICT across ALL presets <<< #
# >>> quick/standard exercise the flat path; detailed the categorical path <<< #
@pytest.mark.parametrize("preset", ["quick", "standard", "detailed"])
def test_strict_string_case_and_whitespace(preset):
    import duckdb
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE l(rpt_dt DATE, v VARCHAR)")
    con.execute("CREATE TABLE r(rpt_dt DATE, v VARCHAR)")
    con.execute("INSERT INTO l VALUES (DATE '2024-01-01','USD'),"
                "(DATE '2024-01-01','usd'),(DATE '2024-01-02','EUR')")
    con.execute("INSERT INTO r VALUES (DATE '2024-01-01','usd  '),"
                "(DATE '2024-01-01','USD'),(DATE '2024-01-02','eur')")

    assert _stats(con, "l", "VARCHAR", False, preset) != \
        _stats(con, "r", "VARCHAR", False, preset)
    sl = _stats(con, "l", "VARCHAR", True, preset)
    sr = _stats(con, "r", "VARCHAR", True, preset)
    assert sl == sr


# >>> date vs zero-time-timestamp text reconcile only under STRICT <<< #
def test_strict_date_zero_time():
    import duckdb
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE l(rpt_dt DATE, v VARCHAR)")
    con.execute("CREATE TABLE r(rpt_dt DATE, v VARCHAR)")
    con.execute("INSERT INTO l VALUES (DATE '2024-01-01','2024-01-01'),"
                "(DATE '2024-01-02','2024-03-05')")
    con.execute("INSERT INTO r VALUES (DATE '2024-01-01','2024-01-01 00:00:00'),"
                "(DATE '2024-01-02','2024-03-05 00:00:00')")

    assert _stats(con, "l", "DATE", False) != _stats(con, "r", "DATE", False)
    assert _stats(con, "l", "DATE", True) == _stats(con, "r", "DATE", True)


# >>> STRICT also reconciles the flat/coarse (quick-preset) path hash <<< #
def test_strict_flat_path():
    import duckdb
    con = duckdb.connect(":memory:")
    con.execute("CREATE TABLE l(rpt_dt DATE, v VARCHAR)")
    con.execute("CREATE TABLE r(rpt_dt DATE, v VARCHAR)")
    con.execute("INSERT INTO l VALUES (DATE '2024-01-01','AB'),(DATE '2024-01-02','cd')")
    con.execute("INSERT INTO r VALUES (DATE '2024-01-01','ab'),(DATE '2024-01-02','CD')")

    def flat(table, strict):
        sql, _ = build_col_sql("duckdb", {"date_col": "rpt_dt"}, "v", "VARCHAR",
                               table, dt_label="all", preset="quick",
                               stats_override={"hash_sum": True}, strict=strict)
        assert "top_n" not in sql  # confirm the flat (no-top10) path
        cur = con.execute(sql)
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, cur.fetchall()[0]))["hash_sum"]

    assert flat("l", False) != flat("r", False)
    assert flat("l", True) == flat("r", True)


# >>> strict resolution: explicit arg > per-pair config > DTRACK_STRICT env <<< #
def test_resolve_strict_precedence(monkeypatch):
    monkeypatch.delenv("DTRACK_STRICT", raising=False)
    assert _resolve_strict({}) is False
    assert _resolve_strict({"strict": True}) is True
    assert _resolve_strict({"strict": True}, strict=False) is False
    monkeypatch.setenv("DTRACK_STRICT", "1")
    assert _resolve_strict({}) is True
    assert _resolve_strict({"strict": False}) is False
    monkeypatch.setenv("DTRACK_STRICT", "0")
    assert _resolve_strict({}) is False


# >>> per-pair `strict` propagates into both side table configs <<< #
def test_strict_config_propagation():
    from dtrack.config import get_all_tables_from_unified
    cfg = {"pairs": {"p": {
        "strict": True,
        "left": {"name": "a", "table": "a", "source": "csv"},
        "right": {"name": "b", "table": "b", "source": "csv"},
    }}}
    tables = get_all_tables_from_unified(cfg)
    assert all(t.get("strict") is True for t in tables)
