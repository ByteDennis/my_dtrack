import pytest

from dtrack.config import validate_unified_pair

from helpers import write_data_csv

_HAS_DUCKDB = __import__("importlib").util.find_spec("duckdb") is not None


# >>> /api/playground/run with engine=csv runs DuckDB over a temp CSV <<< #
@pytest.mark.skipif(not _HAS_DUCKDB, reason="duckdb not installed")
def test_playground_csv_select_count(app_client, tmp_path):
    client, ctx = app_client
    csv_path = write_data_csv(
        tmp_path / "pg.csv", ["trade_date", "ccy", "amt"],
        [["2026-01-01", "USD", 10], ["2026-01-01", "EUR", 20],
         ["2026-01-02", "USD", 30]])
    resp = client.post("/api/playground/run", json={
        "engine": "csv", "conn": csv_path,
        "sql": "SELECT count(*) AS n FROM data", "row_cap": 10})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ok"] is True
    assert body["rows"][0][0] == "3"


# >>> a csv-source pair without conn_macro raises a clear config error <<< #
def test_csv_pair_requires_conn_macro():
    bad = {
        "left": {"name": "a", "table": "a", "source": "csv"},
        "right": {"name": "b", "table": "b", "source": "csv",
                  "conn_macro": "/tmp/b.csv"},
    }
    with pytest.raises(ValueError, match="conn_macro"):
        validate_unified_pair("p", bad)


# >>> a csv-source pair with conn_macro on both sides validates cleanly <<< #
def test_csv_pair_with_conn_macro_ok():
    good = {
        "left": {"name": "a", "table": "a", "source": "csv",
                 "conn_macro": "/tmp/a.csv"},
        "right": {"name": "b", "table": "b", "source": "csv",
                  "conn_macro": "/tmp/b.csv"},
    }
    validate_unified_pair("p", good)
