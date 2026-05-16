"""Tests for the ad-hoc SQLite query endpoint:
    POST /api/query
This one targets the local SQLite project DB, so it doesn't need backend
connections -- but the parent conftest still skips on non-Windows for
suite-level consistency.
"""


def test_query_select_returns_rows(client):
    resp = client.post("/api/query", json={
        "sql": "SELECT 1 AS one, 'two' AS two",
    })
    assert resp.status_code == 200
    body = resp.json()
    assert body["columns"] == ["one", "two"]
    assert body["rows"] == [{"one": 1, "two": "two"}]


def test_query_rejects_empty(client):
    resp = client.post("/api/query", json={"sql": "  "})
    assert resp.status_code == 400
    assert "error" in resp.json()


def test_query_returns_400_on_sql_error(client):
    resp = client.post("/api/query", json={
        "sql": "SELECT * FROM nonexistent_table_xyz",
    })
    assert resp.status_code == 400
    assert "error" in resp.json()


def test_query_ddl_returns_rowcount_message(client):
    # CREATE / INSERT go through the non-SELECT branch which returns a
    # "<verb> executed (N row(s) affected)" message.
    resp = client.post("/api/query", json={
        "sql": "CREATE TABLE IF NOT EXISTS _api_tmp_test (id INTEGER)",
    })
    assert resp.status_code == 200
    body = resp.json()
    assert body["columns"] == []
    assert body["rows"] == []
    assert "executed" in body.get("message", "")


def test_query_pragma_select_branch(client):
    resp = client.post("/api/query", json={"sql": "PRAGMA database_list"})
    assert resp.status_code == 200
    body = resp.json()
    # 'database_list' returns at least one row with columns including 'name'.
    assert "rows" in body and isinstance(body["rows"], list)
