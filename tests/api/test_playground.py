"""Tests for the playground endpoints:
    POST   /api/playground/run                  (requires backend)
    POST   /api/playground/cancel_running       (always safe)
    POST   /api/playground/cancel/{run_id}      (404 path tested)
    POST   /api/playground/sas                  (pure string templating)
    GET    /api/playground/history
    PUT    /api/playground/history/{run_id}
    DELETE /api/playground/history/{run_id}
"""

import pytest

from dtrack.db import insert_playground_run


@pytest.fixture
def seed_history(api_workdir):
    """Insert a few rows into _playground_history so the list/update/delete
    endpoints have something to work with."""
    ids = []
    for engine, sql in (("athena", "SELECT 1"),
                        ("oracle", "SELECT * FROM dual"),
                        ("sas", "proc print data=foo;")):
        rid = insert_playground_run(
            api_workdir["db"],
            engine=engine, conn="cx",
            sql=sql, elapsed_sec=0.1, n_rows=0, status="ok",
        )
        ids.append(rid)
    return ids


def test_history_list_returns_seeded(client, seed_history):
    resp = client.get("/api/playground/history")
    assert resp.status_code == 200
    body = resp.json()
    assert "runs" in body
    returned_ids = {r["id"] for r in body["runs"]}
    assert set(seed_history).issubset(returned_ids)


def test_history_update_note(client, seed_history):
    rid = seed_history[0]
    resp = client.put(f"/api/playground/history/{rid}", json={"note": "hello"})
    assert resp.status_code == 200
    assert resp.json()["ok"] is True

    runs = client.get("/api/playground/history").json()["runs"]
    row = next(r for r in runs if r["id"] == rid)
    assert row["note"] == "hello"


def test_history_update_404_for_missing(client):
    resp = client.put("/api/playground/history/999999", json={"note": "x"})
    assert resp.status_code == 404


def test_history_delete_round_trip(client, seed_history):
    rid = seed_history[-1]
    resp = client.delete(f"/api/playground/history/{rid}")
    assert resp.status_code == 200
    assert resp.json()["ok"] is True

    # Second delete now returns 404.
    resp = client.delete(f"/api/playground/history/{rid}")
    assert resp.status_code == 404


def test_cancel_running_when_idle_returns_empty(client):
    resp = client.post("/api/playground/cancel_running")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["cancelled_ids"] == []


def test_cancel_unknown_run_id_returns_404(client):
    resp = client.post("/api/playground/cancel/999999")
    assert resp.status_code == 404
    assert "not currently running" in resp.json()["error"]


def test_playground_sas_oracle_libname(client):
    resp = client.post("/api/playground/sas", json={
        "kind": "oracle",
        "method": "libname",
        "sql": "select count(*) from cust_daily;",
        "conn_macro": "pb23",
    })
    assert resp.status_code in (200, 400), resp.text
    if resp.status_code == 200:
        body = resp.json()
        assert body["ok"] is True
        assert body["content"]
        assert body["filename"].endswith(".sas")


def test_playground_run_reaches_backend_or_errors_cleanly(client):
    # Happy path requires .env-configured Athena/Oracle. We just exercise
    # routing -- 200 (rows or "error" status JSON), 400 (bad body), or 500.
    resp = client.post("/api/playground/run", json={
        "engine": "athena", "sql": "SELECT 1", "conn": "analytics_db",
        "row_cap": 10,
    })
    assert resp.status_code in (200, 400, 500), resp.text
