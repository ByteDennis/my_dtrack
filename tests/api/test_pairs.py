"""Tests for the pair CRUD + annotation endpoints:
    POST   /api/pairs
    GET    /api/pairs/list
    PUT    /api/pairs/{name}
    DELETE /api/pairs/{name}
    POST   /api/pairs/skip
    POST   /api/pairs/reload
    GET    /api/pairs/{name}/annotations
    PUT    /api/pairs/{name}/annotations
"""

import pytest


@pytest.fixture
def add_oracle_aws_pair():
    """Returns a POST body for a fresh oracle-vs-aws pair (not the t_pair
    fixture). Each call yields a unique name."""
    seq = {"n": 0}
    def _make():
        seq["n"] += 1
        name = f"new_pair_{seq['n']}"
        return {
            "pair_name": name,
            "left": {
                "table": f"LEFT_TBL_{seq['n']}",
                "source": "oracle",
                "conn_macro": "pb23",
                "date_col": "RPT_DT",
                "date_type": "date",
            },
            "right": {
                "table": f"right_tbl_{seq['n']}",
                "source": "aws",
                "conn_macro": "analytics_db",
                "date_col": "rpt_dt",
                "date_type": "string_dash",
            },
            "description": "added by test",
            "mode": "incremental",
        }
    return _make


def test_list_pairs_returns_fixture_pair(client):
    resp = client.get("/api/pairs/list")
    assert resp.status_code == 200
    names = [p["name"] for p in resp.json()["pairs"]]
    assert "t_pair" in names


def test_add_pair_round_trips(client, add_oracle_aws_pair):
    payload = add_oracle_aws_pair()
    resp = client.post("/api/pairs", json=payload)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ok"] is True
    assert body["pair_name"] == payload["pair_name"]

    names = [p["name"] for p in client.get("/api/pairs/list").json()["pairs"]]
    assert payload["pair_name"] in names


def test_add_pair_rejects_missing_name(client):
    resp = client.post("/api/pairs", json={"left": {"table": "X"},
                                            "right": {"table": "y"}})
    assert resp.status_code == 400


def test_add_pair_rejects_missing_tables(client):
    resp = client.post("/api/pairs", json={
        "pair_name": "needs_tables",
        "left": {"source": "oracle"}, "right": {"source": "aws"},
    })
    assert resp.status_code == 400


def test_update_pair_changes_description(client):
    resp = client.put("/api/pairs/t_pair", json={
        "description": "updated description",
    })
    assert resp.status_code == 200

    cfg = client.get("/api/config").json()
    assert cfg["pairs"]["t_pair"]["description"] == "updated description"


def test_update_pair_404_for_unknown(client):
    resp = client.put("/api/pairs/does_not_exist", json={
        "description": "noop",
    })
    assert resp.status_code == 404


def test_delete_pair_removes_from_config(client, add_oracle_aws_pair):
    payload = add_oracle_aws_pair()
    client.post("/api/pairs", json=payload)
    name = payload["pair_name"]

    resp = client.delete(f"/api/pairs/{name}")
    assert resp.status_code == 200

    names = [p["name"] for p in client.get("/api/pairs/list").json()["pairs"]]
    assert name not in names


def test_delete_pair_with_purge_returns_ok(client, add_oracle_aws_pair):
    payload = add_oracle_aws_pair()
    client.post("/api/pairs", json=payload)
    resp = client.delete(f"/api/pairs/{payload['pair_name']}?purge=1")
    assert resp.status_code == 200


def test_skip_pair_persists(client):
    resp = client.post("/api/pairs/skip", json={
        "pair_name": "t_pair", "skip": True,
    })
    assert resp.status_code == 200

    listed = client.get("/api/pairs/list").json()["pairs"]
    t = next(p for p in listed if p["name"] == "t_pair")
    assert t["skip"] is True


def test_skip_pair_rejects_unknown(client):
    resp = client.post("/api/pairs/skip", json={
        "pair_name": "ghost", "skip": True,
    })
    assert resp.status_code == 404


def test_skip_pair_bulk_form(client, add_oracle_aws_pair):
    payload = add_oracle_aws_pair()
    client.post("/api/pairs", json=payload)
    resp = client.post("/api/pairs/skip", json={
        "pairs": {"t_pair": True, payload["pair_name"]: False},
    })
    assert resp.status_code == 200
    assert set(resp.json()["updated"]) == {"t_pair", payload["pair_name"]}


def test_annotations_default_empty(client):
    resp = client.get("/api/pairs/t_pair/annotations")
    assert resp.status_code == 200
    body = resp.json()
    assert body["pair_name"] == "t_pair"
    # The defaults are empty strings inside the nested maps.
    assert body["where_map"]["left"] == ""
    assert body["time_map"]["row"]["left"] == ""


def test_annotations_update_round_trip(client):
    resp = client.put("/api/pairs/t_pair/annotations", json={
        "where_map": {"left": "STATUS='A'", "right": "status='A'"},
        "comment_map": {"row": {"left": "test note", "right": ""}},
    })
    assert resp.status_code == 200

    body = client.get("/api/pairs/t_pair/annotations").json()
    assert body["where_map"]["left"] == "STATUS='A'"
    assert body["comment_map"]["row"]["left"] == "test note"


def test_annotations_404_for_unknown(client):
    resp = client.get("/api/pairs/ghost/annotations")
    assert resp.status_code == 404


def test_pairs_reload_ok(client):
    resp = client.post("/api/pairs/reload")
    assert resp.status_code == 200
