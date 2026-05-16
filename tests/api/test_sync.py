"""Tests for the sync inspector endpoints:
    GET  /api/sync/inspect
    POST /api/sync/push
"""


def test_sync_inspect_returns_per_pair_field_status(client):
    resp = client.get("/api/sync/inspect")
    assert resp.status_code == 200
    body = resp.json()
    assert "pairs" in body
    assert "config_path" in body and "db_path" in body

    pair_entry = next(p for p in body["pairs"] if p["pair_name"] == "t_pair")
    field_names = {f["name"] for f in pair_entry["fields"]}
    assert field_names == {"col_map", "col_filter", "where_map"}
    for f in pair_entry["fields"]:
        assert f["status"] in {"match", "differ", "json_only", "db_only", "empty"}


def test_sync_push_rejects_unknown_field(client):
    resp = client.post("/api/sync/push", json={
        "pair": "t_pair", "field": "ghost", "direction": "json_to_db",
    })
    assert resp.status_code == 400


def test_sync_push_rejects_unknown_direction(client):
    resp = client.post("/api/sync/push", json={
        "pair": "t_pair", "field": "col_map", "direction": "sideways",
    })
    assert resp.status_code == 400


def test_sync_push_rejects_unknown_pair(client):
    resp = client.post("/api/sync/push", json={
        "pair": "ghost_pair", "field": "col_map", "direction": "json_to_db",
    })
    assert resp.status_code == 404


def test_sync_push_where_map_json_to_db_is_blocked(client):
    # where_map is reconstructed at extract time from _row_comparison; the
    # endpoint refuses the json->db direction with an explanatory 400.
    resp = client.post("/api/sync/push", json={
        "pair": "t_pair", "field": "where_map", "direction": "json_to_db",
    })
    assert resp.status_code == 400


def test_sync_push_col_map_json_to_db(client):
    # Seed a col_map in the json side via PUT /api/config.
    client.put("/api/config", json={
        "pairs": {"t_pair": {"col_map": {"LEFT_COL": "right_col"}}},
    })
    resp = client.post("/api/sync/push", json={
        "pair": "t_pair", "field": "col_map", "direction": "json_to_db",
    })
    assert resp.status_code == 200, resp.text
    assert resp.json()["ok"] is True
