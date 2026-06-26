"""Tests for the config endpoints:
    GET  /api/config
    POST /api/config/upload
    PUT  /api/config
    POST /api/sync/reload_json
    GET  /api/global_settings
    POST /api/global_settings
"""
import io
import json


def test_get_config_returns_full_json(client, api_workdir):
    resp = client.get("/api/config")
    assert resp.status_code == 200
    cfg = resp.json()
    assert "pairs" in cfg
    assert "t_pair" in cfg["pairs"]
    assert cfg["pairs"]["t_pair"]["left"]["source"] == "oracle"


def test_put_config_merges_settings(client, api_workdir):
    payload = {
        "settings": {
            "from_date": "2024-01-01",
            "to_date": "2024-12-31",
            "overwrite_cache": True,
        },
    }
    resp = client.put("/api/config", json=payload)
    assert resp.status_code == 200
    assert resp.json()["ok"] is True

    # Round-trip: GET must reflect the merge and preserve unrelated keys.
    cfg = client.get("/api/config").json()
    assert cfg["settings"]["from_date"] == "2024-01-01"
    assert cfg["settings"]["to_date"] == "2024-12-31"
    assert cfg["settings"]["overwrite_cache"] is True
    assert cfg["settings"]["sas_outdir"] == "./sas/"  # untouched


def test_put_config_merges_pair_keys(client):
    resp = client.put("/api/config", json={
        "pairs": {"t_pair": {"description": "edited via PUT"}},
    })
    assert resp.status_code == 200
    cfg = client.get("/api/config").json()
    assert cfg["pairs"]["t_pair"]["description"] == "edited via PUT"


def test_upload_config_replaces_file(client):
    new_cfg = {
        "metadata": {"title": "uploaded"},
        "settings": {"sas_outdir": "./sas/", "aws_outdir": "./csv/"},
        "pairs": {
            "uploaded_pair": {
                "left": {
                    "name": "u_left", "table": "U_LEFT", "source": "oracle",
                    "conn_macro": "pb23", "date_col": "RPT_DT", "date_type": "date",
                },
                "right": {
                    "name": "u_right", "table": "u_right", "source": "aws",
                    "conn_macro": "analytics_db", "date_col": "rpt_dt",
                    "date_type": "string_dash",
                },
            },
        },
    }
    files = {"file": ("dtrack.json", io.BytesIO(json.dumps(new_cfg).encode()),
                      "application/json")}
    resp = client.post("/api/config/upload", files=files)
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ok"] is True
    assert body["pairs"] == 1

    # Original t_pair should be gone after a full replace.
    cfg = client.get("/api/config").json()
    assert "uploaded_pair" in cfg["pairs"]
    assert "t_pair" not in cfg["pairs"]


def test_upload_config_rejects_missing_pairs(client):
    bad = {"metadata": {"title": "no pairs"}}
    files = {"file": ("dtrack.json", io.BytesIO(json.dumps(bad).encode()),
                      "application/json")}
    resp = client.post("/api/config/upload", files=files)
    assert resp.status_code == 400


def test_upload_config_rejects_invalid_json(client):
    files = {"file": ("dtrack.json", io.BytesIO(b"{not-json"),
                      "application/json")}
    resp = client.post("/api/config/upload", files=files)
    assert resp.status_code == 400


def test_sync_reload_json_ok(client):
    resp = client.post("/api/sync/reload_json")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True


def test_global_settings_round_trip(client):
    # Unset -> all keys present, all None.
    resp = client.get("/api/global_settings")
    assert resp.status_code == 200
    keys = {"ora_retry_count", "ora_retry_sleep",
            "hdp_retry_count", "hdp_retry_sleep"}
    assert set(resp.json().keys()) == keys

    # Set a few; ints get coerced from strings; None clears.
    resp = client.post("/api/global_settings", json={
        "ora_retry_count": "5",
        "ora_retry_sleep": 60,
    })
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert body["ora_retry_count"] == 5
    assert body["ora_retry_sleep"] == 60

    # None clears a key.
    resp = client.post("/api/global_settings", json={"ora_retry_sleep": None})
    assert resp.status_code == 200
    assert resp.json()["ora_retry_sleep"] is None
