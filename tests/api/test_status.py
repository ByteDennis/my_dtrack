"""Tests for read-only / metadata endpoints that don't talk to backends:
    GET  /api/status
    POST /api/init
    POST /api/refresh-metadata
    GET  /api/date-types
    GET  /api/constants
"""


def test_api_status_returns_pairs_from_config(client, api_workdir):
    resp = client.get("/api/status")
    assert resp.status_code == 200
    data = resp.json()
    assert "pairs" in data
    assert data["db_path"] == api_workdir["db"]
    assert data["config_path"] == api_workdir["config"]

    names = {p["pair_name"] for p in data["pairs"]}
    assert "t_pair" in names
    t = next(p for p in data["pairs"] if p["pair_name"] == "t_pair")
    assert t["source_left"] == "oracle"
    assert t["source_right"] == "aws"


def test_api_init_creates_db_when_missing(client, app_bound, tmp_path):
    # Point the app at a non-existent DB path; /api/init should create it.
    fresh = tmp_path / "fresh.db"
    app_bound._db_path = str(fresh)
    resp = client.post("/api/init")
    assert resp.status_code == 200
    data = resp.json()
    assert data["action"] in ("created", "refreshed")
    assert fresh.exists()


def test_api_init_refreshes_existing_db(client):
    resp = client.post("/api/init")
    assert resp.status_code == 200
    assert resp.json()["action"] == "refreshed"


def test_api_refresh_metadata_ok(client):
    resp = client.post("/api/refresh-metadata")
    assert resp.status_code == 200
    body = resp.json()
    assert body["ok"] is True
    assert isinstance(body["updated"], int)


def test_api_date_types_returns_known_keys(client):
    resp = client.get("/api/date-types")
    assert resp.status_code == 200
    dt = resp.json()["date_types"]
    # The frontend depends on these specific keys; assert a few stable ones.
    assert isinstance(dt, dict)
    for key in ("date", "timestamp", "string_dash"):
        assert key in dt, f"missing date_type: {key}"


def test_api_constants_exposes_ui_lists(client):
    resp = client.get("/api/constants")
    assert resp.status_code == 200
    c = resp.json()
    for key in ("DATA_SOURCES", "CONNECTION_MACROS", "DATE_COLUMN_TYPES"):
        assert key in c
    assert isinstance(c["DATA_SOURCES"], (list, dict))
