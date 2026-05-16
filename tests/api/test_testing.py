"""Tests for the testing-mode toggle:
    GET  /api/testing
    POST /api/testing
"""

from pathlib import Path

import pytest


def test_testing_status_defaults_off(client, app_bound):
    resp = client.get("/api/testing")
    assert resp.status_code == 200
    body = resp.json()
    assert body["testing"] is False
    assert body["config_path"] == app_bound._config_path


def test_testing_enable_requires_real_testing_dir(client):
    # The testing/ + testing/mock dirs are committed in the repo; enabling
    # should swap _config_path to testing/config.json and set DTRACK_MOCK.
    repo_root = Path(__file__).resolve().parents[2]
    if not (repo_root / "testing" / "config.json").exists():
        pytest.skip("repo's testing/config.json not present in this checkout")
    if not (repo_root / "testing" / "mock").exists():
        pytest.skip("repo's testing/mock not present in this checkout")

    resp = client.post("/api/testing", json={"enabled": True})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["ok"] is True
    assert body["testing"] is True
    assert body["config_path"].endswith("config.json")

    # GET reflects the new state.
    assert client.get("/api/testing").json()["testing"] is True

    # Toggle off.
    resp = client.post("/api/testing", json={"enabled": False})
    assert resp.status_code == 200
    assert resp.json()["testing"] is False
    assert client.get("/api/testing").json()["testing"] is False
