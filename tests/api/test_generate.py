"""Tests for /api/generate -- SQL file generation.

Generation itself doesn't query backends; it just writes extract_*.sql
files to the configured outdirs. These tests are the
cheapest API-level checks of the most complex endpoint.
"""

import os


def test_generate_row_writes_outputs(client, api_workdir):
    resp = client.post("/api/generate", json={
        "type": "row",
        "sas_outdir": api_workdir["sas_outdir"],
        "aws_outdir": api_workdir["aws_outdir"],
        "from_date": "2024-01-01",
        "to_date": "2024-12-31",
        "pair_names": ["t_pair"],
    })
    assert resp.status_code == 200, resp.text
    body = resp.json()
    # Result schema (per the endpoint): {"ok": True, "results": {...}, "output": "..."}
    # We don't pin the exact keys (they've been refactored before) -- the
    # important contract is that something landed in the outdirs.
    assert body.get("ok", True) is True

    sas_files = os.listdir(api_workdir["sas_outdir"])
    aws_files = os.listdir(api_workdir["aws_outdir"])
    written = sas_files + aws_files
    assert any(f.startswith("extract_row") for f in written), (
        f"no extract_row.* files produced. sas={sas_files} aws={aws_files}"
    )


def test_generate_col_quick_preset_writes_sql(client, api_workdir):
    resp = client.post("/api/generate", json={
        "type": "col",
        "preset": "quick",
        "sas_outdir": api_workdir["sas_outdir"],
        "aws_outdir": api_workdir["aws_outdir"],
        "from_date": "2024-01-01",
        "to_date": "2024-12-31",
        "pair_names": ["t_pair"],
    })
    assert resp.status_code == 200, resp.text

    sas_files = os.listdir(api_workdir["sas_outdir"])
    aws_files = os.listdir(api_workdir["aws_outdir"])
    written = sas_files + aws_files
    assert any(f.startswith("extract_col") for f in written), (
        f"no extract_col.* files produced. sas={sas_files} aws={aws_files}"
    )


def test_generate_rejects_unknown_preset(client, api_workdir):
    resp = client.post("/api/generate", json={
        "type": "col",
        "preset": "ultra_super_detailed",
        "sas_outdir": api_workdir["sas_outdir"],
        "aws_outdir": api_workdir["aws_outdir"],
        "pair_names": ["t_pair"],
    })
    assert resp.status_code == 400
    assert "preset" in resp.json()["error"].lower()


def test_generate_rejects_unknown_level(client, api_workdir):
    resp = client.post("/api/generate", json={
        "type": "col",
        "level": "extra_fine",
        "sas_outdir": api_workdir["sas_outdir"],
        "aws_outdir": api_workdir["aws_outdir"],
        "pair_names": ["t_pair"],
    })
    assert resp.status_code == 400
    assert "level" in resp.json()["error"].lower()
