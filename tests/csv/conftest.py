"""Fixtures for the CSV-source reconciliation suite.

Unlike tests/api/ and tests/e2e/ (which need live Oracle/Athena/Hadoop/SAS
backends and are skipped off the Windows dev box), everything here runs on
ANY host: a `source=csv` pair is powered by in-process DuckDB, so the full
generate → run → load → compare pipeline executes with zero external engines.
That's the whole point — CSV↔CSV is the engine-free simulation surface.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest


@pytest.fixture
def sandbox(tmp_path: Path) -> dict:
    """Per-test workspace: initialised sqlite db + an extraction outdir."""
    from dtrack.db import init_database

    db_path = tmp_path / "project.db"
    init_database(str(db_path))
    out = tmp_path / "out"
    out.mkdir()
    left = tmp_path / "left_out"
    left.mkdir()
    right = tmp_path / "right_out"
    right.mkdir()
    return {
        "dir": tmp_path,
        "db": str(db_path),
        "out": str(out),
        "left_out": str(left),
        "right_out": str(right),
    }


# ---------------------------------------------------------------------------
# FastAPI client bound to a sandbox (mirrors tests/api/conftest, minus the
# Windows-only skip — csv endpoints don't need a backend).
# ---------------------------------------------------------------------------

@pytest.fixture
def app_client(tmp_path: Path, monkeypatch):
    """Yield (client, ctx) where ctx has db/config/outdir paths.

    The config starts empty; tests that exercise /api/generate write their own
    csv pair config to ctx['config'] first.
    """
    from dtrack.db import init_database
    from dtrack.web import app as app_mod
    from fastapi.testclient import TestClient

    db_path = tmp_path / "dtrack.db"
    config_path = tmp_path / "dtrack.json"
    sas_out = tmp_path / "sas"
    aws_out = tmp_path / "csv"
    for d in (sas_out, aws_out):
        d.mkdir()
    config_path.write_text(json.dumps(
        {"metadata": {}, "settings": {}, "pairs": {}}, indent=2))
    init_database(str(db_path))

    monkeypatch.setattr(app_mod, "_db_path", str(db_path), raising=False)
    monkeypatch.setattr(app_mod, "_db_dir", str(tmp_path), raising=False)
    monkeypatch.setattr(app_mod, "_config_path", str(config_path), raising=False)
    monkeypatch.setattr(app_mod, "_original_config_path", str(config_path), raising=False)
    monkeypatch.setattr(app_mod, "_testing_mode", False, raising=False)

    ctx = {
        "dir": tmp_path,
        "db": str(db_path),
        "config": str(config_path),
        "sas_outdir": str(sas_out),
        "aws_outdir": str(aws_out),
    }
    with TestClient(app_mod.app) as client:
        yield client, ctx
