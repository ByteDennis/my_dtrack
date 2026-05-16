"""Shared fixtures for the FastAPI route tests in tests/api/.

These tests exercise the HTTP layer end-to-end through Starlette's
TestClient. The non-trivial routes (`/api/extract`, `/api/playground/run`,
`/api/compare/*`, etc.) ultimately reach out to Oracle / Athena / Hadoop /
SAS / CSV backends, so they only work on a host where those connections
are configured via a `.env` file (the same one the /playground page reads).

Until the Linux dev box has that wiring, the whole `tests/api/` package is
skipped on non-Windows hosts -- the Windows dev box is where the .env lives
and where these tests are meant to run.

The fixtures below build a fresh per-test sandbox: a temp directory with an
empty dtrack.db (initialised via dtrack.db.init_database) and a minimal
dtrack.json, then patch the four module-level globals that `serve()`
normally sets (_db_path, _db_dir, _config_path, _original_config_path,
_testing_mode) so each test starts with a clean slate. After the test the
originals are restored so the suite is order-independent.
"""

from __future__ import annotations

import json
import platform
import shutil
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Skip the entire tests/api/ tree on non-Windows hosts.
# Implemented in pytest_collection_modifyitems (not a module-level
# `pytestmark`) so it applies to every test file the collector picks up
# in this directory without each file having to opt in.
# ---------------------------------------------------------------------------

_NOT_WINDOWS_REASON = (
    "tests/api/ needs live backend connections "
    "(oracle / athena / hadoop / sas / csv) configured via .env -- "
    "only available on the Windows dev box. Skipping on this host."
)


def pytest_collection_modifyitems(config, items):  # noqa: D401
    """Skip every test that lives under this directory on non-Windows hosts.

    `pytest_collection_modifyitems` is a global hook even when defined in
    a subdirectory conftest -- pytest invokes it once with the union of
    items across the run. We therefore scope by `item.path` so this
    conftest only affects tests inside tests/api/.
    """
    if platform.system() == "Windows":
        return
    skip_marker = pytest.mark.skip(reason=_NOT_WINDOWS_REASON)
    here = Path(__file__).resolve().parent
    for item in items:
        try:
            item_path = Path(item.path).resolve()
        except (AttributeError, OSError):
            continue
        try:
            item_path.relative_to(here)
        except ValueError:
            continue
        item.add_marker(skip_marker)


# ---------------------------------------------------------------------------
# Sandbox fixtures
# ---------------------------------------------------------------------------

_MINIMAL_CONFIG = {
    "metadata": {"title": "api-test"},
    "settings": {
        "sas_outdir": "./sas/",
        "aws_outdir": "./csv/",
        "from_date": "",
        "to_date": "",
    },
    "pairs": {},
}


def _seed_pairs(cfg: dict) -> dict:
    """Add a minimal oracle-vs-aws pair so endpoints that scan pairs find
    something to chew on (without actually hitting a backend)."""
    cfg = json.loads(json.dumps(cfg))  # deep copy
    cfg["pairs"]["t_pair"] = {
        "description": "fixture pair for api tests",
        "left": {
            "name": "t_left",
            "table": "T_LEFT",
            "source": "oracle",
            "conn_macro": "pb23",
            "date_col": "RPT_DT",
            "date_type": "date",
        },
        "right": {
            "name": "t_right",
            "table": "t_right",
            "source": "aws",
            "conn_macro": "analytics_db",
            "date_col": "rpt_dt",
            "date_type": "string_dash",
        },
        "mode": "incremental",
        "fromDate": "",
        "toDate": "",
    }
    return cfg


@pytest.fixture
def api_workdir(tmp_path: Path) -> dict:
    """Per-test sandbox: temp dtrack.db + dtrack.json + outdirs."""
    db_path = tmp_path / "dtrack.db"
    config_path = tmp_path / "dtrack.json"

    (tmp_path / "sas").mkdir()
    (tmp_path / "csv").mkdir()
    (tmp_path / "output").mkdir()

    config_path.write_text(json.dumps(_seed_pairs(_MINIMAL_CONFIG), indent=2))

    # init_database lives in dtrack.db and is the same call serve() makes
    # when the file doesn't exist.
    from dtrack.db import init_database
    init_database(str(db_path))

    return {
        "dir": tmp_path,
        "db": str(db_path),
        "config": str(config_path),
        "sas_outdir": str(tmp_path / "sas"),
        "aws_outdir": str(tmp_path / "csv"),
        "output_dir": str(tmp_path / "output"),
    }


@pytest.fixture
def app_bound(api_workdir: dict, monkeypatch):
    """Patch the dtrack.web.app module globals that `serve()` normally sets
    so the FastAPI app reads from our sandbox. Yields the app module so
    tests can poke at its state directly when needed.
    """
    from dtrack.web import app as app_mod

    monkeypatch.setattr(app_mod, "_db_path", api_workdir["db"], raising=False)
    monkeypatch.setattr(app_mod, "_db_dir", str(api_workdir["dir"]), raising=False)
    monkeypatch.setattr(app_mod, "_config_path", api_workdir["config"], raising=False)
    monkeypatch.setattr(app_mod, "_original_config_path", api_workdir["config"], raising=False)
    monkeypatch.setattr(app_mod, "_testing_mode", False, raising=False)
    yield app_mod


@pytest.fixture
def client(app_bound):
    """Starlette TestClient bound to the sandboxed app."""
    from fastapi.testclient import TestClient
    with TestClient(app_bound.app) as c:
        yield c


# ---------------------------------------------------------------------------
# Small helpers exposed as fixtures for the tests that need them.
# ---------------------------------------------------------------------------

@pytest.fixture
def write_extract_sql(api_workdir):
    """Drop a tiny extract_{type}.sql into the requested outdir so the
    dry-run / run-sql endpoints have something to parse without needing
    /api/generate to actually run.

    Returns a callable: write_extract_sql(side='left', type='col',
                                          name='t_pair__col', conn='pb23',
                                          sql='SELECT 1 FROM dual')
    """
    def _write(side: str = "left", type: str = "col",
               name: str = "t_pair__col", conn: str = "pb23",
               sql: str = "SELECT 1 AS one FROM dual") -> str:
        # Block format matches dtrack.platforms.base.parse_extract_sql_file:
        #   -- {block_name}
        #   /* @conn_macro=X */
        #   <SQL>
        #   ;
        outdir = api_workdir["sas_outdir"] if side == "left" else api_workdir["aws_outdir"]
        path = Path(outdir) / f"extract_{type}.sql"
        block = (
            f"-- {name}\n"
            f"/* @conn_macro={conn} */\n"
            f"{sql}\n"
            f";\n"
        )
        path.write_text(block)
        return str(path)
    return _write
