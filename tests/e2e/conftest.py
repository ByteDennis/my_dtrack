"""End-to-end CLI workflow tests for dtrack.

One test_{NN}_{step}.py per ONE-SHOT step in manuscript.txt section 4:
    01 init           02 load-columns    03 match-columns
    04 gen-{aws,oracle,sas}                05 load-row
    06 load-col       07 compare-row     08 compare-col
    09 run            (full pipeline)

Each test exercises the actual `dtrack.cli.cmd_*` handler (NOT subprocess)
against the mock fixtures in `testing/mock/`, then asserts the DB / output
state matches what the equivalent FastAPI route would have produced. This
gives confidence the CLI is a first-class peer to the web UI.

WINDOWS-ONLY: same gate as tests/api/conftest.py -- the full mock pipeline
depends on .env wiring (SAS_LIB, AWS_DEFAULT_REGION, ORACLE_MAX_WORKERS,
etc.) that's only set up on the Windows dev box. On any other host the
whole tests/e2e/ tree is collected but skipped.
"""

from __future__ import annotations

import json
import os
import platform
import shutil
from argparse import Namespace
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Skip the entire tests/e2e/ tree on non-Windows hosts
# (mirrors tests/api/conftest.py exactly).
# ---------------------------------------------------------------------------
_NOT_WINDOWS_REASON = (
    "tests/e2e/ exercises the full CLI pipeline via DTRACK_MOCK fixtures; "
    "supporting env wiring (oracle / athena / hadoop / sas) only lives on "
    "the Windows dev box. Skipping on this host."
)


def pytest_collection_modifyitems(config, items):
    """Skip every test that lives under tests/e2e/ on non-Windows hosts.

    Scoped by `item.path` so this conftest only affects tests inside
    tests/e2e/ even though `pytest_collection_modifyitems` is a global hook.
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
# Sandbox + mock-fixture wiring
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[2]
TESTING_DIR = REPO_ROOT / "testing"
MOCK_DIR = TESTING_DIR / "mock"
TESTING_CONFIG = TESTING_DIR / "config.json"


# >>> minimal pairs subset that exercises each platform path in mock mode <<< #
_E2E_PAIRS = {
    "oracle_date_vs_aws_int": {
        # oracle source (extract_oracle), aws right side (extract_aws). Both
        # mock CSVs present in testing/mock as
        #   oracle_pos_daily_row.csv  +  aws_pos_snapshot_row.csv.
        "description": "e2e: oracle DATE vs AWS YYYYMMDD int",
        "left": {
            "name": "pos_daily", "table": "POS_DAILY",
            "source": "oracle", "conn_macro": "pb23",
            "date_col": "POS_DT", "date_type": "date",
        },
        "right": {
            "name": "pos_snapshot", "table": "pos_snapshot",
            "source": "aws", "conn_macro": "analytics_db",
            "date_col": "snapshot_dt", "date_type": "int_compact",
        },
        "mode": "incremental",
        "fromDate": "",
        "toDate": "",
    },
    "aws_vs_databricks": {
        "description": "e2e: AWS Athena YYYYMMDD vs Databricks YYYYMMDD",
        "left": {
            "name": "month_summary", "table": "month_summary",
            "source": "aws", "conn_macro": "analytics_db",
            "date_col": "month_dt", "date_type": "num_yyyymm",
        },
        "right": {
            "name": "pos_daily", "table": "pos_daily",
            "source": "databricks",
            "conn_macro": "main.analytics",
            "date_col": "snapshot_dt", "date_type": "int_compact",
        },
        "mode": "incremental",
        "fromDate": "",
        "toDate": "",
    },
    "sas_dt_vs_aws_compact": {
        # sas source (gen_sas), aws right side. Mock CSVs:
        #   sas_acct_snap_row.csv  +  aws_acct_snap_aws_row.csv.
        "description": "e2e: sas datetime vs AWS compact",
        "left": {
            "name": "acct_snap", "table": "ACCT_SNAP",
            "source": "sas", "conn_macro": "",
            "date_col": "ACCT_DTTM", "date_type": "sas_datetime",
        },
        "right": {
            "name": "acct_snap_aws", "table": "acct_snap",
            "source": "aws", "conn_macro": "analytics_db",
            "date_col": "snap_dt", "date_type": "string_compact",
        },
        "mode": "incremental",
        "fromDate": "",
        "toDate": "",
    },
}


def _write_config(path: Path):
    cfg = {
        "metadata": {"title": "e2e", "subtitle": "CLI pipeline tests"},
        "settings": {
            "sas_outdir": "./sas/",
            "aws_outdir": "./csv/",
            "from_date": "",
            "to_date": "",
        },
        "pairs": _E2E_PAIRS,
    }
    path.write_text(json.dumps(cfg, indent=2))


@pytest.fixture
def workdir(tmp_path: Path, monkeypatch) -> dict:
    """Per-test sandbox.

    Layout:
        tmp_path/
            project.db          (created by test_01_init or pre-seeded)
            pairs.json          (the _E2E_PAIRS subset above)
            csv/                (aws/oracle extract output)
            sas/                (sas extract output)
            output/             (html reports)
            mock/               (copy of testing/mock so the test can
                                 mutate / add new fixtures without touching
                                 the repo)

    DTRACK_MOCK is set so extract_aws / extract_oracle / gen_sas copy
    fixture CSVs instead of hitting backends. Restored by monkeypatch.
    """
    db_path = tmp_path / "project.db"
    config_path = tmp_path / "pairs.json"
    csv_outdir = tmp_path / "csv"
    sas_outdir = tmp_path / "sas"
    output_dir = tmp_path / "output"
    mock_local = tmp_path / "mock"

    for d in (csv_outdir, sas_outdir, output_dir):
        d.mkdir()

    if MOCK_DIR.exists():
        shutil.copytree(MOCK_DIR, mock_local)
    else:  # pragma: no cover - sanity guard
        mock_local.mkdir()

    _write_config(config_path)

    monkeypatch.setenv("DTRACK_MOCK", str(mock_local))
    # Strip any user env that might steer the platform code off the mock
    # path (e.g. an installer test rig that set DTRACK_ORACLE_MOCK on the
    # host shell to a different dir).
    for v in ("DTRACK_ORACLE_MOCK", "DTRACK_ATHENA_MOCK",
              "DTRACK_CSV_MOCK", "DTRACK_HADOOP_MOCK"):
        monkeypatch.delenv(v, raising=False)

    return {
        "dir": tmp_path,
        "db": str(db_path),
        "config": str(config_path),
        "csv_outdir": str(csv_outdir),
        "sas_outdir": str(sas_outdir),
        "output_dir": str(output_dir),
        "mock": str(mock_local),
        "pair_oracle_aws": "oracle_date_vs_aws_int",
        "pair_sas_aws": "sas_dt_vs_aws_compact",
        "pair_databricks": "aws_vs_databricks",
    }


# ---------------------------------------------------------------------------
# CLI invocation helper
# ---------------------------------------------------------------------------

@pytest.fixture
def run_cli():
    """Call a CLI handler directly with an argparse.Namespace.

    Usage:
        run_cli("init", project_db=workdir["db"], force=False,
                refresh=False, migrate=False, config=workdir["config"])

    Every argparse default that the live `argparse` parser would inject
    must be passed explicitly. The handlers use `getattr(args, "foo", X)`
    in most places, so missing keys are tolerated -- but be explicit
    where the handler reads via `args.foo` directly.
    """
    from dtrack import cli as cli_mod

    def _run(subcommand: str, **kw):
        handler_name = f"cmd_{subcommand.replace('-', '_')}"
        handler = getattr(cli_mod, handler_name)
        handler(Namespace(**kw))

    return _run


@pytest.fixture
def db_rows():
    """Helper to read raw rows from a project_db table."""
    import sqlite3

    def _q(db_path: str, sql: str, params=()):
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        try:
            cur = conn.cursor()
            cur.execute(sql, params)
            return [dict(r) for r in cur.fetchall()]
        finally:
            conn.close()

    return _q


@pytest.fixture
def initialized_db(workdir, run_cli):
    """Sandbox with `dtrack init` already run. Used by tests that need the
    DB to exist but don't care about exercising init themselves.
    """
    run_cli("init", project_db=workdir["db"], force=False,
            refresh=False, migrate=False, config=workdir["config"])
    return workdir


@pytest.fixture
def seeded_columns(initialized_db, run_cli, db_rows):
    """initialized_db + `dtrack load-columns` via a per-side CSV. Gives
    the rest of the pipeline a populated _column_meta to chew on.
    """
    import csv

    wd = initialized_db
    # One small metadata CSV per side (5 columns: id, dt, amount, name, code)
    meta_path = Path(wd["dir"]) / "columns_meta.csv"
    # Use lowercase column names so they auto-match across left/right and
    # match-columns runs without prompting.
    rows = [
        # ORACLE side
        {"source": "oracle", "table": "POS_DAILY",
         "column_name": "id", "data_type": "NUMBER"},
        {"source": "oracle", "table": "POS_DAILY",
         "column_name": "pos_dt", "data_type": "DATE"},
        {"source": "oracle", "table": "POS_DAILY",
         "column_name": "amount", "data_type": "NUMBER"},
        {"source": "oracle", "table": "POS_DAILY",
         "column_name": "name", "data_type": "VARCHAR2"},
        {"source": "oracle", "table": "POS_DAILY",
         "column_name": "code", "data_type": "VARCHAR2"},
        # AWS side
        {"source": "aws", "table": "pos_snapshot",
         "column_name": "id", "data_type": "bigint"},
        {"source": "aws", "table": "pos_snapshot",
         "column_name": "snapshot_dt", "data_type": "int"},
        {"source": "aws", "table": "pos_snapshot",
         "column_name": "amount", "data_type": "double"},
        {"source": "aws", "table": "pos_snapshot",
         "column_name": "name", "data_type": "string"},
        {"source": "aws", "table": "pos_snapshot",
         "column_name": "code", "data_type": "string"},
    ]
    with open(meta_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["source", "table",
                                          "column_name", "data_type"])
        w.writeheader()
        w.writerows(rows)

    run_cli("load-columns", project_db=wd["db"], config=wd["config"],
            csv_files=[str(meta_path)])
    return wd
