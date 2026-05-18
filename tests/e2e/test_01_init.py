"""Step 01: `dtrack init` — database initialization.

Parity with web/FastAPI: POST /api/init creates the same schema via
the same db.init_database() helper. After this step the project DB
must contain every table downstream commands expect.
"""

from __future__ import annotations

import os


# Schema tables every other CLI step depends on. If any of these are
# missing, the rest of the pipeline silently produces wrong results
# (load-* will UPSERT into nothing, compare-* will return empty sets).
REQUIRED_TABLES = {
    "_metadata",
    "_row_counts",
    "_col_stats",
    "_column_meta",
    "_table_pairs",
    "_row_comparison",
    "_col_comparison",
}


def _list_tables(db_path: str) -> set[str]:
    import sqlite3
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
        return {row[0] for row in cur.fetchall()}
    finally:
        conn.close()


def test_init_creates_db_with_full_schema(workdir, run_cli):
    """`dtrack init` produces a file with every required schema table.

    Parity contract with POST /api/init: same call into db.init_database,
    same set of tables, same column shapes.
    """
    assert not os.path.exists(workdir["db"])
    run_cli("init", project_db=workdir["db"], force=False,
            refresh=False, migrate=False, config=workdir["config"])
    assert os.path.exists(workdir["db"])
    tables = _list_tables(workdir["db"])
    missing = REQUIRED_TABLES - tables
    assert not missing, f"init dropped required tables: {missing}"


def test_init_refuses_to_clobber_without_force(workdir, run_cli):
    """First init writes the file; second init without --force aborts via
    sys.exit(1). Catch SystemExit to assert the safety net is wired up.
    """
    import pytest

    run_cli("init", project_db=workdir["db"], force=False,
            refresh=False, migrate=False, config=workdir["config"])
    with pytest.raises(SystemExit) as exc:
        run_cli("init", project_db=workdir["db"], force=False,
                refresh=False, migrate=False, config=workdir["config"])
    assert exc.value.code == 1


def test_init_with_force_replaces_existing_db(workdir, run_cli):
    """--force lets a re-init drop and recreate the file."""
    run_cli("init", project_db=workdir["db"], force=False,
            refresh=False, migrate=False, config=workdir["config"])
    mtime1 = os.path.getmtime(workdir["db"])

    # Force-recreate. New file → mtime moves forward.
    import time; time.sleep(1.01)
    run_cli("init", project_db=workdir["db"], force=True,
            refresh=False, migrate=False, config=workdir["config"])
    mtime2 = os.path.getmtime(workdir["db"])
    assert mtime2 > mtime1
    assert REQUIRED_TABLES.issubset(_list_tables(workdir["db"]))


def test_init_migrate_preserves_existing_data(workdir, run_cli, db_rows):
    """`--migrate` is a data-preserving schema reconciliation. After
    insert + migrate, the inserted row must still be present. This is the
    contract the web UI's POST /api/init?refresh=1 relies on too.
    """
    import sqlite3

    run_cli("init", project_db=workdir["db"], force=False,
            refresh=False, migrate=False, config=workdir["config"])

    # Seed _row_counts with a row migrate should preserve.
    conn = sqlite3.connect(workdir["db"])
    try:
        conn.execute(
            "INSERT INTO _row_counts (source_table, dt, row_count) "
            "VALUES (?, ?, ?)",
            ("aws_pos_snapshot", "2024-10-01", 12345),
        )
        conn.commit()
    finally:
        conn.close()

    run_cli("init", project_db=workdir["db"], force=False,
            refresh=False, migrate=True, config=workdir["config"])

    rows = db_rows(
        workdir["db"],
        "SELECT row_count FROM _row_counts WHERE source_table=? AND dt=?",
        ("aws_pos_snapshot", "2024-10-01"),
    )
    assert rows == [{"row_count": 12345}]


def test_init_seeds_starter_config_when_missing(tmp_path, run_cli):
    """If pairs.json doesn't exist next to the new DB, init writes a
    starter (empty pairs / metadata). This is the same scaffold the
    web UI sees when `dtrack serve --config` is omitted.
    """
    db = tmp_path / "p.db"
    cfg = tmp_path / "pairs.json"
    assert not cfg.exists()

    run_cli("init", project_db=str(db), force=False,
            refresh=False, migrate=False, config=str(cfg))
    assert cfg.exists()
    import json
    parsed = json.loads(cfg.read_text())
    assert "pairs" in parsed and parsed["pairs"] == {}
    assert "metadata" in parsed
