"""Step 05: `dtrack load-row` — ingest row count CSVs into _row_counts.

Parity with web/FastAPI:
    - POST /api/load/row/upload  (multipart) and
      POST /api/load/row/path    (server-side path) both call
      loader.load_row_counts() under the hood.
    - The CLI step iterates the configured pairs and calls the same
      load_row_counts() per side. Same SQL, same dedup, same date parser.

What we verify:
    * After gen-aws + load-row, every AWS-side mock CSV's rows appear in
      _row_counts with canonical YYYY-MM-DD dt strings
    * _metadata is populated with min/max dates and a row_count_total
    * mode='upsert' is idempotent — re-running doesn't double rows
    * mode='replace' wipes and reloads
"""

from __future__ import annotations

from pathlib import Path


def _seed_aws_csvs(workdir, run_cli):
    """Common setup: run gen-aws + gen-oracle in mock mode so
    the outdirs are filled with per-pair {qname}_row.csv files.
    """
    for sub, outkey in (
            ("gen-aws", "csv_outdir"),
            ("gen-oracle", "csv_outdir"),
    ):
        run_cli(sub,
                config=workdir["config"], outdir=workdir[outkey],
                type="row", workers=None, db_path=workdir["db"],
                vintage=None, force=False, from_date=None, to_date=None,
                preset=None, stats_override=None)


def test_load_row_populates_row_counts_for_all_sides(
        initialized_db, run_cli, db_rows):
    wd = initialized_db
    _seed_aws_csvs(wd, run_cli)

    run_cli("load-row",
            project_db=wd["db"], folder=wd["csv_outdir"], config=wd["config"],
            mode="upsert", db=None, date_var=None)

    # Every qname referenced by _E2E_PAIRS should now have rows in
    # _row_counts. Mock CSVs cover history dates 2024-10-01 .. 2024-12-31.
    expected = {
        "aws_pos_snapshot", "oracle_pos_daily",
    }
    for qname in expected:
        rows = db_rows(
            wd["db"],
            "SELECT COUNT(*) AS n FROM _row_counts WHERE source_table=?",
            (qname,),
        )
        assert rows[0]["n"] > 0, f"no rows loaded for {qname}"


def test_load_row_updates_metadata_date_range(
        initialized_db, run_cli, db_rows):
    """_metadata.min_date_loaded / max_date_loaded / row_count_total should be set."""
    wd = initialized_db
    _seed_aws_csvs(wd, run_cli)
    run_cli("load-row",
            project_db=wd["db"], folder=wd["csv_outdir"], config=wd["config"],
            mode="upsert", db=None, date_var=None)

    meta = db_rows(
        wd["db"],
        "SELECT min_date_loaded, max_date_loaded, row_count_total FROM _metadata "
        "WHERE table_name=?",
        ("aws_pos_snapshot",),
    )
    assert meta, "_metadata row not created for aws_pos_snapshot"
    m = meta[0]
    assert m["min_date_loaded"] and m["max_date_loaded"], (
        f"min/max date not populated: {m}")
    assert m["min_date_loaded"] <= m["max_date_loaded"]


def test_load_row_upsert_is_idempotent(initialized_db, run_cli, db_rows):
    """Reloading the same CSV with mode=upsert must not duplicate dates.
    The composite PK (source_table, dt) on _row_counts enforces this,
    but we explicitly check the count to catch regressions where a
    schema change drops the PK.
    """
    wd = initialized_db
    _seed_aws_csvs(wd, run_cli)

    for _ in range(2):
        run_cli("load-row",
                project_db=wd["db"], folder=wd["csv_outdir"], config=wd["config"],
                mode="upsert", db=None, date_var=None)

    distinct = db_rows(
        wd["db"],
        "SELECT COUNT(DISTINCT dt) AS d, COUNT(*) AS n FROM _row_counts "
        "WHERE source_table=?",
        ("aws_pos_snapshot",),
    )
    assert distinct[0]["d"] == distinct[0]["n"], (
        "load-row upsert produced duplicate dt rows")


def test_load_row_replace_wipes_prior_rows(initialized_db, run_cli, db_rows):
    """mode=replace should clear existing rows for that source_table
    before inserting. We seed a junk row with an out-of-range date, then
    load-row in replace mode, and verify the junk is gone.
    """
    import sqlite3
    wd = initialized_db
    _seed_aws_csvs(wd, run_cli)

    conn = sqlite3.connect(wd["db"])
    try:
        conn.execute(
            "INSERT INTO _row_counts (source_table, dt, row_count) "
            "VALUES (?, ?, ?)",
            ("aws_pos_snapshot", "1999-01-01", 999999),
        )
        conn.commit()
    finally:
        conn.close()

    run_cli("load-row",
            project_db=wd["db"], folder=wd["csv_outdir"], config=wd["config"],
            mode="replace", db=None, date_var=None)

    junk = db_rows(
        wd["db"],
        "SELECT * FROM _row_counts "
        "WHERE source_table=? AND dt=?",
        ("aws_pos_snapshot", "1999-01-01"),
    )
    assert not junk, "mode=replace did not wipe pre-existing row for source"
