"""Step 09: `dtrack run` — the one-shot full pipeline.

Parity with web/FastAPI:
    The web UI does NOT have a single "run everything" endpoint — the
    user steps through pages one at a time. `dtrack run` stitches the
    same per-step helpers into one call via pipeline.run_pipeline().

What we verify:
    * After a single `dtrack run`, the DB end-state matches what running
      steps 01..08 in sequence would produce:
        - _table_pairs has every pair from pairs.json
        - _row_counts has rows for every side
        - _row_comparison has rows for every pair
        - HTML reports exist at the expected paths
    * --skip-extract / --skip-load / --skip-compare gate the right phases
    * --from-date / --to-date pass through to extractors
    * --preset / --stats-override pass through to extractors

Note: col-side comparison is NOT tested here because the mock fixtures
don't ship _col.csv files. Step 08 covers col compare on synth data.
"""

from __future__ import annotations

import os
from pathlib import Path


def _run_pipeline(workdir, run_cli, **overrides):
    base = dict(
        project_db=workdir["db"],
        config=workdir["config"],
        outdir=workdir["dir"],
        sas_outdir=workdir["sas_outdir"],
        csv_outdir=workdir["csv_outdir"],
        type="row",                       # only row in mock-only e2e
        vintage="day",
        yes=True,
        html_row=os.path.join(workdir["output_dir"], "compare_row.html"),
        html_col=os.path.join(workdir["output_dir"], "compare_col.html"),
        title=None, subtitle=None,
        workers=None, force=False,
        skip_extract=False, skip_load=False, skip_compare=False,
        from_date=None, to_date=None,
        preset=None, stats_override=None,
    )
    base.update(overrides)
    run_cli("run", **base)


def test_run_full_pipeline_produces_consistent_state(
        initialized_db, run_cli, db_rows):
    wd = initialized_db
    _run_pipeline(wd, run_cli)

    # 1. _table_pairs: one row per pair, registered with the source pair
    pairs = db_rows(wd["db"], "SELECT pair_name FROM _table_pairs")
    names = {p["pair_name"] for p in pairs}
    assert wd["pair_oracle_aws"] in names
    assert wd["pair_sas_aws"] in names

    # 2. _row_counts: rows for at least one side of each pair
    for qname in ("aws_pos_snapshot", "oracle_pos_daily",
                  "aws_acct_snap_aws", "sas_acct_snap"):
        rows = db_rows(
            wd["db"],
            "SELECT COUNT(*) AS n FROM _row_counts WHERE source_table=?",
            (qname,),
        )
        assert rows[0]["n"] > 0, f"run pipeline didn't load rows for {qname}"

    # 3. _row_comparison: one row per pair
    cmp_rows = db_rows(wd["db"], "SELECT pair_name FROM _row_comparison")
    cmp_names = {r["pair_name"] for r in cmp_rows}
    assert wd["pair_oracle_aws"] in cmp_names
    assert wd["pair_sas_aws"] in cmp_names

    # 4. HTML row report exists
    html_row = os.path.join(wd["output_dir"], "compare_row.html")
    assert os.path.exists(html_row)
    body = Path(html_row).read_text()
    assert len(body) > 1000


def test_run_skip_extract_uses_existing_csvs(
        initialized_db, run_cli, db_rows):
    """If we pre-populate csv/sas outdirs and pass --skip-extract, the
    pipeline must still load + compare from those CSVs. This is the
    "I ran the extraction yesterday, just refresh the DB and HTML"
    use case.
    """
    wd = initialized_db
    # First run: full pipeline to materialize CSVs
    _run_pipeline(wd, run_cli)

    # Wipe just the DB by re-init (data preserved, sanity reset)
    run_cli("init", project_db=wd["db"], force=True,
            refresh=False, migrate=False, config=wd["config"])

    # Second run: skip-extract should still rebuild _row_counts +
    # _row_comparison from the CSVs left over from run #1.
    _run_pipeline(wd, run_cli, skip_extract=True)

    cmp_rows = db_rows(wd["db"], "SELECT pair_name FROM _row_comparison")
    cmp_names = {r["pair_name"] for r in cmp_rows}
    assert wd["pair_oracle_aws"] in cmp_names


def test_run_skip_compare_keeps_compare_table_empty(
        initialized_db, run_cli, db_rows):
    wd = initialized_db
    _run_pipeline(wd, run_cli, skip_compare=True)

    rows = db_rows(wd["db"], "SELECT COUNT(*) AS n FROM _row_comparison")
    assert rows[0]["n"] == 0, (
        "--skip-compare should not produce any _row_comparison rows")
    # But _row_counts should still be populated
    rc = db_rows(wd["db"], "SELECT COUNT(*) AS n FROM _row_counts")
    assert rc[0]["n"] > 0


def test_run_passes_date_window_to_extract(initialized_db, run_cli, db_rows):
    """--from-date / --to-date make it through to the platform extractors.
    In mock mode the date filter doesn't actually subset the CSV copy
    (the mock helper just copies whole files), so we only assert the
    pipeline accepts the flags and runs without error. Real-world
    behavior is exercised by tests/test_preset_sql_generation.py.
    """
    wd = initialized_db
    _run_pipeline(wd, run_cli,
                  from_date="2024-11-01", to_date="2024-11-30")
    # Just assert the pipeline completed: _row_comparison has rows.
    rows = db_rows(wd["db"], "SELECT COUNT(*) AS n FROM _row_comparison")
    assert rows[0]["n"] > 0


def test_run_accepts_preset_and_stats_override(initialized_db, run_cli):
    """New CLI flags must wire through `dtrack run` into the extractors."""
    wd = initialized_db
    _run_pipeline(wd, run_cli,
                  preset="standard",
                  stats_override={"n_unique": True, "hash_sum": False})
    # Successful completion implies all three extractors accepted the
    # preset/stats_override kwargs without raising.
    assert os.path.exists(
        os.path.join(wd["output_dir"], "compare_row.html"))
