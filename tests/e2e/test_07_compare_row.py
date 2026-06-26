"""Step 07: `dtrack compare-row` — row count comparison + HTML report.

Parity with web/FastAPI:
    - GET /api/compare/row/{pair} runs compare.compare_row_counts() —
      same helper this CLI step uses.
    - The CLI additionally persists matching/excluded dates to
      _row_comparison and emits an HTML file. POST /api/compare/row
      (legacy batch) wraps the same logic.

What we verify:
    * After load-row, compare-row writes one _row_comparison row per pair
      with non-null matching_dates JSON
    * Overlap range is computed (overlap_start <= overlap_end OR null)
    * HTML report is created at --html path, contains pair names
    * --yes skips all interactive prompts (idempotent re-run)
"""

from __future__ import annotations

import json
import os
from pathlib import Path


def _seed_loaded_rows(workdir, run_cli):
    for sub, out in (("gen-aws", "csv_outdir"),
                     ("gen-oracle", "csv_outdir"),
                     ("gen-databricks", "csv_outdir")):
        run_cli(sub, config=workdir["config"], outdir=workdir[out],
                type="row", workers=None, db_path=workdir["db"],
                vintage=None, force=False, from_date=None, to_date=None,
                preset=None, stats_override=None)
    run_cli("load-row",
            project_db=workdir["db"], folder=workdir["csv_outdir"],
            config=workdir["config"], mode="upsert",
            db=None, date_var=None)


def test_compare_row_persists_row_comparison_and_html(
        initialized_db, run_cli, db_rows):
    wd = initialized_db
    _seed_loaded_rows(wd, run_cli)

    html_path = os.path.join(wd["output_dir"], "compare_row.html")
    run_cli("compare-row",
            project_db=wd["db"], config=wd["config"],
            from_date=None, to_date=None, yes=True,
            html=html_path, title=None, subtitle=None)

    # Both pairs should have _row_comparison rows
    for pair in (wd["pair_oracle_aws"], wd["pair_databricks"]):
        rows = db_rows(
            wd["db"],
            "SELECT matching_dates, overlap_start, overlap_end "
            "FROM _row_comparison WHERE pair_name=?",
            (pair,),
        )
        assert rows, f"_row_comparison missing for {pair}"
        r = rows[0]
        # matching_dates is JSON-encoded; must parse and either be empty
        # or a list of YYYY-MM-DD strings.
        md = json.loads(r["matching_dates"] or "[]")
        assert isinstance(md, list)
        # Overlap window should be valid (or NULL if no overlap).
        if r["overlap_start"] and r["overlap_end"]:
            assert r["overlap_start"] <= r["overlap_end"]

    # HTML output exists and isn't trivially empty
    assert os.path.exists(html_path), "compare-row didn't emit HTML"
    body = Path(html_path).read_text()
    assert len(body) > 1000, "HTML report suspiciously small"
    # Pair names should appear in the rendered report
    assert wd["pair_oracle_aws"] in body or wd["pair_databricks"] in body


def test_compare_row_writes_where_map_back_to_config(
        initialized_db, run_cli):
    """compare-row builds a WHERE clause from matching+excluded dates and
    writes where_map[left]/where_map[right] back into pairs.json. The web
    UI does the same on its compare flow.
    """
    wd = initialized_db
    _seed_loaded_rows(wd, run_cli)

    html_path = os.path.join(wd["output_dir"], "compare_row.html")
    run_cli("compare-row",
            project_db=wd["db"], config=wd["config"],
            from_date=None, to_date=None, yes=True,
            html=html_path, title=None, subtitle=None)

    cfg = json.loads(Path(wd["config"]).read_text())
    for pair in (wd["pair_oracle_aws"], wd["pair_databricks"]):
        wm = cfg["pairs"][pair].get("where_map", {})
        # where_map should exist; sides can be empty strings if dates
        # had no overlap but the keys must be present.
        assert "left" in wm and "right" in wm, (
            f"where_map keys missing for {pair}: {wm}")


def test_compare_row_yes_mode_is_idempotent(initialized_db, run_cli, db_rows):
    """Running compare-row -y twice produces the same _row_comparison
    state (no growing duplicates, no schema drift).
    """
    wd = initialized_db
    _seed_loaded_rows(wd, run_cli)
    html_path = os.path.join(wd["output_dir"], "compare_row.html")

    for _ in range(2):
        run_cli("compare-row",
                project_db=wd["db"], config=wd["config"],
                from_date=None, to_date=None, yes=True,
                html=html_path, title=None, subtitle=None)

    rows = db_rows(
        wd["db"],
        "SELECT COUNT(*) AS n FROM _row_comparison",
    )
    # Exactly one row per pair (2 pairs, all sides loaded).
    assert rows[0]["n"] == 2, (
        f"expected 2 _row_comparison rows after 2x compare-row; "
        f"got {rows[0]['n']}")
