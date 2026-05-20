"""Step 10: `dtrack gen-databricks` — Databricks extraction via mock.

Parity with web/FastAPI: POST /api/extract with platform='databricks'
dispatches to extract_databricks, the same entry point this CLI step
calls. Same mock-mode path (DTRACK_MOCK / DTRACK_DATABRICKS_MOCK) the
web flow uses on dev boxes.

What we verify:
    * gen-databricks copies databricks_*_row.csv into csv_outdir
    * The fixture is keyed by qualified_name({source}_{name}) so the
      file must be databricks_pos_daily_row.csv for source=databricks,
      name=pos_daily. Confirms the pair shape works end-to-end.
    * After load-row + compare-row, _row_comparison has a row for the
      AWS-vs-Databricks pair (proves the source is treated as a peer to
      AWS through the comparison engine).
    * gen-databricks accepts the same --preset / --stats-override flags
      we added for the other gen-* commands.
"""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path


def _produced_csvs(outdir, suffix="row"):
    out = Path(outdir)
    if not out.exists():
        return set()
    return {p.name[:-len(f"_{suffix}.csv")]
            for p in out.iterdir() if p.name.endswith(f"_{suffix}.csv")}


def _ensure_aws_month_mock(workdir):
    """The e2e config's AWS-left side is `aws_month_summary`. The shipped
    mock dir already carries `aws_month_summary_row.csv`, but if a future
    refresh drops it this seed keeps the test self-contained.
    """
    mock = Path(workdir["mock"]) / "aws_month_summary_row.csv"
    if mock.exists():
        return
    with open(mock, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date_value", "row_count"])
        for ym in (202410, 202411, 202412):
            w.writerow([ym, 1000 + ym % 100])


def test_gen_databricks_copies_mock_row_csv(initialized_db, run_cli):
    """gen-databricks in mock mode copies databricks_pos_daily_row.csv
    into csv_outdir. AWS-left mock isn't touched (only databricks side).
    """
    wd = initialized_db
    _ensure_aws_month_mock(wd)

    run_cli("gen-databricks",
            config=wd["config"], outdir=wd["csv_outdir"],
            type="row", workers=None, db_path=wd["db"],
            vintage=None, force=False, from_date=None, to_date=None,
            preset=None, stats_override=None)

    produced = _produced_csvs(wd["csv_outdir"], "row")
    assert "databricks_pos_daily" in produced, (
        f"gen-databricks didn't produce databricks_pos_daily_row.csv: "
        f"{produced}")


def test_gen_databricks_accepts_preset_and_stats_override(
        initialized_db, run_cli):
    wd = initialized_db
    _ensure_aws_month_mock(wd)

    run_cli("gen-databricks",
            config=wd["config"], outdir=wd["csv_outdir"],
            type="row", workers=None, db_path=wd["db"],
            vintage=None, force=False, from_date=None, to_date=None,
            preset="standard",
            stats_override="n_unique=on,hash_sum=off")

    assert "databricks_pos_daily" in _produced_csvs(wd["csv_outdir"], "row")


def test_databricks_full_row_workflow(initialized_db, run_cli, db_rows):
    """gen-aws + gen-databricks → load-row → compare-row.

    Both sides land in _row_counts; compare-row writes _row_comparison
    for the AWS-vs-Databricks pair. Proves Databricks is treated as a
    first-class source through the full row-compare workflow.
    """
    wd = initialized_db
    _ensure_aws_month_mock(wd)

    run_cli("gen-aws",
            config=wd["config"], outdir=wd["csv_outdir"],
            type="row", workers=None, db_path=wd["db"],
            vintage=None, force=False, from_date=None, to_date=None,
            preset=None, stats_override=None)
    run_cli("gen-databricks",
            config=wd["config"], outdir=wd["csv_outdir"],
            type="row", workers=None, db_path=wd["db"],
            vintage=None, force=False, from_date=None, to_date=None,
            preset=None, stats_override=None)
    run_cli("load-row",
            project_db=wd["db"], folder=wd["csv_outdir"],
            config=wd["config"], mode="upsert",
            db=None, date_var=None)

    for qname in ("aws_month_summary", "databricks_pos_daily"):
        rows = db_rows(
            wd["db"],
            "SELECT COUNT(*) AS n FROM _row_counts WHERE source_table=?",
            (qname,),
        )
        assert rows[0]["n"] > 0, f"_row_counts missing rows for {qname}"

    html_path = os.path.join(wd["output_dir"], "compare_row.html")
    run_cli("compare-row",
            project_db=wd["db"], config=wd["config"],
            from_date=None, to_date=None, yes=True,
            html=html_path, title=None, subtitle=None)

    rows = db_rows(
        wd["db"],
        "SELECT matching_dates, overlap_start, overlap_end "
        "FROM _row_comparison WHERE pair_name=?",
        (wd["pair_databricks"],),
    )
    assert rows, "_row_comparison missing for AWS-vs-Databricks pair"
    md = json.loads(rows[0]["matching_dates"] or "[]")
    assert isinstance(md, list)


def test_run_pipeline_includes_databricks(initialized_db, run_cli, db_rows):
    """`dtrack run` end-to-end must classify the databricks source and
    call extract_databricks alongside the other extractors.
    """
    wd = initialized_db
    _ensure_aws_month_mock(wd)

    run_cli("run",
            project_db=wd["db"], config=wd["config"],
            outdir=wd["dir"],
            sas_outdir=wd["sas_outdir"], csv_outdir=wd["csv_outdir"],
            type="row", vintage="day", yes=True,
            html_row=os.path.join(wd["output_dir"], "compare_row.html"),
            html_col=os.path.join(wd["output_dir"], "compare_col.html"),
            title=None, subtitle=None,
            workers=None, force=False,
            skip_extract=False, skip_load=False, skip_compare=False,
            from_date=None, to_date=None,
            preset=None, stats_override=None)

    rows = db_rows(
        wd["db"],
        "SELECT COUNT(*) AS n FROM _row_counts WHERE source_table=?",
        ("databricks_pos_daily",),
    )
    assert rows[0]["n"] > 0, "dtrack run didn't load any Databricks rows"

    cmp_rows = db_rows(
        wd["db"],
        "SELECT pair_name FROM _row_comparison WHERE pair_name=?",
        (wd["pair_databricks"],),
    )
    assert cmp_rows, "_row_comparison missing AWS-vs-Databricks pair"
