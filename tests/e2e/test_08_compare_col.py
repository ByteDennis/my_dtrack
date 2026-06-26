"""Step 08: `dtrack compare-col` — column statistics comparison + HTML.

Parity with web/FastAPI:
    - GET /api/compare/col/{pair} runs compare.compare_column_stats() —
      same helper this CLI step uses.
    - matching_dates from _row_comparison constrain which dates the col
      comparison looks at, identical to web's filter.
    - The CLI persists comparison_details JSON to _col_comparison and
      writes an HTML report.

What we verify:
    * Given seeded _col_stats (synth) on both sides + a successful
      compare-row, compare-col writes one _col_comparison row per pair
    * The diff is computed only for COLUMN-MAPPED pairs (auto-match runs
      via seeded_columns fixture)
    * HTML report contains per-column diff sections
    * --no-date-filter disables matching_dates intersection
"""

from __future__ import annotations

import csv
import json
import os
from pathlib import Path


COL_FIELDS = [
    "column_name", "dt", "col_type", "n_total", "n_missing",
    "n_unique", "mean", "std", "min_val", "max_val", "top_10",
    "length_avg", "length_max", "hash_sum",
]


def _write_col_csv(path, rows):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COL_FIELDS)
        w.writeheader()
        for r in rows:
            w.writerow({f: r.get(f, "") for f in COL_FIELDS})


def _row_count_csv(path, start_date="2024-10-01", n=5):
    """Tiny synthetic _row.csv: each side gets the SAME dates so all are
    in the matching set after compare-row runs.
    """
    from datetime import date, timedelta
    rows = []
    d = date.fromisoformat(start_date)
    for i in range(n):
        rows.append((str(d + timedelta(days=i)), 100 + i))
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["date_value", "row_count"])
        w.writerows(rows)


def _seed_pipeline_to_col(workdir, run_cli):
    """Run gen-* + load-row + compare-row so we land on the same state
    that compare-col expects. Also seed synth _col.csv rows on both
    sides of one pair so we have data to diff.
    """
    # Use synth row CSVs with IDENTICAL dates on both sides so matching
    # set covers everything. Overwrite what gen-aws would copy from mock.
    csv_outdir = Path(workdir["csv_outdir"])
    _row_count_csv(csv_outdir / "aws_pos_snapshot_row.csv")
    _row_count_csv(csv_outdir / "oracle_pos_daily_row.csv")

    run_cli("load-row",
            project_db=workdir["db"], folder=workdir["csv_outdir"],
            config=workdir["config"], mode="upsert",
            db=None, date_var=None)

    html_row = os.path.join(workdir["output_dir"], "compare_row.html")
    run_cli("compare-row",
            project_db=workdir["db"], config=workdir["config"],
            from_date=None, to_date=None, yes=True,
            html=html_row, title=None, subtitle=None)

    # Synthetic col stats for the oracle/aws pair. Slight divergence on
    # `amount` so compare-col has something non-trivial to flag.
    left_rows = [
        {"column_name": "id", "dt": "2024-10-01", "col_type": "numeric",
         "n_total": "100", "n_missing": "0", "n_unique": "100",
         "mean": "50.0", "std": "10.0", "min_val": "1", "max_val": "100"},
        {"column_name": "amount", "dt": "2024-10-01", "col_type": "numeric",
         "n_total": "100", "n_missing": "0", "n_unique": "98",
         "mean": "123.45", "std": "12.3",
         "min_val": "1.0", "max_val": "999.0"},
    ]
    right_rows = [
        {"column_name": "id", "dt": "2024-10-01", "col_type": "numeric",
         "n_total": "100", "n_missing": "0", "n_unique": "100",
         "mean": "50.0", "std": "10.0", "min_val": "1", "max_val": "100"},
        {"column_name": "amount", "dt": "2024-10-01", "col_type": "numeric",
         # n_unique differs by 5; this is the diff we'll look for
         "n_total": "100", "n_missing": "0", "n_unique": "93",
         "mean": "123.45", "std": "12.3",
         "min_val": "1.0", "max_val": "999.0"},
    ]
    _write_col_csv(csv_outdir / "oracle_pos_daily_col.csv", left_rows)
    _write_col_csv(csv_outdir / "aws_pos_snapshot_col.csv", right_rows)
    run_cli("load-col",
            project_db=workdir["db"], folder=workdir["csv_outdir"],
            config=workdir["config"], mode="upsert")


def test_compare_col_persists_col_comparison_and_html(
        seeded_columns, run_cli, db_rows):
    """End-to-end: seeded_columns ensures _column_meta is populated and
    match-columns can auto-link the columns. Then we run compare-col and
    verify the _col_comparison row + HTML output.
    """
    wd = seeded_columns
    # Need match-columns to populate col_map before compare-col can find
    # columns to diff.
    run_cli("match-columns", project_db=wd["db"], config=wd["config"],
            mode="merge", yes=True)
    _seed_pipeline_to_col(wd, run_cli)

    html_col = os.path.join(wd["output_dir"], "compare_col.html")
    run_cli("compare-col",
            project_db=wd["db"], config=wd["config"],
            from_date=None, to_date=None, no_date_filter=False,
            vintage="day", yes=True, html=html_col,
            title=None, subtitle=None)

    rows = db_rows(
        wd["db"],
        "SELECT columns_compared, matched_columns, diff_columns "
        "FROM _col_comparison WHERE pair_name=?",
        (wd["pair_oracle_aws"],),
    )
    assert rows, "_col_comparison missing for oracle/aws pair"
    cmp_cols = json.loads(rows[0]["columns_compared"] or "[]")
    diff_cols = json.loads(rows[0]["diff_columns"] or "[]")
    assert cmp_cols, "compare-col compared 0 columns (col_map empty?)"
    # `amount` was seeded with diverging n_unique; it must show up.
    assert "amount" in diff_cols, (
        f"expected 'amount' in diff_columns, got {diff_cols}")

    # HTML produced
    assert os.path.exists(html_col)
    body = Path(html_col).read_text()
    assert "amount" in body.lower() or "diff" in body.lower()


def test_compare_col_no_date_filter_short_circuits_matching_dates(
        seeded_columns, run_cli, db_rows):
    """`--no-date-filter` should bypass the matching_dates intersection.
    We blow away the _row_comparison entry and verify compare-col still
    produces a result.
    """
    import sqlite3

    wd = seeded_columns
    run_cli("match-columns", project_db=wd["db"], config=wd["config"],
            mode="merge", yes=True)
    _seed_pipeline_to_col(wd, run_cli)

    # Wipe matching_dates so an "intersect with empty" would skip
    # everything.
    conn = sqlite3.connect(wd["db"])
    try:
        conn.execute(
            "UPDATE _row_comparison SET matching_dates='[]' WHERE pair_name=?",
            (wd["pair_oracle_aws"],),
        )
        conn.commit()
    finally:
        conn.close()

    html_col = os.path.join(wd["output_dir"], "compare_col_nofilter.html")
    run_cli("compare-col",
            project_db=wd["db"], config=wd["config"],
            from_date=None, to_date=None, no_date_filter=True,
            vintage="day", yes=True, html=html_col,
            title=None, subtitle=None)

    rows = db_rows(
        wd["db"],
        "SELECT columns_compared FROM _col_comparison WHERE pair_name=?",
        (wd["pair_oracle_aws"],),
    )
    assert rows and json.loads(rows[0]["columns_compared"] or "[]"), (
        "compare-col with --no-date-filter should still produce results "
        "when matching_dates is empty")
