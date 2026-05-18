"""Step 06: `dtrack load-col` — ingest column stats CSVs into _col_stats.

Parity with web/FastAPI:
    - POST /api/load/col-stats/upload + /api/load/col-stats/path both call
      loader.load_precomputed_col_stats(). The CLI step iterates pairs
      and calls the same helper for each side.
    - The 14-field CSV schema MUST match what gen-aws / gen-oracle /
      gen-sas emit (column_name, dt, col_type, n_total, n_missing,
      n_unique, mean, std, min_val, max_val, top_10, length_avg,
      length_max, hash_sum).

Mock fixtures in testing/mock/ only carry _row.csv files (history was
never simulated for col stats — only row counts). So this test
synthesizes a tiny per-pair _col.csv per side, with the exact 14-field
schema, then asserts load-col lands the rows in _col_stats.

What we verify:
    * Loaded rows show up in _col_stats with the right source_table
    * The 14-field schema is preserved end-to-end (column types intact)
    * mode='upsert' is idempotent
"""

from __future__ import annotations

import csv
from pathlib import Path


COL_FIELDS = [
    "column_name", "dt", "col_type", "n_total", "n_missing",
    "n_unique", "mean", "std", "min_val", "max_val", "top_10",
    "length_avg", "length_max", "hash_sum",
]


def _write_col_csv(path: Path, rows):
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COL_FIELDS)
        w.writeheader()
        for r in rows:
            # Fill defaults so every field is present (matches what the
            # platform extractors emit).
            w.writerow({f: r.get(f, "") for f in COL_FIELDS})


def _seed_col_csvs(workdir):
    """Drop synthetic col CSVs for the two AWS sides + the oracle side
    into csv_outdir. The (sas_acct_snap) sas side lands in sas_outdir.
    """
    csv_outdir = Path(workdir["csv_outdir"])
    sas_outdir = Path(workdir["sas_outdir"])

    for qname, target in (
            ("aws_pos_snapshot", csv_outdir),
            ("oracle_pos_daily", csv_outdir),
            ("aws_acct_snap_aws", csv_outdir),
            ("sas_acct_snap", sas_outdir),
    ):
        _write_col_csv(target / f"{qname}_col.csv", [
            {"column_name": "id", "dt": "2024-10-01", "col_type": "numeric",
             "n_total": "100", "n_missing": "0", "n_unique": "100",
             "mean": "50.0", "std": "10.0", "min_val": "1", "max_val": "100"},
            {"column_name": "amount", "dt": "2024-10-01",
             "col_type": "numeric",
             "n_total": "100", "n_missing": "2", "n_unique": "98",
             "mean": "123.45", "std": "12.3",
             "min_val": "0.01", "max_val": "999.99"},
            {"column_name": "name", "dt": "2024-10-01",
             "col_type": "categorical",
             "n_total": "100", "n_missing": "0", "n_unique": "5",
             "min_val": "alpha=20", "max_val": "echo=22",
             "top_10": "alpha=20|bravo=21|charlie=18|delta=19|echo=22",
             "length_avg": "5.2", "length_max": "7",
             "hash_sum": "abc123"},
        ])


def test_load_col_populates_col_stats(initialized_db, run_cli, db_rows):
    wd = initialized_db
    _seed_col_csvs(wd)

    for folder in (wd["csv_outdir"], wd["sas_outdir"]):
        run_cli("load-col",
                project_db=wd["db"], folder=folder, config=wd["config"],
                mode="upsert")

    for qname in ("aws_pos_snapshot", "oracle_pos_daily",
                  "aws_acct_snap_aws", "sas_acct_snap"):
        rows = db_rows(
            wd["db"],
            "SELECT COUNT(*) AS n FROM _col_stats WHERE source_table=?",
            (qname,),
        )
        assert rows[0]["n"] == 3, (
            f"_col_stats should have 3 rows for {qname}; got {rows[0]['n']}")


def test_load_col_preserves_14_field_schema(initialized_db, run_cli, db_rows):
    """Spot-check a categorical row to ensure every emitted field made
    it to the DB intact. This is the parity contract with the web flow:
    same loader, same fields, same coercions.
    """
    wd = initialized_db
    _seed_col_csvs(wd)
    run_cli("load-col",
            project_db=wd["db"], folder=wd["csv_outdir"], config=wd["config"],
            mode="upsert")

    rows = db_rows(
        wd["db"],
        "SELECT * FROM _col_stats WHERE source_table=? AND column_name=? "
        "AND dt=?",
        ("aws_pos_snapshot", "name", "2024-10-01"),
    )
    assert rows, "categorical row missing"
    r = rows[0]
    assert r["col_type"] == "categorical"
    assert r["hash_sum"] == "abc123"
    assert r["top_10"] and "alpha=20" in r["top_10"]


def test_load_col_upsert_is_idempotent(initialized_db, run_cli, db_rows):
    wd = initialized_db
    _seed_col_csvs(wd)
    for _ in range(2):
        run_cli("load-col",
                project_db=wd["db"], folder=wd["csv_outdir"], config=wd["config"],
                mode="upsert")

    rows = db_rows(
        wd["db"],
        "SELECT COUNT(*) AS n, "
        "COUNT(DISTINCT column_name||'|'||dt) AS d "
        "FROM _col_stats WHERE source_table=?",
        ("aws_pos_snapshot",),
    )
    assert rows[0]["n"] == rows[0]["d"], (
        "load-col upsert produced duplicate (column_name, dt) rows")
