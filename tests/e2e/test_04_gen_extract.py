"""Step 04: `dtrack gen-aws / gen-oracle / gen-sas` — extraction.

Parity with web/FastAPI:
    - POST /api/extract dispatches by `platform` to the SAME three entry
      points (`extract_aws`, `extract_oracle`, `gen_sas`). Each CLI cmd
      simply unwraps argparse args and calls that entry point.
    - Mock mode (DTRACK_MOCK=<dir>) is the test harness for ALL three: it
      copies fixture `{qname}_row.csv` files into outdir, exercising the
      same `_extract_mock_tables` helper the web flow uses when env is
      set.

What we verify:
    * gen-aws copies the AWS-side row CSV into csv_outdir for every
      AWS pair-side in the config (only AWS sides — not the other side)
    * gen-oracle does the same for ORACLE sides
    * gen-sas does the same for SAS sides
    * --preset / --stats-override flags are accepted by each command
      (sanity: no crash on the new flags we just added)
    * --type row vs --type col routes correctly
"""

from __future__ import annotations

import os
from pathlib import Path


def _produced_csvs(outdir: str, suffix: str = "row") -> set[str]:
    """Return the set of qnames for which {qname}_{suffix}.csv exists."""
    out = Path(outdir)
    if not out.exists():
        return set()
    return {p.name[:-len(f"_{suffix}.csv")]
            for p in out.iterdir()
            if p.name.endswith(f"_{suffix}.csv")}


def test_gen_aws_copies_mock_csvs_for_aws_sides(initialized_db, run_cli):
    """gen-aws should copy AWS-side mock CSVs into csv_outdir. _E2E_PAIRS
    has two AWS sides: aws_pos_snapshot and aws_acct_snap_aws.
    """
    wd = initialized_db

    run_cli("gen-aws",
            config=wd["config"], outdir=wd["csv_outdir"],
            type="row", workers=None, db_path=wd["db"],
            vintage=None, force=False, from_date=None, to_date=None,
            preset=None, stats_override=None)

    produced = _produced_csvs(wd["csv_outdir"], "row")
    assert "aws_pos_snapshot" in produced, (
        f"gen-aws didn't produce aws_pos_snapshot_row.csv: {produced}")
    assert "aws_acct_snap_aws" in produced, (
        f"gen-aws didn't produce aws_acct_snap_aws_row.csv: {produced}")
    # Must not leak into the other side's outdir
    assert _produced_csvs(wd["sas_outdir"], "row") == set()


def test_gen_oracle_copies_mock_csvs_for_oracle_sides(initialized_db,
                                                       run_cli):
    """gen-oracle picks up source='oracle'. Only oracle_pos_daily is an
    oracle source in _E2E_PAIRS.
    """
    wd = initialized_db

    run_cli("gen-oracle",
            config=wd["config"], outdir=wd["csv_outdir"],
            type="row", workers=None, db_path=wd["db"],
            vintage=None, force=False, from_date=None, to_date=None,
            preset=None, stats_override=None)

    produced = _produced_csvs(wd["csv_outdir"], "row")
    assert "oracle_pos_daily" in produced, (
        f"gen-oracle didn't produce oracle_pos_daily_row.csv: {produced}")


def test_gen_sas_processes_sas_sides(initialized_db, run_cli):
    """gen-sas writes the SAS-side fixture (sas_acct_snap). In mock mode
    the helper still emits the per-table CSV; we only assert the SAS-side
    file made it through.
    """
    wd = initialized_db

    run_cli("gen-sas",
            config=wd["config"], outdir=wd["sas_outdir"],
            type="row", db_path=wd["db"],
            vintage=None, from_date=None, to_date=None,
            preset=None, stats_override=None)

    produced = _produced_csvs(wd["sas_outdir"], "row")
    assert "sas_acct_snap" in produced, (
        f"gen-sas didn't produce sas_acct_snap_row.csv: {produced}")


def test_gen_aws_accepts_preset_and_stats_override_flags(
        initialized_db, run_cli):
    """The new --preset / --stats-override flags must not crash the
    extraction path; in mock mode the output CSV is just a copy, so the
    flags' effect (stat masking) is exercised more deeply in the per-
    stat unit tests. Here we only assert the flags are wired without
    breaking the mock pipeline.
    """
    wd = initialized_db

    run_cli("gen-aws",
            config=wd["config"], outdir=wd["csv_outdir"],
            type="row", workers=None, db_path=wd["db"],
            vintage=None, force=False, from_date=None, to_date=None,
            preset="standard",
            stats_override="n_unique=on,hash_sum=off")

    assert "aws_pos_snapshot" in _produced_csvs(wd["csv_outdir"], "row")


def test_gen_aws_type_col_does_not_emit_row_csvs(initialized_db, run_cli):
    """--type col asks ONLY for column stats; row CSVs must not show up.
    Mirrors the /api/extract `type` param dispatch.
    """
    wd = initialized_db

    run_cli("gen-aws",
            config=wd["config"], outdir=wd["csv_outdir"],
            type="col", workers=None, db_path=wd["db"],
            vintage=None, force=False, from_date=None, to_date=None,
            preset=None, stats_override=None)

    assert _produced_csvs(wd["csv_outdir"], "row") == set(), (
        "type=col should not produce row CSVs")
