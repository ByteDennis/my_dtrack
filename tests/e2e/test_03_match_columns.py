"""Step 03: `dtrack match-columns` — auto-match column names per pair.

Parity with web/FastAPI:
    - PUT /api/pairs/{name}/col-mappings persists the same col_map via
      config.set_pair_col_map() + db.sync_config_to_db()
    - This CLI step calls the same two helpers, so the end-state in both
      pairs.json AND _table_pairs.col_mappings is identical.

What we verify:
    * Exact-name auto-matching populates col_map in BOTH pairs.json and
      the DB-side _table_pairs.col_mappings JSON
    * --mode merge preserves existing manual mappings
    * --mode replace drops old mappings and replaces them with auto ones
    * Mismatched columns (no auto-match) do NOT crash the command in
      non-interactive seeding (we pre-seed identical names to avoid the
      prompt path; that is what `seeded_columns` does)
"""

from __future__ import annotations

import json
from pathlib import Path


def test_match_columns_writes_col_map_to_config_and_db(
        seeded_columns, run_cli, db_rows):
    """All 5 identically-named columns auto-match. col_map should appear
    on the pair in both the JSON config and the DB _table_pairs row.
    """
    wd = seeded_columns
    pair = wd["pair_oracle_aws"]

    run_cli("match-columns", project_db=wd["db"], config=wd["config"],
            mode="merge", yes=True)

    # 1. pairs.json side
    cfg = json.loads(Path(wd["config"]).read_text())
    col_map = cfg["pairs"][pair].get("col_map", {})
    # 5 seeded columns; date columns differ ("pos_dt" vs "snapshot_dt")
    # but the rest match by case-insensitive name.
    assert len(col_map) >= 4, f"expected ≥4 auto-matches, got {col_map}"
    assert col_map.get("id") == "id"
    assert col_map.get("amount") == "amount"
    assert col_map.get("name") == "name"
    assert col_map.get("code") == "code"

    # 2. DB side — _table_pairs.col_mappings is a JSON blob
    rows = db_rows(
        wd["db"],
        "SELECT col_mappings FROM _table_pairs WHERE pair_name=?",
        (pair,),
    )
    assert rows, "match-columns did not register the pair in _table_pairs"
    db_map = json.loads(rows[0]["col_mappings"] or "{}")
    assert db_map == col_map, "DB col_mappings drifted from pairs.json"


def test_match_columns_merge_preserves_manual_overrides(
        seeded_columns, run_cli):
    """Pre-existing col_map entries (e.g. mapped by the user via the web
    UI) must survive an auto-match run in --mode merge.
    """
    wd = seeded_columns
    pair = wd["pair_oracle_aws"]

    # Seed a manual override: pretend the user mapped pos_dt -> snapshot_dt
    cfg = json.loads(Path(wd["config"]).read_text())
    cfg["pairs"][pair]["col_map"] = {"pos_dt": "snapshot_dt"}
    Path(wd["config"]).write_text(json.dumps(cfg, indent=2))

    run_cli("match-columns", project_db=wd["db"], config=wd["config"],
            mode="merge", yes=True)

    cfg2 = json.loads(Path(wd["config"]).read_text())
    col_map = cfg2["pairs"][pair]["col_map"]
    assert col_map.get("pos_dt") == "snapshot_dt", (
        "merge dropped a pre-existing manual mapping")
    # And auto-matched entries are still added on top.
    assert col_map.get("id") == "id"


def test_match_columns_replace_drops_existing(seeded_columns, run_cli):
    """--mode replace drops the prior col_map entirely before auto-matching.
    Manual mappings that don't reappear via auto-match are forgotten.
    """
    wd = seeded_columns
    pair = wd["pair_oracle_aws"]

    cfg = json.loads(Path(wd["config"]).read_text())
    # Manual mapping using a column that doesn't exist in _column_meta
    # so auto-match has no way to reproduce it.
    cfg["pairs"][pair]["col_map"] = {"GHOST_COL": "phantom"}
    Path(wd["config"]).write_text(json.dumps(cfg, indent=2))

    run_cli("match-columns", project_db=wd["db"], config=wd["config"],
            mode="replace", yes=True)

    cfg2 = json.loads(Path(wd["config"]).read_text())
    col_map = cfg2["pairs"][pair]["col_map"]
    assert "GHOST_COL" not in col_map, (
        "--mode replace should have dropped the ghost mapping")
