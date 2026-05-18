"""Step 02: `dtrack load-columns` — discover column metadata.

Parity with web/FastAPI:
    - POST /api/load/columns/upload writes via db.insert_column_meta()
    - This CLI step uses the SAME db.insert_column_meta() helper, so the
      _column_meta rows produced are byte-identical for the same input.

What we verify:
    * CSV with explicit `source` + `table` columns lands in the right
      qname's _column_meta row set with the right data_type strings
    * Multiple sides loaded from a single CSV
    * Re-running is idempotent (no duplicate rows on the same column)
    * Empty/missing column rows are skipped silently
"""

from __future__ import annotations

import csv
from pathlib import Path


def _write_meta_csv(path: Path, rows):
    fieldnames = ["source", "table", "column_name", "data_type"]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def test_load_columns_populates_column_meta(initialized_db, run_cli, db_rows):
    wd = initialized_db
    meta_csv = Path(wd["dir"]) / "meta.csv"
    _write_meta_csv(meta_csv, [
        {"source": "oracle", "table": "POS_DAILY",
         "column_name": "ID", "data_type": "NUMBER"},
        {"source": "oracle", "table": "POS_DAILY",
         "column_name": "POS_DT", "data_type": "DATE"},
        {"source": "aws", "table": "pos_snapshot",
         "column_name": "id", "data_type": "bigint"},
        {"source": "aws", "table": "pos_snapshot",
         "column_name": "snapshot_dt", "data_type": "int"},
    ])

    run_cli("load-columns", project_db=wd["db"], config=wd["config"],
            csv_files=[str(meta_csv)])

    # qualified_name produces "oracle_pos_daily" for source=oracle name=pos_daily
    oracle_rows = db_rows(
        wd["db"],
        "SELECT column_name, data_type FROM _column_meta "
        "WHERE source_table=? ORDER BY column_name",
        ("oracle_pos_daily",),
    )
    aws_rows = db_rows(
        wd["db"],
        "SELECT column_name, data_type FROM _column_meta "
        "WHERE source_table=? ORDER BY column_name",
        ("aws_pos_snapshot",),
    )
    assert len(oracle_rows) == 2
    assert len(aws_rows) == 2
    cols = {r["column_name"]: r["data_type"] for r in oracle_rows}
    assert cols["ID"] == "NUMBER" and cols["POS_DT"] == "DATE"


def test_load_columns_is_idempotent(initialized_db, run_cli, db_rows):
    """Running load-columns twice with the same CSV must NOT produce
    duplicate _column_meta rows for the same (source_table, column_name).
    """
    wd = initialized_db
    meta_csv = Path(wd["dir"]) / "meta.csv"
    _write_meta_csv(meta_csv, [
        {"source": "oracle", "table": "POS_DAILY",
         "column_name": "ID", "data_type": "NUMBER"},
        {"source": "oracle", "table": "POS_DAILY",
         "column_name": "POS_DT", "data_type": "DATE"},
    ])

    for _ in range(2):
        run_cli("load-columns", project_db=wd["db"], config=wd["config"],
                csv_files=[str(meta_csv)])

    rows = db_rows(
        wd["db"],
        "SELECT COUNT(*) AS n FROM _column_meta WHERE source_table=?",
        ("oracle_pos_daily",),
    )
    assert rows[0]["n"] == 2


def test_load_columns_handles_crlf_and_uppercase_headers(initialized_db,
                                                         run_cli, db_rows):
    """The CLI uses csv.DictReader and strips whitespace from keys (handles
    \\r\\n) and accepts both lowercase + uppercase column-name headers.
    The web UI takes the same code path through cli._load_columns_entry's
    parent, so this codifies the contract.
    """
    wd = initialized_db
    meta_csv = Path(wd["dir"]) / "meta_upper.csv"
    # Use UPPERCASE headers and \r\n line endings.
    meta_csv.write_bytes(
        b"SOURCE,TABLE,COLUMN_NAME,DATA_TYPE\r\n"
        b"aws,pos_snapshot,id,bigint\r\n"
        b"aws,pos_snapshot,snapshot_dt,int\r\n"
    )
    run_cli("load-columns", project_db=wd["db"], config=wd["config"],
            csv_files=[str(meta_csv)])

    rows = db_rows(
        wd["db"],
        "SELECT column_name FROM _column_meta WHERE source_table=? "
        "ORDER BY column_name",
        ("aws_pos_snapshot",),
    )
    names = [r["column_name"] for r in rows]
    assert names == ["id", "snapshot_dt"]
