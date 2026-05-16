"""Lifeboat (resource-exhaustion) retry path tests.

Historical context: on a TEMP / memory blowup from COUNT(DISTINCT) /
freq-CTE aggregation, the gen-first runners used to fall back to an
embedded lifeboat SQL block. That auto-retry has been retired on
Athena/Oracle/Hadoop in favor of clean single-pass Quick/Standard
presets (Detailed still uses freq tables but doesn't auto-retry).

These tests now cover only the surviving plumbing:

  1. The lifeboat SQL **builders** are still defined and produce the
     right shape — they're available for a future sidecar / opt-in
     re-execution but aren't wired into the runner.
  2. The parsers tolerate old extract_col.sql files that still carry
     `/* @degrade_sql ... @end_degrade */` blocks (Trino treats `/*` as
     a comment, so they pass through harmlessly).
  3. The comparator still treats `partial='oracle_degraded'` /
     `'athena_degraded'` rows from historical CSVs as "n_unique /
     hash_sum skipped" rather than mismatch.
"""

import os
import re
import tempfile
import textwrap

import pytest

from dtrack.platforms.oracle import (
    _build_lifeboat_col_sql_oracle,
    parse_oracle_sql_file,
)
from dtrack.platforms.athena import (
    _build_lifeboat_col_sql_athena,
    parse_sql_file,
)
from dtrack.compare import _has_col_differences


# ---------------------------------------------------------------------------
# Lifeboat SQL shape — same row schema as the full query, NULL on the
# aggregates that would spill TEMP / memory.
# ---------------------------------------------------------------------------

def _oracle_lifeboat_numeric():
    return _build_lifeboat_col_sql_oracle(
        tbl_cfg={'date_col': 'AS_OF_DT', 'where': '', 'vintage': 'all'},
        col='EMAIL_RQST_GID',
        dtype='NUMBER',
        full_table='SCHEMA.TABLE',
        cte_prefix='',
        vintage='all',
        date_dtype=None,
        date_format=None,
        dt_label=None,
    )


def test_oracle_lifeboat_numeric_drops_count_distinct():
    """COUNT(DISTINCT) is the TEMP-spilling aggregate; lifeboat must NULL it."""
    sql = _oracle_lifeboat_numeric()
    assert 'COUNT(DISTINCT' not in sql
    assert 'NULL' in sql.upper()
    # Streaming aggregates survive — n_total/n_missing are single-pass and
    # the comparator's diff still works on them.
    assert 'COUNT(*)' in sql
    assert 'n_missing' in sql


def test_oracle_lifeboat_categorical_drops_freq_cte():
    """The freq GROUP BY is the spilling hash table; lifeboat skips the
    entire CTE pipeline so even subscriber_key-class columns survive."""
    sql = _build_lifeboat_col_sql_oracle(
        tbl_cfg={'date_col': 'AS_OF_DT', 'where': '', 'vintage': 'all'},
        col='SUBSCRIBER_KEY',
        dtype='VARCHAR2',
        full_table='SCHEMA.TABLE',
        cte_prefix='',
    )
    assert 'WITH freq' not in sql
    assert 'ranked' not in sql
    assert 'lenhash' not in sql
    assert 'hash_sum' in sql  # column still present (empty)
    assert 'COUNT(DISTINCT' not in sql


def test_oracle_lifeboat_preserves_column_order():
    """CSV header is fixed; lifeboat output must include every field."""
    sql = _oracle_lifeboat_numeric()
    # Order matters for the CSV row layout. n_total before n_missing before
    # n_unique etc.
    fields = ['n_total', 'n_missing', 'n_unique', 'mean', 'std',
              'min_val', 'max_val', 'top_10',
              'length_avg', 'length_max', 'hash_sum']
    positions = [sql.find(f) for f in fields]
    assert all(p > 0 for p in positions), f"missing fields in {sql}"
    assert positions == sorted(positions), \
        f"field order changed: {list(zip(fields, positions))}"


def test_oracle_lifeboat_groups_by_vintage_when_dt_label_absent():
    """When vintage is set with no dt_label, lifeboat must GROUP BY the
    bucketed date so every vintage row still reports row counts."""
    sql = _build_lifeboat_col_sql_oracle(
        tbl_cfg={'date_col': 'AS_OF_DT', 'where': '', 'vintage': 'month'},
        col='ID',
        dtype='NUMBER',
        full_table='SCHEMA.TABLE',
        cte_prefix='',
        vintage='month',
    )
    assert 'GROUP BY' in sql
    # dt_label takes precedence when set — used by bucket-mode emission.
    sql_label = _build_lifeboat_col_sql_oracle(
        tbl_cfg={'date_col': 'AS_OF_DT', 'where': '', 'vintage': 'month'},
        col='ID',
        dtype='NUMBER',
        full_table='SCHEMA.TABLE',
        cte_prefix='',
        vintage='month',
        dt_label='2024-01-01',
    )
    assert 'GROUP BY' not in sql_label
    assert "'2024-01-01' AS dt" in sql_label


def test_athena_lifeboat_numeric_drops_count_distinct():
    sql = _build_lifeboat_col_sql_athena(
        col_name='SUBSCRIBER_KEY',
        col_q='"SUBSCRIBER_KEY"',
        col_dtype='varchar',
        is_numeric=False,
        dt_select="'2024-01-01' AS dt",
        group_by='',
        full_table='db.tbl',
        wc='1=1',
        cte_prefix='',
    )
    assert 'COUNT(DISTINCT' not in sql
    assert 'WITH freq' not in sql
    assert 'CAST(NULL AS BIGINT) AS n_unique' in sql


# ---------------------------------------------------------------------------
# SQL file format — the parsers must extract @degrade_sql AND strip it from
# the main SQL so the runner doesn't execute the comment text.
# ---------------------------------------------------------------------------

def test_athena_parse_sql_file_clean_blocks():
    """Fresh extract_col.sql files no longer carry a @degrade_sql lifeboat
    block (matches the oracle/hadoop layout). parse_sql_file should return
    blocks with just `name` + `sql` — no `degrade_sql` key in the dict."""
    body = textwrap.dedent("""\
        -- dtrack AWS col extraction queries
        -- Generated: 2026-05-14

        -- db.tbl/col_a/_all
        SELECT COUNT(*) FROM db.tbl;

        -- db.tbl/col_b/_all
        SELECT 1;
    """)
    with tempfile.NamedTemporaryFile('w', suffix='.sql', delete=False) as f:
        f.write(body)
        path = f.name
    try:
        blocks = parse_sql_file(path)
    finally:
        os.unlink(path)

    assert len(blocks) == 2
    assert blocks[0]['name'] == 'db.tbl/col_a/_all'
    assert blocks[0]['sql'] == 'SELECT COUNT(*) FROM db.tbl'
    assert 'degrade_sql' not in blocks[0]
    assert blocks[1]['sql'] == 'SELECT 1'


def test_athena_parse_sql_file_tolerates_old_degrade_blocks():
    """Old extract_col.sql files in the wild may still embed
    `/* @degrade_sql ... @end_degrade */`. Trino treats `/* ... */` as
    a comment so we don't need to strip it — but the parser must not
    crash and must keep the main SQL intact."""
    body = textwrap.dedent("""\
        -- db.tbl/col_a/_all
        SELECT COUNT(*) FROM db.tbl;
        /* @degrade_sql
        SELECT COUNT(*) AS n_total FROM db.tbl
        @end_degrade */
    """)
    with tempfile.NamedTemporaryFile('w', suffix='.sql', delete=False) as f:
        f.write(body)
        path = f.name
    try:
        blocks = parse_sql_file(path)
    finally:
        os.unlink(path)

    assert len(blocks) == 1
    assert 'SELECT COUNT(*) FROM db.tbl' in blocks[0]['sql']
    assert 'degrade_sql' not in blocks[0]


def test_oracle_parse_sql_file_extracts_degrade_and_conn_macro():
    body = textwrap.dedent("""\
        -- dtrack Oracle col extraction queries
        -- Generated: 2026-05-14

        -- schema.tbl/EMAIL_RQST_GID/2024-Q1
        /* @conn_macro=tmp23 */
        SELECT COUNT(*), COUNT(DISTINCT EMAIL_RQST_GID) FROM schema.tbl
        ;
        /* @degrade_sql
        SELECT COUNT(*) AS n_total, NULL AS n_unique FROM schema.tbl
        @end_degrade */
    """)
    with tempfile.NamedTemporaryFile('w', suffix='.sql', delete=False) as f:
        f.write(body)
        path = f.name
    try:
        blocks = parse_oracle_sql_file(path)
    finally:
        os.unlink(path)

    assert len(blocks) == 1
    assert blocks[0]['conn_macro'] == 'tmp23'
    assert 'COUNT(DISTINCT' in blocks[0]['sql']
    assert '@degrade_sql' not in blocks[0]['sql']
    assert blocks[0]['degrade_sql'] is not None
    assert 'NULL AS n_unique' in blocks[0]['degrade_sql']


# ---------------------------------------------------------------------------
# Comparator — degraded rows must skip n_unique/hash_sum diffs, not flag
# them as drift.
# ---------------------------------------------------------------------------

def test_has_col_differences_suppresses_n_unique_when_degraded():
    """A lifeboat row with n_unique=NULL vs a full row with n_unique=N
    must NOT count as a difference, because the lifeboat path didn't even
    compute distinct values."""
    comp = {
        'n_total_diff': 0,
        'n_missing_diff': 0,
        'n_unique_diff': 1_000_000,   # big "diff" but...
        'n_unique_skipped': True,     # ...degraded → skip
        'partial_left': 'oracle_degraded',
        'partial_right': '',
    }
    assert _has_col_differences(comp) is False


def test_has_col_differences_still_flags_n_unique_when_not_degraded():
    """Sanity: when no partial flag is set, an n_unique diff is real drift."""
    comp = {
        'n_total_diff': 0,
        'n_missing_diff': 0,
        'n_unique_diff': 5,
        'n_unique_skipped': False,
        'partial_left': '',
        'partial_right': '',
    }
    assert _has_col_differences(comp) is True


def test_has_col_differences_still_flags_n_total_when_degraded():
    """n_total is COUNT(*) — survives the lifeboat unchanged. A real
    diff on n_total IS drift even when one side ran the lifeboat."""
    comp = {
        'n_total_diff': 42,
        'n_missing_diff': 0,
        'n_unique_diff': 0,
        'n_unique_skipped': True,
        'partial_left': 'oracle_degraded',
        'partial_right': '',
    }
    assert _has_col_differences(comp) is True


# ---------------------------------------------------------------------------
# Round-trip: builder → file → parser. The shape must survive the round
# trip so a fresh deployment doesn't drop the lifeboat after a regen.
# ---------------------------------------------------------------------------

def test_athena_emit_and_parse_roundtrip():
    """A freshly-written extract_col.sql block round-trips through
    parse_sql_file with `name` + `sql` and no lifeboat metadata."""
    main_sql = "SELECT 'all' AS dt, 'col_a' AS column_name, " \
               "COUNT(DISTINCT col_a) AS n_unique FROM db.tbl"
    body = (
        "-- dtrack AWS col extraction queries\n\n"
        "-- db.tbl/col_a/_all\n"
        f"{main_sql}\n"
        ";\n"
    )
    with tempfile.NamedTemporaryFile('w', suffix='.sql', delete=False) as f:
        f.write(body)
        path = f.name
    try:
        blocks = parse_sql_file(path)
    finally:
        os.unlink(path)
    assert len(blocks) == 1
    assert blocks[0]['name'] == 'db.tbl/col_a/_all'
    assert 'COUNT(DISTINCT col_a)' in blocks[0]['sql']
    assert 'degrade_sql' not in blocks[0]
