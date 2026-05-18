"""Verify col-stats SQL generation honors preset / Custom-stats selection.

Pure-string assertions on the generated SQL — no real engine touched.
The tests pin three behaviors:

  1. Each canonical preset (quick / standard / detailed) emits only the
     stats declared in constants.STATS_PRESETS and blanks every other
     stat with a '' literal so the 14-column CSV schema stays stable.

  2. Cross-dialect parity: every dialect that wraps base.build_col_sql
     (oracle, hadoop, duckdb) shows the same field-presence shape for
     the same preset.

  3. A per-field stats_override dict (the Custom-checkbox state on
     col_gen) lets the user uncheck a stat the smallest-covering
     preset would have included (e.g. tick n_unique, untick hash_sum
     → emitted SQL must NOT contain a hash computation).

The Custom-override behavior is the regression site: today col_gen.js
snaps Custom up to the smallest preset whose stats are a superset of
the user's ticks, which silently re-adds hash_sum / top_10 the user
explicitly disabled. The override path closes that hole.
"""

import pytest


# ---------------------------------------------------------------------------
# SQL-text probes
# ---------------------------------------------------------------------------

# >>> n_unique is computed (vs blanked as `0 AS n_unique`) <<< #
def _has_n_unique(sql):
    return "0 AS n_unique" not in sql

# >>> hash_sum is computed (any per-dialect hash) <<< #
def _has_hash_sum(sql):
    if "'' AS hash_sum" in sql:
        return False
    markers = ("STANDARD_HASH", "md5(", "MD5(",
               "hash16", "HASH16", "HiveHash", "ORA_HASH")
    return any(m in sql for m in markers)

# >>> top_10 is computed (the freq-table aggregator landed) <<< #
def _has_top_10(sql):
    if "'' AS top_10" in sql:
        return False
    up = sql.upper()
    markers = ("LISTAGG", "ARRAY_JOIN", "COLLECT_LIST", "STRING_AGG",
               "ARRAY_AGG")
    return any(m in up for m in markers)

# >>> length_avg is computed (vs blanked) <<< #
def _has_length_avg(sql):
    if "'' AS length_avg" in sql:
        return False
    return "LENGTH(" in sql or "length(" in sql


_TBL = {"date_col": "RPT_DT", "where": "", "vintage": "all"}
_DIALECTS = ("oracle", "hadoop", "duckdb")


def _build(dialect, **kwargs):
    from dtrack.platforms.base import build_col_sql
    sql, _ = build_col_sql(
        dialect, _TBL, "ACCT_ID", "VARCHAR2",
        "SCHEMA.TBL", cte_prefix="", **kwargs)
    return sql


# ---------------------------------------------------------------------------
# 1. Canonical presets — what's IN and what's blanked
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("dialect", _DIALECTS)
def test_quick_blanks_n_unique_hash_top10(dialect):
    sql = _build(dialect, preset="quick")
    assert not _has_n_unique(sql), f"{dialect} quick should NOT compute n_unique"
    assert not _has_hash_sum(sql), f"{dialect} quick should NOT compute hash_sum"
    assert not _has_top_10(sql),   f"{dialect} quick should NOT compute top_10"
    assert _has_length_avg(sql),   f"{dialect} quick must keep length_avg"


@pytest.mark.parametrize("dialect", _DIALECTS)
def test_standard_adds_n_unique_and_hash_but_no_top10(dialect):
    sql = _build(dialect, preset="standard")
    assert _has_n_unique(sql),     f"{dialect} standard must compute n_unique"
    assert _has_hash_sum(sql),     f"{dialect} standard must compute hash_sum"
    assert not _has_top_10(sql),   f"{dialect} standard should NOT compute top_10"
    assert _has_length_avg(sql),   f"{dialect} standard must keep length_avg"


@pytest.mark.parametrize("dialect", _DIALECTS)
def test_detailed_computes_everything(dialect):
    sql = _build(dialect, preset="detailed")
    # detailed gets n_unique via the freq-table (COUNT(*) FROM freq) rather
    # than COUNT(DISTINCT), so the `0 AS n_unique` literal must NOT be
    # present — that's the "blanked" signal across all presets.
    assert "0 AS n_unique" not in sql, f"{dialect} detailed must NOT blank n_unique"
    assert _has_hash_sum(sql),         f"{dialect} detailed must compute hash_sum"
    assert _has_top_10(sql),           f"{dialect} detailed must compute top_10"
    assert _has_length_avg(sql),       f"{dialect} detailed must keep length_avg"


# ---------------------------------------------------------------------------
# 2. Per-preset schema stability — the 14 result columns are always present
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("dialect", _DIALECTS)
@pytest.mark.parametrize("preset", ("quick", "standard", "detailed"))
def test_canonical_14_column_schema_present(dialect, preset):
    sql = _build(dialect, preset=preset)
    for col in ("AS dt", "AS column_name", "AS col_type", "AS n_total",
                "AS n_missing", "AS n_unique", "AS mean", "AS std",
                "AS top_10", "AS length_avg", "AS length_max", "AS hash_sum"):
        assert col in sql, f"{dialect}/{preset}: missing `{col}` in result row"


# ---------------------------------------------------------------------------
# 3. Custom override — only the user-ticked stats get computed
# ---------------------------------------------------------------------------
# The col_gen "Custom" radio + per-stat checkboxes should drive exactly
# which fields land in the SQL, NOT snap to a wider canonical preset.

_CUSTOM_BASE = {  # mirror of constants.STATS_PRESETS shape
    "n_total":   True,
    "n_missing": True,
    "n_unique":  False,
    "min_max":   True,
    "length":    True,
    "hash_sum":  False,
    "top_10":    False,
}


def _custom(**overrides):
    return {**_CUSTOM_BASE, **overrides}


@pytest.mark.parametrize("dialect", _DIALECTS)
def test_custom_unique_only_no_hash_no_top10(dialect):
    """User ticks n_unique but UN-ticks hash_sum and top_10.

    The regression: today the system snaps this to preset='standard'
    which forces hash_sum back on. The override should prevent that.
    """
    sql = _build(dialect, preset="quick",
                  stats_override=_custom(n_unique=True))
    assert _has_n_unique(sql), f"{dialect} custom: n_unique must be computed"
    assert not _has_hash_sum(sql), (
        f"{dialect} custom: hash_sum was un-ticked but the SQL still "
        f"computes it (snapping to preset='standard' is the bug)"
    )
    assert not _has_top_10(sql), f"{dialect} custom: top_10 should stay blank"


@pytest.mark.parametrize("dialect", _DIALECTS)
def test_custom_hash_only_no_unique(dialect):
    """User ticks hash_sum but un-ticks n_unique."""
    sql = _build(dialect, preset="quick",
                  stats_override=_custom(hash_sum=True))
    assert not _has_n_unique(sql), (
        f"{dialect} custom: n_unique was un-ticked but the SQL still "
        f"computes it"
    )
    assert _has_hash_sum(sql), f"{dialect} custom: hash_sum must be computed"
    assert not _has_top_10(sql), f"{dialect} custom: top_10 should stay blank"


@pytest.mark.parametrize("dialect", _DIALECTS)
def test_custom_drop_length(dialect):
    """Length stats can be turned off independently."""
    sql = _build(dialect, preset="quick", stats_override=_custom(length=False))
    assert not _has_length_avg(sql), f"{dialect} custom: length_avg must be blank"


@pytest.mark.parametrize("dialect", _DIALECTS)
def test_custom_all_off_emits_canonical_row(dialect):
    """Everything optional un-ticked: n_total / n_missing always present,
    every other slot blanked. Result row still 14 columns wide."""
    off = _custom(n_unique=False, min_max=False, length=False,
                  hash_sum=False, top_10=False)
    sql = _build(dialect, preset="quick", stats_override=off)
    assert not _has_n_unique(sql)
    assert not _has_hash_sum(sql)
    assert not _has_top_10(sql)
    assert not _has_length_avg(sql)
    assert "'' AS min" in sql or "'' AS min_val" in sql, (
        f"{dialect} custom: min_val must be blank when min_max=False"
    )


@pytest.mark.parametrize("dialect", _DIALECTS)
def test_custom_override_wins_over_preset_arg(dialect):
    """preset='detailed' + stats_override matters: override is authoritative."""
    sql = _build(dialect, preset="detailed", stats_override=_custom())  # all-off-except-default
    assert not _has_hash_sum(sql), (
        f"{dialect}: when override says no hash, preset='detailed' must NOT "
        f"re-enable it"
    )
    assert not _has_top_10(sql), (
        f"{dialect}: when override says no top_10, preset='detailed' must NOT "
        f"re-enable it"
    )


# ---------------------------------------------------------------------------
# 4. Athena Quick path — same behavior on the dialect-specific builder
# ---------------------------------------------------------------------------

def test_athena_quick_blanks_n_unique_hash_top10():
    from dtrack.platforms.athena import _build_coarse_sql
    sql = _build_coarse_sql(
        col_name="ACCT_ID",
        col_q='"ACCT_ID"',
        col_type="categorical",
        dt_select="'all' AS dt",
        group_by="",
        full_table="db.tbl",
        wc="1=1",
        cte_prefix="",
        preset="quick",
    )
    assert "0 AS n_unique" in sql, "athena quick must blank n_unique"
    assert "'' AS hash_sum" in sql, "athena quick must blank hash_sum"
    assert "'' AS top_10" in sql,   "athena quick must blank top_10"
    assert "LENGTH(" in sql,        "athena quick must compute length_avg"


def test_athena_quick_custom_unique_only_no_hash():
    from dtrack.platforms.athena import _build_coarse_sql
    sql = _build_coarse_sql(
        col_name="ACCT_ID", col_q='"ACCT_ID"', col_type="categorical",
        dt_select="'all' AS dt", group_by="",
        full_table="db.tbl", wc="1=1", cte_prefix="",
        preset="quick",
        stats_override=_custom(n_unique=True),
    )
    assert "COUNT(DISTINCT" in sql, "athena custom: n_unique must be computed"
    assert "'' AS hash_sum" in sql, (
        "athena custom: hash_sum was un-ticked but the SQL still computes it"
    )


def test_athena_quick_custom_hash_only_no_unique():
    from dtrack.platforms.athena import _build_coarse_sql
    sql = _build_coarse_sql(
        col_name="ACCT_ID", col_q='"ACCT_ID"', col_type="categorical",
        dt_select="'all' AS dt", group_by="",
        full_table="db.tbl", wc="1=1", cte_prefix="",
        preset="quick",
        stats_override=_custom(hash_sum=True),
    )
    assert "0 AS n_unique" in sql, "athena custom: n_unique must be blank"
    assert "'' AS hash_sum" not in sql, "athena custom: hash_sum must be computed"
    assert "md5(" in sql.lower() or "hash16" in sql.lower(), (
        "athena custom: hash_sum sql should include the md5/hash16 expression"
    )


def test_athena_quick_custom_drop_length():
    from dtrack.platforms.athena import _build_coarse_sql
    sql = _build_coarse_sql(
        col_name="ACCT_ID", col_q='"ACCT_ID"', col_type="categorical",
        dt_select="'all' AS dt", group_by="",
        full_table="db.tbl", wc="1=1", cte_prefix="",
        preset="quick",
        stats_override=_custom(length=False),
    )
    assert "'' AS length_avg" in sql, "athena custom: length_avg must be blank"
    assert "'' AS length_max" in sql, "athena custom: length_max must be blank"


# ---------------------------------------------------------------------------
# 5. Constants table integrity — preset → field flags
# ---------------------------------------------------------------------------

def test_stats_presets_quick_has_no_n_unique():
    from dtrack.constants import STATS_PRESETS
    assert STATS_PRESETS["quick"]["n_unique"] is False
    assert STATS_PRESETS["quick"]["hash_sum"] is False
    assert STATS_PRESETS["quick"]["top_10"]   is False


def test_stats_presets_standard_adds_n_unique_and_hash():
    from dtrack.constants import STATS_PRESETS
    assert STATS_PRESETS["standard"]["n_unique"] is True
    assert STATS_PRESETS["standard"]["hash_sum"] is True
    assert STATS_PRESETS["standard"]["top_10"]   is False


def test_stats_presets_detailed_enables_top_10():
    from dtrack.constants import STATS_PRESETS
    assert STATS_PRESETS["detailed"]["top_10"] is True
    assert STATS_PRESETS["detailed"]["hash_sum"] is True
