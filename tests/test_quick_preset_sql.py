"""Quick-preset SQL shape guarantees across all four extract backends.

The col_gen page exposes three stats presets — quick / standard / detailed —
each of which selects a subset of stats from `constants.STATS_PRESETS`.
'quick' is the cheap-aggregate path: no COUNT(DISTINCT), no md5/hash_sum,
no top_10, no freq-table WITH chain. The promise is engine-uniform: on
Oracle / Hadoop / CSV (DuckDB) / Athena the generated extract_col.sql
must do only that minimal work so partitioned / heavy tables don't pay
the freq-table cost.

These tests inspect the *generated SQL string* directly, asserting both
that expensive operators are absent AND that the 14-column CSV schema
aliases are still present (downstream loader depends on that shape).

Why split across all four backends:
  - Oracle / Hadoop / CSV all route through `base.build_col_sql`, but
    each has its own dialect-specific SQL fragments. A regression in any
    one dialect's flat branch would re-introduce expensive ops on that
    engine only.
  - Athena has its own writer (`_build_coarse_sql` in athena.py); the
    Quick path was added separately. Bundling all four in one file means
    "preset=quick must stay flat" is one assertion target across the
    matrix.
"""

import re


_QUICK_EXPENSIVE_PATTERNS = [
    # COUNT(DISTINCT col): real cardinality scan. Should be the literal
    # `0 AS n_unique` placeholder under quick instead.
    (r"COUNT\(\s*DISTINCT", "COUNT(DISTINCT)"),
    # Athena/Hadoop/DuckDB freq-table hash recipes.
    (r"\bMD5\s*\(", "MD5 hash"),
    (r"TO_UTF8\s*\(", "TO_UTF8 (Athena hash16 input)"),
    (r"STANDARD_HASH\s*\(", "STANDARD_HASH (Oracle hash16)"),
    (r"STRING_TO_RAW\s*\(", "STRING_TO_RAW (Oracle hash16 input)"),
    # Cross-engine "freq-weighted SUM(cnt * ...)" — never appears in
    # quick because there's no freq table at all.
    (r"SUM\s*\(\s*cnt\s*\*", "SUM(cnt * ...) freq-weighted aggregate"),
    (r"SUM\s*\(\s*CAST\s*\(\s*cnt\s+AS", "SUM(CAST(cnt AS ...)) freq-weighted"),
    # top_10 listagg / array_join expressions.
    (r"\bLISTAGG\b", "LISTAGG (top_10)"),
    (r"\bARRAY_JOIN\b", "ARRAY_JOIN (top_10)"),
    (r"COLLECT_LIST\s*\(", "collect_list (Hive top_10)"),
    (r"\bSTRING_AGG\b", "string_agg (DuckDB top_10)"),
    # WITH-chain CTEs from any of the freq-table paths.
    (r"\bWITH\s+freq\s+AS\b", "WITH freq CTE"),
    (r"_coarse_freq\b", "_coarse_freq CTE (Athena)"),
    (r"_coarse_lenhash\b", "_coarse_lenhash CTE (Athena)"),
    (r"\blenhash\s+AS\b", "lenhash CTE (base)"),
    (r"\branked\s+AS\b", "ranked CTE"),
    (r"\bsstats\s+AS\b", "sstats (rank-weighted mean/std)"),
]


_REQUIRED_ALIASES_14 = [
    "n_total", "n_missing", "n_unique",
    "mean", "std", "min_val", "max_val",
    "top_10", "length_avg", "length_max", "hash_sum",
]


def _assert_quick_shape(sql, dialect_label):
    """Assert `sql` is the lean quick-preset form: no expensive ops, but
    every 14-column CSV alias still present so the loader sees a stable
    row schema."""
    for pattern, label in _QUICK_EXPENSIVE_PATTERNS:
        m = re.search(pattern, sql, flags=re.IGNORECASE)
        assert m is None, (
            f"{dialect_label}: quick preset SQL still contains "
            f"`{label}` (matched: {m.group(0)!r}). Generated SQL:\n{sql}"
        )
    for alias in _REQUIRED_ALIASES_14:
        assert f"AS {alias}" in sql or f"as {alias}" in sql.lower(), (
            f"{dialect_label}: quick preset SQL is missing the "
            f"`{alias}` column alias — breaks the 14-column CSV schema."
        )


# ---------------------------------------------------------------------------
# base.build_col_sql — covers oracle / hadoop / duckdb dialects
# ---------------------------------------------------------------------------

def _build_base_quick(dialect, dtype):
    from dtrack.platforms.base import build_col_sql
    tbl_cfg = {'date_col': 'DT', 'name': 't',
               'where': '', 'columns': {'C': dtype}}
    sql, _ = build_col_sql(
        dialect, tbl_cfg, 'C', dtype, 'TBL', '',
        vintage='all', date_dtype=None, date_format=None,
        dt_label='all', preset='quick',
    )
    return sql


def test_oracle_quick_sql_is_flat_categorical():
    sql = _build_base_quick('oracle', 'VARCHAR2')
    _assert_quick_shape(sql, "oracle/categorical")
    assert "0 AS n_unique" in sql


def test_oracle_quick_sql_is_flat_numeric():
    sql = _build_base_quick('oracle', 'NUMBER')
    _assert_quick_shape(sql, "oracle/numeric")


def test_hadoop_quick_sql_is_flat_categorical():
    sql = _build_base_quick('hadoop', 'STRING')
    _assert_quick_shape(sql, "hadoop/categorical")
    assert "0 AS n_unique" in sql


def test_hadoop_quick_sql_is_flat_numeric():
    sql = _build_base_quick('hadoop', 'BIGINT')
    _assert_quick_shape(sql, "hadoop/numeric")


def test_duckdb_quick_sql_is_flat_categorical():
    sql = _build_base_quick('duckdb', 'VARCHAR')
    _assert_quick_shape(sql, "duckdb/categorical")
    assert "0 AS n_unique" in sql


def test_duckdb_quick_sql_is_flat_numeric():
    sql = _build_base_quick('duckdb', 'BIGINT')
    _assert_quick_shape(sql, "duckdb/numeric")


# ---------------------------------------------------------------------------
# Athena — direct _build_coarse_sql with preset='quick'
# ---------------------------------------------------------------------------

def _build_athena_quick(col_type, col_dtype):
    from dtrack.platforms.athena import _build_coarse_sql
    return _build_coarse_sql(
        col_name='C',
        col_q='"C"',
        col_type=col_type,
        dt_select="'all' AS dt",
        group_by='',
        full_table='db.tbl',
        wc='1=1',
        cte_prefix='',
        col_dtype=col_dtype,
        preset='quick',
    )


def test_athena_quick_sql_is_flat_categorical():
    sql = _build_athena_quick('categorical', 'varchar')
    _assert_quick_shape(sql, "athena/categorical")
    assert "0 AS n_unique" in sql


def test_athena_quick_sql_is_flat_numeric():
    sql = _build_athena_quick('numeric', 'bigint')
    _assert_quick_shape(sql, "athena/numeric")


def test_athena_default_coarse_still_uses_freq_table():
    """Sanity check: preset=None (legacy coarse) keeps the freq-table
    path so we haven't accidentally collapsed it for callers that still
    want it."""
    from dtrack.platforms.athena import _build_coarse_sql
    sql = _build_coarse_sql(
        col_name='C', col_q='"C"',
        col_type='categorical',
        dt_select="'all' AS dt", group_by='',
        full_table='db.tbl', wc='1=1', cte_prefix='',
        col_dtype='varchar',
    )
    assert '_coarse_freq' in sql
    assert '_coarse_lenhash' in sql


# ---------------------------------------------------------------------------
# /api/extract/run-sql outdir routing — Oracle/Hadoop cache must land in
# sas_outdir/.cache.db, not aws_outdir/.cache.db, when the body omits
# outdir. Tests exercise the helper directly (no FastAPI client needed).
# ---------------------------------------------------------------------------

def test_side_outdir_left_platforms_use_sas_outdir(tmp_path, monkeypatch):
    """oracle / hadoop / sas resolve to settings.sas_outdir."""
    import json
    from dtrack.web import app as webapp

    db = tmp_path / "dt.db"
    db.write_text("")
    cfg = tmp_path / "dtrack.json"
    cfg.write_text(json.dumps({
        "settings": {"sas_outdir": "./LEFT/", "aws_outdir": "./RIGHT/"},
        "pairs": {},
    }))
    monkeypatch.setattr(webapp, "_db_path", str(db))
    monkeypatch.setattr(webapp, "_db_dir", str(tmp_path))
    monkeypatch.setattr(webapp, "_config_path", str(cfg))

    for platform in ("oracle", "hadoop", "sas"):
        got = webapp._side_outdir(platform)
        assert got.endswith("LEFT"), (
            f"{platform}: expected sas_outdir (LEFT), got {got!r}"
        )


def test_side_outdir_right_platforms_use_aws_outdir(tmp_path, monkeypatch):
    """aws / athena resolve to settings.aws_outdir."""
    import json
    from dtrack.web import app as webapp

    db = tmp_path / "dt.db"
    db.write_text("")
    cfg = tmp_path / "dtrack.json"
    cfg.write_text(json.dumps({
        "settings": {"sas_outdir": "./LEFT/", "aws_outdir": "./RIGHT/"},
        "pairs": {},
    }))
    monkeypatch.setattr(webapp, "_db_path", str(db))
    monkeypatch.setattr(webapp, "_db_dir", str(tmp_path))
    monkeypatch.setattr(webapp, "_config_path", str(cfg))

    for platform in ("aws", "athena"):
        got = webapp._side_outdir(platform)
        assert got.endswith("RIGHT"), (
            f"{platform}: expected aws_outdir (RIGHT), got {got!r}"
        )


def test_side_outdir_falls_back_when_settings_missing(tmp_path, monkeypatch):
    """No saved settings → './sas/' for left and './csv/' for right.
    Guards against an unsaved-config user run silently picking the wrong
    side and contaminating the other directory's .cache.db."""
    import json
    from dtrack.web import app as webapp

    db = tmp_path / "dt.db"
    db.write_text("")
    cfg = tmp_path / "dtrack.json"
    cfg.write_text(json.dumps({"pairs": {}}))  # no `settings` key
    monkeypatch.setattr(webapp, "_db_path", str(db))
    monkeypatch.setattr(webapp, "_db_dir", str(tmp_path))
    monkeypatch.setattr(webapp, "_config_path", str(cfg))

    assert webapp._side_outdir("oracle").endswith("sas")
    assert webapp._side_outdir("hadoop").endswith("sas")
    assert webapp._side_outdir("aws").endswith("csv")
