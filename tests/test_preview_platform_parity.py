"""Parameterized parity tests for /api/preview.

Verify that:
  1. The preview SQL for every supported source contains dialect markers that
     match what the matching extractor would actually run.
  2. The output_file extension is `.sql` for SQL platforms.
  3. Unsupported sources surface a clear error string, not silent Oracle SQL.

These tests do not hit a real backend — they call `_build_preview_sql` directly
with hand-crafted side configs. The goal is to prevent the "preview was wrong
for Databricks" regression from coming back.
"""

import pytest

from dtrack.web.app import _build_preview_sql


# ----- fixtures ----------------------------------------------------------------

def _side_cfg(source, **overrides):
    cfg = {
        "source":     source,
        "table":      "trades",
        "date_col":   "trade_date",
        "date_type":  "date",
        "conn_macro": {
            "aws":        "analytics_db",
            "oracle":     "pb23",
            "hadoop":     "hive_prod",
            "databricks": "main.warehouse",
            "csv":        "./csv/",
        }.get(source, ""),
        "where":      "",
    }
    cfg.update(overrides)
    return cfg


# ----- per-platform smoke tests -----------------------------------------------

PLATFORM_CASES = [
    # (source, expected_extension, must_contain_substrings, must_not_contain)
    pytest.param(
        "aws", ".sql",
        ["SELECT", "FROM", "row_count", "analytics_db.trades", "GROUP BY"],
        ["connect to oracle", "proc sql"],
        id="aws",
    ),
    pytest.param(
        "hadoop", ".sql",
        ["SELECT", "FROM", "row_count", "hive_prod.trades", "GROUP BY"],
        ["connect to oracle", "proc sql"],
        id="hadoop",
    ),
    pytest.param(
        "databricks", ".sql",
        ["SELECT", "FROM", "row_count", "warehouse.trades", "GROUP BY"],
        ["connect to oracle", "proc sql"],
        id="databricks",
    ),
    pytest.param(
        "csv", ".sql",
        ["SELECT", "row_count"],
        ["connect to oracle", "proc sql"],
        id="csv",
    ),
    pytest.param(
        "oracle", ".sql",
        ["SELECT", "FROM", "ROW_COUNT", "trades"],
        ["connect to oracle", "proc sql"],
        id="oracle-direct",
    ),
]


@pytest.mark.parametrize(
    "source,ext,must_contain,must_not_contain", PLATFORM_CASES
)
def test_preview_dialect_and_extension(source, ext, must_contain, must_not_contain):
    cfg = _side_cfg(source)
    out = _build_preview_sql(
        pair_name="trades_pair",
        side="left",
        cfg=cfg,
        from_date="2026-01-01",
        to_date="2026-04-30",
        parallel=0,
    )
    assert "sql" in out and "output_file" in out
    assert out["output_file"].endswith(ext), (
        f"{source}: expected extension {ext!r}, got {out['output_file']!r}"
    )
    sql_norm = out["sql"].lower()
    for s in must_contain:
        assert s.lower() in sql_norm, (
            f"{source}: expected substring {s!r} missing from preview SQL\n"
            f"--- SQL ---\n{out['sql']}"
        )
    for s in must_not_contain:
        assert s.lower() not in sql_norm, (
            f"{source}: forbidden substring {s!r} found in preview SQL\n"
            f"--- SQL ---\n{out['sql']}"
        )


def test_preview_unsupported_source_does_not_fall_into_oracle():
    """A bogus source should produce an explicit error marker, not silently
    emit an Oracle PROC SQL block. This was the original databricks bug."""
    cfg = _side_cfg("aws")
    cfg["source"] = "totally-made-up"
    out = _build_preview_sql(
        pair_name="p", side="left", cfg=cfg,
        from_date="2026-01-01", to_date="2026-04-30",
    )
    assert "unsupported source" in out["sql"].lower()
    # And critically, must NOT look like Oracle.
    assert "connect to oracle" not in out["sql"].lower()


# ----- cross-engine pair parity ------------------------------------------------

PAIRS = [
    ("aws",        "databricks"),
    ("hadoop",     "databricks"),
    ("oracle",     "aws"),
    ("hadoop",     "aws"),
    ("oracle",     "databricks"),
    ("csv",        "csv"),
]


@pytest.mark.parametrize("left_src,right_src", PAIRS,
                         ids=lambda s: s if isinstance(s, str) else "x")
def test_pair_preview_emits_distinct_sql(left_src, right_src):
    """Both sides of a mixed-engine pair must produce non-empty, distinct SQL
    that does not fall through to the wrong dialect."""
    left  = _build_preview_sql("p", "left",  _side_cfg(left_src),
                                "2026-01-01", "2026-04-30")
    right = _build_preview_sql("p", "right", _side_cfg(right_src),
                                "2026-01-01", "2026-04-30")
    assert left["sql"].strip(), f"empty left preview for {left_src}"
    assert right["sql"].strip(), f"empty right preview for {right_src}"
    if left_src != right_src:
        assert left["sql"] != right["sql"], (
            f"{left_src} vs {right_src}: previews must differ "
            "(if identical, one side fell through to the wrong dialect)"
        )


def test_generate_has_databricks_branch():
    """/api/generate must surface SOMETHING for databricks tables — at minimum
    a log line + discovery — so the user sees databricks activity in the log.
    """
    import inspect
    from dtrack.web import app as web_app

    src = inspect.getsource(web_app.api_generate_files)
    assert "databricks_tables" in src, (
        "/api/generate has no databricks_tables branch — databricks pairs "
        "produce no log output (regression)."
    )
    assert "Databricks" in src or "databricks" in src.lower(), (
        "/api/generate databricks branch should print a status line."
    )
