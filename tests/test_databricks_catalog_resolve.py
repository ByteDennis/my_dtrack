"""Resolution rules for Databricks conn_macro + DB_DEFAULT_CATALOG.

These run on every host (no Windows skip) because the helper is a
pure-Python string manipulation with no backend dependency. They guard
the contract documented in dtrack/platforms/databricks.py's module
docstring -- in particular, the DB_DEFAULT_CATALOG prepend rule.
"""

from __future__ import annotations

import pytest

from dtrack.platforms.databricks import _resolve_catalog_schema, _full_table


def test_resolve_returns_none_when_macro_is_empty(monkeypatch):
    monkeypatch.delenv("DB_DEFAULT_CATALOG", raising=False)
    assert _resolve_catalog_schema(None) is None
    assert _resolve_catalog_schema("") is None
    assert _resolve_catalog_schema("   ") is None


def test_resolve_passthrough_when_macro_has_dot(monkeypatch):
    monkeypatch.setenv("DB_DEFAULT_CATALOG", "ignored")
    assert _resolve_catalog_schema("main.analytics") == "main.analytics"
    assert _resolve_catalog_schema("  prod.fact_loans  ") == "prod.fact_loans"


def test_resolve_prepends_default_catalog_when_only_schema(monkeypatch):
    monkeypatch.setenv("DB_DEFAULT_CATALOG", "main")
    assert _resolve_catalog_schema("analytics") == "main.analytics"
    assert _resolve_catalog_schema("  trades  ") == "main.trades"


def test_resolve_passes_bare_schema_through_when_default_missing(monkeypatch):
    monkeypatch.delenv("DB_DEFAULT_CATALOG", raising=False)
    assert _resolve_catalog_schema("analytics") == "analytics"


def test_full_table_uses_resolved_catalog_schema(monkeypatch):
    monkeypatch.setenv("DB_DEFAULT_CATALOG", "main")
    tbl = {"conn_macro": "analytics", "table": "pos_daily"}
    assert _full_table(tbl) == "main.analytics.pos_daily"


def test_full_table_keeps_two_part_macro_intact(monkeypatch):
    monkeypatch.setenv("DB_DEFAULT_CATALOG", "ignored")
    tbl = {"conn_macro": "prod.fact_loans", "table": "snapshot"}
    assert _full_table(tbl) == "prod.fact_loans.snapshot"


def test_full_table_returns_bare_table_when_macro_missing(monkeypatch):
    monkeypatch.delenv("DB_DEFAULT_CATALOG", raising=False)
    tbl = {"conn_macro": "", "table": "trades"}
    assert _full_table(tbl) == "trades"
