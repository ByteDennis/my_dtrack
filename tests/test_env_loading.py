"""Tests for env-file precedence and Oracle macro overrides."""

import json
import os

import pytest

from dtrack.db import (
    MACRO2SVC,
    _load_macro_overrides,
    resolve_oracle_macro,
)


@pytest.fixture
def clean_env(monkeypatch):
    """Strip any test-relevant env so each test starts blank."""
    for k in ("DTRACK_ORACLE_MACROS", "DTRACK_ORACLE_MACROS_FILE",
              "DTRACK_ENV_FILE", "DTRACK_ENV_FILE_LOADED"):
        monkeypatch.delenv(k, raising=False)
    yield


def test_no_overrides_returns_empty_dict(clean_env):
    assert _load_macro_overrides() == {}


def test_inline_blob_parses(clean_env, monkeypatch):
    monkeypatch.setenv("DTRACK_ORACLE_MACROS", "pb40:svc_x,pb50:svc_y")
    extra = _load_macro_overrides()
    assert extra == {"pb40": "svc_x", "pb50": "svc_y"}


def test_inline_blob_skips_malformed_entries(clean_env, monkeypatch):
    monkeypatch.setenv("DTRACK_ORACLE_MACROS", "pb40:svc_x,broken,pb50:svc_y,")
    extra = _load_macro_overrides()
    assert extra == {"pb40": "svc_x", "pb50": "svc_y"}


def test_macro_file_overrides(clean_env, monkeypatch, tmp_path):
    p = tmp_path / "macros.json"
    p.write_text(json.dumps({"pb60": "svc_z"}))
    monkeypatch.setenv("DTRACK_ORACLE_MACROS_FILE", str(p))
    assert _load_macro_overrides() == {"pb60": "svc_z"}


def test_inline_and_file_merge_file_wins(clean_env, monkeypatch, tmp_path):
    p = tmp_path / "macros.json"
    p.write_text(json.dumps({"pb40": "FILE_WINS"}))
    monkeypatch.setenv("DTRACK_ORACLE_MACROS", "pb40:inline,pb50:svc_y")
    monkeypatch.setenv("DTRACK_ORACLE_MACROS_FILE", str(p))
    extra = _load_macro_overrides()
    assert extra == {"pb40": "FILE_WINS", "pb50": "svc_y"}


def test_resolve_returns_builtin_when_no_override(clean_env):
    assert resolve_oracle_macro("pb23") == MACRO2SVC["pb23"]


def test_resolve_returns_override_when_set(clean_env, monkeypatch):
    monkeypatch.setenv("DTRACK_ORACLE_MACROS", "pb99:custom_svc")
    assert resolve_oracle_macro("pb99") == "custom_svc"


def test_resolve_returns_none_for_unknown(clean_env):
    assert resolve_oracle_macro("definitely_not_a_macro") is None


def test_oracle_connect_kwargs_skip_env(monkeypatch):
    """oracle_connect should not error on missing PCDS_USR if user kw is given.

    We can't actually connect (no Oracle), so we patch oracledb.connect to
    capture the args and confirm they came from kwargs, not env.
    """
    monkeypatch.delenv("PCDS_USR", raising=False)
    monkeypatch.delenv("LDAP_BASE", raising=False)
    monkeypatch.delenv("DTRACK_MOCK", raising=False)
    monkeypatch.delenv("DTRACK_ORACLE_MOCK", raising=False)

    captured = {}

    class _FakeOracleDB:
        def connect(self, **kwargs):
            captured.update(kwargs)
            return "connection-handle"

    fake = _FakeOracleDB()
    monkeypatch.setitem(__import__("sys").modules, "oracledb", fake)

    from dtrack.db import oracle_connect
    handle = oracle_connect("pb23", user="alice", password="secret",
                            service="svc_override", ldap_base="")
    assert handle == "connection-handle"
    assert captured == {"user": "alice", "password": "secret", "dsn": "svc_override"}
