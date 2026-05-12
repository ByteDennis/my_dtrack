"""Hadoop-source tests for the SAS code-generation path.

Originally exercised both Oracle and Hadoop libname / approx-distinct behaviour
via gen_sas. The Oracle path was retired when Oracle moved to the Python-direct
extraction module (dtrack.platforms.oracle), so Oracle-source assertions here
were deleted along with that code. The remaining test pins the Hadoop-side
APPROX_DISTINCT toggle, which still flows through gen_sas + column_hadoop.sas.
"""

import json

import pytest


@pytest.fixture
def isolated_env(monkeypatch):
    monkeypatch.setenv("PCDS_USR", "alice")
    monkeypatch.setenv("PB23_PWD", "topsecret")
    monkeypatch.setenv("EMAIL_TO", "test@example.com")
    monkeypatch.setenv("SAS_LIB", "/tmp/saslib")
    monkeypatch.setenv("OUT_DIR", "/tmp/saslib")
    monkeypatch.setenv("SEED", "42")
    yield


def test_approx_distinct_on_hadoop(isolated_env, tmp_path, monkeypatch):
    """DTRACK_APPROX_DISTINCT=1 + Hadoop source -> APPROX_DISTINCT in generated SAS."""
    monkeypatch.setenv("DTRACK_APPROX_DISTINCT", "1")
    monkeypatch.setenv("HDP_SERVER", "hadoop.example.com")
    monkeypatch.setenv("HDP_URI", "jdbc:hive2://hadoop.example.com:10000/usrf")
    cfg = {
        "pairs": {
            "demo": {
                "left": {
                    "name": "src", "table": "MYTBL", "source": "hadoop",
                    "conn_macro": "hdp", "date_col": "DT", "date_type": "date",
                    "columns": {"AMT": "DOUBLE"},
                },
                "right": {
                    "name": "snk", "table": "snk_ds", "source": "sas",
                    "conn_macro": "work", "date_col": "DT", "date_type": "date",
                    "columns": {"AMT": "NUMBER"},
                },
                "fromDate": "2025-01-01", "toDate": "2025-01-31",
            }
        },
        "metadata": {},
    }
    cfg_path = tmp_path / "cfg.json"
    cfg_path.write_text(json.dumps(cfg))
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    from dtrack.platforms.hadoop import gen_sas
    gen_sas(str(cfg_path), str(out_dir), types=["col"])

    sas_files = sorted(out_dir.glob("*.sas"))
    assert sas_files, "gen_sas produced no .sas files"
    sas = sas_files[0].read_text()
    assert "APPROX_DISTINCT(" in sas
    assert "ELSE AMT END" in sas
