"""source=hadoop vs source=hadoop_sas split.

Contract (improvement 2):
  * source=hadoop      -> direct hadoop_connect; yields extract_*.sql only.
  * source=hadoop_sas  -> SAS code-gen (gen_sas); yields extract_*.sas only.

These two are mutually exclusive: a hadoop table no longer produces BOTH a
.sas and a .sql. Also pins the DESCRIBE-based column discovery for Databricks
and the hadoop direct path.
"""

import json
import os
import tempfile

import pytest

from dtrack.web.app import _build_preview_sql
from dtrack.platforms.databricks import _discover_columns_databricks


# ----- preview routes the file extension by source -----------------------------

def _cfg(source):
    return {
        "source": source, "table": "trades", "date_col": "trade_date",
        "date_type": "date", "conn_macro": "hive_prod", "where": "",
    }


def test_preview_hadoop_yields_sql():
    out = _build_preview_sql("p", "left", _cfg("hadoop"),
                             "2026-01-01", "2026-04-30")
    assert out["output_file"] == "extract_row.sql"


def test_preview_hadoop_sas_yields_sas():
    out = _build_preview_sql("p", "left", _cfg("hadoop_sas"),
                             "2026-01-01", "2026-04-30")
    assert out["output_file"] == "extract_row.sas"
    assert "hive_prod.trades" in out["sql"]


# ----- gen_sas includes hadoop_sas, excludes plain hadoop ----------------------

@pytest.fixture
def _sas_env(monkeypatch):
    for k, v in {
        "PCDS_USR": "alice", "PB23_PWD": "x", "EMAIL_TO": "t@e.com",
        "SAS_LIB": "/tmp/saslib", "OUT_DIR": "/tmp/saslib", "SEED": "42",
        "HDP_SERVER": "hadoop.example.com",
        "HDP_URI": "jdbc:hive2://hadoop.example.com:10000/usrf",
    }.items():
        monkeypatch.setenv(k, v)
    yield


def test_gen_sas_handles_hadoop_sas_only(_sas_env, tmp_path):
    cfg = {
        "pairs": {
            "demo": {
                "left": {
                    "name": "viasas", "table": "VIASAS_TBL", "source": "hadoop_sas",
                    "conn_macro": "hdp", "date_col": "DT", "date_type": "date",
                    "columns": {"AMT": "DOUBLE"},
                },
                "right": {
                    "name": "snk", "table": "snk_ds", "source": "sas",
                    "conn_macro": "work", "date_col": "DT", "date_type": "date",
                    "columns": {"AMT": "NUMBER"},
                },
                "fromDate": "2025-01-01", "toDate": "2025-01-31",
            },
            "direct": {
                "left": {
                    "name": "directhdp", "table": "DIRECT_TBL", "source": "hadoop",
                    "conn_macro": "hdp", "date_col": "DT", "date_type": "date",
                    "columns": {"AMT": "DOUBLE"},
                },
                "right": {
                    "name": "snk2", "table": "snk2_ds", "source": "sas",
                    "conn_macro": "work", "date_col": "DT", "date_type": "date",
                    "columns": {"AMT": "NUMBER"},
                },
                "fromDate": "2025-01-01", "toDate": "2025-01-31",
            },
        },
        "metadata": {},
    }
    cfg_path = tmp_path / "cfg.json"
    cfg_path.write_text(json.dumps(cfg))
    out_dir = tmp_path / "out"
    out_dir.mkdir()

    from dtrack.platforms.hadoop import gen_sas
    gen_sas(str(cfg_path), str(out_dir), types=["row"])

    sas_files = sorted(out_dir.glob("*.sas"))
    assert sas_files, "gen_sas produced no .sas files"
    sas = sas_files[0].read_text()
    # hadoop_sas table is generated; the direct-hadoop table is NOT.
    assert "VIASAS_TBL" in sas
    assert "DIRECT_TBL" not in sas


# ----- Databricks DESCRIBE column discovery ------------------------------------

class _FakeCursor:
    def __init__(self, rows):
        self._rows = rows
        self.executed = []

    def execute(self, sql):
        self.executed.append(sql)

    def fetchall(self):
        return self._rows


def test_databricks_describe_discovery_stops_at_partition_section():
    rows = [
        ("amt", "double", ""),
        ("ccy", "string", ""),
        ("", "", ""),                       # blank separator
        ("# Partition Information", "", ""),
        ("dt", "date", ""),                 # partition key — must be excluded
    ]
    cur = _FakeCursor(rows)
    cols = _discover_columns_databricks(cur, "main", "warehouse", "trades")
    assert cols == {"amt": "double", "ccy": "string"}
    assert cur.executed == ["DESCRIBE `main`.`warehouse`.`trades`"]


def test_hadoop_describe_returns_empty_in_mock_mode(monkeypatch):
    monkeypatch.setenv("DTRACK_MOCK", "1")
    from dtrack.platforms.hadoop import _discover_columns_hadoop
    assert _discover_columns_hadoop("hive_prod", "trades") == {}
