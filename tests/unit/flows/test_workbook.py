import json

import pytest
from openpyxl import load_workbook

from pair_recon.core.metadata.workbook import (
    PAIR_WORKBOOK_COLUMNS,
    create_pair_workbook,
    read_pair_workbook,
    upsert_pair_configuration,
)


class Sql:
    def __init__(self):
        self.calls = []

    def execute(self, statement, parameters=None):
        self.calls.append((statement, parameters or {}))
        return []


def test_create_and_read_pair_workbook(tmp_path):
    path = create_pair_workbook(tmp_path / "pairs.xlsx")
    workbook = load_workbook(path)
    assert workbook.sheetnames == ["pairs"]
    assert list(workbook["pairs"].tables) == ["PairReconPairs"]
    sheet = workbook["pairs"]
    sheet.append(
        [
            "orders",
            True,
            '{"right":{"source":"databricks"},"left":{"source":"oracle"}}',
            "SELECT * FROM L",
            "SELECT * FROM R",
            3,
            None,
        ]
    )
    workbook.save(path)

    rows = read_pair_workbook(path)

    assert rows == [
        {
            "pair_name": "orders",
            "enabled": True,
            "config_json": ('{"left":{"source":"oracle"},"right":{"source":"databricks"}}'),
            "left_sql": "SELECT * FROM L",
            "right_sql": "SELECT * FROM R",
            "vintage_sample_size": 3,
            "vintage_values": None,
        }
    ]


def test_pair_workbook_validation(tmp_path):
    path = create_pair_workbook(tmp_path / "pairs.xlsx")
    workbook = load_workbook(path)
    sheet = workbook["pairs"]
    sheet.append(["orders", "yes", "{}", None, None, 2, '["2026-01-01"]'])
    workbook.save(path)

    with pytest.raises(ValueError, match="not both"):
        read_pair_workbook(path)


def test_upsert_pair_configuration_preserves_runtime_columns():
    sql = Sql()
    rows = [
        {
            "pair_name": "orders",
            "enabled": True,
            "config_json": "{}",
            "left_sql": None,
            "right_sql": "SELECT 1",
            "vintage_sample_size": None,
            "vintage_values": ["2026-01-01"],
        }
    ]

    result = upsert_pair_configuration(sql, "main.recon.pair_recon_pair_meta", rows)

    assert result["rows"] == 1
    assert result["pair_sql_table"] == "main.recon.pair_recon_pair_sql"
    assert len(sql.calls) == 2
    meta_sql, meta_values = sql.calls[0]
    pair_sql, pair_values = sql.calls[1]
    assert "latest_run_id, latest_status" in meta_sql
    assert "t.latest_run_id" not in meta_sql and "t.latest_status" not in meta_sql
    assert "MERGE INTO main.recon.pair_recon_pair_sql" in pair_sql
    assert meta_values["enabled"] == "true"
    assert json.loads(pair_values["vintage_values"]) == ["2026-01-01"]


def test_pair_workbook_columns_are_stable():
    assert PAIR_WORKBOOK_COLUMNS == (
        "pair_name",
        "enabled",
        "config_json",
        "left_sql",
        "right_sql",
        "vintage_sample_size",
        "vintage_values",
    )
