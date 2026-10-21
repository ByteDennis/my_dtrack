import pytest
from support import MemoryStore

from pair_recon.app.catalog import Catalog
from pair_recon_right.catalog import error_rows


class Sql:
    def __init__(self):
        self.calls = []

    def execute(self, statement, parameters=None):
        self.calls.append((statement, parameters or {}))
        return []


def test_pairs_load_companion_metadata():
    class MetadataSql(Sql):
        def execute(self, statement, parameters=None):
            self.calls.append((statement, parameters or {}))
            if "WHERE enabled = true" in statement:
                return [{"pair_name": "orders", "config_json": {"left": {}, "right": {}}}]
            if "pair_recon_column_map" in statement:
                return [
                    {
                        "pair_name": "orders",
                        "left_column": "AMOUNT",
                        "right_column": "amount",
                        "data_type": "numeric",
                    }
                ]
            if "pair_recon_pair_sql" in statement:
                return [
                    {
                        "pair_name": "orders",
                        "left_sql": "SELECT 1",
                        "right_sql": None,
                        "vintage_sample_size": 3,
                        "vintage_values": None,
                    }
                ]
            return []

    sql = MetadataSql()
    catalog = Catalog(sql, "c.s.pairs", "c.s.status", "c.s.errors", "c.s.progress")
    assert catalog.column_map_table == "c.s.pair_recon_column_map"
    assert catalog.pair_sql_table == "c.s.pair_recon_pair_sql"
    row = catalog.pairs()[0]
    assert row["column_map_rows"][0]["left_column"] == "AMOUNT"
    assert row["pair_sql"]["left_sql"] == "SELECT 1"
    assert row["pair_sql"]["vintage_sample_size"] == 3


def test_configuration_rows_include_disabled_pairs_and_sql():
    class MetadataSql(Sql):
        def execute(self, statement, parameters=None):
            self.calls.append((statement, parameters or {}))
            if "FROM c.s.pairs ORDER BY" in statement:
                return [
                    {"pair_name": "disabled", "enabled": False},
                    {"pair_name": "orders", "enabled": True},
                ]
            if "pair_recon_pair_sql" in statement:
                return [{"pair_name": "orders", "right_sql": "SELECT 1"}]
            return []

    catalog = Catalog(MetadataSql(), "c.s.pairs", "c.s.status", "c.s.errors", "c.s.progress")

    assert catalog.configuration_rows() == [
        {"pair_name": "disabled", "enabled": False, "pair_sql": None},
        {
            "pair_name": "orders",
            "enabled": True,
            "pair_sql": {"pair_name": "orders", "right_sql": "SELECT 1"},
        },
    ]


def test_pairs_reject_duplicate_metadata():
    class DuplicateSql(Sql):
        def execute(self, statement, parameters=None):
            if "WHERE enabled = true" in statement:
                return [{"pair_name": "p", "config_json": {}}]
            if "pair_recon_column_map" in statement:
                row = {
                    "pair_name": "p",
                    "left_column": "A",
                    "right_column": "a",
                    "data_type": None,
                }
                return [row, row]
            return []

    with pytest.raises(ValueError, match="duplicate column mapping"):
        Catalog(DuplicateSql(), "c.s.pairs", "c.s.status", "c.s.errors", "c.s.progress").pairs()


@pytest.mark.parametrize("state", ["running", "complete", "left_complete", "failed"])
def test_status(state):
    sql = Sql()
    catalog = Catalog(
        sql,
        "main.recon.pair_meta",
        "main.recon.run_status",
        "main.recon.run_error",
        "main.recon.progress_snapshot",
    )
    catalog.status(
        run_id="run-1",
        pair="orders",
        owner="me",
        state=state,
        timezone="America/New_York",
    )
    statement, values = sql.calls[-1]
    assert "MERGE INTO main.recon.run_status" in statement
    assert values["run_id"] == "run-1" and values["state"] == state
    assert (values["ended"] is not None) == (state in {"left_complete", "failed"})


def test_progress():
    sql = Sql()
    catalog = Catalog(sql, "c.s.pair_meta", "c.s.run_status", "c.s.run_error", "c.s.progress")
    catalog.progress(
        {
            "snapshot_ts": "2026-10-18T10:00:00",
            "run_id": "run-1",
            "pair_name": "orders",
            "state": "left_col_check",
            "units_expected": 4,
            "units_ready": 2,
            "units_complete": 1,
            "units_failed": 0,
            "worker_threads": 3,
            "detail": "{}",
        }
    )
    statement, values = sql.calls[-1]
    assert "INSERT INTO c.s.progress" in statement
    assert values["worker_threads"] == 3


def test_finish():
    sql = Sql()
    store = MemoryStore()
    store.put_json(
        "pairs/orders/row_check/result.json",
        {"partitions": {"all": {"left": 2, "right": 1, "verdict": "count_diff"}}},
    )
    catalog = Catalog(sql, "c.s.pairs", "c.s.status", "c.s.errors", "c.s.progress")
    result = catalog.finish(
        store,
        run_id="r1",
        pair="orders",
        owner="me",
        state="different",
        timezone="America/New_York",
    )
    statements = [statement for statement, _values in sql.calls]
    assert result == {"status": "different", "errors": 1}
    assert any("INSERT INTO c.s.errors" in statement for statement in statements)
    assert any("UPDATE c.s.pairs" in statement for statement in statements)


def test_error_rows_use_attribute_registry():
    from datetime import UTC, datetime

    store = MemoryStore()
    store.put_json(
        "pairs/orders/col_check/results/u.json",
        {
            "pair": "orders",
            "vintage": "2026-01",
            "column": "AMOUNT",
            "mean_left": 1.0,
            "mean_right": 1.1,
            "mean_match": False,
            "n_unique_left": 2,
            "n_unique_right": 2,
            "n_unique_diff": 0,
        },
    )
    rows = error_rows(store, "r1", "orders", datetime(2026, 1, 1, tzinfo=UTC))
    assert [(row["check_name"], row["left_value"], row["right_value"]) for row in rows] == [
        ("mean", "1.0", "1.1")
    ]
