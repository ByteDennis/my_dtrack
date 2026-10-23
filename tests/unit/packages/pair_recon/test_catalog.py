from datetime import UTC, datetime

import pytest
from support import MemoryStore

from pair_recon.app.catalog import Catalog
from pair_recon_right.catalog import (
    ERROR_COLUMNS,
    STATUS_RUN_COLUMNS,
    STATUS_SUMMARY_COLUMNS,
    append_error_rows,
    error_rows,
    result_summary,
    status_run_values,
)


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


@pytest.mark.parametrize(
    "state",
    ["running", "complete", "passed", "different", "left_complete", "failed", "cancelled"],
)
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
        from_date="2026-10-01",
        to_date="2026-10-21",
        vintage="month",
        sample_rate=0.1,
        vintage_sample="5",
    )
    statement, values = sql.calls[-1]
    assert "MERGE INTO main.recon.run_status" in statement
    assert values["run_id"] == "run-1" and values["state"] == state
    assert (values["ended"] is not None) == (
        state in {"passed", "different", "left_complete", "failed", "cancelled"}
    )
    assert "ended_at, row_total" in statement
    assert "CAST(:ended AS TIMESTAMP_NTZ)" in statement
    assert set(STATUS_RUN_COLUMNS) <= values.keys()
    assert values["sample_rate"] == 0.1 and values["vintage_sample"] == "5"
    assert values["row_total"] == 0 and values["column_differences"] == 0


def test_fail_updates_terminal_metadata_without_result_summary():
    sql = Sql()
    catalog = Catalog(sql, "c.s.pairs", "c.s.status", "c.s.errors", "c.s.progress")

    result = catalog.fail(
        run_id="r1",
        pair="orders",
        owner="me",
        state="failed",
        timezone="America/New_York",
        detail="Right task failed",
    )

    statements = [statement for statement, _values in sql.calls]
    status_statement, status_values = sql.calls[0]
    assert result == {"status": "failed", "errors": 1}
    assert "MERGE INTO c.s.status" in status_statement
    assert "row_total" not in status_statement
    assert not set(STATUS_SUMMARY_COLUMNS) & status_values.keys()
    assert "DELETE FROM c.s.errors" in statements[1]
    assert "INSERT INTO c.s.errors" in statements[2]
    assert "UPDATE c.s.pairs" in statements[3]
    assert sql.calls[2][1]["detail"] == "Right task failed"


def test_clear_removes_all_rows_for_daily_run_id():
    sql = Sql()
    catalog = Catalog(sql, "c.s.pair_meta", "c.s.run_status", "c.s.run_error", "c.s.progress")

    catalog.clear("orders-me-20261021")

    assert [statement for statement, _values in sql.calls] == [
        "DELETE FROM c.s.run_error WHERE run_id=:run_id",
        "DELETE FROM c.s.progress WHERE run_id=:run_id",
        "DELETE FROM c.s.run_status WHERE run_id=:run_id",
    ]
    assert all(values == {"run_id": "orders-me-20261021"} for _statement, values in sql.calls)


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
            "payload": "{}",
        }
    )
    statement, values = sql.calls[-1]
    assert "MERGE INTO c.s.progress" in statement
    assert "ON t.run_id = s.run_id" in statement
    assert values["worker_threads"] == 3


def test_status_run_values_exposes_effective_selection():
    assert status_run_values(
        {
            "fromDate": "2026-01-01",
            "toDate": "2026-03-31",
            "vintage": "month",
            "sample": 0.1,
            "vintage_sample_size": 5,
        }
    ) == {
        "from_date": "2026-01-01",
        "to_date": "2026-03-31",
        "vintage": "month",
        "sample_rate": 0.1,
        "vintage_sample": "5",
    }
    assert status_run_values({"sample": False, "vintage_values": ["2026-01-01"]}) == {
        "from_date": None,
        "to_date": None,
        "vintage": None,
        "sample_rate": 1.0,
        "vintage_sample": '["2026-01-01"]',
    }


def test_finish():
    sql = Sql()
    store = MemoryStore()
    store.put_json(
        "row_check/result.json",
        {"partitions": {"all": {"left": 2, "right": 1, "result": "count_diff"}}},
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
    status_values = next(
        values for statement, values in sql.calls if "MERGE INTO c.s.status" in statement
    )
    assert status_values["row_total"] == 1
    assert status_values["row_differences"] == 1
    assert any("INSERT INTO c.s.errors" in statement for statement in statements)
    assert any("UPDATE c.s.pairs" in statement for statement in statements)


def test_result_summary_counts_row_column_and_runtime_outcomes():
    store = MemoryStore()
    store.put_json(
        "row_check/result.json",
        {
            "partitions": {
                "2026-01-01": {"result": "match"},
                "2026-01-02": {"result": "count_diff"},
            }
        },
    )
    store.put_json("col_check/results/a.json", {"verdict": "match"})
    store.put_json("col_check/results/b.json", {"verdict": "diff"})
    store.put_json("col_check/failures/c.json", {"error": "boom"})

    assert result_summary(store) == {
        "row_total": 2,
        "row_matches": 1,
        "row_differences": 1,
        "column_total": 2,
        "column_matches": 1,
        "column_differences": 1,
        "runtime_failures": 1,
    }


def test_result_summary_excludes_stale_outcomes_when_current_plan_has_no_units():
    store = MemoryStore()
    store.put_json("plan.json", {"pair": "p", "windows": [], "columns": []})
    store.put_json(
        "col_check/results/stale.json",
        {"unit_id": "stale", "pair": "p", "verdict": "diff", "mean_match": False},
    )
    store.put_json(
        "col_check/failures/stale.json",
        {"unit_id": "stale-failure", "pair": "p", "error": "old"},
    )

    assert result_summary(store)["column_total"] == 0
    assert result_summary(store)["runtime_failures"] == 0
    assert error_rows(store, "r1", "p", datetime(2026, 1, 1, tzinfo=UTC)) == []


def test_error_rows_use_attribute_registry():
    store = MemoryStore()
    store.put_json(
        "col_check/results/u.json",
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


def test_append_error_rows_uses_target_schema_for_all_null_columns():
    class Writer:
        def __init__(self):
            self.saved = None

        def mode(self, value):
            assert value == "append"
            return self

        def saveAsTable(self, value):
            self.saved = value

    class Frame:
        schema = object()

        def __init__(self):
            self.write = Writer()

        def select(self, *columns):
            assert columns == ERROR_COLUMNS
            return self

    class Spark:
        def __init__(self):
            self.target = Frame()
            self.created = None

        def table(self, name):
            assert name == "c.s.errors"
            return self.target

        def createDataFrame(self, rows, schema=None):
            assert schema is self.target.schema
            self.created = Frame()
            return self.created

    rows = [
        {
            "run_id": "r1",
            "check_name": "row_count",
            "pair_name": "orders",
            "vintage": None,
            "column_name": None,
            "partition_value": "2026-10-02",
            "left_value": "10",
            "right_value": "11",
            "detail": "count_diff",
            "payload": "{}",
            "created_at": datetime(2026, 10, 22, tzinfo=UTC),
        }
    ]
    spark = Spark()

    append_error_rows(spark, "c.s.errors", rows)

    assert spark.created.write.saved == "c.s.errors"
