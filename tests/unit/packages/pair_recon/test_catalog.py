from datetime import UTC, datetime

import pytest
from support import MemoryStore

from pair_recon.app.catalog import Catalog
from pair_recon_right.catalog import (
    ERROR_COLUMNS,
    STATUS_RUN_COLUMNS,
    STATUS_SUMMARY_COLUMNS,
    WINDOW_COLUMNS,
    append_rows,
    error_rows,
    result_summary,
    run_state,
    status_run_values,
    window_rows,
)
from pair_recon_right.engine.common.model.manifest import unit_id_for


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
        "DELETE FROM c.s.pair_recon_window_history WHERE run_id=:run_id",
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
    assert result == {"status": "different", "errors": 1, "windows": 0}
    status_values = next(
        values for statement, values in sql.calls if "MERGE INTO c.s.status" in statement
    )
    assert status_values["row_total"] == 1
    assert status_values["row_differences"] == 1
    assert any("INSERT INTO c.s.errors" in statement for statement in statements)
    assert "DELETE FROM c.s.pair_recon_window_history WHERE run_id=:run_id" in statements
    assert not any("INSERT INTO c.s.pair_recon_window_history" in s for s in statements)
    assert any("UPDATE c.s.pairs" in statement for statement in statements)


def _completed_window_store(*, fail_column=None, diff_column=None, count_diff=False):
    store = MemoryStore()
    plan = {
        "pair": "orders",
        "run_id": "r1",
        "version": 2,
        "vintage": "month",
        "partition_mode": "date",
        "left": {"source": "oracle"},
        "right": {"source": "databricks"},
        "sample": {"mode": "fraction", "rate": 0.1},
        "columns": ["AMOUNT", "QTY"],
        "col_map": {"AMOUNT": "amount", "QTY": "qty"},
        "excluded_partitions": ["2026-01-09", "2026-01-10"],
        "windows": [
            {"bucket": "2026-01-01", "partitions": ["2026-01-02", "2026-01-03"], "n_left": 5},
            {"bucket": "2026-02-01", "partitions": ["2026-02-01"], "n_left": 3},
        ],
        "expected_units": 4,
    }
    store.put_json("plan.json", plan)
    partitions = {
        "2026-01-02": {"result": "match"},
        "2026-01-03": {"result": "count_diff" if count_diff else "match"},
        "2026-02-01": {"result": "match"},
    }
    store.put_json(
        "row_check/result.json",
        {"pair": "orders", "partitions": partitions, "date_synonyms": {}},
    )
    for window in plan["windows"]:
        for column in plan["columns"]:
            uid = unit_id_for(plan, window, column)
            if window["bucket"] == "2026-01-01" and column == fail_column:
                store.put_json(f"col_check/failures/{uid}.json", {"unit_id": uid, "error": "x"})
                continue
            verdict = (
                "diff" if window["bucket"] == "2026-01-01" and column == diff_column else "match"
            )
            store.put_json(
                f"col_check/results/{uid}.json",
                {"unit_id": uid, "pair": "orders", "verdict": verdict},
            )
    return store


def test_window_rows_record_only_fully_compared_windows():
    now = datetime(2026, 1, 31, 12, 0, tzinfo=UTC)
    rows = window_rows(_completed_window_store(), "r1", "orders", "me", now)
    assert [(row["window_value"], row["result"]) for row in rows] == [
        ("2026-01-01", "match"),
        ("2026-02-01", "match"),
    ]
    first = rows[0]
    assert tuple(first) == WINDOW_COLUMNS
    assert (first["partitions"], first["excluded_partitions"], first["columns"]) == (2, 2, 2)
    assert (first["units"], first["matches"], first["differences"]) == (2, 2, 0)
    assert first["sample_rate"] == 0.1 and first["checked_at"] == now
    assert (first["vintage"], first["run_id"], first["owner"]) == ("month", "r1", "me")

    different = window_rows(_completed_window_store(diff_column="QTY"), "r1", "orders", "me", now)
    assert [(row["window_value"], row["result"], row["differences"]) for row in different] == [
        ("2026-01-01", "different", 1),
        ("2026-02-01", "match", 0),
    ]
    failed = window_rows(_completed_window_store(fail_column="QTY"), "r1", "orders", "me", now)
    assert [row["window_value"] for row in failed] == ["2026-02-01"]
    unmatched = window_rows(_completed_window_store(count_diff=True), "r1", "orders", "me", now)
    assert [row["window_value"] for row in unmatched] == ["2026-02-01"]


def test_finish_inserts_window_history_rows():
    sql = Sql()
    store = _completed_window_store()
    catalog = Catalog(sql, "c.s.pairs", "c.s.status", "c.s.errors", "c.s.progress")
    result = catalog.finish(
        store,
        run_id="r1",
        pair="orders",
        owner="me",
        state="passed",
        timezone="America/New_York",
    )
    assert result == {"status": "passed", "errors": 0, "windows": 2}
    statement, values = next(
        (statement, values)
        for statement, values in sql.calls
        if "INSERT INTO c.s.pair_recon_window_history" in statement
    )
    assert "CAST(:r0_checked_at AS TIMESTAMP_NTZ)" in statement
    assert values["r0_window_value"] == "2026-01-01" and values["r1_window_value"] == "2026-02-01"
    assert values["r0_result"] == "match" and values["r0_pair_name"] == "orders"


def test_finish_and_clear_survive_a_missing_window_history_table(caplog):
    class NoHistorySql(Sql):
        def execute(self, statement, parameters=None):
            super().execute(statement, parameters)
            if "pair_recon_window_history" in statement:
                raise RuntimeError("[TABLE_OR_VIEW_NOT_FOUND] c.s.pair_recon_window_history")
            return []

    sql = NoHistorySql()
    catalog = Catalog(sql, "c.s.pairs", "c.s.status", "c.s.errors", "c.s.progress")
    with caplog.at_level("ERROR"):
        result = catalog.finish(
            _completed_window_store(),
            run_id="r1",
            pair="orders",
            owner="me",
            state="passed",
            timezone="America/New_York",
        )
        catalog.clear("r1")
    assert result == {"status": "passed", "errors": 0, "windows": 0}
    statements = [statement for statement, _values in sql.calls]
    assert any("UPDATE c.s.pairs" in statement for statement in statements)
    assert statements[-1] == "DELETE FROM c.s.status WHERE run_id=:run_id"
    assert caplog.text.count("window history not written") == 2


def test_checked_windows_skip_the_current_run():
    class HistorySql(Sql):
        def execute(self, statement, parameters=None):
            super().execute(statement, parameters)
            return [{"window_value": "2026-02-01"}, {"window_value": "2026-01-01"}]

    sql = HistorySql()
    catalog = Catalog(sql, "c.s.pairs", "c.s.status", "c.s.errors", "c.s.progress")
    assert catalog.checked_windows("orders", "month", exclude_run_id="orders-me-20261024") == [
        "2026-01-01",
        "2026-02-01",
    ]
    statement, values = sql.calls[-1]
    assert statement == (
        "SELECT DISTINCT window_value FROM c.s.pair_recon_window_history "
        "WHERE pair_name=:pair AND vintage=:vintage AND run_id<>:run_id"
    )
    assert values == {"pair": "orders", "vintage": "month", "run_id": "orders-me-20261024"}


@pytest.mark.parametrize(
    ("outcome", "state"),
    [
        ({"row_errors": 0, "column_errors": 0, "failures": 0}, "passed"),
        ({"row_errors": 0, "column_errors": 2, "failures": 0}, "different"),
        ({"row_errors": 1, "column_errors": 0, "failures": 0}, "different"),
        ({"row_errors": 0, "column_errors": 2, "failures": 1}, "incomplete"),
        ({}, "passed"),
    ],
)
def test_run_state_marks_runtime_failures_incomplete(outcome, state):
    assert run_state(outcome) == state


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


def test_append_rows_uses_target_schema_for_all_null_columns():
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

    append_rows(spark, "c.s.errors", ERROR_COLUMNS, rows)

    assert spark.created.write.saved == "c.s.errors"
