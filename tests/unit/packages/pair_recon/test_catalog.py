import pytest
from support import MemoryStore

from pair_recon.app.catalog import Catalog


class Sql:
    def __init__(self):
        self.calls = []

    def execute(self, statement, parameters=None):
        self.calls.append((statement, parameters or {}))
        return []


@pytest.mark.parametrize("state", ["running", "complete", "failed"])
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
