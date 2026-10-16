import csv
import io

from support import MemoryStore

from dtrack_right.common.manifest import unit_id
from dtrack_right.common.protocol import (
    get_run_status,
    refresh_units_status,
    right_ready_path,
    timestamp,
    write_run_status,
)


def _plan():
    return {
        "pair": "orders",
        "windows": [{"bucket": "2026-01", "partitions": ["2026-01-01"]}],
        "columns": ["amount", "status"],
    }


def _status_rows(store):
    refresh_units_status(store)
    text = store.get_bytes("_status/units.csv").decode()
    return {row["column"]: row for row in csv.DictReader(io.StringIO(text))}


def test_unit_states():
    store = MemoryStore()
    store.put_json("pairs/orders/plan.json", _plan())
    assert {row["state"] for row in _status_rows(store).values()} == {"waiting_left"}

    amount = unit_id("orders", "2026-01", "amount")
    status = unit_id("orders", "2026-01", "status")
    pointer = {
        "pair": "orders",
        "window": "2026-01",
        "column": "amount",
        "right_column": "amount",
    }
    store.commit_unit(1, amount, {"n_total": 1}, pointer)
    assert _status_rows(store)["amount"]["state"] == "left_ready"

    store.put_json(
        right_ready_path("orders", "2026-01"),
        {"pair": "orders", "window": "2026-01", "right_ready_at": timestamp(store)},
    )
    assert {row["state"] for row in _status_rows(store).values()} == {"right_ready"}

    store.put_result(amount, {"pair": "orders", "verdict": "match"})
    store.commit_unit(2, status, {"n_total": 1}, {**pointer, "column": "status"})
    store.put_dead(status, {"pair": "orders", "error": "boom"})
    rows = _status_rows(store)
    assert rows["amount"]["state"] == "complete" and rows["amount"]["verdict"] == "match"
    assert rows["status"]["state"] == "failed" and rows["status"]["error"] == "boom"
    assert rows["amount"]["completed_at"] and rows["status"]["completed_at"]


def test_status_timezone():
    store = MemoryStore()
    write_run_status(
        store,
        {"state": "left_row_check", "run_id": "r", "role": "left"},
        timezone_name="Asia/Tokyo",
    )
    status = get_run_status(store)
    assert status["timezone"] == "Asia/Tokyo" and status["updated_at"].endswith("+09:00")
    write_run_status(store, {"state": "complete", "role": "right"})
    assert get_run_status(store)["timezone"] == "Asia/Tokyo"
    assert get_run_status(store)["updated_at"].endswith("+09:00")
