import csv
import io
from datetime import datetime

from support import MemoryStore

from pair_recon_right.engine.common.model.manifest import build_manifest, unit_id_for, window_id
from pair_recon_right.engine.common.model.protocol import (
    UNITS_STATUS,
    get_run_status,
    list_units,
    plan_path,
    refresh_units_status,
    right_ready_path,
    timestamp,
    unit_prefix,
    write_run_status,
)


def _plan():
    return build_manifest(
        "run",
        "orders",
        {
            "left": {"source": "oracle", "date_col": "dt"},
            "right": {"source": "databricks", "date_col": "dt"},
            "col_map": {"amount": "amount", "status": "status"},
            "vintage": "month",
        },
        {
            "gate_decision": {"passed": True},
            "partitions": {"2026-01-01": {"left": 1, "right": 1, "result": "match"}},
        },
    )


def _status_rows(store):
    refresh_units_status(store)
    text = store.get_bytes(UNITS_STATUS).decode()
    return {row["column"]: row for row in csv.DictReader(io.StringIO(text))}


def test_unit():
    store = MemoryStore()
    plan = _plan()
    store.put_json(plan_path("orders"), plan)
    assert {row["state"] for row in _status_rows(store).values()} == {"waiting_left"}

    window = plan["windows"][0]
    amount = unit_id_for(plan, window, "amount")
    status = unit_id_for(plan, window, "status")
    pointer = {
        "pair": "orders",
        "window": window["bucket"],
        "column": "amount",
        "right_column": "amount",
    }
    store.commit_units(
        1,
        [{**pointer, "unit_id": amount, "left_statistics": {"n_total": 1}}],
    )
    assert _status_rows(store)["amount"]["state"] == "left_ready"

    store.put_json(
        right_ready_path("orders", window["bucket"]),
        {
            "pair": "orders",
            "window": window["bucket"],
            "window_id": window_id(plan, window),
            "right_ready_at": timestamp(store),
        },
    )
    assert {row["state"] for row in _status_rows(store).values()} == {"right_ready"}

    store.put_result(amount, {"pair": "orders", "verdict": "match"})
    store.commit_units(
        2,
        [
            {
                **pointer,
                "column": "status",
                "unit_id": status,
                "left_statistics": {"n_total": 1},
            }
        ],
    )
    store.put_dead(status, {"pair": "orders", "error": "boom"})
    rows = _status_rows(store)
    assert rows["amount"]["state"] == "complete" and rows["amount"]["verdict"] == "match"
    assert rows["status"]["state"] == "failed" and rows["status"]["error"] == "boom"
    assert rows["amount"]["completed_at"] and rows["status"]["completed_at"]


def test_unit_batch_expands_to_logical_units():
    store = MemoryStore()
    units = [
        {
            "pair": "orders",
            "window": "2026-01",
            "column": column,
            "unit_id": column,
            "left_statistics": {"n_total": 1},
        }
        for column in ("amount", "status")
    ]
    store.commit_units(1, units)

    assert store.list_keys(unit_prefix("orders")) == [f"{unit_prefix('orders')}00000001-0002.json"]
    assert [row["unit_id"] for _key, row in list_units(store)] == ["amount", "status"]


def test_status():
    store = MemoryStore()
    write_run_status(
        store,
        {"state": "left_row_check", "run_id": "r", "role": "left"},
        timezone_name="Asia/Tokyo",
    )
    status = get_run_status(store)
    assert status["timezone"] == "Asia/Tokyo" and status["updated_at"].endswith("+09:00")
    assert datetime.fromisoformat(status["updated_at"]).microsecond == 0
    write_run_status(store, {"state": "complete", "role": "right"})
    assert get_run_status(store)["timezone"] == "Asia/Tokyo"
    assert get_run_status(store)["updated_at"].endswith("+09:00")


def test_environment_timezone_default(monkeypatch):
    monkeypatch.setenv("PAIR_RECON_TIMEZONE", "Asia/Tokyo")
    store = MemoryStore()

    write_run_status(store, {"state": "started"})

    status = get_run_status(store)
    assert status["timezone"] == "Asia/Tokyo"
    assert status["updated_at"].endswith("+09:00")
    assert timestamp().endswith("+09:00")


def test_sample_window_selection_uses_explicit_preference_order():
    from pair_recon_left.common.model.protocol import select_sample_window as left_select
    from pair_recon_right.engine.common.model.protocol import select_sample_window as right_select

    for select in (left_select, right_select):
        available = ["2026-01-01", "2026-02-01"]
        assert select({}, available) == "2026-01-01"
        assert select({"sample_partitions": ["2026-02-01"]}, available) == "2026-02-01"
        assert (
            select({"sample_partitions": ["2026-03-01", "2026-02-01"]}, available) == "2026-02-01"
        )
        assert select({"sample_partitions": ["2026-03-01"]}, available) is None
