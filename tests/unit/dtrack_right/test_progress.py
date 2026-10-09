from datetime import UTC, datetime, timedelta

from support import MemoryStore

from dtrack_right import progress
from dtrack_right.backends.store import VolumeStore
from dtrack_right.common.protocol import (
    left_row_path,
    plan_path,
    row_result_path,
    write_run_status,
)

NOW = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)


def _run(store, *, state="right_col_check", heartbeat=None, dead=0, gate=True):
    store.put_json(
        left_row_path("orders"), {"run_id": "r1", "pair": "orders", "counts": {"2026-01-01": 5}}
    )
    store.put_json(
        row_result_path("orders"),
        {
            "run_id": "r1",
            "pair": "orders",
            "summary": {"match": 3, "count_diff": 1, "left_only": 0, "right_only": 0},
            "gate_decision": {"policy": "auto", "passed": gate, "excluded_ratio": 0.25},
        },
    )
    store.put_json(
        plan_path("orders"), {"pair": "orders", "expected_units": 4, "windows": [], "columns": []}
    )
    store.put_json(left_row_path("later"), {"run_id": "r1", "pair": "later", "counts": {}})
    store.put_json(
        "_status/left_row_check.closed.json",
        {"pairs": ["later", "orders"], "closed_at": "2026-10-09T10:00:00+00:00"},
    )
    store.close_row_check(["orders"])
    for sequence, uid in enumerate(["u1", "u2", "u3"], start=1):
        store.commit_unit(
            sequence,
            uid,
            {"v": sequence},
            {"pair": "orders", "window": "w", "column": f"c{sequence}"},
        )
    store.put_result("u1", {"verdict": "match"})
    store.put_result("u2", {"verdict": "diff"})
    if dead:
        store.put_dead("u3", {"error": "boom"})
    write_run_status(store, {"state": state, "role": "right", "run_id": "r1"})
    store.heartbeat(
        (NOW - timedelta(minutes=heartbeat if heartbeat is not None else 1)).timestamp()
    )
    return store


def test_snapshot_reads_counts_gate_units_and_heartbeat_from_the_run_documents():
    rows = {
        row["pair_name"]: row
        for row in progress.snapshot(_run(MemoryStore(), dead=1), "r1", now=NOW)
    }
    orders, later = rows["orders"], rows["later"]
    assert set(orders) == set(progress.SNAPSHOT_COLUMNS)
    assert (
        orders["snapshot_ts"] == "2026-10-09T12:00:00+00:00" and orders["run_mode"] == "distributed"
    )
    assert (orders["row_days_total"], orders["row_days_match"], orders["row_days_count_diff"]) == (
        4,
        3,
        1,
    )
    assert orders["gate_passed"] is True and orders["gate_excluded_ratio"] == 0.25
    assert orders["units_expected"] == 4 and orders["units_published"] == 3
    assert orders["units_compared"] == 2 and orders["units_dead"] == 1 and orders["units_diff"] == 1
    assert orders["first_unit_at"] and orders["last_result_at"] and orders["row_done_at"]
    assert (
        orders["heartbeat_at"] == "2026-10-09T11:59:00+00:00"
        and orders["report_published"] is False
    )
    assert (
        later["units_expected"] is None
        and later["row_done_at"] is None
        and later["row_left_done_at"]
    )


def test_snapshot_prefers_the_units_status_csv_when_present():
    store = _run(MemoryStore())
    store.put_bytes(
        "_status/units.csv",
        b"pair,unit_id,state,left_ready_at,completed_at,verdict\norders,u9,complete,t1,t2,match\n",
        content_type="text/csv",
    )
    row = next(r for r in progress.snapshot(store, "r1", now=NOW) if r["pair_name"] == "orders")
    assert (row["units_published"], row["units_compared"], row["units_diff"]) == (1, 1, 0)


def test_auto_blockers_open_and_resolve_deterministically():
    rows = progress.snapshot(_run(MemoryStore(), dead=1, heartbeat=45, gate=False), "r1", now=NOW)
    orders = [r for r in rows if r["pair_name"] == "orders"]
    stuck = {("r1", "orders"): (NOW - timedelta(hours=3)).isoformat()}
    blockers = {
        b["category"]: b for b in progress.auto_blockers(orders, now=NOW, last_progress_at=stuck)
    }
    open_rules = {name for name, b in blockers.items() if b["resolved_at"] is None}
    assert open_rules == {"GATE_FAILED", "DEAD_UNITS", "STALE_HEARTBEAT", "NO_PROGRESS"}
    assert (
        blockers["DEAD_UNITS"]["severity"] == "P1"
        and blockers["GATE_FAILED"]["stage"] == "ROW_CHECK"
    )
    assert blockers["ROW_COUNT_DIFF"]["resolved_at"] == blockers["ROW_COUNT_DIFF"]["updated_at"]
    again = progress.auto_blockers(orders, now=NOW + timedelta(minutes=15), last_progress_at=stuck)
    assert {b["blocker_id"] for b in again} == {b["blocker_id"] for b in blockers.values()}
    healthy = progress.snapshot(_run(MemoryStore()), "r1", now=NOW)
    assert all(
        b["resolved_at"]
        for b in progress.auto_blockers(healthy, now=NOW)
        if b["category"] != "ROW_COUNT_DIFF"
    )


def test_collect_skips_runs_complete_for_longer_than_a_day_and_survives_a_broken_run(tmp_path):
    root = VolumeStore(tmp_path)
    _run(root.child("active"))
    old = _run(root.child("old"), state="complete")
    old.put_json(
        "_status/run.json",
        {**old.get_json("_status/run.json"), "updated_at": (NOW - timedelta(days=3)).isoformat()},
    )
    (tmp_path / "broken" / "_status").mkdir(parents=True)
    (tmp_path / "broken" / "_status" / "run.json").write_text("{not json")
    rows = progress.collect([str(tmp_path)], now=NOW)
    by_run = {}
    for row in rows:
        by_run.setdefault(row["run_id"], []).append(row)
    assert set(by_run) == {"active", "broken"}
    assert by_run["broken"][0]["collector_error"] and by_run["broken"][0]["pair_name"] == "*"
    assert by_run["active"][0]["run_root"] == f"{tmp_path}/active"


def test_sql_renders_literals_casts_and_the_manual_row_guard():
    rows = progress.snapshot(_run(MemoryStore()), "r1", now=NOW)
    insert = progress.insert_snapshot_sql("c.s", rows)
    assert insert.startswith("INSERT INTO c.s.dtrack_progress_snapshot (snapshot_ts, run_id")
    assert (
        "CAST('2026-10-09T12:00:00+00:00' AS TIMESTAMP)" in insert
        and "NULL" in insert
        and "true" in insert
    )
    merge = progress.merge_blockers_sql("c.s", progress.auto_blockers(rows, now=NOW))
    assert "t.source = 'AUTO'" in merge and "WHEN NOT MATCHED AND s.resolved_at IS NULL" in merge
    assert [s.split("(")[0].strip() for s in progress.ddl("c.s")] == [
        "CREATE TABLE IF NOT EXISTS c.s.dtrack_pair_plan",
        "CREATE TABLE IF NOT EXISTS c.s.dtrack_progress_snapshot",
        "CREATE TABLE IF NOT EXISTS c.s.dtrack_pair_blocker",
    ]
    status, daily = progress.views("c.s")
    for token in (
        "'NOT_STARTED'",
        "'STALLED'",
        "'AT_RISK'",
        "eta_confidence",
        "progress_pct",
        "WHERE p.active",
    ):
        assert token in status
    assert daily.startswith("CREATE OR REPLACE VIEW c.s.v_pair_daily")
    plan = progress.plan_rows_from_config({"pairs": {"a": {}, "b": {"skip": True}}}, now=NOW)
    assert [r["pair_name"] for r in plan] == [
        "a"
    ] and "WHEN NOT MATCHED" in progress.merge_plan_sql("c.s", plan)
    assert progress.insert_snapshot_sql("c.s", []) == ""
