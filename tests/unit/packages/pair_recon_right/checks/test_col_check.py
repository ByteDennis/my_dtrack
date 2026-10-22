import threading
import time

import pytest
from support import STATS, FakeTarget, MemoryStore, left_document

from pair_recon_right.engine.checks import col_check
from pair_recon_right.engine.checks.col_check import ColCheck, DrainScheduler, compare_statistics
from pair_recon_right.engine.checks.row_check import run_row_check
from pair_recon_right.engine.common.model.manifest import build_manifest, unit_id, unit_id_for
from pair_recon_right.engine.common.model.protocol import list_results


def test_identical():
    result = compare_statistics(pair="p", window="w", column="c", left=STATS, right=STATS)
    assert result["verdict"] == "match" and result["n_total_diff"] == 0
    assert result["mean_diff"] == 0.0 and result["length_avg_match"] is None


def test_stat():
    changed = {
        **STATS,
        "n_unique": 3,
        "mean": 1.6,
        "min_val": "0",
        "hash_sum": "4",
        "top_10": "[]",
    }
    result = compare_statistics(pair="p", window="w", column="c", left=STATS, right=changed)
    assert result["verdict"] == "diff" and result["n_unique_diff"] == 1
    assert result["mean_match"] is False and result["min_match"] is False
    assert result["hash_sum_match"] is False and result["top_10_right"] == "[]"
    selected = compare_statistics(
        pair="p",
        window="w",
        column="c",
        left=STATS,
        right=changed,
        statistics=["n_unique", "hash_sum"],
    )
    assert selected["verdict"] == "diff"
    assert "n_total_diff" not in selected and "mean_match" not in selected
    assert selected["n_unique_diff"] == 1 and selected["hash_sum_match"] is False


def test_extremes():
    left = {**STATS, "col_type": "numeric", "min_val": "100", "max_val": "7.5"}
    right = {**left, "min_val": "100.0", "max_val": "7.50"}
    result = compare_statistics(pair="p", window="w", column="c", left=left, right=right)
    assert result["min_match"] is True and result["max_match"] is True
    text = {**STATS, "col_type": "categorical", "min_val": "007"}
    result = compare_statistics(
        pair="p", window="w", column="c", left=text, right={**text, "min_val": "7"}
    )
    assert result["min_match"] is False and result["verdict"] == "diff"
    blank = {**left, "min_val": None}
    assert (
        compare_statistics(pair="p", window="w", column="c", left=blank, right=right)["min_match"]
        is False
    )


def test_float():
    right = {**STATS, "mean": 1.5 + 1e-9, "std": "n/a"}
    result = compare_statistics(
        pair="p", window="w", column="c", left={**STATS, "std": "n/a"}, right=right
    )
    assert result["mean_match"] is True and result["std_match"] is True
    assert result["std_diff"] is None and result["verdict"] == "match"
    blank = compare_statistics(pair="p", window="w", column="c", left={}, right={})
    assert blank["n_total_left"] == 0 and blank["min_left"] == "" and blank["col_type"] == ""


def _manifest(pair="p", windows=("2026-01-01", "2026-02-01")):
    return {"pair": pair, "windows": [{"bucket": w, "partitions": [w]} for w in windows]}


def test_ready():
    scheduler = DrainScheduler()
    scheduler.add_manifest(_manifest())
    scheduler.add_manifest(_manifest())
    scheduler.add_unit({"pair": "p", "window": "2026-02-01", "unit_id": "u"})
    first = scheduler.next_task()
    assert (first.kind, first.window) == ("prefetch", "2026-02-01")
    scheduler.mark_right_ready("p", "2026-02-01", {"amount": STATS})
    assert scheduler.next_task().kind == "compare"
    scheduler.add_unit({"pair": "p", "window": "2026-02-01", "unit_id": "v"})
    assert scheduler.next_task().payload["unit_id"] == "v"


def test_retry():
    scheduler = DrainScheduler(prefetch_depth=1)
    scheduler.add_manifest(_manifest())
    assert scheduler.next_task().window == "2026-01-01"
    assert scheduler.next_task() is None
    scheduler.mark_prefetch_failed("p", "2026-01-01")
    assert scheduler.next_task().window == "2026-01-01"
    scheduler.requeue_unit({"pair": "p", "window": "2026-01-01", "unit_id": "u"})
    assert scheduler.next_task().kind == "compare"


def test_fetched():
    scheduler = DrainScheduler(prefetch_depth=5)
    scheduler.add_manifest(_manifest(windows=("a",)))
    scheduler.mark_right_ready("p", "a", {})
    assert scheduler.next_task() is None


def _drained_store(sample_mode="all"):
    store = MemoryStore()
    doc = left_document(sample=None if sample_mode == "all" else 1)
    store.put_json("row_check/left.json", doc)
    store.put_json("_status/left_row_check.closed.json", {"pairs": ["p"]})
    run_row_check(store, lambda _cfg: FakeTarget())
    manifest = store.get_json("plan.json")
    uid = unit_id("p", "2026-01-01", "amount")
    pointer = {"pair": "p", "window": "2026-01-01", "column": "amount", "right_column": "amount"}
    store.commit_units(1, [{**pointer, "unit_id": uid, "left_statistics": STATS}])
    store.close_manifests(["p"], manifest["expected_units"])
    store.heartbeat()
    return store, uid


def test_unit(caplog):
    store, uid = _drained_store()
    target = FakeTarget()
    with caplog.at_level("INFO", logger="pair_recon.right.col_check"):
        results = ColCheck(store, lambda _cfg: target, poll_interval=0).run()
    assert results[0]["verdict"] == "match" and results[0]["right_col"] == "amount"
    assert [value["unit_id"] for _key, value in list_results(store)] == [uid]
    assert len(store.list_keys("col_check/result_batches/")) == 1
    assert target.calls == [("p", "2026-01-01", None)]
    assert f"B right stats p 2026-01-01: 1 column(s); first amount = {STATS}" in caplog.text
    assert (
        "A compare p 2026-01-01 amount -> MATCH [numeric] n_total=2|2 n_missing=0|0 "
        "n_unique=2|2 mean=1.5|1.5 std=0.7|0.7 min=1|1 max=2|2 "
        "length_avg=None|None length_max=None|None hash_sum=3|3"
    ) in caplog.text


def test_result_detail():
    changed = {**STATS, "n_unique": 3, "mean": 1.6}
    result = compare_statistics(pair="p", window="w", column="c", left=STATS, right=changed)
    assert col_check.result_detail(result, diff_only=True) == "n_unique=2|3 mean=1.5|1.6"
    same = compare_statistics(pair="p", window="w", column="c", left=STATS, right=STATS)
    assert col_check.result_detail(same, diff_only=True) == ""
    assert col_check.result_detail(same).startswith("n_total=2|2 n_missing=0|0 n_unique=2|2")


def test_right_worker_budget_runs_window_queries_in_parallel():
    pair = {
        "version": 2,
        "left": {"source": "oracle", "date_col": "dt"},
        "right": {"source": "databricks", "date_col": "dt"},
        "col_map": {"amount": "amount"},
        "vintage": "month",
    }
    row_check = {
        "gate_decision": {"passed": True},
        "partitions": {
            "2026-01-01": {"left": 1, "right": 1, "result": "match"},
            "2026-02-01": {"left": 1, "right": 1, "result": "match"},
        },
    }
    manifest = build_manifest("run", "p", pair, row_check)
    store = MemoryStore()
    store.put_json("plan.json", manifest)
    for sequence, window in enumerate(manifest["windows"], 1):
        uid = unit_id_for(manifest, window, "amount")
        store.commit_units(
            sequence,
            [
                {
                    "pair": "p",
                    "window": window["bucket"],
                    "column": "amount",
                    "unit_id": uid,
                    "left_statistics": STATS,
                }
            ],
        )
    store.close_manifests(["p"], manifest["expected_units"], plan_ids={"p": manifest["plan_id"]})
    store.heartbeat()
    lock = threading.Lock()
    active = 0
    peak = 0

    class ParallelTarget(FakeTarget):
        def window_statistics(self, *_args):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            time.sleep(0.05)
            with lock:
                active -= 1
            return {"amount": STATS}

    results = ColCheck(
        store,
        lambda _cfg: ParallelTarget(),
        poll_interval=0,
        right_workers=2,
    ).run()

    assert len(results) == 2
    assert peak == 2


def test_fixed():
    store, _uid = _drained_store(sample_mode="fixed")
    target = FakeTarget()
    with pytest.raises(TimeoutError):
        ColCheck(store, lambda _cfg: target, poll_interval=0, idle_timeout=0).run()
    assert target.calls == []
    store.put_json("fixed_keys.json", {"pair": "p", "windows": {"2026-01-01": [["7"]]}})
    results = ColCheck(store, lambda _cfg: target, poll_interval=0).run()
    assert results and target.calls[-1] == ("p", "2026-01-01", [["7"]])


def test_resume():
    store, _uid = _drained_store()
    ColCheck(store, lambda _cfg: FakeTarget(), poll_interval=0).run()
    target = FakeTarget()
    assert ColCheck(store, lambda _cfg: target, poll_interval=0).run() == []
    assert target.calls == []


def test_v2():
    pair = {
        "version": 2,
        "left": {"source": "oracle", "date_col": "dt"},
        "right": {"source": "databricks", "date_col": "dt"},
        "col_map": {"amount": "amount"},
        "vintage": "month",
    }
    row_check = {
        "gate_decision": {"passed": True},
        "partitions": {
            "2026-01-01": {"left": 1, "right": 1, "result": "match"},
            "2026-01-02": {"left": 1, "right": 2, "result": "count_diff"},
            "2026-02-01": {"left": 1, "right": 1, "result": "match"},
        },
    }
    store = MemoryStore()
    first = build_manifest("run", "p", pair, row_check)
    store.put_json("plan.json", first)
    for sequence, window in enumerate(first["windows"], 1):
        uid = unit_id_for(first, window, "amount")
        store.commit_units(
            sequence,
            [
                {
                    "pair": "p",
                    "window": window["bucket"],
                    "column": "amount",
                    "unit_id": uid,
                    "left_statistics": STATS,
                }
            ],
        )
    store.close_manifests(["p"], 2, plan_ids={"p": first["plan_id"]})
    ColCheck(store, lambda _cfg: FakeTarget(), poll_interval=0).run()

    second = build_manifest("run", "p", {**pair, "excluded_partitions": ["2026-01-02"]}, row_check)
    store.put_json("plan.json", second)
    january = second["windows"][0]
    changed_uid = unit_id_for(second, january, "amount")
    store.commit_units(
        3,
        [
            {
                "pair": "p",
                "window": january["bucket"],
                "column": "amount",
                "unit_id": changed_uid,
                "left_statistics": STATS,
            }
        ],
    )
    store.close_manifests(["p"], 2, plan_ids={"p": second["plan_id"]})
    target = FakeTarget()
    results = ColCheck(store, lambda _cfg: target, poll_interval=0).run()
    assert len(results) == 1
    assert target.calls == [("p", "2026-01-01", None)]


def test_prefetch():
    store, uid = _drained_store()

    class Broken(FakeTarget):
        def window_statistics(self, *_args):
            raise ValueError("boom")

    results = ColCheck(store, lambda _cfg: Broken(), poll_interval=0, max_attempts=2).run()
    failure = store.get_json(f"col_check/failures/{uid}.json")
    assert results == [] and failure["error"] == "boom"


def test_compare():
    store, uid = _drained_store()
    target = FakeTarget(stats={"amount": STATS})
    unit_key = next(key for key in store.objects if key.startswith("units/"))
    store.objects[unit_key]["units"][0].pop("left_statistics")
    results = ColCheck(store, lambda _cfg: target, poll_interval=0, max_attempts=2).run()
    assert results == [] and store.exists(f"col_check/failures/{uid}.json")


def test_poll_discovers_late_lower_sequence_batch():
    pair = {
        "version": 2,
        "left": {"source": "oracle", "date_col": "dt"},
        "right": {"source": "databricks", "date_col": "dt"},
        "col_map": {"amount": "amount", "status": "status"},
    }
    row_check = {
        "gate_decision": {"passed": True},
        "partitions": {"2026-01-01": {"left": 1, "right": 1, "result": "match"}},
    }
    manifest = build_manifest("run", "p", pair, row_check)
    window = manifest["windows"][0]
    amount = unit_id_for(manifest, window, "amount")
    status = unit_id_for(manifest, window, "status")
    store = MemoryStore()
    store.put_json("plan.json", manifest)
    store.commit_units(
        2,
        [
            {
                "pair": "p",
                "window": window["bucket"],
                "column": "status",
                "unit_id": status,
                "left_statistics": STATS,
            }
        ],
    )
    checker = ColCheck(store, lambda _cfg: FakeTarget(), poll_interval=0)
    checker.poll()
    store.commit_units(
        1,
        [
            {
                "pair": "p",
                "window": window["bucket"],
                "column": "amount",
                "unit_id": amount,
                "left_statistics": STATS,
            }
        ],
    )
    checker.poll()

    assert checker.seen == {amount, status}


def test_idle():
    store, _uid = _drained_store()
    store.objects["_state/left.json"].pop("col_check_closed_at")
    store.objects.pop("_status/col_check.closed.json")
    calls = []

    def on_idle():
        calls.append(1)
        if len(calls) == 2:
            store.close_manifests(["p"], 1)
        return len(calls) <= 2

    results = ColCheck(store, lambda _cfg: FakeTarget(), poll_interval=0, on_idle=on_idle).run()
    assert len(results) == 1 and len(calls) >= 2


def test_stale(monkeypatch):
    store, _uid = _drained_store()
    store.objects["_state/left.json"].pop("col_check_closed_at")
    store.objects.pop("_status/col_check.closed.json")
    monkeypatch.setattr(store, "heartbeat_age", lambda now=None: 999.0)
    monkeypatch.setattr(col_check.time, "sleep", lambda _s: None)
    with pytest.raises(RuntimeError, match="heartbeat is stale"):
        ColCheck(store, lambda _cfg: FakeTarget(), poll_interval=0, heartbeat_timeout=10).run()
