import pytest
from support import STATS, FakeTarget, MemoryStore, left_document

from dtrack_right.checks import col_check
from dtrack_right.checks.col_check import ColCheck, DrainScheduler, compare_statistics
from dtrack_right.checks.row_check import run_row_check
from dtrack_right.common.manifest import unit_id


def test_compare_statistics_matches_identical_sides():
    result = compare_statistics(pair="p", window="w", column="c", left=STATS, right=STATS)
    assert result["verdict"] == "match" and result["n_total_diff"] == 0
    assert result["mean_diff"] == 0.0 and result["length_avg_match"] is None


def test_compare_statistics_flags_count_float_extreme_and_hash_drift():
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


# >>> numeric extremes compare by value (100 == 100.0); categorical ones stay verbatim (007 != 7) <<< #
def test_numeric_extremes_compare_by_value_and_text_extremes_verbatim():
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


def test_compare_statistics_tolerates_float_noise_and_compares_text_verbatim():
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


def test_scheduler_prioritizes_ready_compare_over_prefetch():
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


def test_scheduler_limits_speculative_prefetch_and_retries_failed_ones_first():
    scheduler = DrainScheduler(prefetch_depth=1)
    scheduler.add_manifest(_manifest())
    assert scheduler.next_task().window == "2026-01-01"
    assert scheduler.next_task() is None
    scheduler.mark_prefetch_failed("p", "2026-01-01")
    assert scheduler.next_task().window == "2026-01-01"
    scheduler.requeue_unit({"pair": "p", "window": "2026-01-01", "unit_id": "u"})
    assert scheduler.next_task().kind == "compare"


def test_scheduler_skips_windows_already_fetched():
    scheduler = DrainScheduler(prefetch_depth=5)
    scheduler.add_manifest(_manifest(windows=("a",)))
    scheduler.mark_right_ready("p", "a", {})
    assert scheduler.next_task() is None


def _drained_store(sample_mode="all"):
    store = MemoryStore()
    doc = left_document(sample=None if sample_mode == "all" else 1)
    store.put_json("row_check/p.left.json", doc)
    store.put_json("row_check/_LEFT_CLOSED", {"pairs": ["p"]})
    run_row_check(store, lambda _cfg: FakeTarget())
    manifest = store.get_json("manifest/p.json")
    uid = unit_id("p", "2026-01-01", "amount")
    pointer = {"pair": "p", "window": "2026-01-01", "column": "amount", "right_column": "amount"}
    store.commit_unit(1, uid, STATS, pointer)
    store.close_manifests(["p"], manifest["expected_units"])
    store.heartbeat()
    return store, uid


def test_col_check_compares_a_unit_and_records_the_result():
    store, uid = _drained_store()
    target = FakeTarget()
    results = ColCheck(store, lambda _cfg: target, poll_interval=0).run()
    assert results[0]["verdict"] == "match" and results[0]["right_col"] == "amount"
    assert store.exists(f"results/{uid}.json")
    assert target.calls == [("p", "2026-01-01", None)]


def test_col_check_waits_for_fixed_sample_keys_before_prefetching():
    store, _uid = _drained_store(sample_mode="fixed")
    target = FakeTarget()
    with pytest.raises(TimeoutError):
        ColCheck(store, lambda _cfg: target, poll_interval=0, idle_timeout=0).run()
    assert target.calls == []
    store.put_json("manifest/p.keys.json", {"pair": "p", "windows": {"2026-01-01": [["7"]]}})
    results = ColCheck(store, lambda _cfg: target, poll_interval=0).run()
    assert results and target.calls[-1] == ("p", "2026-01-01", [["7"]])


def test_col_check_resumes_from_results_already_on_the_store():
    store, _uid = _drained_store()
    ColCheck(store, lambda _cfg: FakeTarget(), poll_interval=0).run()
    target = FakeTarget()
    assert ColCheck(store, lambda _cfg: target, poll_interval=0).run() == []
    assert target.calls == []


def test_a_failing_prefetch_dead_letters_every_unit_of_the_window():
    store, uid = _drained_store()

    class Broken(FakeTarget):
        def window_statistics(self, *_args):
            raise ValueError("boom")

    results = ColCheck(store, lambda _cfg: Broken(), poll_interval=0, max_attempts=2).run()
    assert results == [] and store.get_json(f"dead/{uid}.json")["error"] == "boom"


def test_a_failing_compare_is_retried_then_dead_lettered():
    store, uid = _drained_store()
    target = FakeTarget(stats={"amount": STATS})
    store.objects.pop(f"units/{uid}.json")
    results = ColCheck(store, lambda _cfg: target, poll_interval=0, max_attempts=2).run()
    assert results == [] and store.exists(f"dead/{uid}.json")


def test_col_check_calls_on_idle_before_giving_up():
    store, _uid = _drained_store()
    store.objects.pop("manifest/_CLOSED")
    calls = []

    def on_idle():
        calls.append(1)
        if len(calls) == 2:
            store.close_manifests(["p"], 1)
        return len(calls) <= 2

    results = ColCheck(store, lambda _cfg: FakeTarget(), poll_interval=0, on_idle=on_idle).run()
    assert len(results) == 1 and len(calls) >= 2


def test_col_check_stops_when_the_left_heartbeat_is_stale(monkeypatch):
    store, _uid = _drained_store()
    store.objects.pop("manifest/_CLOSED")
    monkeypatch.setattr(store, "heartbeat_age", lambda now=None: 999.0)
    monkeypatch.setattr(col_check.time, "sleep", lambda _s: None)
    with pytest.raises(RuntimeError, match="heartbeat is stale"):
        ColCheck(store, lambda _cfg: FakeTarget(), poll_interval=0, heartbeat_timeout=10).run()
