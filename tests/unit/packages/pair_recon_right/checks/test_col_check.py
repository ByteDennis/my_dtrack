import csv
import gzip
import io
import threading
import time

import pytest
from support import STATS, FakeTarget, MemoryStore, left_document

from pair_recon_right.engine.checks import col_check
from pair_recon_right.engine.checks.col_check import ColCheck, DrainScheduler, compare_statistics
from pair_recon_right.engine.checks.row_check import run_row_check
from pair_recon_right.engine.common.model.manifest import build_manifest, unit_id, unit_id_for
from pair_recon_right.engine.common.model.protocol import (
    DIAGNOSTIC_PREFIX,
    LEFT_STATE,
    RESULT_BATCH_PREFIX,
    failure_path,
    fixed_keys_path,
    get_right_state,
    left_row_path,
    list_results,
    plan_path,
    right_sample_prefix,
    right_stats_path,
    unit_prefix,
    window_count_path,
    write_left_state,
)


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
    doc = left_document(sample={"all": None, "fixed": 1}.get(sample_mode, 0.5))
    store.put_json(left_row_path("p"), doc)
    write_left_state(store, {"pairs": ["p"], "row_check_closed_at": "now"})
    run_row_check(store, lambda _cfg: FakeTarget())
    manifest = store.get_json(plan_path("p"))
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
    assert len(store.list_keys(RESULT_BATCH_PREFIX)) == 1
    assert target.calls == [("p", "2026-01-01", None)]
    assert f"B right stats p 2026-01-01: 1 column(s); first amount = {STATS}" in caplog.text
    assert (
        "A compare p 2026-01-01 amount -> MATCH [numeric] n_total=2|2 n_missing=0|0 "
        "n_unique=2|2 mean=1.5|1.5 std=0.7|0.7 min=1|1 max=2|2 "
        "length_avg=None|None length_max=None|None hash_sum=3|3"
    ) in caplog.text


def test_only_different_columns_persist_a_right_sample():
    store, _uid = _drained_store()

    class SampleTarget(FakeTarget):
        def window_sample(self, _manifest, _window, limit):
            return {
                "pairs": [
                    {
                        "left_column": "id",
                        "right_column": "id",
                        "context": True,
                    },
                    {
                        "left_column": "amount",
                        "right_column": "amount",
                        "context": False,
                    },
                ],
                "rows": [[1, 99]],
                "limit": limit,
            }

    target = SampleTarget(stats={"amount": {**STATS, "n_unique": 9}})
    ColCheck(store, lambda _cfg: target, poll_interval=0).run()

    prefix = right_sample_prefix("p", "2026-01-01")
    manifest = store.get_json(f"{prefix}manifest.json")
    assert manifest["right_rows"] == 1
    body = gzip.decompress(store.get_bytes(f"{prefix}right.csv.gz"))
    assert list(csv.reader(io.StringIO(body.decode()))) == [["id", "amount"], ["1", "99"]]

    matched_store, _uid = _drained_store()
    ColCheck(matched_store, lambda _cfg: SampleTarget(), poll_interval=0).run()
    assert matched_store.list_keys(DIAGNOSTIC_PREFIX) == []


def test_stats_only_stages_right_statistics_and_sample_without_results():
    store, _uid = _drained_store()

    class SampleTarget(FakeTarget):
        def window_sample(self, _manifest, _window, limit):
            return {
                "pairs": [
                    {
                        "left_column": "amount",
                        "right_column": "amount",
                        "context": False,
                    }
                ],
                "rows": [[2]],
                "limit": limit,
            }

    ColCheck(store, lambda _cfg: SampleTarget(), poll_interval=0, stats_only=True).run()

    assert list_results(store) == []
    assert store.get_json(right_stats_path("p", "2026-01-01"))["statistics"] == {"amount": STATS}
    assert store.exists(f"{right_sample_prefix('p', '2026-01-01')}right.csv.gz")
    assert get_right_state(store)["phase"] == "stats_ready"


def test_total_mismatch_stops_the_right_after_recording_the_diff():
    store, uid = _drained_store(sample_mode="fraction")
    target = FakeTarget(stats={"amount": {**STATS, "n_total": 5}})
    with pytest.raises(col_check.SampleCountMismatch) as info:
        ColCheck(store, lambda _cfg: target, poll_interval=0).run()
    message = str(info.value)
    assert "p 2026-01-01: Left counted 2 rows but Right counted 5" in message
    assert "sampled key sets differ" in message and "keys " in message
    stored = dict(list_results(store))
    assert [value["unit_id"] for value in stored.values()] == [uid]
    assert next(iter(stored.values()))["verdict"] == "diff"
    same_total = FakeTarget(stats={"amount": {**STATS, "n_unique": 9}})
    store, _uid = _drained_store(sample_mode="fraction")
    results = ColCheck(store, lambda _cfg: same_total, poll_interval=0).run()
    assert results[0]["verdict"] == "diff"


def test_first_window_count_is_compared_before_statistics():
    class CountingTarget(FakeTarget):
        def __init__(self, count, **options):
            super().__init__(**options)
            self.count = count
            self.counts = []

        def window_count(self, manifest, window):
            self.counts.append((manifest["pair"], window["bucket"]))
            return self.count

    store, uid = _drained_store(sample_mode="fraction")
    store.put_json(window_count_path("p", "2026-01-01"), {"n_total": 2})
    target = CountingTarget(5)
    with pytest.raises(col_check.SampleCountMismatch) as info:
        ColCheck(store, lambda _cfg: target, poll_interval=0).run()
    assert "Left counted 2 rows but Right counted 5" in str(info.value)
    assert "compared before statistics" in str(info.value)
    assert target.counts == [("p", "2026-01-01")] and target.calls == []
    assert list_results(store) == []

    store, uid = _drained_store(sample_mode="fraction")
    store.put_json(window_count_path("p", "2026-01-01"), {"n_total": 2})
    agreeing = CountingTarget(2)
    results = ColCheck(store, lambda _cfg: agreeing, poll_interval=0).run()
    assert [row["unit_id"] for row in results] == [uid]
    assert agreeing.counts == [("p", "2026-01-01")] and len(agreeing.calls) == 1

    store, uid = _drained_store(sample_mode="fraction")
    absent = CountingTarget(9)
    results = ColCheck(store, lambda _cfg: absent, poll_interval=0).run()
    assert [row["unit_id"] for row in results] == [uid] and absent.counts == [("p", "2026-01-01")]


def test_total_gate_works_without_n_total_in_checks():
    manifest = {"pair": "p", "sample": {"mode": "fraction", "rate": 0.1}, "right_key_cols": ["id"]}
    left = {**STATS, "n_total": 2}
    right = {**STATS, "n_total": 3}
    result = compare_statistics(
        pair="p", window="w", column="c", left=left, right=right, statistics=["n_unique"]
    )
    assert result["verdict"] == "match" and "n_total_left" not in result
    with pytest.raises(
        col_check.SampleCountMismatch, match="Left counted 2 rows but Right counted 3"
    ):
        col_check.require_same_total(manifest, "w", "c", left, right)
    col_check.require_same_total(manifest, "w", "c", left, {**STATS, "n_total": 2})
    col_check.require_same_total(manifest, "w", "c", {"n_unique": 1}, right)


def test_progress_bar_is_plain_text_on_stdout(capsys):
    assert col_check.tqdm.__module__ == "tqdm.std"
    store, _uid = _drained_store()
    ColCheck(store, lambda _cfg: FakeTarget(), poll_interval=0).run()
    captured = capsys.readouterr()
    assert "Right comparison" in captured.out and "received=1/1" in captured.out
    assert "Right comparison" not in captured.err


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
    store.put_json(plan_path("p"), manifest)
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
        def window_statistics(self, *_args, **_kwargs):
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


def test_fixed(monkeypatch):
    store, _uid = _drained_store(sample_mode="fixed")
    target = FakeTarget()
    monkeypatch.setattr(store, "heartbeat_age", lambda now=None: 999.0)
    monkeypatch.setattr(col_check.time, "sleep", lambda _s: None)
    with pytest.raises(RuntimeError, match="heartbeat is stale"):
        ColCheck(store, lambda _cfg: target, poll_interval=0, heartbeat_timeout=10).run()
    assert target.calls == []
    monkeypatch.setattr(store, "heartbeat_age", lambda now=None: 0.0)
    store.put_json(fixed_keys_path("p"), {"pair": "p", "windows": {"2026-01-01": [["7"]]}})
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
    store.put_json(plan_path("p"), first)
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
    store.put_json(plan_path("p"), second)
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
        def window_statistics(self, *_args, **_kwargs):
            raise ValueError("boom")

    results = ColCheck(store, lambda _cfg: Broken(), poll_interval=0, max_attempts=2).run()
    failure = store.get_json(failure_path("p", uid))
    assert results == [] and failure["error"] == "boom"


def test_compare():
    store, uid = _drained_store()
    target = FakeTarget(stats={"amount": STATS})
    unit_key = next(key for key in store.objects if key.startswith(unit_prefix("p")))
    store.objects[unit_key]["units"][0].pop("left_statistics")
    results = ColCheck(store, lambda _cfg: target, poll_interval=0, max_attempts=2).run()
    assert results == [] and store.exists(failure_path("p", uid))


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
    store.put_json(plan_path("p"), manifest)
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
    store.objects[LEFT_STATE].pop("col_check_closed_at")
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
    store.objects[LEFT_STATE].pop("col_check_closed_at")
    monkeypatch.setattr(store, "heartbeat_age", lambda now=None: 999.0)
    monkeypatch.setattr(col_check.time, "sleep", lambda _s: None)
    with pytest.raises(RuntimeError, match="heartbeat is stale"):
        ColCheck(store, lambda _cfg: FakeTarget(), poll_interval=0, heartbeat_timeout=10).run()


def test_unreadable_heartbeat_does_not_fail_the_right(monkeypatch, caplog):
    store, _uid = _drained_store()
    store.objects[LEFT_STATE].pop("col_check_closed_at")
    reads = []

    def flaky(now=None):
        reads.append(1)
        if len(reads) == 1:
            raise OSError("IncompleteRead(0 bytes read, 481 more expected)")
        store.close_manifests(["p"], 1)
        return 0.0

    monkeypatch.setattr(store, "heartbeat_age", flaky)
    monkeypatch.setattr(col_check.time, "sleep", lambda _s: None)
    with caplog.at_level("WARNING", logger="pair_recon.right.col_check"):
        results = ColCheck(store, lambda _cfg: FakeTarget(), poll_interval=0).run()
    assert len(results) == 1 and len(reads) >= 2
    assert "left heartbeat unreadable" in caplog.text


# >>> batch progress reaches _protocol/right.json while a window is still being fetched <<< #
def test_statistics_progress_is_published_while_fetching():
    store, _uid = _drained_store()
    seen = []

    class Reporting(FakeTarget):
        def window_statistics(self, manifest, window, progress=None):
            progress(1, 1)
            seen.append(get_right_state(store)["fetching"])
            return self.stats

    ColCheck(store, lambda _cfg: Reporting(), poll_interval=0).run()
    assert seen[0][0]["pair"] == "p" and seen[0][0]["columns_done"] == 1
    assert seen[0][0]["columns_total"] == 1 and get_right_state(store)["fetching"] == []
