import pytest
from support import MemoryStore

from dtrack_right.col_check import ColCheck
from dtrack_right.manifest import unit_id
from dtrack_right.row_check import run_row_check
from dtrack_right.scheduler import DrainScheduler
from dtrack_right.store import VolumeStore, resolve_run

STATS = {
    "col_type": "numeric",
    "n_total": 2,
    "n_missing": 0,
    "n_unique": 2,
    "hash_sum": "3",
    "mean": 1.5,
    "std": 0.7,
    "min_val": "1",
    "max_val": "2",
    "length_avg": None,
    "length_max": None,
    "top_10": "",
}


def left_doc(pair="p", counts=None, sample=0.5):
    return {
        "run_id": "run",
        "pair": pair,
        "left": {"source": "oracle", "table": "A", "date_col": "dt"},
        "right": {"source": "databricks", "table": "a", "date_col": "dt"},
        "bounds": {"from": "2026-01-01", "to": "2026-01-03"},
        "date_synonyms": {},
        "counts": counts or {"2026-01-01": 1, "2026-01-02": 1, "2026-01-03": 5},
        "pair_config": {
            "left": {"source": "oracle", "table": "A", "date_col": "dt"},
            "right": {"source": "databricks", "table": "a", "date_col": "dt"},
            "col_map": {"amount": "amount"},
            "vintage": "month",
            "key": "id",
            "sample": sample,
        },
        "timing": {"left_count_s": 0.1},
    }


class FakeTarget:
    def __init__(self, counts=None, stats=None):
        self.counts = counts or {"2026-01-01": 1, "2026-01-02": 2}
        self.stats = stats or {"amount": STATS}
        self.calls = []

    def preflight(self):
        return {"ok": True}

    def partition_counts(self, *_args):
        return self.counts

    def window_statistics(self, manifest, window):
        self.calls.append((manifest["pair"], window["bucket"], window.get("key_tuples")))
        return self.stats


def test_right_row_check_reconciles_pending_left_documents_and_writes_manifest_with_holes():
    store = MemoryStore()
    store.put_json("row_check/p.left.json", left_doc())
    store.put_json("row_check/_LEFT_CLOSED", {"pairs": ["p"]})
    assert run_row_check(store, lambda _cfg: FakeTarget()) == 1
    doc = store.get_json("row_check/p.json")
    assert doc["summary"] == {"match": 1, "count_diff": 1, "left_only": 1, "right_only": 0}
    manifest = store.get_json("manifest/p.json")
    assert manifest["windows"][0]["partitions"] == ["2026-01-01"]
    assert store.exists("row_check/_CLOSED") and store.exists("reports/p_compare_row.csv")
    assert run_row_check(store, lambda _cfg: FakeTarget()) == 0


def test_right_row_check_manual_gate_reports_without_manifest():
    store = MemoryStore()
    store.put_json("row_check/p.left.json", left_doc())
    run_row_check(store, lambda _cfg: FakeTarget(), gate="manual")
    assert store.exists("row_check/p.json") and not store.exists("manifest/p.json")


def test_scheduler_prioritizes_ready_compare_over_prefetch():
    scheduler = DrainScheduler()
    manifest = {"pair": "p", "windows": [{"bucket": "2026-01-01", "partitions": ["2026-01-01"]}]}
    scheduler.add_manifest(manifest)
    scheduler.add_unit({"pair": "p", "window": "2026-01-01", "unit_id": "u"})
    assert scheduler.next_task().kind == "prefetch"
    scheduler.mark_right_ready("p", "2026-01-01", {"amount": STATS})
    assert scheduler.next_task().kind == "compare"


def _drained_store(sample_mode="all"):
    store = MemoryStore()
    store.put_json("row_check/p.left.json", left_doc(sample=None if sample_mode == "all" else 1))
    store.put_json("row_check/_LEFT_CLOSED", {"pairs": ["p"]})
    run_row_check(store, lambda _cfg: FakeTarget())
    manifest = store.get_json("manifest/p.json")
    uid = unit_id("p", "2026-01-01", "amount")
    store.commit_unit(
        1,
        uid,
        STATS,
        {"pair": "p", "window": "2026-01-01", "column": "amount", "right_column": "amount"},
    )
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
    col_check = ColCheck(store, lambda _cfg: target, poll_interval=0, idle_timeout=0)
    with pytest.raises(TimeoutError):
        col_check.run()
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


def test_col_check_dead_letters_after_max_attempts():
    store, uid = _drained_store()

    class Broken(FakeTarget):
        def window_statistics(self, *_args):
            raise ValueError("boom")

    results = ColCheck(store, lambda _cfg: Broken(), poll_interval=0, max_attempts=2).run()
    assert results == [] and store.exists(f"dead/{uid}.json")


def test_volume_store_writes_data_before_pointer_and_lists_incrementally(tmp_path):
    store = VolumeStore(tmp_path / "runs" / "r1")
    store.commit_unit(1, "a", {"value": 1}, {"pair": "p"})
    store.commit_unit(2, "b", {"value": 2}, {"pair": "p"})
    assert store.exists("units/a.json")
    assert store.list_keys("_INDEX/", start_after="_INDEX/00000001-a.json") == [
        "_INDEX/00000002-b.json"
    ]
    assert store.get_json("_INDEX/00000002-b.json")["payload_key"] == "units/b.json"
    assert store.ready_unclaimed() == 2
    store.put_result("a", {"verdict": "match"})
    assert store.ready_unclaimed() == 1


def test_resolve_run_picks_newest_unfinished_run_under_a_root(tmp_path):
    root = VolumeStore(tmp_path / "runs")
    root.child("20260901T000000Z-aaa").put_json("_STATUS.json", {"state": "complete"})
    root.child("20260902T000000Z-bbb").put_json("row_check/p.left.json", {"pair": "p"})
    root.child("20260903T000000Z-ccc").put_json("row_check/p.left.json", {"pair": "p"})
    store, run_id = resolve_run(str(tmp_path / "runs"))
    assert run_id == "20260903T000000Z-ccc" and store.exists("row_check/p.left.json")
    store, run_id = resolve_run(str(tmp_path / "runs"), "20260902T000000Z-bbb")
    assert run_id == "20260902T000000Z-bbb"
    store, run_id = resolve_run(str(tmp_path / "runs" / "20260902T000000Z-bbb"))
    assert run_id == "20260902T000000Z-bbb"
    assert resolve_run(str(tmp_path / "empty")) == (None, None)
