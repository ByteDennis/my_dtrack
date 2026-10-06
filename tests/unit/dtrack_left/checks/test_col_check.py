import pandas as pd
import pytest
from support import MemoryStore, all_match_manifest, pairs_config

from dtrack_left.backends.sources import FileSource
from dtrack_left.checks import col_check
from dtrack_left.checks.col_check import (
    ColCheckPipeline,
    LeftRuntime,
    iter_windows_round_robin,
    publish_fixed_keys,
    wait_for_manifests,
)
from dtrack_left.checks.row_check import run_row_check
from dtrack_left.common.manifest import build_manifest, unit_id


def manifest():
    return {
        "protocol_version": 1,
        "run_id": "run",
        "pair": "pair",
        "sample": {"mode": "all"},
        "vintage": "month",
        "key_cols": [],
        "right_key_cols": [],
        "columns": ["amount"],
        "col_map": {"amount": "amount"},
        "windows": [{"bucket": "2026-01", "partitions": ["2026-01-01"], "n_left": 2}],
        "left": {"source": "csv", "date_col": "dt"},
        "right": {"source": "databricks", "date_col": "dt"},
        "strict": False,
        "expected_units": 1,
    }


STATS = {"col_type": "numeric", "n_total": 2, "n_missing": 0, "n_unique": 2, "hash_sum": "3"}


def test_pipeline_commits_payload_before_pointer_and_closes_only_when_complete():
    store = MemoryStore()
    row = manifest()
    store.put_json("manifest/pair.json", row)
    published = ColCheckPipeline(
        store,
        pull_window=lambda _manifest, _window: "pulled",
        compute_stats=lambda _manifest, _window, _pulled: {"amount": STATS},
    ).run([row])
    uid = unit_id("pair", "2026-01", "amount")
    assert published == 1
    assert store.events.index(("put", f"units/{uid}.json")) < next(
        i for i, event in enumerate(store.events) if event[1].startswith("_INDEX/")
    )
    assert store.exists("manifest/_CLOSED")


def test_pipeline_skips_units_already_indexed():
    store = MemoryStore()
    row = manifest()
    store.put_json("manifest/pair.json", row)
    args = {
        "pull_window": lambda _m, _w: "pulled",
        "compute_stats": lambda _m, _w, _p: {"amount": STATS},
    }
    assert ColCheckPipeline(store, **args).run([row]) == 1
    assert ColCheckPipeline(store, **args).run([row]) == 0


def test_wait_for_manifests_returns_only_after_right_side_closes_row_check():
    store = MemoryStore()
    store.put_json("manifest/pair.json", manifest())
    store.put_json("manifest/pair.keys.json", {"pair": "pair", "windows": {}})
    with pytest.raises(TimeoutError):
        wait_for_manifests(store, timeout=0, poll_interval=0)
    store.put_json("row_check/_CLOSED", {"pairs": ["pair"]})
    assert [m["pair"] for m in wait_for_manifests(store, timeout=0, poll_interval=0)] == ["pair"]


def test_wait_for_manifests_fails_fast_on_right_preflight_failure():
    store = MemoryStore()
    store.put_json("_PREFLIGHT.json", {"ok": False, "pairs": {"pair": {"error": "no table"}}})
    with pytest.raises(RuntimeError, match="preflight failed"):
        wait_for_manifests(store, timeout=0, poll_interval=0)


def test_wait_for_manifests_polls_until_closed_and_filters_pairs(monkeypatch):
    store = MemoryStore()
    store.put_json("_STATUS.json", {"state": "right_waiting"})
    store.put_json("manifest/pair.json", manifest())
    store.put_json("manifest/other.json", {**manifest(), "pair": "other"})
    sleeps = []
    monkeypatch.setattr(
        col_check.time,
        "sleep",
        lambda seconds: sleeps.append(seconds) or store.close_row_check(["pair", "other"]),
    )
    found = wait_for_manifests(store, timeout=60, poll_interval=7, pair_names=["other"])
    assert [m["pair"] for m in found] == ["other"] and sleeps == [7]


def test_windows_are_interleaved_round_robin_across_pairs():
    a = {"pair": "a", "windows": [{"bucket": 1}, {"bucket": 2}, {"bucket": 3}]}
    b = {"pair": "b", "windows": [{"bucket": 1}]}
    order = [(m["pair"], w["bucket"]) for m, w in iter_windows_round_robin([a, b, {"pair": "c"}])]
    assert order == [("a", 1), ("b", 1), ("a", 2), ("a", 3)]


def test_backpressure_waits_while_too_many_units_are_unclaimed(monkeypatch):
    store = MemoryStore()
    row = manifest()
    store.put_json("manifest/pair.json", row)
    levels = iter([5, 5, 0])
    monkeypatch.setattr(store, "ready_unclaimed", lambda: next(levels))
    naps = []
    monkeypatch.setattr(col_check.time, "sleep", naps.append)
    ColCheckPipeline(
        store,
        pull_window=lambda _m, _w: "pulled",
        compute_stats=lambda _m, _w, _p: {"amount": STATS},
        wip=1,
        backpressure_sleep=0.5,
    ).run([row])
    assert naps == [0.5, 0.5]


def test_pipeline_leaves_manifests_open_when_units_are_missing():
    store = MemoryStore()
    row = manifest()
    store.put_json("manifest/pair.json", {**row, "expected_units": 3})
    ColCheckPipeline(
        store,
        pull_window=lambda _m, _w: "pulled",
        compute_stats=lambda _m, _w, _p: {"amount": STATS},
    ).run([row])
    assert not store.exists("manifest/_CLOSED")
    assert store.get_json("_STATUS.json") == {"state": "left_col_check_partial", "indexed_units": 1}


def _fixed_manifest(synthetic, count=3):
    pair = pairs_config(synthetic.left)["pairs"]["customers"]
    counts = FileSource({**pair["left"]}).partition_counts("2026-01-01", "2026-01-31")
    pair = {**pair, "sample": count}
    return all_match_manifest(build_manifest, "run", "customers", pair, counts)


def test_fixed_sample_keys_are_published_once_and_reused_by_the_runtime(synthetic, tmp_path):
    store = MemoryStore()
    manifest_row = _fixed_manifest(synthetic)
    document = publish_fixed_keys(store, manifest_row, tmp_path / "keys")
    keys = document["windows"]["2026-01-01"]
    assert len(keys) == 3 and manifest_row["windows"][0]["key_tuples"] == keys
    assert publish_fixed_keys(store, manifest_row, tmp_path / "keys") is None
    assert publish_fixed_keys(store, manifest(), tmp_path) is None
    fresh = _fixed_manifest(synthetic)
    sampled = LeftRuntime(tmp_path / "work", store).pull_window(fresh, fresh["windows"][0])
    assert sampled.name == "left.csv"
    assert not sampled.with_name("left.sampled.csv").exists()
    frame = pd.read_csv(sampled, dtype=object)
    assert sorted(frame["CUST_ID"].unique()) == sorted(key[0] for key in keys)
    stats = LeftRuntime.compute_stats(fresh, None, sampled)
    assert set(stats) == {"CUST_ID", "TIER", "BALANCE"} and stats["TIER"]["n_total"] == len(frame)
    assert stats["BALANCE"]["col_type"] == "numeric"
    forced = LeftRuntime.compute_stats(
        {**fresh, "col_type_overrides": {"BALANCE": "categorical"}}, None, sampled
    )
    assert forced["BALANCE"]["col_type"] == "categorical"


def test_row_check_then_col_check_on_real_files(synthetic, tmp_path):
    store = MemoryStore()
    config = pairs_config(synthetic.left)
    run_id, _ = run_row_check(config, store, run_id="real", pair_names=["orders"])
    left = store.get_json("row_check/orders.left.json")
    right = FileSource(
        {"source": "csv", "path": str(synthetic.right / "orders.csv"), "date_col": "business_date"}
    ).partition_counts("2026-01-01", "2026-02-28")
    agreed = {day: n for day, n in left["counts"].items() if right.get(day) == n}
    manifest_row = all_match_manifest(build_manifest, run_id, "orders", left["pair_config"], agreed)
    assert [len(w["partitions"]) for w in manifest_row["windows"]] == [29, 27]
    store.put_json("manifest/orders.json", manifest_row)
    store.close_row_check(["orders"])
    manifests = wait_for_manifests(store, timeout=1, poll_interval=0)
    runtime = LeftRuntime(tmp_path / "work", store)
    published = ColCheckPipeline(
        store, pull_window=runtime.pull_window, compute_stats=runtime.compute_stats
    ).run(manifests)
    assert published == 8 and store.get_json("manifest/_CLOSED")["expected_units"] == 8
    pointers = [row for _, row in store.list_json("_INDEX/")]
    unit = store.get_json(pointers[0]["payload_key"])
    assert unit["n_total"] > 0 and {"n_unique", "hash_sum", "min_val"} <= set(unit)
    assert sorted({row["column"] for row in pointers}) == ["AMOUNT", "ORDER_ID", "REGION", "STATUS"]
    assert store.get_json("_STATUS.json")["state"] == "left_col_check_closed"
