import pandas as pd
import pytest
from support import MemoryStore, all_match_manifest, pairs_config

from dtrack_left.backends.sources import FileSource, write_rows
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
from dtrack_left.common.protocol import list_units


def manifest():
    return {
        "protocol_version": 2,
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


def test_pipeline_commits():
    store = MemoryStore()
    row = manifest()
    store.put_json("pairs/pair/plan.json", row)
    published = ColCheckPipeline(
        store,
        pull_window=lambda _manifest, _window: "pulled",
        compute_stats=lambda _manifest, _window, _pulled: {"amount": STATS},
    ).run([row])
    uid = unit_id("pair", "2026-01", "amount")
    assert published == 1
    assert ("put", f"pairs/pair/units/00000001-{uid}.json") in store.events
    assert store.exists("_status/col_check.closed.json")


def test_query_computes(caplog):
    store = MemoryStore()
    columns = [f"c{i}" for i in range(5)]
    row = {
        **manifest(),
        "columns": columns,
        "col_map": {column: column for column in columns},
        "expected_units": len(columns),
        "left": {
            "source": "oracle",
            "date_col": "dt",
            "query": f"SELECT dt, {', '.join(columns)} FROM wide_source",
        },
    }
    store.put_json("pairs/pair/plan.json", row)
    pulled = []
    computed = []

    def pull(window_manifest, _window):
        pulled.append(list(window_manifest["columns"]))
        return "pulled"

    def compute(window_manifest, _window, _pulled):
        computed.append(list(window_manifest["columns"]))
        return {column: STATS for column in window_manifest["columns"]}

    with caplog.at_level("INFO", logger="dtrack.left.col_check"):
        published = ColCheckPipeline(
            store,
            pull_window=pull,
            compute_stats=compute,
            pull_workers=1,
            stat_workers=1,
        ).run([row])

    assert published == 5
    assert pulled == [columns]
    assert computed == pulled
    assert "run_ready=5/5" in caplog.text


def test_left_runtime(tmp_path):
    row = {**manifest(), "version": 2, "plan_id": "plan-one", "key_families": []}
    calls = []

    class Source:
        def pull_window(self, partitions, columns, output, **options):
            calls.append((partitions, columns, options))
            selected = [row["left"]["date_col"], *row["key_cols"], *columns]
            return write_rows(output, selected, [["2026-01-01", "1"]])

    runtime = LeftRuntime(tmp_path, reuse_pulled=True)
    runtime.sources["pair"] = Source()
    window = row["windows"][0]

    first = runtime.pull_window(row, window)
    assert runtime.pull_window(row, window) == first
    assert len(calls) == 1

    changed = {**row, "plan_id": "plan-two"}
    runtime.pull_window(changed, window)
    assert len(calls) == 2

    runtime.force = True
    runtime.pull_window(changed, window)
    assert len(calls) == 3


def test_pipeline_skips():
    store = MemoryStore()
    row = manifest()
    store.put_json("pairs/pair/plan.json", row)
    args = {
        "pull_window": lambda _m, _w: "pulled",
        "compute_stats": lambda _m, _w, _p: {"amount": STATS},
    }
    assert ColCheckPipeline(store, **args).run([row]) == 1
    assert ColCheckPipeline(store, **args).run([row]) == 0


def test_wait_manifests():
    store = MemoryStore()
    store.put_json("pairs/pair/plan.json", manifest())
    store.put_json("pairs/pair/fixed_keys.json", {"pair": "pair", "windows": {}})
    with pytest.raises(TimeoutError):
        wait_for_manifests(store, timeout=0, poll_interval=0)
    store.close_row_check(["pair"])
    assert [m["pair"] for m in wait_for_manifests(store, timeout=0, poll_interval=0)] == ["pair"]


def test_wait_fails():
    store = MemoryStore()
    store.put_json("_PREFLIGHT.json", {"ok": False, "pairs": {"pair": {"error": "no table"}}})
    with pytest.raises(RuntimeError, match="preflight failed"):
        wait_for_manifests(store, timeout=0, poll_interval=0)


def test_wait_polls(monkeypatch):
    store = MemoryStore()
    store.put_json("_status/run.json", {"state": "right_waiting"})
    store.put_json("pairs/pair/plan.json", manifest())
    store.put_json("pairs/other/plan.json", {**manifest(), "pair": "other"})
    sleeps = []
    monkeypatch.setattr(
        col_check.time,
        "sleep",
        lambda seconds: sleeps.append(seconds) or store.close_row_check(["pair", "other"]),
    )
    found = wait_for_manifests(store, timeout=60, poll_interval=7, pair_names=["other"])
    assert [m["pair"] for m in found] == ["other"] and sleeps == [7]


def test_windows_interleaved():
    a = {"pair": "a", "windows": [{"bucket": 1}, {"bucket": 2}, {"bucket": 3}]}
    b = {"pair": "b", "windows": [{"bucket": 1}]}
    order = [(m["pair"], w["bucket"]) for m, w in iter_windows_round_robin([a, b, {"pair": "c"}])]
    assert order == [("a", 1), ("b", 1), ("a", 2), ("a", 3)]


def test_backpressure_reports(monkeypatch, caplog):
    store = MemoryStore()
    row = manifest()
    store.put_json("pairs/pair/plan.json", row)
    levels = iter([5, 5, 0])
    monkeypatch.setattr(store, "ready_unclaimed", lambda: next(levels))
    naps = []
    monkeypatch.setattr(col_check.time, "sleep", naps.append)
    with caplog.at_level("INFO", logger="dtrack.left.col_check"):
        ColCheckPipeline(
            store,
            pull_window=lambda _m, _w: "pulled",
            compute_stats=lambda _m, _w, _p: {"amount": STATS},
            wip=1,
            backpressure_sleep=0.5,
        ).run([row])
    assert naps == [0.5, 0.5]
    waiting = [
        record.message for record in caplog.records if record.message.startswith("backpressure:")
    ]
    assert waiting == [
        (
            "backpressure: ready_unclaimed=5, wip=1, over_limit=4; "
            "left_ready=0/1, right_finished=0/1; waiting for Right (0.0s)"
        )
    ]
    assert "backpressure cleared: ready_unclaimed=0, wip=1" in caplog.text


def test_statistics_progress(caplog):
    import time

    store = MemoryStore()
    row = manifest()
    store.put_json("pairs/pair/plan.json", row)

    def compute(_manifest, _window, _pulled, *, progress):
        progress("load", 1, 1)
        time.sleep(0.03)
        progress("infer", 1, 1)
        progress("aggregate", 1, 1)
        progress("finalize", 1, 1)
        return {"amount": STATS}

    with caplog.at_level("INFO", logger="dtrack.left.progress"):
        ColCheckPipeline(
            store,
            pull_window=lambda _m, _w: "pulled",
            compute_stats=compute,
            progress_interval=0.01,
        ).run([row])

    assert "statistics progress:" in caplog.text
    assert "phase_steps=" in caplog.text
    assert "shards_running=" in caplog.text
    assert "attributes_calculated=" in caplog.text
    assert "attributes_uploaded=" in caplog.text


def test_pipeline_publishes():
    store = MemoryStore()
    columns = ["a", "b"]
    row = {
        **manifest(),
        "columns": columns,
        "col_map": {column: column for column in columns},
        "windows": [
            {"bucket": "2026-01", "partitions": ["2026-01-01"], "n_left": 2},
            {"bucket": "2026-02", "partitions": ["2026-02-01"], "n_left": 2},
        ],
        "expected_units": 4,
    }
    store.put_json("pairs/pair/plan.json", row)
    active = 0
    max_active = 0
    observed = []

    def compute(_manifest, window, _pulled, *, progress, on_batch):
        nonlocal active, max_active
        active += 1
        max_active = max(max_active, active)
        progress("load", 1, 1)
        for index, column in enumerate(columns, start=1):
            on_batch({column: STATS})
            observed.append((window["bucket"], column, len(list_units(store))))
            progress("infer", index, len(columns))
            progress("aggregate", index, len(columns))
            progress("finalize", index, len(columns))
        active -= 1
        return {}

    published = ColCheckPipeline(
        store,
        pull_window=lambda _manifest, window: window["bucket"],
        compute_stats=compute,
        pull_workers=2,
        stat_workers=4,
    ).run([row])

    assert published == 4
    assert max_active == 1
    assert observed == [
        ("2026-01", "a", 1),
        ("2026-01", "b", 2),
        ("2026-02", "a", 3),
        ("2026-02", "b", 4),
    ]


def test_pipeline_leaves():
    store = MemoryStore()
    row = manifest()
    store.put_json("pairs/pair/plan.json", {**row, "expected_units": 3})
    ColCheckPipeline(
        store,
        pull_window=lambda _m, _w: "pulled",
        compute_stats=lambda _m, _w, _p: {"amount": STATS},
    ).run([row])
    assert not store.exists("_status/col_check.closed.json")
    assert store.get_json("_status/run.json")["state"] == "left_col_check_partial"
    assert store.get_json("_status/run.json")["indexed_units"] == 1


def _fixed_manifest(synthetic, count=3):
    pair = pairs_config(synthetic.left)["pairs"]["customers"]
    counts = FileSource({**pair["left"]}).partition_counts("2026-01-01", "2026-01-31")
    pair = {**pair, "sample": count}
    return all_match_manifest(build_manifest, "run", "customers", pair, counts)


def test_fixed_sample(synthetic, tmp_path):
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


def test_version_two(synthetic, tmp_path):
    store = MemoryStore()
    pair = pairs_config(synthetic.left)["pairs"]["customers"]
    counts = FileSource({**pair["left"]}).partition_counts("2026-01-01", "2026-01-31")
    first = all_match_manifest(
        build_manifest, "run", "customers", {**pair, "version": 2, "sample": 3}, counts
    )
    original = publish_fixed_keys(store, first, tmp_path / "keys")
    second = all_match_manifest(
        build_manifest,
        "run",
        "customers",
        {**pair, "version": 2, "sample": 3, "excluded_columns": ["TIER"]},
        counts,
    )
    refreshed = publish_fixed_keys(store, second, tmp_path / "keys")
    assert refreshed["plan_id"] == second["plan_id"] != first["plan_id"]
    assert refreshed["windows"] == original["windows"]


def test_row_check(synthetic, tmp_path):
    store = MemoryStore()
    config = pairs_config(synthetic.left)
    run_id, _ = run_row_check(config, store, run_id="real", pair_names=["orders"])
    left = store.get_json("pairs/orders/row_check/left.json")
    right = FileSource(
        {"source": "csv", "path": str(synthetic.right / "orders.csv"), "date_col": "business_date"}
    ).partition_counts("2026-01-01", "2026-02-28")
    agreed = {day: n for day, n in left["counts"].items() if right.get(day) == n}
    manifest_row = all_match_manifest(build_manifest, run_id, "orders", left["pair_config"], agreed)
    assert [len(w["partitions"]) for w in manifest_row["windows"]] == [29, 27]
    store.put_json("pairs/orders/plan.json", manifest_row)
    store.close_row_check(["orders"])
    manifests = wait_for_manifests(store, timeout=1, poll_interval=0)
    runtime = LeftRuntime(tmp_path / "work", store)
    published = ColCheckPipeline(
        store, pull_window=runtime.pull_window, compute_stats=runtime.compute_stats
    ).run(manifests)
    assert published == 8
    assert store.get_json("_status/col_check.closed.json")["expected_units"] == 8
    pointers = [row for _, row in store.list_json("pairs/orders/units/")]
    unit = pointers[0]["left_statistics"]
    assert unit["n_total"] > 0 and {"n_unique", "hash_sum", "min_val"} <= set(unit)
    assert sorted({row["column"] for row in pointers}) == ["AMOUNT", "ORDER_ID", "REGION", "STATUS"]
    assert store.get_json("_status/run.json")["state"] == "left_col_check_closed"


def test_pipeline_pulling():
    store = MemoryStore()
    row = manifest()
    store.put_json("pairs/pair/plan.json", row)
    pulled = []
    args = {
        "pull_window": lambda _m, window: pulled.append(window["bucket"]) or "pulled",
        "compute_stats": lambda _m, _w, _p: {"amount": STATS},
    }
    assert ColCheckPipeline(store, **args).run([row]) == 1
    assert ColCheckPipeline(store, **args).run([row]) == 0
    assert pulled == ["2026-01"]


def test_version_rerun():
    pair = {
        "version": 2,
        "left": {"source": "csv", "date_col": "dt"},
        "right": {"source": "databricks", "date_col": "dt"},
        "col_map": {"amount": "amount"},
        "vintage": "month",
    }
    row_check = {
        "gate_decision": {"passed": True},
        "partitions": {
            "2026-01-01": {"left": 1, "right": 1, "verdict": "match"},
            "2026-01-02": {"left": 1, "right": 2, "verdict": "count_diff"},
            "2026-02-01": {"left": 1, "right": 1, "verdict": "match"},
        },
    }
    store = MemoryStore()
    pulled = []
    args = {
        "pull_window": lambda _m, window: pulled.append(window["bucket"]) or "pulled",
        "compute_stats": lambda _m, _w, _p: {"amount": STATS},
    }
    first = build_manifest("run", "pair", pair, row_check)
    store.put_json("pairs/pair/plan.json", first)
    assert ColCheckPipeline(store, **args).run([first]) == 2

    second = build_manifest(
        "run", "pair", {**pair, "excluded_partitions": ["2026-01-02"]}, row_check
    )
    store.put_json("pairs/pair/plan.json", second)
    assert ColCheckPipeline(store, **args).run([second]) == 1
    assert pulled == ["2026-01-01", "2026-02-01", "2026-01-01"]
