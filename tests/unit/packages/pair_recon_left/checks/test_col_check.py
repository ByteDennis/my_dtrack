import pytest
from support import MemoryStore

from pair_recon_left.backends.sources import write_rows
from pair_recon_left.common.checks.col_check import (
    ColCheckPipeline,
    LeftRuntime,
    wait_for_manifests,
)
from pair_recon_left.common.model.manifest import unit_id


def manifest():
    return {
        "protocol_version": 2,
        "run_id": "run",
        "pair": "pair",
        "sample": {"mode": "all"},
        "vintage": "all",
        "partition_mode": "all",
        "key_cols": [],
        "right_key_cols": [],
        "key_families": [],
        "columns": ["amount"],
        "col_map": {"amount": "amount"},
        "windows": [{"bucket": "all", "partitions": ["all"], "n_left": 2}],
        "left": {"source": "oracle"},
        "right": {"source": "databricks", "table": "orders"},
        "expected_units": 1,
    }


def test_pipeline():
    store = MemoryStore()
    row = manifest()
    store.put_json("plan.json", row)
    stats = {"amount": {"col_type": "numeric", "n_unique": 2, "hash_sum": "3"}}
    count = ColCheckPipeline(
        store,
        pull_window=lambda *_args: "pulled",
        compute_stats=lambda *_args: stats,
    ).run([row])
    uid = unit_id("pair", "all", "amount")
    assert count == 1 and store.exists(f"units/00000001-{uid}.json")


def test_left_only_pipeline_does_not_wait_for_right(monkeypatch):
    store = MemoryStore()
    row = manifest()
    store.put_json("plan.json", row)
    stats = {"amount": {"col_type": "numeric", "n_unique": 2, "hash_sum": "3"}}
    pipeline = ColCheckPipeline(
        store,
        pull_window=lambda *_args: "pulled",
        compute_stats=lambda *_args: stats,
        wip=1,
        wait_for_right=False,
    )
    monkeypatch.setattr(
        pipeline,
        "_wait_for_capacity",
        lambda *_args: pytest.fail("left-only mode must not wait for Right"),
    )

    assert pipeline.run([row]) == 1
    assert store.exists("_status/col_check.closed.json")


def test_wait_for_manifests_honors_abort_signal():
    def abort():
        raise RuntimeError("Right failed")

    with pytest.raises(RuntimeError, match="Right failed"):
        wait_for_manifests(MemoryStore(), timeout=1800, abort_if=abort)


def test_pipeline_honors_abort_signal_before_work():
    pulled = []

    def abort():
        raise RuntimeError("Right failed")

    pipeline = ColCheckPipeline(
        MemoryStore(),
        pull_window=lambda *_args: pulled.append(True),
        compute_stats=lambda *_args: {},
        abort_if=abort,
    )

    with pytest.raises(RuntimeError, match="Right failed"):
        pipeline.run([manifest()])
    assert pulled == []


def test_cache(tmp_path):
    row = {**manifest(), "version": 2, "plan_id": "one"}
    calls = []

    class Source:
        def pull_window(self, _partitions, columns, output, **_options):
            calls.append(columns)
            return write_rows(output, columns, [["1"]])

    runtime = LeftRuntime(tmp_path)
    runtime.sources["pair"] = Source()
    window = row["windows"][0]
    runtime.pull_window(row, window)
    runtime.pull_window(row, window)
    assert calls == [["amount"]]
