import threading

import pytest
from support import MemoryStore

from pair_recon_left.backends.sources import write_rows
from pair_recon_left.common.checks.col_check import (
    ColCheckPipeline,
    LeftRuntime,
    wait_for_manifests,
    wait_for_row_check_result,
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


def test_pipeline(caplog):
    store = MemoryStore()
    row = manifest()
    store.put_json("plan.json", row)
    stats = {"amount": {"col_type": "numeric", "n_unique": 2, "hash_sum": "3"}}
    with caplog.at_level("INFO"):
        count = ColCheckPipeline(
            store,
            pull_window=lambda *_args: "pulled",
            compute_stats=lambda *_args: stats,
        ).run([row])
    uid = unit_id("pair", "all", "amount")
    assert count == 1 and store.exists("units/00000001-0001.json")
    assert store.get_json("units/00000001-0001.json")["units"][0]["unit_id"] == uid
    assert "left data pull started: pair=pair vintage=all partitions=1 columns=1" in caplog.text
    assert "left data ready for statistics: pair=pair vintage=all" in caplog.text


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
    assert store.get_json("_state/left.json")["phase"] == "col_check_closed"


def test_wait_for_manifests_honors_abort_signal():
    def abort():
        raise RuntimeError("Right failed")

    with pytest.raises(RuntimeError, match="Right failed"):
        wait_for_manifests(MemoryStore(), abort_if=abort)


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


def test_pipeline_abort_does_not_wait_for_a_running_pull():
    store = MemoryStore()
    row = manifest()
    store.put_json("plan.json", row)
    started, release = threading.Event(), threading.Event()

    def pull(*_args):
        started.set()
        release.wait(5)
        return "pulled"

    def abort():
        if started.is_set():
            raise RuntimeError("Right failed")

    pipeline = ColCheckPipeline(
        store,
        pull_window=pull,
        compute_stats=lambda *_args: {},
        abort_if=abort,
        progress_interval=0.01,
    )
    outcome = {}

    def run():
        try:
            pipeline.run([row])
        except RuntimeError as exc:
            outcome["error"] = str(exc)

    worker = threading.Thread(target=run)
    worker.start()
    worker.join(5)
    assert not worker.is_alive() and outcome["error"] == "Right failed"
    assert not release.is_set()
    release.set()


def test_wait_for_row_check_result_raises_on_unexcluded_mismatches():
    store = MemoryStore()
    store.put_json(
        "plan.json",
        {"pair": "pair", "version": 2, "excluded_partitions": ["2026-01-02"]},
    )
    store.put_json(
        "row_check/result.json",
        {
            "pair": "pair",
            "date_synonyms": {},
            "partitions": {
                "2026-01-01": {"result": "count_diff"},
                "2026-01-02": {"result": "count_diff"},
                "2026-01-03": {"result": "match"},
            },
        },
    )
    with pytest.raises(
        RuntimeError,
        match=r"unexcluded mismatched partitions.*\(pair: 2026-01-01\)",
    ):
        wait_for_row_check_result(store, "pair")


def test_wait_for_row_check_result_waits_for_the_right_then_accepts_matches(caplog):
    store = MemoryStore()
    store.put_json("plan.json", {"pair": "pair", "version": 2})
    polls = []

    def abort():
        polls.append(True)
        if len(polls) == 2:
            store.put_json(
                "row_check/result.json",
                {
                    "pair": "pair",
                    "partitions": {"all": {"result": "match"}},
                    "summary": {"match": 1},
                },
            )

    with caplog.at_level("INFO"):
        result = wait_for_row_check_result(store, "pair", poll_interval=0.01, abort_if=abort)
    assert result["summary"] == {"match": 1} and len(polls) == 2
    assert store.exists("_status/heartbeat.json")
    assert "waiting for the Right row check before pulling Left data: pair=pair" in caplog.text
    assert "Right row check accepted: pair=pair partitions=1" in caplog.text


def test_wait_for_row_check_result_waits_until_aborted():
    polls = []

    def abort():
        polls.append(True)
        if len(polls) == 3:
            raise RuntimeError("Right failed")

    with pytest.raises(RuntimeError, match="Right failed"):
        wait_for_row_check_result(MemoryStore(), "pair", poll_interval=0.001, abort_if=abort)
    assert len(polls) == 3


def test_cache(tmp_path, caplog):
    row = {**manifest(), "version": 2, "plan_id": "one"}
    calls = []

    class Source:
        def pull_window(self, _partitions, columns, output, **_options):
            calls.append(columns)
            return write_rows(output, columns, [["1"]])

    runtime = LeftRuntime(tmp_path)
    runtime.sources["pair"] = Source()
    window = row["windows"][0]
    with caplog.at_level("INFO"):
        runtime.pull_window(row, window)
    runtime.pull_window(row, window)
    assert calls == [["amount"]]
    assert "left data pull completed: pair=pair vintage=all sampled_rows=1 path=" in caplog.text
