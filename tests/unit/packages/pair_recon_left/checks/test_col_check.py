import threading

import pytest
from support import MemoryStore

from pair_recon_left.backends.sources import write_rows
from pair_recon_left.common.checks.col_check import (
    ColCheckPipeline,
    LeftRuntime,
    format_row_count_differences,
    wait_for_manifests,
    wait_for_row_check_result,
)
from pair_recon_left.common.model.manifest import unit_id
from pair_recon_left.common.model.protocol import (
    LEFT_STATE,
    difference_keys_path,
    plan_path,
    row_result_path,
    unit_batch_path,
    window_count_path,
)


def manifest():
    return {
        "protocol_version": 4,
        "run_id": "run",
        "pair": "pair",
        "sample": {"mode": "all"},
        "vintage": "all",
        "partition": "all",
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
    store.put_json(plan_path("pair"), row)
    stats = {"amount": {"col_type": "numeric", "n_unique": 2, "hash_sum": "3"}}
    with caplog.at_level("INFO"):
        count = ColCheckPipeline(
            store,
            pull_window=lambda *_args: "pulled",
            compute_stats=lambda *_args: stats,
        ).run([row])
    uid = unit_id("pair", "all", "amount")
    assert count == 1 and store.exists(unit_batch_path(1, 1))
    assert store.get_json(unit_batch_path(1, 1))["units"][0]["unit_id"] == uid
    assert "left data pull started: pair=pair vintage=all partitions=1 columns=1" in caplog.text
    assert "left data ready for statistics: pair=pair vintage=all" in caplog.text


def test_pipeline_only_pulls_columns_without_reusable_units():
    store = MemoryStore()
    row = manifest()
    row["columns"] = ["amount", "status"]
    row["col_map"]["status"] = "status"
    row["expected_units"] = 2
    store.put_json(plan_path("pair"), row)
    store.commit_units(
        1,
        [
            {
                "unit_id": unit_id("pair", "all", "amount"),
                "left_statistics": {"hash_sum": "3"},
                "pair": "pair",
                "window": "all",
                "column": "amount",
                "right_column": "amount",
            }
        ],
    )
    pulled = []

    def pull(work_manifest, _window):
        pulled.append(work_manifest["columns"])
        return "pulled"

    def compute(work_manifest, *_args, **_kwargs):
        return {
            column: {"col_type": "categorical", "n_unique": 2, "hash_sum": "3"}
            for column in work_manifest["columns"]
        }

    assert ColCheckPipeline(store, pull_window=pull, compute_stats=compute).run([row]) == 1
    assert pulled == [["status"]]


def test_left_only_pipeline_does_not_wait_for_right(monkeypatch):
    store = MemoryStore()
    row = manifest()
    store.put_json(plan_path("pair"), row)
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
    assert store.get_json(LEFT_STATE)["phase"] == "col_check_closed"


def test_wait_for_manifests_honors_abort_signal():
    def abort():
        raise RuntimeError("Right failed")

    with pytest.raises(RuntimeError, match="Right failed"):
        wait_for_manifests(MemoryStore(), abort_if=abort)


def test_pipeline_passes_query_status_to_compute_stats():
    store = MemoryStore()
    row = manifest()
    store.put_json(plan_path("pair"), row)
    seen = []

    def compute(_manifest, _window, _value, *, query_status):
        query_status("aggregate", 0, 42.0)
        query_status("aggregate", 0, None)
        seen.append(query_status)
        return {"amount": {"col_type": "numeric", "n_unique": 2, "hash_sum": "3"}}

    assert (
        ColCheckPipeline(store, pull_window=lambda *_args: "pulled", compute_stats=compute).run(
            [row]
        )
        == 1
    )
    assert len(seen) == 1 and callable(seen[0])


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
    store.put_json(plan_path("pair"), row)
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
        plan_path("pair"),
        {"pair": "pair", "version": 2, "excluded_partitions": ["2026-01-02"]},
    )
    store.put_json(
        row_result_path("pair"),
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


def test_row_count_difference_table_is_compact_and_directional():
    report = format_row_count_differences(
        "orders",
        {
            "2026-01-01": {"left": 1200, "right": 1000, "result": "count_diff"},
            "2026-01-02": {"left": 50, "right": 75, "result": "count_diff"},
            "2026-01-03": {"left": 7, "right": None, "result": "left_only"},
            "2026-01-04": {"left": None, "right": 9, "result": "right_only"},
        },
    )

    assert report.splitlines() == [
        "Row-count differences: orders (4)",
        "Partition    Left  Right  Difference",
        "----------  -----  -----  ----------",
        "2026-01-01  1,200  1,000  Left +200",
        "2026-01-02     50     75  Right +25",
        "2026-01-03      7      -  Left only",
        "2026-01-04      -      9  Right only",
    ]


def test_wait_for_row_check_result_waits_for_the_right_then_accepts_matches(caplog):
    store = MemoryStore()
    store.put_json(plan_path("pair"), {"pair": "pair", "version": 2})
    polls = []

    def abort():
        polls.append(True)
        if len(polls) == 2:
            store.put_json(
                row_result_path("pair"),
                {
                    "pair": "pair",
                    "partitions": {"all": {"result": "match"}},
                    "summary": {"match": 1},
                },
            )

    with caplog.at_level("INFO"):
        result = wait_for_row_check_result(store, "pair", poll_interval=0.01, abort_if=abort)
    assert result["summary"] == {"match": 1} and len(polls) == 2
    assert store.exists(LEFT_STATE)
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
            return write_rows(output, columns, [["a\nb"]]), 1

    store = MemoryStore()
    runtime = LeftRuntime(tmp_path, store)
    runtime.sources["pair"] = Source()
    window = row["windows"][0]
    with caplog.at_level("INFO"):
        runtime.pull_window(row, window)
    assert store.get_json(window_count_path("pair", "all"))["n_total"] == 1
    store.delete_keys([window_count_path("pair", "all")])
    runtime.pull_window(row, window)
    assert calls == [["amount"]]
    assert store.get_json(window_count_path("pair", "all"))["n_total"] == 1
    assert "left data pull completed: pair=pair vintage=all sampled_rows=1 path=" in caplog.text


def test_sas_input_reloads_as_standard_left_csv_without_boundary_fields(tmp_path):
    sample = tmp_path / "left_sample.csv"
    sample.write_text(
        "__pair_recon_request_id,id,amount\ncontract-1,1,10\n",
        encoding="utf-8",
    )
    row = {
        **manifest(),
        "version": 2,
        "plan_id": "one",
        "sample": {"mode": "fraction", "rate": 0.1, "threshold": 100_000},
        "key_cols": ["id"],
        "key_families": ["numeric"],
    }
    runtime = LeftRuntime(
        tmp_path / "work",
        MemoryStore(),
        source_configs={
            "pair": {
                "source": "csv",
                "path": str(sample),
                "pre_sampled": True,
                "source_contract_id": "contract-1",
            }
        },
    )

    output = runtime.pull_window(row, row["windows"][0])

    assert output.read_text(encoding="utf-8") == "id,amount\n1,10\n"
    assert "sas" not in output.read_text(encoding="utf-8").casefold()


def test_left_pull_publishes_bounded_difference_keys(tmp_path):
    row = {
        **manifest(),
        "version": 2,
        "plan_id": "one",
        "key_cols": ["id"],
        "right_key_cols": ["id"],
        "key_families": ["numeric"],
        "difference_sample_rows": 2,
    }

    class Source:
        def pull_window(self, _partitions, _columns, output, **_options):
            return write_rows(output, ["id", "amount"], [["01", 1], ["2", 2], ["3", 3]]), 3

    store = MemoryStore()
    runtime = LeftRuntime(tmp_path, store)
    runtime.sources["pair"] = Source()
    runtime.pull_window(row, row["windows"][0])

    assert store.get_json(difference_keys_path("pair", "all"))["key_tuples"] == [
        ["01"],
        ["2"],
    ]
