import pytest
from support import STATS, FakeTarget, MemoryStore

from pair_recon.app.runner import retry_failed
from pair_recon_left.common.checks import col_check as left_col_check
from pair_recon_left.common.checks.col_check import ColCheckPipeline
from pair_recon_right.engine.checks.col_check import ColCheck, ResultBatchCommitError
from pair_recon_right.engine.common.model.manifest import build_manifest, unit_id_for
from pair_recon_right.engine.common.model.protocol import (
    LEFT_STATE,
    RESULT_BATCH_PREFIX,
    RIGHT_STATE,
    col_check_close_document,
    get_left_state,
    get_right_state,
    list_failures,
    list_results,
    row_check_is_closed,
    write_left_state,
    write_right_state,
)


def _two_column_store():
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
    store = MemoryStore()
    store.put_json("plan.json", manifest)
    units = [
        {
            "pair": "p",
            "window": window["bucket"],
            "column": column,
            "right_column": column,
            "unit_id": unit_id_for(manifest, window, column),
            "plan_id": manifest["plan_id"],
            "left_statistics": STATS,
        }
        for column in manifest["columns"]
    ]
    store.commit_units(1, units)
    store.close_manifests(["p"], len(units), plan_ids={"p": manifest["plan_id"]})
    store.heartbeat()
    return store, manifest


def test_role_state_replaces_hot_signal_files():
    store = MemoryStore()

    store.heartbeat(now=10)
    store.close_manifests(["p"], 2, plan_ids={"p": "plan"})
    store.close_row_check(["p"])

    left = get_left_state(store)
    right = get_right_state(store)
    assert left["heartbeat_epoch_s"] == 10
    assert left["expected_units"] == 2
    assert left["plan_ids"] == {"p": "plan"}
    assert left["col_check_closed_at"]
    assert right["row_check_closed_at"]
    assert store.exists(LEFT_STATE) and store.exists(RIGHT_STATE)
    assert store.exists("_status/heartbeat.json")
    assert store.exists("_status/row_check.closed.json")
    assert store.exists("_status/col_check.closed.json")
    assert row_check_is_closed(store)
    assert col_check_close_document(store)["expected_units"] == 2

    store.put_json("_status/col_check.closed.json", {"expected_units": 99})
    write_left_state(
        store,
        {"phase": "col_check", "plan_ids": {"p": "new-plan"}, "col_check_closed_at": None},
    )
    assert col_check_close_document(store) is None


def test_right_writes_one_result_batch_and_terminal_snapshots():
    store, manifest = _two_column_store()

    target = FakeTarget(stats={"amount": STATS, "status": STATS})
    results = ColCheck(store, lambda _cfg: target, poll_interval=0).run()

    assert len(results) == 2
    assert len(store.list_keys(RESULT_BATCH_PREFIX)) == 1
    assert store.list_keys("col_check/results/") == []
    assert store.list_keys("col_check/failures/") == []
    assert store.list_keys("col_check/right_ready/") == []
    assert {value["unit_id"] for _key, value in list_results(store)} == {
        unit_id_for(manifest, manifest["windows"][0], column) for column in manifest["columns"]
    }
    assert list_failures(store) == []
    right = get_right_state(store)
    assert right["phase"] == "complete"
    assert right["completed_units"] == 2
    assert right["completed_batches"] == 1
    assert store.exists("_status/units.csv")


def test_result_batch_reader_keeps_legacy_per_unit_compatibility():
    store = MemoryStore()
    store.put_json(
        "col_check/results/legacy.json",
        {"unit_id": "legacy", "pair": "p", "verdict": "match"},
    )
    store.commit_results(
        [{"unit_id": "new", "pair": "p", "verdict": "match"}],
    )
    store.put_json(
        "col_check/failures/failed.json",
        {"unit_id": "failed", "pair": "p", "error": "boom"},
    )

    assert {value["unit_id"] for _key, value in list_results(store)} == {"legacy", "new"}
    assert {value["unit_id"] for _key, value in list_failures(store)} == {"failed"}


def test_protocol_two_plan_keeps_per_unit_results_for_rolling_upgrade():
    store, _manifest = _two_column_store()
    store.objects["plan.json"]["protocol_version"] = 2
    target = FakeTarget(stats={"amount": STATS, "status": STATS})

    ColCheck(store, lambda _cfg: target, poll_interval=0).run()

    assert store.list_keys(RESULT_BATCH_PREFIX) == []
    assert len(store.list_keys("col_check/results/")) == 2


def test_matching_results_are_chunked_and_units_csv_is_terminal_only():
    columns = [f"column_{index:03d}" for index in range(130)]
    pair = {
        "version": 2,
        "left": {"source": "oracle", "date_col": "dt"},
        "right": {"source": "databricks", "date_col": "dt"},
        "col_map": {column: column for column in columns},
    }
    row_check = {
        "gate_decision": {"passed": True},
        "partitions": {"2026-01-01": {"left": 1, "right": 1, "result": "match"}},
    }
    manifest = build_manifest("run", "p", pair, row_check)
    window = manifest["windows"][0]
    store = MemoryStore()
    store.put_json("plan.json", manifest)
    units = [
        {
            "pair": "p",
            "window": window["bucket"],
            "column": column,
            "right_column": column,
            "unit_id": unit_id_for(manifest, window, column),
            "plan_id": manifest["plan_id"],
            "left_statistics": STATS,
        }
        for column in columns
    ]
    store.commit_units(1, units)
    store.close_manifests(["p"], len(units), plan_ids={"p": manifest["plan_id"]})

    target = FakeTarget(stats={column: STATS for column in columns})
    results = ColCheck(
        store,
        lambda _cfg: target,
        poll_interval=0,
        result_batch_size=64,
    ).run()

    assert len(results) == 130
    assert len(store.list_keys(RESULT_BATCH_PREFIX)) == 3
    assert store.list_keys("col_check/results/") == []
    assert sum(event == ("put", "_status/units.csv") for event in store.events) == 1


def test_retry_failed_keeps_batched_matches_and_deletes_retryable_objects():
    store = MemoryStore()
    store.commit_results([{"unit_id": "matched", "pair": "p", "verdict": "match"}])
    store.put_result("different", {"pair": "p", "verdict": "diff"})
    store.put_dead("failed", {"pair": "p", "error": "boom"})

    assert retry_failed(store) == 2

    assert {value["unit_id"] for _key, value in list_results(store)} == {"matched"}
    assert list_failures(store) == []
    assert len(store.list_keys(RESULT_BATCH_PREFIX)) == 1


def test_wip_uses_role_state_without_scanning_unit_or_result_objects(monkeypatch):
    store = MemoryStore()
    store.put_json("plan.json", {"pair": "p", "plan_id": "plan"})
    write_left_state(
        store,
        {"plan_ids": {"p": "plan"}, "published_units": 90},
    )
    write_right_state(
        store,
        {"plan_ids": {"p": "plan"}, "completed_units": 70},
    )

    def fail_scan(*_args, **_kwargs):
        pytest.fail("state fast path must not scan protocol objects")

    monkeypatch.setattr(left_col_check, "list_units", fail_scan)
    monkeypatch.setattr(left_col_check, "list_results", fail_scan)
    monkeypatch.setattr(left_col_check, "list_failures", fail_scan)
    pipeline = ColCheckPipeline(
        store,
        pull_window=lambda *_args: None,
        compute_stats=lambda *_args: None,
    )

    assert pipeline._unit_progress({str(index) for index in range(100)}) == (90, 70, 20)


def test_right_rejects_plan_change_while_running():
    store, manifest = _two_column_store()
    checker = ColCheck(store, lambda _cfg: FakeTarget(), poll_interval=0)
    assert checker.poll() > 0
    changed = {**manifest, "plan_id": "replacement-plan"}
    store.put_json("plan.json", changed)

    with pytest.raises(RuntimeError, match="plan changed while Right was running"):
        checker.poll()


def test_right_recovers_when_batch_payload_lands_before_state_update():
    class TransientCommitStore(MemoryStore):
        fail_after_first_put = True

        def commit_results(self, results):
            committed = super().commit_results(results)
            if self.fail_after_first_put:
                self.fail_after_first_put = False
                raise RuntimeError("lost acknowledgement")
            return committed

    source, manifest = _two_column_store()
    store = TransientCommitStore()
    store.objects = source.objects
    store.events = source.events
    target = FakeTarget(stats={"amount": STATS, "status": STATS})

    ColCheck(store, lambda _cfg: target, poll_interval=0, result_batch_size=1).run()

    expected = {
        unit_id_for(manifest, manifest["windows"][0], column) for column in manifest["columns"]
    }
    assert {value["unit_id"] for _key, value in list_results(store)} == expected
    assert list_failures(store) == []
    assert len(store.list_keys(RESULT_BATCH_PREFIX)) == 2


def test_batch_storage_failure_does_not_turn_matches_into_unit_failures():
    class FailingCommitStore(MemoryStore):
        def commit_results(self, _results):
            raise OSError("volume unavailable")

    source, _manifest = _two_column_store()
    store = FailingCommitStore()
    store.objects = source.objects
    target = FakeTarget(stats={"amount": STATS, "status": STATS})

    with pytest.raises(ResultBatchCommitError, match="after 3 attempts"):
        ColCheck(store, lambda _cfg: target, poll_interval=0, result_batch_size=1).run()

    assert list_results(store) == []
    assert list_failures(store) == []
