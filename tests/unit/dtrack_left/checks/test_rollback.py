import json
from pathlib import Path

import boto3
import pytest
from support import MemoryStore, pairs_config

from dtrack_left.backends import store as store_module
from dtrack_left.checks import rollback
from dtrack_left.cli import main
from dtrack_left.common.protocol import list_results, list_units
from dtrack_local import pipeline
from dtrack_local.csv_target import CsvAnchorTarget


# >>> a finished local run on the e2e synthetic files, and a way to reopen it with a changed config <<< #
@pytest.fixture
def finished(synthetic, tmp_path):
    def reopen(config=None, pairs=None):
        return pipeline.open_run(
            config or pairs_config(synthetic.left),
            run_id="r",
            out=tmp_path / "runs",
            pairs=pairs,
            target_factory=lambda spec: CsvAnchorTarget(spec, synthetic.root / "right", tmp_path),
        )

    first = pipeline.run_all(reopen())
    assert first["findings"] == {"row_count_diff": 1, "partition_missing": 2, "col_stat_diff": 2}
    return reopen


@pytest.mark.slow
def test_col_rollback_rebuilds_the_manifest_from_stage_two_settings_only(finished, synthetic):
    run = finished()
    report_keys = run.store.list_keys("report/")
    assert report_keys
    config = pairs_config(synthetic.left)
    config["pairs"]["orders"]["col_map"].pop("STATUS")
    dry = rollback.rollback(run.store, "orders", "col", config=config)
    assert dry["keys"][:2] == [
        "_status/col_check.closed.json",
        "_status/run.json",
    ]
    assert "deleted" not in dry
    assert set(report_keys).issubset(dry["keys"])
    assert all(Path(row["location"]).is_absolute() for row in dry["targets"])
    assert [row["key"] for row in dry["targets"]] == dry["keys"]
    assert run.store.exists("_status/col_check.closed.json")
    done = rollback.rollback(run.store, "orders", "col", config=config, apply=True)
    assert done["deleted"] == dry["planned"] and done["expected_units"] == 6
    assert done["failed"] == done["missing"] == 0
    assert {row["status"] for row in done["deletions"]} == {"deleted"}
    assert not run.store.list_keys("report/")
    assert not any(document["pair"] == "orders" for _, document in list_results(run.store))
    run = finished(config, pairs=["orders"])
    pipeline.col_check(run)
    summary = pipeline.report(run)["summary"]
    assert summary["status"] == "DONE" and summary["units_completed"] == 9
    assert summary["findings"]["col_stat_diff"] == 1
    moved = pairs_config(synthetic.left)
    moved["pairs"]["orders"]["fromDate"] = "2026-01-02"
    with pytest.raises(ValueError, match=r"Stage 1 settings changed \(fromDate\)"):
        rollback.rollback(run.store, "orders", "col", config=moved)
    with pytest.raises(ValueError, match="only be used"):
        rollback.rollback(run.store, "orders", "dead", config=config)


@pytest.mark.slow
def test_row_rollback_recounts_one_pair_and_ends_where_it_started(finished):
    run = finished()
    done = rollback.rollback(run.store, "orders", "row", apply=True)
    assert "pairs/orders/row_check/result.json" in done["keys"]
    assert "pairs/orders/plan.json" in done["keys"]
    assert run.store.exists("pairs/customers/row_check/result.json")
    assert not run.store.exists("_status/row_check.closed.json")
    run = finished(pairs=["orders"])
    pipeline.row_check(run)
    pipeline.col_check(run)
    summary = pipeline.report(run)["summary"]
    assert summary["findings"] == {"row_count_diff": 1, "partition_missing": 2, "col_stat_diff": 2}
    assert summary["units_completed"] == 11 and summary["status"] == "DONE"


@pytest.mark.slow
def test_row_rollback_with_config_restarts_selected_left_pairs_in_same_run(finished, synthetic):
    run = finished()
    config = pairs_config(synthetic.left)
    config["pairs"]["orders"]["fromDate"] = "2026-01-02"

    dry = rollback.rollback_many(
        run.store,
        ["orders"],
        "row",
        config=config,
    )
    assert "row_check" not in dry
    assert run.store.get_json("pairs/orders/row_check/left.json")["bounds"]["from"] == "2026-01-01"

    done = rollback.rollback_many(
        run.store,
        ["orders"],
        "row",
        config=config,
        apply=True,
    )
    assert done["row_check"] == {"run_id": "r", "pairs": 1}
    assert run.store.get_json("pairs/orders/row_check/left.json")["bounds"]["from"] == "2026-01-02"
    left_closed = run.store.get_json("_status/left_row_check.closed.json")
    assert left_closed["pairs"] == ["orders"] and left_closed["run_id"] == "r"
    assert run.store.exists("pairs/customers/row_check/left.json")
    assert not run.store.exists("_status/row_check.closed.json")
    assert all("dtrack row-check" not in step for step in done["next"])


@pytest.mark.slow
def test_row_config_dry_run_never_queries_sources(finished, synthetic, monkeypatch):
    run = finished()
    config = pairs_config(synthetic.left)

    def unexpected(*_args, **_kwargs):
        raise AssertionError("dry-run queried a source")

    monkeypatch.setattr(rollback, "run_row_check", unexpected)
    outcome = rollback.rollback_many(run.store, ["orders", "customers"], "row", config=config)
    assert "row_check" not in outcome


@pytest.mark.parametrize("stage", ["row", "col"])
@pytest.mark.slow
def test_config_rejects_skipped_pair_before_deleting(finished, synthetic, stage):
    run = finished()
    config = pairs_config(synthetic.left)
    config["pairs"]["orders"]["skip"] = True
    before = list_results(run.store)

    with pytest.raises(ValueError, match="skips selected pair"):
        rollback.rollback(run.store, "orders", stage, config=config, apply=True)

    assert list_results(run.store) == before
    assert run.store.exists("_status/row_check.closed.json")


@pytest.mark.slow
def test_dead_rollback_only_touches_dead_units_of_the_pair(finished):
    run = finished()
    run.store.put_json("pairs/orders/col_check/failures/x.json", {"pair": "orders", "unit_id": "x"})
    run.store.put_json(
        "pairs/customers/col_check/failures/y.json", {"pair": "customers", "unit_id": "y"}
    )
    assert rollback.rollback(run.store, "orders", "dead", apply=True)["keys"] == [
        "pairs/orders/col_check/failures/x.json"
    ]
    assert run.store.exists("pairs/customers/col_check/failures/y.json")
    with pytest.raises(ValueError, match="stage must be"):
        rollback.plan(run.store, "orders", "all")


def test_force_reset_removes_only_the_requested_stage_artifacts():
    store = MemoryStore()
    store.put_json("pairs/p/row_check/left.json", {"pair": "p", "run_id": "r"})
    store.put_json("pairs/p/plan.json", {"pair": "p"})
    store.put_json("pairs/p/fixed_keys.json", {"pair": "p"})
    store.put_json("pairs/p/units/00000001-u.json", {"pair": "p", "unit_id": "u"})
    store.put_json("pairs/p/col_check/results/u.json", {"pair": "p", "unit_id": "u"})
    store.put_json("_status/col_check.closed.json", {"pairs": ["p"]})
    store.put_json("_status/run.json", {"run_id": "r"})

    removed = rollback.reset_for_rerun(store, ["p"], "col")

    assert "pairs/p/units/00000001-u.json" in removed
    assert store.exists("pairs/p/row_check/left.json") and store.exists("pairs/p/plan.json")
    assert not store.exists("pairs/p/fixed_keys.json")
    assert not list_units(store) and not list_results(store)

    rollback.reset_for_rerun(store, ["p"], "row")
    assert not store.exists("pairs/p/row_check/left.json")
    assert not store.exists("pairs/p/plan.json")


@pytest.mark.slow
def test_batch_rollback_deduplicates_shared_markers_and_deletes_every_selected_pair(
    finished, monkeypatch
):
    run = finished()
    pairs = ["orders", "customers"]
    dry = rollback.rollback_many(run.store, pairs, "col")
    assert dry["pairs"] == pairs
    assert dry["keys"].count("_status/col_check.closed.json") == 1
    assert set(dry["by_pair"]) == set(pairs)
    assert len(dry["keys"]) == len(set(dry["keys"]))
    done = rollback.rollback_many(run.store, pairs, "col", apply=True)
    assert done["deleted"] == dry["planned"] and done["failed"] == 0
    assert not list_results(run.store)
    assert not list_units(run.store)


@pytest.mark.slow
def test_batch_config_builds_every_manifest_before_deleting(finished, synthetic):
    run = finished()
    config = pairs_config(synthetic.left)
    config["pairs"]["orders"]["col_map"].pop("STATUS")
    config["pairs"]["customers"]["sample"] = 0
    before_left = {
        pair: run.store.get_json(f"pairs/{pair}/row_check/left.json")
        for pair in ("orders", "customers")
    }
    before_results = list_results(run.store)

    with pytest.raises(ValueError, match="fixed sample must be positive"):
        rollback.rollback_many(
            run.store,
            ["orders", "customers"],
            "col",
            config=config,
            apply=True,
        )

    assert run.store.exists("_status/col_check.closed.json")
    assert list_results(run.store) == before_results
    for pair, left_doc in before_left.items():
        assert run.store.get_json(f"pairs/{pair}/row_check/left.json") == left_doc


@pytest.mark.slow
def test_pair_resolution_supports_all_and_refuses_typos_before_deleting(finished):
    run = finished()
    available = rollback.discover_pairs(run.store)
    assert available == ["customers", "orders"]
    assert rollback.resolve_pairs(run.store, ["all"]) == available
    assert rollback.resolve_pairs(run.store, ["orders", "customers", "orders"]) == [
        "orders",
        "customers",
    ]
    with pytest.raises(ValueError, match="cannot be combined"):
        rollback.resolve_pairs(run.store, ["all", "orders"])
    with pytest.raises(ValueError, match="unknown pair"):
        rollback.resolve_pairs(run.store, ["ordres"])
    assert run.store.exists("_status/col_check.closed.json")


def test_rollback_deletes_from_s3_run_folders(s3_bucket):
    store = rollback.open_location(f"s3://{s3_bucket}/runs/r")
    store.put_json("pairs/p/col_check/results/u.json", {"pair": "p", "unit_id": "u"})
    store.put_json("pairs/q/col_check/results/v.json", {"pair": "q", "unit_id": "v"})
    outcome = rollback.rollback(store, "p", "col", apply=True)
    assert outcome["keys"] == ["pairs/p/col_check/results/u.json"] and outcome["deleted"] == 1
    assert outcome["targets"] == [
        {
            "key": "pairs/p/col_check/results/u.json",
            "location": f"s3://{s3_bucket}/runs/r/pairs/p/col_check/results/u.json",
            "object_key": "runs/r/pairs/p/col_check/results/u.json",
        }
    ]
    assert outcome["deletions"][0]["status"] == "deleted"
    left = boto3.client("s3").list_objects_v2(Bucket=s3_bucket)["Contents"]
    assert sorted(row["Key"] for row in left) == ["runs/r/pairs/q/col_check/results/v.json"]
    assert any("dtrack col-check --run-s3 s3://" in step for step in outcome["next"])


@pytest.mark.slow
def test_rollback_command_is_a_dry_run_until_yes(finished, capsys):
    run = finished()
    assert (
        main(
            [
                "tools",
                "rollback",
                "--run-s3",
                str(run.folder.parent),
                "--run-id",
                run.run_id,
                "--pair",
                "orders",
                "--stage",
                "dead",
            ]
        )
        == 0
    )
    dry = json.loads(capsys.readouterr().out)
    assert dry["planned"] == 0 and dry["targets"] == [] and "add --yes" in dry["dry_run"]
    argv = [
        "tools",
        "rollback",
        "--run-s3",
        str(run.folder.parent),
        "--run-id",
        run.run_id,
        "--pair",
        "orders",
        "--stage",
        "col",
        "--yes",
    ]
    assert main(argv) == 0
    outcome = json.loads(capsys.readouterr().out)
    assert outcome["deleted"] > 0 and "dry_run" not in outcome
    assert any("notebook 02" in step for step in outcome["next"])
    assert rollback.next_steps_many("row", ["p"], "s3://b/r")[0].startswith(
        "distributed: dtrack row-check"
    )


@pytest.mark.slow
def test_rollback_command_accepts_repeated_pairs_and_all(finished, capsys):
    run = finished()
    base = [
        "tools",
        "rollback",
        "--run-s3",
        str(run.folder.parent),
        "--run-id",
        run.folder.name,
        "--stage",
        "col",
    ]
    assert main([*base, "--pair", "orders", "--pair", "customers"]) == 0
    multiple = json.loads(capsys.readouterr().out)
    assert multiple["pairs"] == ["orders", "customers"]

    assert main([*base, "--pair", "all"]) == 0
    all_pairs = json.loads(capsys.readouterr().out)
    assert all_pairs["pairs"] == ["customers", "orders"]

    assert main([*base, "--pair", "all", "--pair", "orders"]) == 2
    assert "cannot be combined" in capsys.readouterr().err

    assert main([*base, "--pair", "ordres", "--yes"]) == 2
    assert "unknown pair" in capsys.readouterr().err
    assert run.store.exists("_status/col_check.closed.json")


def test_local_target_rejects_paths_outside_the_run(tmp_path):
    store = rollback.LocalFolder(tmp_path / "run")
    store.heartbeat(now=123.0)
    assert store.get_json("_status/heartbeat.json")["epoch_s"] == 123.0
    with pytest.raises(ValueError, match="outside the run folder"):
        rollback.locate(store, "../other.json")


def test_local_folder_retries_a_transient_windows_access_denied(tmp_path, monkeypatch):
    store = rollback.LocalFolder(tmp_path / "run")
    store.put_bytes("_status/run.json", b"old")
    actual_replace = Path.replace
    attempts = 0

    def transient_failure(temporary, path):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            error = PermissionError("temporarily locked")
            error.winerror = 5
            raise error
        return actual_replace(temporary, path)

    monkeypatch.setattr(Path, "replace", transient_failure)
    monkeypatch.setattr(store_module.time, "sleep", lambda _delay: None)

    store.put_bytes("_status/run.json", b"new")

    assert store.get_bytes("_status/run.json") == b"new"
    assert attempts == 2
    assert not list(tmp_path.rglob("*.part"))


def test_delete_reports_a_target_that_disappeared_after_planning(tmp_path):
    store = rollback.LocalFolder(tmp_path / "run")
    path = store.root / "pairs" / "p" / "col_check" / "results" / "u.json"
    path.parent.mkdir(parents=True)
    path.write_text("{}")
    target = rollback.locate(store, "pairs/p/col_check/results/u.json")
    path.unlink()
    outcome = rollback.delete(store, [target])
    assert outcome["deleted"] == outcome["failed"] == 0
    assert outcome["missing"] == 1
    assert outcome["deletions"] == [{**target, "status": "missing"}]
