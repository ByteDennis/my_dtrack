import json

import boto3
import pytest
from support import pairs_config

from dtrack_left.checks import rollback
from dtrack_left.cli import main
from dtrack_local import pipeline
from dtrack_right.backends.targets import CsvAnchorTarget


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


def test_col_rollback_rebuilds_the_manifest_from_stage_two_settings_only(finished, synthetic):
    run = finished()
    config = pairs_config(synthetic.left)
    config["pairs"]["orders"]["col_map"].pop("STATUS")
    dry = rollback.rollback(run.store, "orders", "col", config=config)
    assert dry["keys"][:2] == ["_STATUS.json", "manifest/_CLOSED"] and "deleted" not in dry
    assert run.store.exists("manifest/_CLOSED")
    done = rollback.rollback(run.store, "orders", "col", config=config, apply=True)
    assert done["deleted"] == dry["planned"] and done["expected_units"] == 6
    assert not any(document["pair"] == "orders" for _, document in run.store.list_json("results/"))
    run = finished(config, pairs=["orders"])
    pipeline.col_check(run)
    summary = pipeline.report(run)["summary"]
    assert summary["status"] == "DONE" and summary["units_completed"] == 9
    assert summary["findings"]["col_stat_diff"] == 1
    moved = pairs_config(synthetic.left)
    moved["pairs"]["orders"]["fromDate"] = "2026-01-02"
    with pytest.raises(ValueError, match=r"Stage 1 settings changed \(fromDate\)"):
        rollback.rollback(run.store, "orders", "col", config=moved)
    with pytest.raises(ValueError, match="only rebuilds"):
        rollback.rollback(run.store, "orders", "row", config=config)


def test_row_rollback_recounts_one_pair_and_ends_where_it_started(finished):
    run = finished()
    done = rollback.rollback(run.store, "orders", "row", apply=True)
    assert "row_check/orders.json" in done["keys"] and "manifest/orders.json" in done["keys"]
    assert run.store.exists("row_check/customers.json") and not run.store.exists(
        "row_check/_CLOSED"
    )
    run = finished(pairs=["orders"])
    pipeline.row_check(run)
    pipeline.col_check(run)
    summary = pipeline.report(run)["summary"]
    assert summary["findings"] == {"row_count_diff": 1, "partition_missing": 2, "col_stat_diff": 2}
    assert summary["units_completed"] == 11 and summary["status"] == "DONE"


def test_dead_rollback_only_touches_dead_units_of_the_pair(finished):
    run = finished()
    run.store.put_json("dead/x.json", {"pair": "orders", "unit_id": "x"})
    run.store.put_json("dead/y.json", {"pair": "customers", "unit_id": "y"})
    assert rollback.rollback(run.store, "orders", "dead", apply=True)["keys"] == ["dead/x.json"]
    assert run.store.exists("dead/y.json")
    with pytest.raises(ValueError, match="stage must be"):
        rollback.plan(run.store, "orders", "all")


def test_rollback_deletes_from_s3_run_folders(s3_bucket):
    store = rollback.open_location(f"s3://{s3_bucket}/runs/r")
    store.put_json("row_check/_CLOSED", {"pairs": ["p"]})
    store.put_json("results/u.json", {"pair": "p"})
    store.put_json("results/v.json", {"pair": "q"})
    outcome = rollback.rollback(store, "p", "col", apply=True)
    assert outcome["keys"] == ["results/u.json"] and outcome["deleted"] == 1
    left = boto3.client("s3").list_objects_v2(Bucket=s3_bucket)["Contents"]
    assert sorted(row["Key"] for row in left) == [
        "runs/r/results/v.json",
        "runs/r/row_check/_CLOSED",
    ]
    assert any("dtrack col-check --s3 s3://" in step for step in outcome["next"])


def test_rollback_command_is_a_dry_run_until_yes(finished, tmp_path, capsys):
    folder = str(finished().folder)
    assert main(["rollback", "--run", folder, "--pair", "orders", "--stage", "dead"]) == 0
    dry = json.loads(capsys.readouterr().out)
    assert dry["planned"] == 0 and "add --yes" in dry["dry_run"]
    argv = ["rollback", "--run", folder, "--pair", "orders", "--stage", "col", "--yes"]
    assert main(argv) == 0
    outcome = json.loads(capsys.readouterr().out)
    assert outcome["deleted"] > 0 and "dry_run" not in outcome
    assert any("notebook 02" in step for step in outcome["next"])
    assert rollback.next_steps("row", "p", "s3://b/r")[0].startswith(
        "distributed: dtrack row-check"
    )
