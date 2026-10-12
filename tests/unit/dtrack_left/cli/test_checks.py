import json

from support import all_match_manifest, pairs_config

from dtrack_left.backends.store import S3Store
from dtrack_left.cli import main
from dtrack_left.common.manifest import build_manifest


def _config(synthetic, tmp_path):
    path = tmp_path / "config.json"
    path.write_text(json.dumps(pairs_config(synthetic.left)))
    return str(path)


def test_row_check_command_writes_left_counts_to_s3(s3_bucket, synthetic, tmp_path, capsys):
    root = f"s3://{s3_bucket}/runs"
    config = _config(synthetic, tmp_path)
    argv = [
        "row-check",
        "--config",
        config,
        "--run-s3",
        root,
        "--run-id",
        "r1",
        "--pair",
        "orders",
    ]
    assert main([*argv, "--workers", "1", "--log-level", "debug"]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "pairs": 1,
        "run": f"{root}/r1",
        "run_id": "r1",
    }
    store = S3Store(f"{root}/r1")
    closed = store.get_json("_status/left_row_check.closed.json")
    assert closed["pairs"] == ["orders"] and closed["run_id"] == "r1"
    assert store.get_json("pairs/orders/row_check/left.json")["counts"]["2026-01-15"] == 30


def test_col_check_command_publishes_units_once_manifests_are_released(
    s3_bucket, synthetic, tmp_path, capsys, monkeypatch
):
    root = f"s3://{s3_bucket}/runs"
    monkeypatch.setenv("DTRACK_RUN_S3", root)
    assert (
        main(
            [
                "row-check",
                "--config",
                _config(synthetic, tmp_path),
                "--run-id",
                "r2",
                "--pair",
                "orders",
            ]
        )
        == 0
    )
    capsys.readouterr()
    store = S3Store(f"{root}/r2")
    left = store.get_json("pairs/orders/row_check/left.json")
    assert left["run_id"] == "r2"
    manifest = all_match_manifest(
        build_manifest, left["run_id"], "orders", left["pair_config"], left["counts"]
    )
    store.put_json("pairs/orders/plan.json", manifest)
    store.close_row_check(["orders"])
    argv = ["col-check", "--work-dir", str(tmp_path / "work"), "--wait-timeout", "1"]
    assert main([*argv, "--poll-interval", "0", "--pull-workers", "1", "--wip", "50"]) == 0
    assert json.loads(capsys.readouterr().out) == {
        "pairs": ["orders"],
        "published_units": 8,
        "run": f"{root}/r2",
        "run_id": "r2",
    }
    assert store.get_json("_status/col_check.closed.json")["expected_units"] == 8


def test_left_run_completes_version_two_without_a_right_signal(
    s3_bucket, synthetic, tmp_path, capsys
):
    root = f"s3://{s3_bucket}/runs"
    document = pairs_config(synthetic.left)
    document["version"] = 2
    path = tmp_path / "version2.json"
    path.write_text(json.dumps(document))
    assert (
        main(
            [
                "left-run",
                "--config",
                str(path),
                "--run-s3",
                root,
                "--run-id",
                "left-first",
                "--pair",
                "orders",
                "--workers",
                "1",
                "--pull-workers",
                "1",
                "--stat-workers",
                "1",
                "--work-dir",
                str(tmp_path / "left-first-work"),
            ]
        )
        == 0
    )
    outcome = json.loads(capsys.readouterr().out)
    assert outcome["published_units"] == 8 and outcome["pairs"] == ["orders"]
    store = S3Store(f"{root}/left-first")
    assert store.get_json("pairs/orders/plan.json")["version"] == 2
    assert store.exists("_status/col_check.closed.json")
    assert not store.exists("_status/row_check.closed.json")


# >>> --run-s3 stays stable; row-check appends either the supplied or generated run id <<< #
def test_row_check_appends_a_generated_run_id_to_the_runs_root(
    s3_bucket, synthetic, tmp_path, capsys
):
    root = f"s3://{s3_bucket}/runs"
    config = _config(synthetic, tmp_path)
    assert main(["row-check", "--config", config, "--run-s3", root, "--pair", "orders"]) == 0
    outcome = json.loads(capsys.readouterr().out)
    assert outcome["run"] == f"{root}/{outcome['run_id']}"
    assert S3Store(outcome["run"]).exists("pairs/orders/row_check/left.json")


def test_row_check_rejects_a_nested_run_id(s3_bucket, synthetic, tmp_path, capsys):
    root = f"s3://{s3_bucket}/runs"
    assert (
        main(
            [
                "row-check",
                "--config",
                _config(synthetic, tmp_path),
                "--run-s3",
                root,
                "--run-id",
                "../other",
            ]
        )
        == 2
    )
    assert "one non-empty folder" in capsys.readouterr().err
