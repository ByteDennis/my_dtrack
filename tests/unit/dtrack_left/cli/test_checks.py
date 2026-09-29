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
    uri = f"s3://{s3_bucket}/runs/r1"
    config = _config(synthetic, tmp_path)
    argv = ["row-check", "--config", config, "--s3", uri, "--run-id", "r1", "--pair", "orders"]
    assert main([*argv, "--workers", "1", "--log-level", "debug"]) == 0
    assert json.loads(capsys.readouterr().out) == {"pairs": 1, "run_id": "r1"}
    store = S3Store(uri)
    assert store.get_json("row_check/_LEFT_CLOSED") == {"pairs": ["orders"], "run_id": "r1"}
    assert store.get_json("row_check/orders.left.json")["counts"]["2026-01-15"] == 30


def test_col_check_command_publishes_units_once_manifests_are_released(
    s3_bucket, synthetic, tmp_path, capsys, monkeypatch
):
    uri = f"s3://{s3_bucket}/runs/r2"
    monkeypatch.setenv("DTRACK_S3", uri)
    assert main(["row-check", "--config", _config(synthetic, tmp_path), "--pair", "orders"]) == 0
    capsys.readouterr()
    store = S3Store(uri)
    left = store.get_json("row_check/orders.left.json")
    assert left["run_id"] == "r2"
    manifest = all_match_manifest(
        build_manifest, left["run_id"], "orders", left["pair_config"], left["counts"]
    )
    store.put_json("manifest/orders.json", manifest)
    store.close_row_check(["orders"])
    argv = ["col-check", "--work-dir", str(tmp_path / "work"), "--wait-timeout", "1"]
    assert main([*argv, "--poll-interval", "0", "--pull-workers", "1", "--wip", "50"]) == 0
    assert json.loads(capsys.readouterr().out) == {"pairs": ["orders"], "published_units": 8}
    assert store.get_json("manifest/_CLOSED")["expected_units"] == 8


# >>> the run id is the --s3 folder, so data, documents and report always share one name <<< #
def test_row_check_refuses_a_run_id_that_differs_from_the_run_folder(synthetic, tmp_path, capsys):
    config = _config(synthetic, tmp_path)
    assert main(["row-check", "--config", config, "--s3", "s3://b/runs/A", "--run-id", "B"]) == 2
    assert "differs from the --s3 run folder 'A'" in capsys.readouterr().err
    assert main(["row-check", "--config", config, "--s3", "s3://b"]) == 2
    assert "must name the run folder" in capsys.readouterr().err
