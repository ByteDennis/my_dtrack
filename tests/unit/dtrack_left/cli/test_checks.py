import json

from support import all_match_manifest, pairs_config

from dtrack_left.backends.store import S3Store
from dtrack_left.cli import checks as checks_cli
from dtrack_left.cli import main
from dtrack_left.common.manifest import build_manifest
from dtrack_left.common.protocol import list_units


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


def test_row_check_reuses_the_same_run_unless_force_is_set(
    s3_bucket, synthetic, tmp_path, capsys, monkeypatch
):
    root = f"s3://{s3_bucket}/runs"
    argv = [
        "row-check",
        "--config",
        _config(synthetic, tmp_path),
        "--run-s3",
        root,
        "--run-id",
        "reusable",
        "--pair",
        "orders",
        "--workers",
        "1",
    ]
    original = checks_cli.run_row_check
    calls = []

    def tracked(*args, **kwargs):
        calls.append(kwargs["run_id"])
        return original(*args, **kwargs)

    monkeypatch.setattr(checks_cli, "run_row_check", tracked)
    assert main(argv) == 0
    capsys.readouterr()
    assert main(argv) == 0
    capsys.readouterr()
    assert calls == ["reusable"]

    assert main([*argv, "--force"]) == 0
    capsys.readouterr()
    assert calls == ["reusable", "reusable"]


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
    argv = [
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
    assert main(argv) == 0
    outcome = json.loads(capsys.readouterr().out)
    assert outcome["published_units"] == 8 and outcome["pairs"] == ["orders"]
    store = S3Store(f"{root}/left-first")
    assert store.get_json("pairs/orders/plan.json")["version"] == 2
    assert store.exists("_status/col_check.closed.json")
    assert not store.exists("_status/row_check.closed.json")

    assert main(argv) == 0
    assert json.loads(capsys.readouterr().out)["published_units"] == 0
    assert main([*argv, "--force"]) == 0
    assert json.loads(capsys.readouterr().out)["published_units"] == 8
    assert len(list_units(store, ["orders"])) == 8


def _sas_config(tmp_path):
    document = {
        "version": 2,
        "pairs": {
            "ledger": {
                "left": {
                    "source": "oracle",
                    "table": "LEDGER",
                    "date_col": "BUSINESS_DATE",
                    "date_type": "date",
                    "conn_macro": "finance",
                    "user": "must-not-leak",
                    "password": "must-not-leak",
                    "dsn": "must-not-leak",
                },
                "right": {
                    "source": "databricks",
                    "table": "main.finance.ledger",
                    "date_col": "business_date",
                    "date_type": "date",
                },
                "fromDate": "2026-01-01",
                "toDate": "2026-01-02",
                "vintage": "month",
                "key": "ACCOUNT_ID",
                "key_families": {"ACCOUNT_ID": "numeric"},
                "sample": 1,
                "excluded_partitions": ["2026-01-02"],
                "excluded_columns": ["ACCOUNT_ID"],
                "col_map": {
                    "ACCOUNT_ID": "account_id",
                    "AMOUNT": "amount",
                    "SAS_DT": {"right": "event_date", "type": "sas_datetime_date"},
                },
                "col_type_overrides": {"AMOUNT": "numeric"},
            }
        },
    }
    path = tmp_path / "sas.json"
    path.write_text(json.dumps(document))
    return path


def test_left_run_exports_and_imports_one_sas_bundle(s3_bucket, tmp_path, capsys, monkeypatch):
    config = _sas_config(tmp_path)
    bundle = tmp_path / "sas-bundle"
    assert (
        main(
            [
                "left-run",
                "--config",
                str(config),
                "--run-id",
                "sas-run",
                "--export-sas",
                str(bundle),
            ]
        )
        == 0
    )
    exported = json.loads(capsys.readouterr().out)
    assert exported["mode"] == "export-sas" and exported["pairs"] == ["ledger"]
    request = json.loads((bundle / "request.json").read_text())
    serialized = json.dumps(request)
    assert "must-not-leak" not in serialized
    pair = request["pairs"]["ledger"]
    assert pair["config"]["col_map"] == {
        "ACCOUNT_ID": "account_id",
        "AMOUNT": "amount",
        "SAS_DT": "event_date",
    }
    assert pair["config"]["column_types"]["SAS_DT"]["left_date_type"] == {
        "type": "sas",
        "format": "datetime",
    }
    script = (bundle / "left-run.sas").read_text()
    assert "connect to oracle" in script.lower()
    assert "STANDARD_HASH" in script and "PROC EXPORT" in script.upper()
    assert "intnx('month'" in script and 'ACCOUNT_ID AS "ACCOUNT_ID"' in script

    output = bundle / "output"
    pair_output = output / pair["directory"]
    (pair_output / "counts.csv").write_text(
        "dtrack_partition,dtrack_count\n2026-01-01,2\n2026-01-02,3\n"
    )
    (pair_output / "windows" / "2026-01-01.csv").write_text(
        "BUSINESS_DATE,ACCOUNT_ID,AMOUNT,SAS_DT\n2026-01-01,7,12.50,2082844800\n"
    )
    (pair_output / "keys" / "2026-01-01.csv").write_text("ACCOUNT_ID\n7\n")
    (output / "_SUCCESS.json").write_text(
        json.dumps(
            {
                "format": request["format"],
                "run_id": request["run_id"],
                "config_hash": request["config_hash"],
            },
            sort_keys=True,
            separators=(",", ":"),
        )
    )

    root = f"s3://{s3_bucket}/sas-runs"
    monkeypatch.setenv("DTRACK_RUN_S3", root)
    assert (
        main(
            [
                "left-run",
                "--config",
                str(config),
                "--import-sas",
                str(bundle),
                "--pull-workers",
                "1",
                "--stat-workers",
                "1",
            ]
        )
        == 0
    )
    imported = json.loads(capsys.readouterr().out)
    assert imported == {
        "mode": "import-sas",
        "pairs": ["ledger"],
        "published_units": 2,
        "run": f"{root}/sas-run",
        "run_id": "sas-run",
    }
    store = S3Store(f"{root}/sas-run")
    assert store.get_json("pairs/ledger/row_check/left.json")["counts"] == {
        "2026-01-01": 2,
        "2026-01-02": 3,
    }
    plan = store.get_json("pairs/ledger/plan.json")
    assert plan["columns"] == ["AMOUNT", "SAS_DT"]
    assert plan["col_map"] == {"AMOUNT": "amount", "SAS_DT": "event_date"}
    assert plan["windows"] == [{"bucket": "2026-01-01", "n_left": 2, "partitions": ["2026-01-01"]}]
    assert store.get_json("pairs/ledger/fixed_keys.json")["windows"] == {"2026-01-01": [["7"]]}
    assert store.get_json("_status/col_check.closed.json")["expected_units"] == 2


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
