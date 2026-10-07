import json
import threading
from types import SimpleNamespace

import boto3
import duckdb
import pytest
from support import MemoryStore, pairs_config

from dtrack_local import pipeline
from dtrack_local.csv_target import CsvAnchorTarget


# >>> the full local plan on the e2e synthetic files: CSV anchor as the Right side, report and backup to S3 <<< #
def test_run_all_matches_the_distributed_protocol_and_backs_up(synthetic, tmp_path, s3_bucket):
    def anchor(config):
        return CsvAnchorTarget(config, synthetic.root / "right", tmp_path / "rw")

    def open_local():
        return pipeline.open_run(
            pairs_config(synthetic.left), run_id="r1", out=tmp_path / "runs", target_factory=anchor
        )

    outcome = pipeline.run_all(open_local(), run_s3=f"s3://{s3_bucket}/runs")
    assert outcome["status"] == "DONE" and outcome["findings"] == {
        "row_count_diff": 1,
        "partition_missing": 2,
        "col_stat_diff": 2,
    }
    assert outcome["row_check"]["orders"]["summary"] == {
        "match": 56,
        "count_diff": 1,
        "left_only": 1,
        "right_only": 1,
    }
    assert outcome["col_check"]["compared"] == 11 and outcome["col_check"]["dead"] == 0
    assert set(outcome["landing"]) == {"left__orders", "left__customers"}
    assert outcome["error_log"].endswith("r1/_local/logs/errors.log")
    audit = (tmp_path / "runs/r1/audit.txt").read_text()
    assert "SELECT" in audit and "-- timestamp=" not in audit
    assert list((tmp_path / "runs/r1/audit").glob("*.txt"))
    keys = {row["Key"] for row in boto3.client("s3").list_objects_v2(Bucket=s3_bucket)["Contents"]}
    assert "runs/r1/row_check/orders.json" in keys and "runs/r1/report/index.html" in keys
    assert "runs/r1/report/summary.json" in keys
    assert "runs/r1/audit.txt" in keys and any(key.startswith("runs/r1/audit/") for key in keys)
    assert not any(key.startswith("runs/r1/_local/") for key in keys)

    again = pipeline.run_all(open_local())
    assert again["findings"] == outcome["findings"] and again["landing"] == outcome["landing"]
    assert open_local().status()["state"] == "complete"


class FakeWarehouse:
    def __init__(self, rows):
        self.sql = []
        self.rows_out = rows

    def to_csv(self, sql, output, columns, **_kwargs):
        from dtrack_left.backends.sources import write_rows

        self.sql.append(sql)
        write_rows(output, columns, self.rows_out)
        return len(self.rows_out)

    def rows(self, sql, **_kwargs):
        self.sql.append(sql)
        return [(row[0],) for row in self.rows_out[:16]]


MANIFEST = {
    "pair": "orders",
    "columns": ["AMOUNT", "STATUS"],
    "col_map": {"AMOUNT": "amount", "STATUS": "status"},
    "right_key_cols": ["order_id"],
    "key_families": ["numeric"],
    "sample": {"mode": "all"},
    "strict": False,
    "col_type_overrides": {},
}


def test_warehouse_target_pulls_native_columns_and_computes_statistics_locally(tmp_path):
    rows = [("2026-01-01", "1", "10.50", "OPEN"), ("2026-01-02", "2", "3", None)]
    warehouse = FakeWarehouse(rows)
    landing = pipeline.Landing(tmp_path / "data.duckdb")
    config = {"catalog": "c", "schema": "s", "table": "orders", "date_col": "business_date"}
    target = pipeline.WarehouseTarget(
        warehouse, config, tmp_path / "w", landing=landing
    )
    window = {"bucket": "2026-01-01", "partitions": ["2026-01-01", "2026-01-02"]}
    stats = target.window_statistics(MANIFEST, window)
    assert set(stats) == {"amount", "status"} and stats["amount"]["n_total"] == 2
    assert stats["status"]["n_missing"] == 1
    sql = next(item for item in warehouse.sql if item.startswith("SELECT `business_date`,"))
    assert sql.startswith("SELECT `business_date`, `order_id`, `amount`, `status`")
    assert " AS STRING) AS " not in sql
    assert "FROM `c`.`s`.`orders` WHERE" in sql
    assert landing.tables() == {"right__orders": 2}
    target.window_statistics(MANIFEST, window)
    assert landing.tables() == {"right__orders": 2}


def test_sample_filters_in_the_warehouse_before_transfer(tmp_path):
    warehouse = FakeWarehouse([("2026-01-01", "1", "10.50", "OPEN")])
    config = {"table": "orders", "date_col": "dt"}
    target = pipeline.WarehouseTarget(warehouse, config, tmp_path)
    manifest = {
        **MANIFEST,
        "sample": {"mode": "fraction", "threshold": 2**32, "rate": 1.0},
    }
    stats = target.window_statistics(manifest, {"bucket": "b", "partitions": ["2026-01-01"]})
    pulled = next(item for item in warehouse.sql if item.startswith("SELECT `dt`,"))
    assert "md5(" in pulled and stats["amount"]["n_total"] == 1
    assert not (tmp_path / "orders" / "b" / "right.sampled.csv").exists()


def test_local_col_check_consumes_while_left_is_still_publishing(tmp_path, monkeypatch):
    events = []
    consumer_started = threading.Event()
    manifest = {"pair": "orders", "sample": {"mode": "all"}}

    class Producer:
        def __init__(self, *_args, **_kwargs):
            pass

        def run(self, manifests):
            assert manifests == [manifest]
            events.append("producer-start")
            consumer_started.wait(1)
            events.append("producer-finish")
            return 1

    class Consumer:
        def __init__(self, *_args, **_kwargs):
            pass

        def run(self):
            events.append("consumer-start")
            consumer_started.set()
            return [{"verdict": "match"}]

    store = MemoryStore()
    run = SimpleNamespace(
        store=store,
        local=tmp_path,
        landing=None,
        target_factory=lambda _config: None,
        pairs=None,
    )
    monkeypatch.setattr(pipeline, "wait_for_manifests", lambda *_args, **_kwargs: [manifest])
    monkeypatch.setattr(pipeline, "publish_fixed_keys", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pipeline, "ColCheckPipeline", Producer)
    monkeypatch.setattr(pipeline, "ColCheck", Consumer)

    result = pipeline.col_check(run)

    assert events == ["producer-start", "consumer-start", "producer-finish"]
    assert result == {"published_units": 1, "compared": 1, "verdicts": {"match": 1}, "dead": 0}


def test_local_col_check_surfaces_a_concurrent_left_failure(tmp_path, monkeypatch, caplog):
    producer_failed = threading.Event()
    manifest = {"pair": "orders", "sample": {"mode": "all"}}

    class BrokenProducer:
        def __init__(self, *_args, **_kwargs):
            pass

        def run(self, _manifests):
            producer_failed.set()
            raise RuntimeError("left failed")

    class IdleConsumer:
        def __init__(self, *_args, on_idle, **_kwargs):
            self.on_idle = on_idle

        def run(self):
            assert producer_failed.wait(1)
            self.on_idle()

    run = SimpleNamespace(
        store=MemoryStore(),
        local=tmp_path,
        landing=None,
        target_factory=lambda _config: None,
        pairs=None,
    )
    monkeypatch.setattr(pipeline, "wait_for_manifests", lambda *_args, **_kwargs: [manifest])
    monkeypatch.setattr(pipeline, "publish_fixed_keys", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(pipeline, "ColCheckPipeline", BrokenProducer)
    monkeypatch.setattr(pipeline, "ColCheck", IdleConsumer)

    with pytest.raises(RuntimeError, match="left failed"):
        pipeline.col_check(run)
    assert "local col-check failed" in caplog.text


def test_landing_keeps_raw_text_per_bucket(tmp_path):
    csv = tmp_path / "w.csv"
    csv.write_text("ID,AMOUNT\n007,1.50\n")
    landing = pipeline.Landing(tmp_path / "d.duckdb")
    assert landing.land("left", "p", "2026-01-01", csv) == 1
    assert landing.land("left", "p", "2026-02-01", csv) == 1
    with duckdb.connect(str(tmp_path / "d.duckdb"), read_only=True) as connection:
        rows = connection.execute('SELECT * FROM "left__p" ORDER BY dtrack_bucket').fetchall()
    assert rows == [("007", "1.50", "2026-01-01"), ("007", "1.50", "2026-02-01")]
    assert json.loads(json.dumps(landing.tables())) == {"left__p": 2}


def test_cli_runs_the_plan_and_preflight_reports_failures(synthetic, tmp_path, monkeypatch, capsys):
    from dtrack_local.cli import main

    config = tmp_path / "c.json"
    config.write_text(json.dumps(pairs_config(synthetic.left)))
    opened = []

    def open_run(path, **kwargs):
        opened.append(kwargs)
        right = synthetic.root / "right"
        return pipeline.LocalRun(
            pipeline.load_config(path),
            kwargs["run_id"] or "cli",
            tmp_path / "runs" / (kwargs["run_id"] or "cli"),
            lambda spec: CsvAnchorTarget(spec, right, tmp_path / "rw"),
            pipeline.Landing(tmp_path / "d.duckdb"),
            kwargs["pairs"],
        )

    monkeypatch.setattr(pipeline, "open_run", open_run)
    assert (
        main(["run", "--config", str(config), "--pair", "orders", "--inline"]) == 0
    )
    outcome = json.loads(capsys.readouterr().out)
    assert outcome["status"] == "DONE" and outcome["findings"]["row_count_diff"] == 1
    assert opened[0]["pairs"] == ["orders"] and opened[0]["inline"] is True
    (synthetic.root / "right" / "orders.csv").rename(tmp_path / "moved.csv")
    try:
        assert main(["preflight", "--config", str(config), "--run-id", "p"]) == 1
    finally:
        (tmp_path / "moved.csv").rename(synthetic.root / "right" / "orders.csv")
    assert json.loads(capsys.readouterr().out)["orders"]["right"]["ok"] is False


def test_local_config_supplies_every_optional_run_value(tmp_path, monkeypatch, capsys):
    import dtrack_local.cli as local_cli

    config = tmp_path / "config.json"
    document = pairs_config(tmp_path)
    document["local"] = {
        "run-id": "config-run",
        "out": str(tmp_path / "configured-runs"),
        "profile": "config-profile",
        "warehouse-id": "config-warehouse",
        "inline": True,
        "pair": "orders, customers",
        "log-level": "WARNING",
        "run": {
            "gate": "manual",
            "run-s3": "s3://config/runs",
        },
    }
    config.write_text(json.dumps(document))
    seen = {}

    monkeypatch.setattr(
        pipeline,
        "open_run",
        lambda path, **kwargs: seen.update(path=path, open=kwargs) or "run",
    )
    monkeypatch.setattr(
        pipeline,
        "run_all",
        lambda run, **kwargs: seen.update(run=run, execute=kwargs)
        or {
            "run_id": "config-run",
            "folder": "folder",
            "status": "DONE",
            "findings": {},
            "backup": {},
            "landing": {},
            "error_log": "errors.log",
        },
    )

    assert local_cli.main(["run", "--config", str(config)]) == 0
    json.loads(capsys.readouterr().out)
    assert seen["open"] == {
        "run_id": "config-run",
        "out": str(tmp_path / "configured-runs"),
        "profile": "config-profile",
        "warehouse_id": "config-warehouse",
        "inline": True,
        "pairs": ["orders", "customers"],
    }
    assert seen["execute"] == {
        "gate": "manual",
        "run_s3": "s3://config/runs",
    }


def test_cli_then_config_then_environment_precedence(tmp_path, monkeypatch, capsys):
    import dtrack_local.cli as local_cli

    config = tmp_path / "config.json"
    document = pairs_config(tmp_path)
    document["local"] = {"run-id": "config-run", "run": {"run-s3": "s3://config/runs"}}
    config.write_text(json.dumps(document))
    opened, executed = [], []
    monkeypatch.setattr(
        pipeline,
        "open_run",
        lambda _path, **kwargs: opened.append(kwargs) or "run",
    )
    monkeypatch.setattr(
        pipeline,
        "run_all",
        lambda _run, **kwargs: executed.append(kwargs)
        or {
            "run_id": kwargs.get("run_id", "r"),
            "folder": "folder",
            "status": "DONE",
            "findings": {},
            "backup": {},
            "landing": {},
            "error_log": "errors.log",
        },
    )
    monkeypatch.setenv("DTRACK_RUN_ID", "env-run")
    monkeypatch.setenv("DTRACK_RUN_S3", "s3://env/runs")

    assert local_cli.main(["run", "--config", str(config)]) == 0
    capsys.readouterr()
    assert opened[-1]["run_id"] == "config-run"
    assert executed[-1]["run_s3"] == "s3://config/runs"

    argv = ["run", "--config", str(config), "--run-id", "cli-run", "--run-s3", "s3://cli/runs"]
    assert local_cli.main(argv) == 0
    capsys.readouterr()
    assert opened[-1]["run_id"] == "cli-run"
    assert executed[-1]["run_s3"] == "s3://cli/runs"


def test_local_help_reads_config_before_showing_effective_defaults(tmp_path, monkeypatch, capsys):
    from dtrack_local.cli import main

    config = tmp_path / "config.json"
    document = pairs_config(tmp_path)
    document["local"] = {
        "run-id": "config-run",
        "run": {"run-s3": "s3://config/runs"},
    }
    config.write_text(json.dumps(document))
    monkeypatch.setenv("DTRACK_RUN_ID", "env-run")
    monkeypatch.setenv("DTRACK_RUN_S3", "s3://env/runs")

    assert main(["run", "--config", str(config), "--help"]) == 0
    output = " ".join(capsys.readouterr().out.split())
    assert "default: config-run" in output
    assert "default: s3://config/runs" in output
    assert "default: env-run" not in output
    assert "default: s3://env/runs" not in output


def test_local_help_shows_effective_environment_defaults(monkeypatch, capsys):
    from dtrack_local.cli import main

    monkeypatch.setenv("DTRACK_RUN_ID", "env-run")
    monkeypatch.setenv("DTRACK_RUN_S3", "s3://env/runs")
    monkeypatch.setenv("DTRACK_LOCAL_INLINE", "1")
    assert main(["run", "--help"]) == 0
    output = capsys.readouterr().out
    compact = " ".join(output.split())
    assert "--inline" in output and "--disposition" not in output
    assert "--sample-locally" not in output and "--sample-in-warehouse" not in output
    assert "--backup-data" not in output and "--include-data" not in output
    assert "DTRACK_PAIRS" in compact and "DTRACK_DEBUG" in compact
    assert "default: env-run" in compact
    assert "default: s3://env/runs" in compact
    assert "DTRACK_LOCAL_INLINE" in compact and "default: 1" in compact


def test_key_check_cli_runs_the_production_sample_for_one_window(
    synthetic, tmp_path, monkeypatch, capsys
):
    import dtrack_local.cli as local_cli

    config = tmp_path / "c.json"
    config.write_text(json.dumps(pairs_config(synthetic.left)))
    seen = {}
    source = SimpleNamespace(
        partition_counts=lambda start, end: (
            seen.update(window=(start, end)) or {"2026-01-02": 4, "2026-01-03": 5}
        )
    )
    check = SimpleNamespace(
        sample={"mode": "fraction", "rate": 0.1, "threshold": 1},
        selected_keys=[],
        artifacts={"report": str(tmp_path / "keys.txt")},
        summary=lambda: {"pair": "orders", "sample mode": "fraction", "ok": True},
    )
    monkeypatch.setattr(local_cli, "source_from_config", lambda _spec: source)
    monkeypatch.setattr(local_cli, "_keycheck_target", lambda *_args, **_kwargs: "target")
    monkeypatch.setattr(
        local_cli.keycheck,
        "run_sample_check",
        lambda *args, **kwargs: seen.update(args=args, kwargs=kwargs) or check,
    )
    code = local_cli.main(
        [
            "key-check",
            "--config",
            str(config),
            "--pair",
            "orders",
            "--from-date",
            "2026-01-01",
            "--to-date",
            "2026-01-03",
            "--out",
            str(tmp_path / "out"),
            "--key",
            "ACCT_ID",
            "--family",
            "ACCT_ID=numeric",
            "--sample",
            "25",
        ]
    )
    outcome = json.loads(capsys.readouterr().out)
    assert code == 0 and outcome["partitions"] == ["2026-01-02", "2026-01-03"]
    assert outcome["left window rows"] == 9 and outcome["sample mode"] == "fraction"
    assert seen["window"] == ("2026-01-01", "2026-01-03")
    assert seen["args"][4] == ["2026-01-02", "2026-01-03"]
    checked_pair = seen["args"][1]
    assert checked_pair["key"] == "ACCT_ID" and checked_pair["sample"] == "25"
    assert checked_pair["key_families"]["ACCT_ID"] == "numeric"
