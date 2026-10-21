import json
import logging
import threading
from types import SimpleNamespace

import pytest
from support import MemoryStore

from pair_recon.app import cli, runner
from pair_recon.app.runner import make_run_id, pair_config, retry_failed, write_request_snapshot
from pair_recon.core import debug, sas
from pair_recon.core.progress import ProgressSampler, progress_snapshot
from pair_recon.core.sas import export_bundle
from pair_recon_left.common.model.protocol import (
    plan_path,
    result_path,
    unit_path,
    write_run_status,
)
from pair_recon_left.common.runtime.run_logging import CONSOLE_LOG_FORMAT, ZonedFormatter
from pair_recon_right.engine.backends.store import VolumeStore as LocalStore
from pair_recon_right.engine.backends.targets import CsvTarget
from pair_recon_right.engine.checks.row_check import run_row_check as run_right_row_check
from pair_recon_right.engine.common.model.attribute_checks import (
    AttributeCheck,
    attribute_check_names,
    get_attribute_check,
)
from pair_recon_right.engine.job import run as run_right


def test_runid():
    value = make_run_id("订单 pair", "测试 owner", "America/New_York")
    assert "/" not in value and "订单-pair-测试-owner" in value


def test_timezone_default(monkeypatch):
    monkeypatch.setenv("PAIR_RECON_TIMEZONE", "Asia/Tokyo")
    assert cli.timezone_default() == "Asia/Tokyo"


def test_pair_metadata_tables_override_json():
    config = pair_config(
        {
            "pair_name": "orders",
            "config_json": {
                "left": {"source": "oracle", "query": "old", "query_windows": {"old": {}}},
                "right": {
                    "source": "databricks",
                    "query": "old",
                    "query_windows": {"old": {}},
                },
                "col_map": {"OLD": "old"},
            },
            "column_map_rows": [
                {
                    "left_column": "AMOUNT",
                    "right_column": "amount",
                    "data_type": "numeric",
                },
                {
                    "left_column": "SAS_DT",
                    "right_column": "event_date",
                    "data_type": "sas_datetime_date",
                },
                {
                    "left_column": "POSTED_AT",
                    "right_column": "posted_at",
                    "data_type": '{"type":"date","left_date_type":"timestamp",'
                    '"right_date_type":"date"}',
                },
            ],
            "pair_sql": {
                "left_sql": " SELECT * FROM L; ",
                "right_sql": "",
                "vintage_sample_size": "2",
                "vintage_values": None,
            },
        }
    )
    assert config["col_map"] == {
        "AMOUNT": "amount",
        "SAS_DT": "event_date",
        "POSTED_AT": "posted_at",
    }
    assert config["col_type_overrides"] == {"AMOUNT": "numeric"}
    assert config["column_types"]["SAS_DT"] == "sas_datetime_date"
    assert config["column_types"]["POSTED_AT"]["right_date_type"] == "date"
    assert config["left"]["query"] == "SELECT * FROM L"
    assert "query" not in config["right"] and "query_windows" not in config["right"]
    assert config["vintage_sample_size"] == 2


def test_pair_metadata_reads_specified_vintages_from_json_cell():
    config = pair_config(
        {
            "pair_name": "orders",
            "config_json": {"left": {}, "right": {}},
            "pair_sql": {
                "left_sql": None,
                "right_sql": None,
                "vintage_sample_size": None,
                "vintage_values": '["2026-01-01", "2026-04-01"]',
            },
        }
    )
    assert config["vintage_values"] == ["2026-01-01", "2026-04-01"]


def test_request_snapshot_contains_catalog_and_effective_config(tmp_path):
    settings = {
        "run_id": "r1",
        "work_dir": str(tmp_path),
        "timezone": "America/New_York",
        "owner": "me",
        "profile": "corp",
        "_catalog_row": {
            "pair_name": "orders",
            "config_json": '{"left":{"source":"oracle","user":"hidden"}}',
            "column_map_rows": [
                {
                    "left_column": "A",
                    "right_column": "a",
                    "data_type": "numeric",
                }
            ],
            "pair_sql": {"left_sql": "SELECT * FROM L", "right_sql": None},
        },
    }
    config = {
        "pairs": {
            "orders": {
                "left": {
                    "source": "oracle",
                    "password": "hidden",
                    "ACCESS_TOKEN": "also-hidden",
                },
                "right": {"source": "databricks"},
                "col_map": {"A": "a"},
            }
        }
    }
    path = write_request_snapshot(settings, config)
    value = json.loads(path.read_text(encoding="utf-8"))
    assert path == tmp_path / "r1" / "request.json"
    assert value["settings"]["owner"] == "me" and "_catalog_row" not in value["settings"]
    assert value["catalog"]["column_map_rows"][0]["left_column"] == "A"
    assert value["catalog"]["pair_sql"]["left_sql"] == "SELECT * FROM L"
    assert "user" not in value["catalog"]["config_json"]["left"]
    assert "password" not in value["effective_config"]["pairs"]["orders"]["left"]
    assert "ACCESS_TOKEN" not in value["effective_config"]["pairs"]["orders"]["left"]
    assert not path.with_name("request.json.part").exists()


def test_attribute_check_registry_and_multiselect(monkeypatch):
    names = attribute_check_names()
    assert names == (
        "n_total",
        "n_missing",
        "n_unique",
        "mean",
        "std",
        "min",
        "max",
        "length_avg",
        "length_max",
        "hash_sum",
    )
    assert all(isinstance(get_attribute_check(name), AttributeCheck) for name in names)
    answers = iter(("1,3,3", ""))
    monkeypatch.setattr("builtins.input", lambda _prompt: next(answers))
    assert cli.choose_many("Checks", ["row_count", *names], "") == [
        "row_count",
        "n_missing",
    ]


def test_retry():
    store = MemoryStore()
    store.put_json(
        "pairs/p/row_check/result.json",
        {"partitions": {"all": {"verdict": "match"}}},
    )
    store.put_json("pairs/p/col_check/results/good.json", {"verdict": "match"})
    store.put_json("pairs/p/col_check/results/bad.json", {"verdict": "diff"})
    store.put_json("pairs/p/col_check/failures/error.json", {"error": "boom"})
    assert retry_failed(store) == 2
    assert store.exists("pairs/p/col_check/results/good.json")
    assert not store.exists("pairs/p/col_check/results/bad.json")
    assert not store.exists("pairs/p/col_check/failures/error.json")


def test_logging():
    record = logging.LogRecord("pair_recon", logging.INFO, "", 0, "done", (), None)
    record.created = 0
    record.msecs = 0
    rendered = ZonedFormatter(CONSOLE_LOG_FORMAT, "America/New_York").format(record)
    assert rendered.startswith("1969-12-31T19:00:00.000-0500 INFO pair_recon: done")


def test_debug(monkeypatch):
    events = []
    attach = SimpleNamespace(
        listen=lambda address: events.append(("listen", address)),
        wait_for_client=lambda: events.append("wait"),
        breakpoint=lambda: events.append("break"),
    )
    ipdb = SimpleNamespace(runcall=lambda callback: events.append("ipdb") or callback())
    monkeypatch.setattr(debug, "import_module", lambda name: attach if name == "debugpy" else ipdb)
    assert debug.run_debugger(None, lambda: "plain") == "plain"
    assert debug.run_debugger("attach", lambda: "attached") == "attached"
    assert debug.run_debugger("ipdb", lambda: "stepped") == "stepped"
    assert events == [("listen", ("0.0.0.0", 5678)), "wait", "break", "ipdb"]


@pytest.mark.parametrize(
    ("module", "argv"), [(cli, ["--debug", "attach"]), (sas, ["export", "--debug", "attach"])]
)
def test_modes(monkeypatch, module, argv):
    seen = []
    monkeypatch.setattr(module, "run_debugger", lambda mode, _callback: seen.append(mode) or 7)
    assert module.main(argv) == 7 and seen == ["attach"]


def test_progress():
    store = MemoryStore()
    write_run_status(
        store, {"state": "left_col_check", "run_id": "r", "timezone": "America/New_York"}
    )
    store.put_json(plan_path("p"), {"pair": "p", "expected_units": 2, "windows": [], "columns": []})
    store.put_json(unit_path("p", 1, "u1"), {"pair": "p", "unit_id": "u1"})
    store.put_json(result_path("p", "u1"), {"pair": "p", "unit_id": "u1", "verdict": "match"})
    seen = []
    ready = threading.Event()

    def capture(row):
        seen.append((threading.current_thread().name, row))
        if threading.current_thread().name == "pair_recon-progress":
            ready.set()

    sampler = ProgressSampler(store, "r", "p", "America/New_York", capture, interval=0.01)
    sampler.start()
    assert ready.wait(1)
    sampler.stop("passed")
    row = progress_snapshot(store, "r", "p", "America/New_York")
    assert row["units_expected"] == 2 and row["units_complete"] == 1
    assert any(name == "pair_recon-progress" for name, _row in seen)
    assert seen[-1][1]["state"] == "passed"

    def fail(_row):
        raise ConnectionError("offline")

    failed = ProgressSampler(
        store,
        "r",
        "p",
        "America/New_York",
        fail,
    )
    with pytest.raises(ConnectionError, match="offline"):
        failed.start(required=True)


def test_sas(tmp_path):
    pair = {
        "partition_mode": "all",
        "left": {"source": "oracle", "table": "LEFT_TABLE"},
        "right": {"source": "databricks", "table": "RIGHT_TABLE"},
        "key": "ID",
        "col_map": {"VALUE": "value"},
        "strict": False,
    }
    bundle = tmp_path / "sas"
    result = export_bundle("orders", pair, bundle, rate=0.1)
    assert set(result["scripts"]) == {"left"}
    result = export_bundle("orders", pair, bundle, rate=0.1, sides=("right",))
    assert set(result["scripts"]) == {"right"}
    assert "STANDARD_HASH" in (bundle / "left.sas").read_text(encoding="utf-8")
    assert "md5" in (bundle / "right.sas").read_text(encoding="utf-8")
    assert "SELECT ID, VALUE" in (bundle / "left.sas").read_text(encoding="utf-8")
    assert "SELECT `ID`, `value`" in (bundle / "right.sas").read_text(encoding="utf-8")
    assert (bundle / "left.sas").is_file() and (bundle / "right.sas").is_file()


@pytest.mark.parametrize(
    ("left_input", "right_input"),
    [
        ("catalog", "catalog"),
        ("hadoop", "catalog"),
        ("csv", "catalog"),
        ("catalog", "csv"),
        ("csv", "csv"),
    ],
)
def test_prompt(monkeypatch, left_input, right_input, tmp_path):
    metadata = {
        "left": {"source": "oracle", "table": "L", "dsn": "hive"},
        "right": {"source": "databricks", "table": "R"},
        "partition_mode": "all",
        "col_map": {"A": "a"},
    }

    class Catalog:
        def __init__(self, *_args):
            pass

        def pairs(self):
            return [{"pair_name": "orders", "config_json": metadata}]

        def run(self, _run_id):
            return None

    defaults = {
        "owner": "me",
        "profile": "corp",
        "warehouse_id": "warehouse",
        "pair_table": "main.recon.pair_recon_pair_meta",
        "status_table": "main.recon.pair_recon_run_status",
        "error_table": "main.recon.pair_recon_run_error",
        "volume_root": "/Volumes/main/recon/artifacts/runs",
        "right_job_id": "42",
        "pair": "orders",
        "left_input": left_input,
        "right_input": right_input,
        "left_csv": str(tmp_path / "left_sample.csv"),
        "right_csv": str(tmp_path / "right_sample.csv"),
        "hadoop_schema": "risk",
        "timezone": "America/New_York",
        "partition_mode": "all",
        "checks": "row_count,hash_sum",
        "sample": "all",
        "strict": False,
    }
    (tmp_path / "request.yaml").write_text(
        "format: pair-recon-sas-v1\npair: orders\npartition_mode: all\n"
        "generated:\n  left:\n    rate: 0.1\n  right:\n    rate: 0.1\n"
        "outputs:\n  left: left_sample.csv\n  right: right_sample.csv\n",
        encoding="utf-8",
    )
    monkeypatch.setattr(cli, "Catalog", Catalog)
    monkeypatch.setattr(cli, "make_run_id", lambda *_args: "r-new")
    monkeypatch.setattr("builtins.input", lambda _prompt: "")
    settings, config, action, _client, _catalog = cli.prompts(
        defaults, workspace_factory=lambda _profile: SimpleNamespace(statement_execution=object())
    )
    assert settings["run_id"] == "r-new" and action == "new"
    assert config["pairs"]["orders"]["statistics"] == ["hash_sum"]
    expected = {"catalog": "oracle", "hadoop": "hadoop", "csv": "csv"}[left_input]
    assert config["pairs"]["orders"]["left"]["source"] == expected
    if left_input == "csv":
        assert config["pairs"]["orders"]["sample"] == 0.1
        assert config["pairs"]["orders"]["pre_sampled"] is True
    if left_input == "hadoop":
        assert config["pairs"]["orders"]["left"]["schema"] == "risk"
        assert config["pairs"]["orders"]["left"]["dsn"] == "hive"
    if right_input == "csv":
        assert config["pairs"]["orders"]["right"]["source"] == "csv"
        assert config["pairs"]["orders"]["right_pre_sampled"] is True


def test_local(monkeypatch, tmp_path):
    store = MemoryStore()
    events = []

    class Root:
        def __init__(self, *_args, **_kwargs):
            pass

        def child(self, _run_id):
            return store

    class Sampler:
        def __init__(self, *_args, **_kwargs):
            pass

        def start(self, **_kwargs):
            events.append("start")
            return self

        def stop(self, state, **_kwargs):
            events.append(state)

    class Catalog:
        progress = staticmethod(lambda _row: None)

        def status(self, **values):
            events.append(values["state"])

        def finish(self, _store, **values):
            events.append(f"finish:{values['state']}")

    def left(*_args):
        _args[-1]()
        return {"row": "ready", "units": 2}

    monkeypatch.setattr(runner, "VolumeStore", Root)
    monkeypatch.setattr(runner, "ProgressSampler", Sampler)
    monkeypatch.setattr(runner, "configure_left_logging", lambda *_args: None)
    monkeypatch.setattr(runner, "run_left", left)
    monkeypatch.setattr(runner, "run_right_row_check", lambda *_args: events.append("row"))
    monkeypatch.setattr(
        runner,
        "run_right",
        lambda *_args, **_kwargs: {"row_errors": 0, "column_errors": 0, "failures": 0},
    )
    settings = {
        "run_id": "r1",
        "profile": "corp",
        "volume_root": "/Volumes/c/s/v/runs",
        "work_dir": str(tmp_path),
        "timezone": "America/New_York",
        "owner": "me",
        "pair_table": "c.s.pairs",
        "status_table": "c.s.status",
        "error_table": "c.s.errors",
    }
    client = SimpleNamespace(files=object(), jobs=SimpleNamespace())
    config = {"pairs": {"orders": {"right": {"source": "csv"}}}}
    result = runner.launch(
        settings, config, action="new", workspace_client=client, catalog=Catalog()
    )
    assert result["state"] == "passed" and result["left"]["units"] == 2
    assert events == ["running", "start", "row", "finish:passed", "passed"]


def test_csvrun(tmp_path):
    left_path = tmp_path / "left.csv"
    right_path = tmp_path / "right.csv"
    left_path.write_text("ID,VALUE\n1,文字\n2,✓\n", encoding="utf-8")
    right_path.write_text("id,value\n1,文字\n2,✓\n", encoding="utf-8")
    pair = {
        "version": 2,
        "partition_mode": "all",
        "left": {"source": "csv", "path": str(left_path)},
        "right": {"source": "csv", "path": str(right_path), "pre_sampled": True},
        "pre_sampled": True,
        "right_pre_sampled": True,
        "sample": False,
        "checks": ["row_count", "hash_sum", "n_unique"],
        "statistics": ["hash_sum", "n_unique"],
        "col_map": {"ID": "id", "VALUE": "value"},
    }
    config = {"version": 2, "timezone": "America/New_York", "pairs": {"orders": pair}}
    store = LocalStore(tmp_path / "run")
    factory = lambda values: CsvTarget(values)
    left = runner.run_left(
        config,
        store,
        "r1",
        "orders",
        tmp_path / "work",
        release=lambda: run_right_row_check(store, factory),
    )
    right = run_right(None, factory, run_id="r1", store=store, poll_interval=0)
    assert left["units"] == 2
    assert right == {
        "row_errors": 0,
        "column_errors": 0,
        "failures": 0,
        "results": 2,
        "new_results": 2,
    }
