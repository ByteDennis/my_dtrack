import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
from support import MemoryStore

from pair_recon.app import cli, runner
from pair_recon.app.runner import (
    local_run_directory,
    make_run_id,
    pair_config,
    resolve_job_id,
    retry_failed,
    write_request_snapshot,
)
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
    morning = make_run_id(
        "订单 pair",
        "测试 owner",
        "America/New_York",
        now=datetime(2026, 10, 21, 9, 46, 33, tzinfo=ZoneInfo("America/New_York")),
    )
    evening = make_run_id(
        "订单 pair",
        "测试 owner",
        "America/New_York",
        now=datetime(2026, 10, 21, 22, 1, tzinfo=ZoneInfo("America/New_York")),
    )
    assert morning == evening == "订单-pair-测试-owner-20261021"
    assert local_run_directory("outputs", morning, "America/New_York") == (
        Path("outputs/2026-10-21") / morning
    )
    assert local_run_directory(
        "outputs",
        "custom-run",
        "America/New_York",
        now=datetime(2026, 10, 22, 9, tzinfo=ZoneInfo("America/New_York")),
    ) == Path("outputs/2026-10-22/custom-run")


def test_resolve_job_id_accepts_id_or_unique_name():
    jobs = SimpleNamespace(
        list=lambda **_options: [
            SimpleNamespace(job_id=42, settings=SimpleNamespace(name="test-run"))
        ]
    )
    client = SimpleNamespace(jobs=jobs)

    assert resolve_job_id(client, "42") == 42
    assert resolve_job_id(client, "test-run") == 42


@pytest.mark.parametrize(
    ("rows", "message"),
    [
        ([], "not found"),
        (
            [
                SimpleNamespace(job_id=41, settings=SimpleNamespace(name="test-run")),
                SimpleNamespace(job_id=42, settings=SimpleNamespace(name="test-run")),
            ],
            "not unique",
        ),
    ],
)
def test_resolve_job_id_rejects_missing_or_duplicate_name(rows, message):
    client = SimpleNamespace(jobs=SimpleNamespace(list=lambda **_options: rows))

    with pytest.raises(ValueError, match=message):
        resolve_job_id(client, "test-run")


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
                    "data_type": '{"type":"date","left_date_type":"date","right_date_type":"date"}',
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


def test_pair_metadata_builds_config_from_explicit_columns():
    config = pair_config(
        {
            "pair_name": "orders",
            "key_columns": ["ORDER_ID", "LINE_ID"],
            "vintage": "month",
            "left_source": "oracle",
            "left_conn": "PCDS",
            "left_table": "SALES.ORDERS",
            "left_date_col": "ORDER_DT",
            "left_options_json": '{"where":"ACTIVE = 1"}',
            "right_source": "databricks_query",
            "right_conn": None,
            "right_table": None,
            "right_date_col": "order_dt",
            "right_options_json": None,
            "run_defaults_json": (
                '{"checks":["row_count","hash_sum"],"run_mode":"left-only","sample":0.1}'
            ),
            "column_map_rows": [
                {
                    "left_column": "ORDER_ID",
                    "right_column": "order_id",
                    "data_type": "numeric",
                }
            ],
            "pair_sql": {
                "left_sql": None,
                "left_query_windows_json": None,
                "right_sql": "SELECT * FROM main.sales.orders",
                "right_query_windows_json": None,
            },
        }
    )

    assert config["key"] == "ORDER_ID,LINE_ID"
    assert config["vintage"] == "month"
    assert config["sample"] == 0.1
    assert config["left"] == {
        "source": "oracle",
        "conn_macro": "PCDS",
        "schema": "SALES",
        "table": "ORDERS",
        "date_col": "ORDER_DT",
        "where": "ACTIVE = 1",
    }
    assert config["right"] == {
        "source": "databricks",
        "kind": "query",
        "date_col": "order_dt",
        "query": "SELECT * FROM main.sales.orders",
    }
    assert config["_run_defaults"] == {
        "checks": ["row_count", "hash_sum"],
        "run_mode": "left-only",
        "sample": 0.1,
    }
    runtime = cli.run_config(
        "orders",
        config,
        {
            "partition_mode": "date",
            "sample": 0.1,
            "strict": False,
            "checks": ["row_count", "hash_sum"],
            "from_date": "2026-01-01",
            "to_date": "2026-01-31",
            "vintage": "month",
        },
        timezone="America/New_York",
    )
    assert runner.validate_config(runtime)["pairs"]["orders"]["sample"] == 0.1
    assert "_run_defaults" not in runtime["pairs"]["orders"]
    assert "run_mode" not in runtime["pairs"]["orders"]


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
    assert path == local_run_directory(tmp_path, "r1", settings["timezone"]) / "request.json"
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
        "row_check/result.json",
        {"partitions": {"all": {"verdict": "match"}}},
    )
    store.put_json("col_check/results/good.json", {"verdict": "match"})
    store.put_json("col_check/results/bad.json", {"verdict": "diff"})
    store.put_json("col_check/failures/error.json", {"error": "boom"})
    assert retry_failed(store) == 2
    assert store.exists("col_check/results/good.json")
    assert not store.exists("col_check/results/bad.json")
    assert not store.exists("col_check/failures/error.json")


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
    ipdb = SimpleNamespace(post_mortem=lambda traceback: events.append(("ipdb", traceback)))
    monkeypatch.setattr(debug, "import_module", lambda name: attach if name == "debugpy" else ipdb)
    assert debug.run_debugger(None, lambda: "plain") == "plain"
    assert debug.run_debugger("attach", lambda: "attached") == "attached"
    assert debug.run_debugger("console", lambda: "stepped") == "stepped"
    assert events == [("listen", ("0.0.0.0", 5678)), "wait", "break"]

    def fail():
        raise RuntimeError("stop here")

    with pytest.raises(RuntimeError, match="stop here"):
        debug.run_debugger("console", fail)
    assert events[-1][0] == "ipdb"
    traceback = events[-1][1]
    while traceback.tb_next:
        traceback = traceback.tb_next
    assert traceback.tb_frame.f_code.co_name == "fail"


@pytest.mark.parametrize(
    ("module", "argv", "expected"),
    [
        (cli, ["--debug", "console"], "console"),
        (cli, ["--debug", "attach"], "attach"),
        (sas, ["export", "--debug", "console"], "console"),
        (sas, ["export", "--debug", "attach"], "attach"),
    ],
)
def test_modes(monkeypatch, module, argv, expected):
    seen = []
    monkeypatch.setattr(module, "run_debugger", lambda mode, _callback: seen.append(mode) or 7)
    assert module.main(argv) == 7 and seen == [expected]


def test_run_debug_mode_reraises_original_error(monkeypatch):
    def fail(_defaults):
        raise RuntimeError("original failure")

    monkeypatch.setattr(cli, "load_environment", lambda: None)
    monkeypatch.setattr(cli, "load_defaults", lambda _path: {})
    monkeypatch.setattr(cli, "prompts", fail)
    args = SimpleNamespace(config=None)

    assert cli.execute(args) == 1
    with pytest.raises(RuntimeError, match="original failure"):
        cli.execute(args, raise_errors=True)


def test_main_program_enables_error_reraising_in_debug_mode(monkeypatch):
    seen = []

    def execute(_args, *, raise_errors=False):
        seen.append(raise_errors)
        return 0

    monkeypatch.setattr(cli, "execute", execute)
    monkeypatch.setattr(cli, "run_debugger", lambda _mode, callback: callback())
    monkeypatch.setattr(debug.sys, "gettrace", lambda: None)

    assert cli.main_program([]) == 0
    assert cli.main_program(["--debug", "attach"]) == 0
    monkeypatch.setattr(debug.sys, "gettrace", lambda: object())
    assert cli.main_program([]) == 0
    assert seen == [False, True, True]


def test_progress():
    store = MemoryStore()
    write_run_status(
        store, {"state": "left_col_check", "run_id": "r", "timezone": "America/New_York"}
    )
    store.put_json("row_check/left.json", {"counts": {"all": 2}})
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
    assert json.loads(row["detail"])["left_rows"] == 2
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


def test_sas_default_bundle_uses_output_root(monkeypatch, tmp_path, capsys):
    target = tmp_path / "artifacts"
    seen = {}
    monkeypatch.setenv("PAIR_RECON_OUTPUT_DIR", str(target))
    monkeypatch.setattr(sas, "_catalog_pair", lambda *_args: ("orders", {}))

    def export(pair_name, pair, bundle, rate, sides):
        seen.update(pair_name=pair_name, pair=pair, bundle=bundle, rate=rate, sides=sides)
        return {"bundle": str(bundle), "scripts": {}}

    monkeypatch.setattr(sas, "export_bundle", export)
    args = SimpleNamespace(
        defaults=tmp_path / ".pair_recon.yaml",
        pair=None,
        side="left",
        bundle=None,
        rate=0.01,
    )

    assert sas.execute(args) == 0
    assert seen["bundle"] == target / "sas"
    assert target.is_dir()
    assert json.loads(capsys.readouterr().out)["bundle"] == str(target / "sas")


@pytest.mark.parametrize(
    ("run_mode", "left_input", "right_input", "existing"),
    [
        ("full", "database", "database", True),
        ("full", "csv", "database", False),
        ("full", "database", "csv", False),
        ("full", "csv", "csv", False),
        ("left-only", "database", "database", False),
    ],
)
def test_prompt(monkeypatch, run_mode, left_input, right_input, existing, tmp_path):
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
            return {"state": "failed"} if existing else None

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
        "run_mode": run_mode,
        "left_input": left_input,
        "right_input": right_input,
        "left_csv": str(tmp_path / "left_sample.csv"),
        "right_csv": str(tmp_path / "right_sample.csv"),
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
    prompts = []
    monkeypatch.setattr("builtins.input", lambda prompt: prompts.append(prompt) or "")
    monkeypatch.setenv("PAIR_RECON_OUTPUT_DIR", str(tmp_path / "artifacts"))
    settings, config, action, _client, _catalog = cli.prompts(
        defaults, workspace_factory=lambda _profile: SimpleNamespace(statement_execution=object())
    )
    assert settings["run_id"] == "r-new"
    assert action == ("restart" if existing else "new")
    assert not any("Run id" in prompt or "Existing run action" in prompt for prompt in prompts)
    assert settings["run_mode"] == run_mode
    assert settings["left_input"] == left_input
    assert settings["work_dir"] == str(tmp_path / "artifacts")
    assert (tmp_path / "artifacts").is_dir()
    assert config["pairs"]["orders"]["statistics"] == ["hash_sum"]
    expected = {"database": "oracle", "csv": "csv"}[left_input]
    assert config["pairs"]["orders"]["left"]["source"] == expected
    if left_input == "csv":
        assert config["pairs"]["orders"]["sample"] == 0.1
        assert config["pairs"]["orders"]["pre_sampled"] is True
    if run_mode == "left-only":
        assert settings["right_input"] == "none"
        assert config["pairs"]["orders"]["right"]["source"] == "databricks"
    else:
        assert settings["right_input"] == right_input
    if right_input == "csv":
        assert config["pairs"]["orders"]["right"]["source"] == "csv"
        assert config["pairs"]["orders"]["right_pre_sampled"] is True


@pytest.mark.parametrize(
    ("defaults", "environment", "expected"),
    [
        ({}, "environment-owner", "environment-owner"),
        ({"owner": "saved-owner"}, "environment-owner", "environment-owner"),
        ({"owner": "saved-owner"}, None, "saved-owner"),
    ],
)
def test_prompt_owner_default(monkeypatch, defaults, environment, expected):
    seen = {}

    def stop(label, default=None, **_options):
        seen[label] = default
        raise RuntimeError("stop after owner")

    if environment is None:
        monkeypatch.delenv("USERNAME", raising=False)
    else:
        monkeypatch.setenv("USERNAME", environment)
    monkeypatch.setattr(cli, "ask", stop)
    with pytest.raises(RuntimeError, match="stop after owner"):
        cli.prompts(defaults)
    assert seen == {"Run owner": expected}


def test_prompt_uses_dbx_environment_defaults(monkeypatch):
    seen = {}

    def stop(label, default=None, **_options):
        seen[label] = default
        if label == "Volume runs path":
            raise RuntimeError("stop after dbx defaults")
        return default

    monkeypatch.setenv("USERNAME", "owner")
    monkeypatch.setenv("DBX_PROFILE", "team")
    monkeypatch.setenv("DBX_WAREHOUSE_ID", "warehouse-from-env")
    monkeypatch.setenv("DBX_PAIR_TABLE", "main.recon.pairs_from_env")
    monkeypatch.setenv("DBX_STATUS_TABLE", "main.recon.status_from_env")
    monkeypatch.setenv("DBX_ERROR_TABLE", "main.recon.errors_from_env")
    monkeypatch.setenv("DBX_RUN_PATH", "/Volumes/main/recon/runs_from_env")
    monkeypatch.setattr(cli, "ask", stop)

    with pytest.raises(RuntimeError, match="stop after dbx defaults"):
        cli.prompts(
            {
                "profile": "saved",
                "warehouse_id": "saved-warehouse",
                "pair_table": "main.recon.saved_pairs",
                "status_table": "main.recon.saved_status",
                "error_table": "main.recon.saved_errors",
                "volume_root": "/Volumes/main/recon/saved_runs",
            }
        )

    assert seen["Databricks profile"] == "team"
    assert seen["SQL warehouse id"] == "warehouse-from-env"
    assert seen["Pair metadata table"] == "main.recon.pairs_from_env"
    assert seen["Run status table"] == "main.recon.status_from_env"
    assert seen["Run error table"] == "main.recon.errors_from_env"
    assert seen["Volume runs path"] == "/Volumes/main/recon/runs_from_env"


def test_synchronize_pair_workbook_upserts_only_changes(monkeypatch, tmp_path):
    workbook = tmp_path / "pairs.xlsx"
    workbook.touch()
    rows = [
        {"pair_name": "same", "enabled": True},
        {"pair_name": "changed", "enabled": True},
    ]
    changed = [rows[1]]
    seen = {}
    catalog = SimpleNamespace(configuration_rows=lambda: [{"pair_name": "same"}])
    monkeypatch.setattr(cli, "read_pair_workbook", lambda path: rows if path == workbook else [])
    monkeypatch.setattr(
        cli,
        "changed_pair_configurations",
        lambda local, remote: (
            changed if local == rows and remote == [{"pair_name": "same"}] else []
        ),
    )
    monkeypatch.setattr(
        cli,
        "upsert_pair_configuration",
        lambda sql, table, values: (
            seen.update(sql=sql, table=table, rows=values)
            or {"pair_meta_table": table, "rows": 1, "sql_rows": 0, "pairs": ["changed"]}
        ),
    )

    actual, result = cli.synchronize_pair_workbook(
        "sql", catalog, "main.recon.pair_recon_pair_meta", workbook
    )

    assert actual == rows
    assert result["pairs"] == ["changed"] and result["source"] == str(workbook)
    assert seen["rows"] == changed


def test_pair_workbook_path_uses_environment(monkeypatch, tmp_path):
    workbook = tmp_path / "pairs.xlsx"
    workbook.touch()
    monkeypatch.setenv("DBX_PAIR_WORKBOOK", str(workbook))

    assert cli.pair_workbook_path({"pair_workbook": "ignored.xlsx"}) == workbook


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

        def clear(self, _run_id):
            pass

        def status(self, **values):
            events.append(values["state"])

        def finish(self, _store, **values):
            events.append(f"finish:{values['state']}")

    def left(*_args, **_kwargs):
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
    assert local_run_directory(tmp_path, "r1", settings["timezone"]).is_dir()
    assert events == ["running", "start", "row", "finish:passed", "passed"]


def test_left_only_does_not_start_or_run_right(monkeypatch, tmp_path):
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

        def clear(self, _run_id):
            pass

        def status(self, **values):
            events.append(values["state"])

        def finish(self, _store, **values):
            events.append(f"finish:{values['state']}")

    def left(*_args, **kwargs):
        events.append(f"left-only:{kwargs['left_only']}")
        return {"row": "ready", "units": 3}

    def right_called(*_args, **_kwargs):
        pytest.fail("Right must not run in left-only mode")

    monkeypatch.setattr(runner, "VolumeStore", Root)
    monkeypatch.setattr(runner, "ProgressSampler", Sampler)
    monkeypatch.setattr(runner, "configure_left_logging", lambda *_args: None)
    monkeypatch.setattr(runner, "run_left", left)
    monkeypatch.setattr(runner, "run_right_row_check", right_called)
    monkeypatch.setattr(runner, "run_right", right_called)
    settings = {
        "run_id": "left-1",
        "run_mode": "left-only",
        "profile": "corp",
        "volume_root": "/Volumes/c/s/v/runs",
        "work_dir": str(tmp_path),
        "timezone": "America/New_York",
        "owner": "me",
        "pair_table": "c.s.pairs",
        "status_table": "c.s.status",
        "error_table": "c.s.errors",
        "right_job_id": "42",
    }
    jobs = SimpleNamespace(run_now=right_called, cancel_run=right_called)
    client = SimpleNamespace(files=object(), jobs=jobs)
    config = {"pairs": {"orders": {"right": {"source": "databricks"}}}}

    result = runner.launch(
        settings, config, action="new", workspace_client=client, catalog=Catalog()
    )

    assert result["mode"] == "left-only"
    assert result["state"] == "left_complete"
    assert result["right"] == {"state": "not_run"}
    assert result["volume_run_root"] == "memory://run"
    assert store.get_json("_status/run.json")["state"] == "left_complete"
    assert events == [
        "running",
        "start",
        "left-only:True",
        "finish:left_complete",
        "left_complete",
    ]


def test_launch_writes_early_failure_to_left_log(monkeypatch, tmp_path):
    states = []

    class Root:
        def __init__(self, *_args, **_kwargs):
            pass

        def child(self, _run_id):
            return MemoryStore()

    def missing_job(job_id, **_kwargs):
        assert job_id == 42
        raise RuntimeError("job 42 was not found")

    class Catalog:
        def clear(self, _run_id):
            pass

        def progress(self, _row):
            pass

        def status(self, **values):
            states.append(values["state"])

    monkeypatch.setattr(runner, "VolumeStore", Root)
    settings = {
        "run_id": "failed-1",
        "profile": "corp",
        "volume_root": "/Volumes/c/s/v/runs",
        "work_dir": str(tmp_path),
        "timezone": "America/New_York",
        "owner": "me",
        "pair_table": "c.s.pairs",
        "status_table": "c.s.status",
        "error_table": "c.s.errors",
        "right_job_id": "test-run",
    }
    client = SimpleNamespace(
        files=object(),
        jobs=SimpleNamespace(
            list=lambda **_options: [
                SimpleNamespace(job_id=42, settings=SimpleNamespace(name="test-run"))
            ],
            run_now=missing_job,
        ),
    )
    config = {"pairs": {"orders": {"right": {"source": "databricks"}}}}

    with pytest.raises(RuntimeError, match="job 42 was not found"):
        runner.launch(
            settings,
            config,
            action="new",
            workspace_client=client,
            catalog=Catalog(),
        )

    content = (
        local_run_directory(tmp_path, "failed-1", settings["timezone"]) / "left.log"
    ).read_text(encoding="utf-8")
    assert "run started" in content
    assert "starting Right Databricks job: reference=test-run job_id=42" in content
    assert "run failed" in content
    assert "RuntimeError: job 42 was not found" in content
    assert states == ["running", "failed"]


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


def test_left_only_csvrun_uploads_left_artifacts(tmp_path):
    left_path = tmp_path / "left.csv"
    left_path.write_text("ID,VALUE\n1,文字\n2,✓\n", encoding="utf-8")
    pair = {
        "version": 2,
        "partition_mode": "all",
        "left": {"source": "csv", "path": str(left_path)},
        "right": {"source": "databricks", "table": "orders"},
        "sample": False,
        "checks": ["row_count", "hash_sum", "n_unique"],
        "statistics": ["hash_sum", "n_unique"],
        "col_map": {"ID": "id", "VALUE": "value"},
    }
    config = {"version": 2, "timezone": "America/New_York", "pairs": {"orders": pair}}
    store = LocalStore(tmp_path / "run")

    left = runner.run_left(
        config,
        store,
        "left-1",
        "orders",
        tmp_path / "work",
        left_only=True,
    )

    units = store.list_json("units/")
    assert left == {"row": "ready", "units": 2}
    assert store.get_json("row_check/left.json")["counts"] == {"all": 2}
    assert store.get_json("plan.json")["expected_units"] == 2
    assert len(units) == 2
    assert all("left_statistics" in value for _key, value in units)
    assert store.exists("_status/col_check.closed.json")
    assert not store.list_keys("col_check/results/")


def test_left_only_rejects_snapshot_pair(tmp_path):
    config = {
        "version": 2,
        "timezone": "America/New_York",
        "pairs": {
            "orders": {
                "snapshot": True,
                "left": {"source": "csv", "path": str(tmp_path / "left.csv")},
                "right": {
                    "source": "databricks",
                    "table": "orders",
                    "date_col": "event_date",
                    "date_type": "date",
                },
                "checks": ["row_count", "hash_sum"],
                "col_map": {"VALUE": "value"},
            }
        },
    }

    with pytest.raises(ValueError, match="snapshot pairs"):
        runner.run_left(
            config,
            MemoryStore(),
            "r1",
            "orders",
            tmp_path / "work",
            left_only=True,
        )
