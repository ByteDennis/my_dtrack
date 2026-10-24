import json
import logging
import threading
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest
import yaml
from support import MemoryStore

from pair_recon.app import cli, runner
from pair_recon.app.runner import (
    StatementSql,
    local_run_directory,
    make_run_id,
    mismatched_partitions,
    pair_config,
    resolve_job_id,
    retry_failed,
    write_request_snapshot,
)
from pair_recon.core import debug, sas
from pair_recon.core.progress import ProgressSampler, progress_snapshot
from pair_recon.core.sas import export_bundle
from pair_recon_left.common.checks import col_check as left_col_check
from pair_recon_left.common.model.protocol import (
    plan_path,
    result_path,
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


def test_excluded_partition_ranges_and_month_numbers():
    assert cli.excluded_partition_values("2025-04-02->2025-04-05, 2025-04-10") == [
        "2025-04-02",
        "2025-04-03",
        "2025-04-04",
        "2025-04-05",
        "2025-04-10",
    ]
    assert (
        cli.excluded_partitions_text(
            ["2025-04-02", "2025-04-03", "2025-04-04", "2025-04-05", "2025-04-10"]
        )
        == "2025-04-02->2025-04-05, 2025-04-10"
    )
    assert cli.excluded_partition_values(
        "2025-04-02->2025-04-05, 2025-05-10",
        {"type": "number", "format": "month"},
    ) == ["2025-04-01", "2025-05-01"]


def test_mismatched_partitions_parses_only_the_selected_pair():
    error = RuntimeError(
        "version 2 row-check found unexcluded mismatched partitions; "
        "add them to excluded_partitions (other: 2026-09-01; "
        "orders: 2026-10-02, 2026-10-05, 2026-10-02)"
    )

    assert mismatched_partitions(error, "orders") == ["2026-10-02", "2026-10-05"]
    assert mismatched_partitions(RuntimeError("another failure"), "orders") == []


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("all", {"vintage_sample_size": None, "vintage_values": None}),
        ("5", {"vintage_sample_size": 5, "vintage_values": None}),
        (
            "2026-01-01, 2026-03-01",
            {
                "vintage_sample_size": None,
                "vintage_values": ["2026-01-01", "2026-03-01"],
            },
        ),
    ],
)
def test_vintage_sample_prompt_value(raw, expected):
    assert cli.vintage_sample_value(raw) == expected


def test_run_config_applies_prompted_vintage_sample():
    runtime = cli.run_config(
        "orders",
        {
            "left": {},
            "right": {},
            "vintage_sample_size": 2,
            "vintage_values": None,
        },
        {
            "partition_mode": "date",
            "checks": ["row_count"],
            "sample": 0.1,
            "strict": False,
            "audit": False,
            "from_date": "2026-01-01",
            "to_date": "2026-03-31",
            "vintage": "month",
            "vintage_sample_size": 5,
            "vintage_values": None,
        },
        timezone="America/New_York",
    )

    assert runtime["pairs"]["orders"]["vintage_sample_size"] == 5
    assert "vintage_values" not in runtime["pairs"]["orders"]

    all_mode = cli.run_config(
        "orders",
        runtime["pairs"]["orders"],
        {
            "partition_mode": "all",
            "checks": ["row_count"],
            "sample": False,
            "strict": False,
            "audit": False,
        },
        timezone="America/New_York",
    )["pairs"]["orders"]
    assert "vintage_sample_size" not in all_mode
    assert "vintage_values" not in all_mode


def test_add_mismatched_partitions_normalizes_month_date_type():
    settings = {
        "pair": "orders",
        "left_date_type": {"type": "number", "format": "month"},
        "excluded_partitions": ["2026-08-01"],
    }
    config = {"pairs": {"orders": {}}}
    error = RuntimeError(
        "version 2 row-check found unexcluded mismatched partitions "
        "(orders: 2026-09-03, 2026-09-18, 2026-10-02)"
    )

    assert cli.add_mismatched_partitions(settings, config, error) == [
        "2026-09-01",
        "2026-10-01",
    ]
    assert settings["excluded_partitions"] == [
        "2026-08-01",
        "2026-09-01",
        "2026-10-01",
    ]
    assert config["pairs"]["orders"]["excluded_partitions"] == settings["excluded_partitions"]


@pytest.mark.parametrize("value", ["20250402", "2025-04-05->2025-04-02", "2025/04/02"])
def test_excluded_partition_ranges_reject_invalid_values(value):
    with pytest.raises(ValueError, match="Excluded partition"):
        cli.excluded_partition_values(value)


def test_statement_sql_reports_waiting_and_completion(monkeypatch):
    def response(state, rows=None):
        return SimpleNamespace(
            statement_id="statement-1",
            status=SimpleNamespace(state=state, error=None),
            manifest=SimpleNamespace(
                schema=SimpleNamespace(columns=[SimpleNamespace(name="value")])
            ),
            result=SimpleNamespace(data_array=rows),
        )

    pending = response("PENDING")
    running = response("RUNNING")
    succeeded = response("SUCCEEDED", [["ready"]])
    remaining = iter([running, succeeded])
    submitted = {}

    def execute_statement(*_args, **kwargs):
        submitted.update(kwargs)
        return pending

    api = SimpleNamespace(
        execute_statement=execute_statement,
        get_statement=lambda _statement_id: next(remaining),
    )
    messages = []
    monkeypatch.setattr(runner.time, "sleep", lambda _seconds: None)

    rows = StatementSql(
        SimpleNamespace(statement_execution=api),
        "warehouse-1",
        reporter=messages.append,
    ).execute("SELECT 'ready' AS value")

    assert rows == [{"value": "ready"}]
    assert submitted["wait_timeout"] == "10s"
    assert "submitted to warehouse warehouse-1" in messages[0]
    assert any("pending" in message for message in messages)
    assert any("running" in message for message in messages)
    assert "completed" in messages[-1] and "1 row(s)" in messages[-1]


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

    if not rows:
        assert resolve_job_id(client, "test-run", required=False) is None


def test_sync_right_package_repoints_existing_job(monkeypatch):
    settings = {"right_job_id": "test-run"}
    client = SimpleNamespace(
        jobs=SimpleNamespace(
            list=lambda **_options: [
                SimpleNamespace(job_id=42, settings=SimpleNamespace(name="test-run"))
            ],
            get=lambda _job_id: SimpleNamespace(),
        )
    )
    monkeypatch.setattr(
        cli,
        "sync_right",
        lambda _client, _parent: {"notebook": "/Users/me/pair_recon_right_job"},
    )
    monkeypatch.setattr(cli, "repoint_job", lambda *_args: True)
    monkeypatch.setattr(cli, "create_job", lambda *_args: pytest.fail("must not create job"))

    result = cli.sync_right_package(settings, client)

    assert result == {
        "notebook": "/Users/me/pair_recon_right_job",
        "job_id": 42,
        "job_created": False,
        "job_updated": True,
    }
    assert settings["right_job_id"] == "42"


def test_sync_right_package_creates_missing_job_with_environment_name(monkeypatch):
    class MissingJob(Exception):
        error_code = "RESOURCE_DOES_NOT_EXIST"

    settings = {"right_job_id": "999"}
    client = SimpleNamespace(
        jobs=SimpleNamespace(get=lambda _job_id: (_ for _ in ()).throw(MissingJob()))
    )
    seen = {}
    monkeypatch.setenv("DBX_CLUSTER_ID", "cluster-1")
    monkeypatch.setenv("DBX_JOB_NAME", "daily-reconciliation")
    monkeypatch.setattr(
        cli,
        "sync_right",
        lambda _client, _parent: {"notebook": "/Users/me/pair_recon_right_job"},
    )
    monkeypatch.setattr(
        cli,
        "create_job",
        lambda _client, name, notebook, cluster: (
            seen.update(name=name, notebook=notebook, cluster=cluster) or 314
        ),
    )

    result = cli.sync_right_package(settings, client)

    assert seen == {
        "name": "daily-reconciliation",
        "notebook": "/Users/me/pair_recon_right_job",
        "cluster": "cluster-1",
    }
    assert result["job_id"] == 314 and result["job_created"] is True
    assert settings["right_job_id"] == "314"


def test_execute_saves_created_job_id_in_yaml(monkeypatch, tmp_path):
    defaults = tmp_path / ".pair_recon.yaml"
    settings = {
        "pair": "orders",
        "run_id": "orders-me-20261021",
        "timezone": "America/New_York",
        "sync_remote": True,
        "right_job_id": "missing-job",
        "email_to": "",
    }
    config = {"pairs": {"orders": {}}}
    monkeypatch.setattr(cli, "DEFAULTS", defaults)
    monkeypatch.setattr(cli, "load_environment", lambda: None)
    monkeypatch.setattr(
        cli, "prompts", lambda _defaults: (settings, config, "new", object(), object())
    )
    monkeypatch.setattr(cli, "configure_console_logging", lambda *_args: None)
    monkeypatch.setattr(cli, "yes_no", lambda *_args: True)
    monkeypatch.setenv("DBX_WORKSPACE_PDIR", "apps")

    def sync(values, _client):
        values["right_job_id"] = "314"
        return {"job_id": 314, "job_created": True}

    def snapshot(*_args):
        assert yaml.safe_load(defaults.read_text(encoding="utf-8"))["right_job_id"] == "314"
        return tmp_path / "request.json"

    monkeypatch.setattr(cli, "sync_right_package", sync)
    monkeypatch.setattr(cli, "write_request_snapshot", snapshot)
    monkeypatch.setattr(cli, "launch", lambda *_args, **_kwargs: {"state": "passed"})
    args = SimpleNamespace(config=None, log_level="INFO")

    assert cli.execute(args, raise_errors=True) == 0
    assert yaml.safe_load(defaults.read_text(encoding="utf-8"))["right_job_id"] == "314"


def test_execute_saves_yaml_when_run_now_is_no(monkeypatch, tmp_path):
    defaults = tmp_path / ".pair_recon.yaml"
    settings = {
        "owner": "me",
        "pair": "orders",
        "run_id": "orders-me-20261021",
        "timezone": "America/New_York",
        "sync_remote": False,
        "email_to": "",
    }
    config = {"pairs": {"orders": {}}}
    monkeypatch.setattr(cli, "DEFAULTS", defaults)
    monkeypatch.setattr(cli, "load_environment", lambda: None)
    monkeypatch.setattr(
        cli, "prompts", lambda _defaults: (settings, config, "new", object(), object())
    )
    monkeypatch.setattr(cli, "configure_console_logging", lambda *_args: None)
    monkeypatch.setattr(cli, "yes_no", lambda label, _default: label != "Run now")
    monkeypatch.setattr(
        cli,
        "write_request_snapshot",
        lambda *_args: pytest.fail("a declined run must not create a request snapshot"),
    )
    monkeypatch.setattr(cli, "launch", lambda *_args, **_kwargs: pytest.fail("must not run"))
    args = SimpleNamespace(config=None, log_level="INFO")

    assert cli.execute(args, raise_errors=True) == 0
    saved = yaml.safe_load(defaults.read_text(encoding="utf-8"))
    assert saved["owner"] == "me" and saved["pair"] == "orders"
    assert "run_id" not in saved


def test_execute_undo_from_run_now_reopens_previous_question(monkeypatch, tmp_path):
    defaults = tmp_path / ".pair_recon.yaml"
    responses = iter(("first-owner", "undo", "final-owner", "no"))
    monkeypatch.setattr("builtins.input", lambda _prompt: next(responses))
    monkeypatch.setattr(cli, "DEFAULTS", defaults)
    monkeypatch.setattr(cli, "load_environment", lambda: None)

    def prompt(_defaults):
        owner = cli._PROMPT_SESSION.get().answer("Run owner", "saved-owner")
        return (
            {
                "owner": owner,
                "pair": "orders",
                "run_id": "orders-me-20261021",
                "timezone": "America/New_York",
                "sync_remote": False,
                "email_to": "",
            },
            {"pairs": {"orders": {}}},
            "new",
            object(),
            object(),
        )

    monkeypatch.setattr(cli, "prompts", prompt)
    monkeypatch.setattr(cli, "configure_console_logging", lambda *_args: None)
    monkeypatch.setattr(cli, "launch", lambda *_args, **_kwargs: pytest.fail("must not run"))
    args = SimpleNamespace(config=None, log_level="INFO")

    assert cli.execute(args, raise_errors=True) == 0
    assert yaml.safe_load(defaults.read_text(encoding="utf-8"))["owner"] == "final-owner"


def test_execute_defaults_mode_skips_all_interaction(monkeypatch, tmp_path):
    defaults = tmp_path / "run.yaml"
    defaults.write_text("pair: orders\n", encoding="utf-8")
    settings = {
        "pair": "orders",
        "run_id": "orders-me-20261022",
        "timezone": "America/New_York",
        "sync_remote": False,
        "email_to": "",
    }
    config = {"pairs": {"orders": {}}}
    seen = {}
    monkeypatch.setattr(cli, "load_environment", lambda: None)

    def prompt(values, *, interactive=True):
        seen["defaults"] = values
        seen["interactive"] = interactive
        return settings, config, "new", object(), object()

    monkeypatch.setattr(cli, "prompts", prompt)
    monkeypatch.setattr(cli, "configure_console_logging", lambda *_args: None)
    monkeypatch.setattr(cli, "yes_no", lambda *_args: pytest.fail("must not prompt"))
    monkeypatch.setattr(cli, "write_request_snapshot", lambda *_args: tmp_path / "request.json")
    monkeypatch.setattr(cli, "launch", lambda *_args, **_kwargs: {"state": "passed"})
    args = SimpleNamespace(
        config=None,
        defaults_path=defaults,
        earlystop=False,
        force=False,
        log_level="INFO",
    )

    assert cli.execute(args, raise_errors=True) == 0
    assert seen == {"defaults": {"pair": "orders"}, "interactive": False}


def test_execute_automatically_adds_exclusions_and_resumes_same_run(monkeypatch, tmp_path):
    defaults = tmp_path / "run.yaml"
    defaults.write_text("pair: orders\n", encoding="utf-8")
    settings = {
        "pair": "orders",
        "run_id": "orders-me-20261022",
        "timezone": "America/New_York",
        "sync_remote": False,
        "email_to": "",
        "excluded_partitions": ["2026-10-01"],
    }
    config = {"pairs": {"orders": {"excluded_partitions": ["2026-10-01"]}}}
    calls = []
    snapshots = []
    monkeypatch.setattr(cli, "load_environment", lambda: None)
    monkeypatch.setattr(
        cli,
        "prompts",
        lambda _defaults, *, interactive: (settings, config, "new", object(), object()),
    )
    monkeypatch.setattr(cli, "configure_console_logging", lambda *_args: None)

    def snapshot(values, effective):
        snapshots.append(list(effective["pairs"]["orders"]["excluded_partitions"]))
        return tmp_path / "request.json"

    def launch(_settings, _config, *, action, **_kwargs):
        calls.append((action, _settings["run_id"]))
        if len(calls) == 1:
            raise RuntimeError(
                "version 2 row-check found unexcluded mismatched partitions; "
                "add them to excluded_partitions (orders: 2026-10-02, 2026-10-05)"
            )
        return {"state": "passed"}

    monkeypatch.setattr(cli, "write_request_snapshot", snapshot)
    monkeypatch.setattr(cli, "launch", launch)
    args = SimpleNamespace(
        config=None,
        defaults_path=defaults,
        earlystop=False,
        force=False,
        log_level="INFO",
    )

    assert cli.execute(args, raise_errors=True) == 0
    assert calls == [
        ("new", "orders-me-20261022"),
        ("resume", "orders-me-20261022"),
    ]
    assert snapshots == [
        ["2026-10-01"],
        ["2026-10-01", "2026-10-02", "2026-10-05"],
    ]
    saved = yaml.safe_load(defaults.read_text(encoding="utf-8"))
    assert saved["excluded_partitions"] == ["2026-10-01", "2026-10-02", "2026-10-05"]
    assert "earlystop" not in saved and "force" not in saved


def test_execute_earlystop_reports_mismatch_without_retry(monkeypatch, tmp_path):
    defaults = tmp_path / "run.yaml"
    defaults.write_text("pair: orders\n", encoding="utf-8")
    settings = {
        "pair": "orders",
        "run_id": "orders-me-20261022",
        "timezone": "America/New_York",
        "sync_remote": False,
        "email_to": "",
        "excluded_partitions": ["2026-10-01"],
    }
    config = {"pairs": {"orders": {"excluded_partitions": ["2026-10-01"]}}}
    calls = []
    monkeypatch.setattr(cli, "load_environment", lambda: None)
    monkeypatch.setattr(
        cli,
        "prompts",
        lambda _defaults, *, interactive: (settings, config, "new", object(), object()),
    )
    monkeypatch.setattr(cli, "configure_console_logging", lambda *_args: None)
    monkeypatch.setattr(cli, "write_request_snapshot", lambda *_args: tmp_path / "request.json")

    def launch(*_args, action, **_kwargs):
        calls.append(action)
        raise RuntimeError(
            "version 2 row-check found unexcluded mismatched partitions; "
            "add them to excluded_partitions (orders: 2026-10-02)"
        )

    monkeypatch.setattr(cli, "launch", launch)
    args = SimpleNamespace(
        config=None,
        defaults_path=defaults,
        earlystop=True,
        force=False,
        log_level="INFO",
    )

    with pytest.raises(RuntimeError, match="unexcluded mismatched partitions"):
        cli.execute(args, raise_errors=True)

    assert calls == ["new"]
    assert settings["excluded_partitions"] == ["2026-10-01"]
    assert config["pairs"]["orders"]["excluded_partitions"] == ["2026-10-01"]


def test_execute_force_overrides_resume_with_restart(monkeypatch, tmp_path):
    settings = {
        "pair": "orders",
        "run_id": "orders-me-20261022",
        "timezone": "America/New_York",
        "sync_remote": False,
        "email_to": "",
    }
    config = {"pairs": {"orders": {}}}
    actions = []
    monkeypatch.setattr(cli, "load_environment", lambda: None)
    monkeypatch.setattr(cli, "load_defaults", lambda _path: {})
    monkeypatch.setattr(
        cli,
        "prompts",
        lambda _defaults: (settings, config, "resume", object(), object()),
    )
    monkeypatch.setattr(cli, "configure_console_logging", lambda *_args: None)
    monkeypatch.setattr(cli, "yes_no", lambda *_args: True)
    monkeypatch.setattr(cli, "save_defaults", lambda *_args: None)
    monkeypatch.setattr(cli, "write_request_snapshot", lambda *_args: tmp_path / "request.json")
    monkeypatch.setattr(
        cli,
        "launch",
        lambda *_args, action, **_kwargs: actions.append(action) or {"state": "passed"},
    )
    args = SimpleNamespace(
        config=None,
        defaults_path=None,
        earlystop=False,
        force=True,
        log_level="INFO",
    )

    assert cli.execute(args, raise_errors=True) == 0
    assert actions == ["restart"]


def test_main_program_parses_defaults_earlystop_and_force(monkeypatch, tmp_path):
    seen = {}
    defaults = tmp_path / "run.yaml"
    monkeypatch.setattr(
        cli,
        "execute",
        lambda args, **_kwargs: seen.update(vars(args)) or 0,
    )
    monkeypatch.setattr(cli, "run_debugger", lambda _mode, callback: callback())

    assert cli.main_program(["--defaults", str(defaults), "--earlystop", "--force"]) == 0
    assert seen["defaults_path"] == defaults
    assert seen["earlystop"] is True
    assert seen["force"] is True


def test_main_program_rejects_removed_default_alias():
    with pytest.raises(SystemExit):
        cli.main_program(["--default"])


def test_timezone_default(monkeypatch):
    monkeypatch.setenv("PAIR_RECON_TIMEZONE", "Asia/Tokyo")
    assert cli.timezone_default() == "Asia/Tokyo"


@pytest.mark.parametrize(("raw", "expected"), [("3", 3), (8, 8)])
def test_positive_int(monkeypatch, raw, expected):
    monkeypatch.setattr(cli, "ask", lambda *_args, **_kwargs: raw)
    assert cli.positive_int("Worker budget", 2) == expected


@pytest.mark.parametrize("raw", ["0", "-1", "1.5", "many"])
def test_positive_int_rejects_invalid_budget(monkeypatch, raw):
    monkeypatch.setattr(cli, "ask", lambda *_args, **_kwargs: raw)
    with pytest.raises(ValueError, match="must be a positive integer"):
        cli.positive_int("Worker budget", 2)


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
            "audit": True,
            "checks": ["row_count", "hash_sum"],
            "from_date": "2026-01-01",
            "to_date": "2026-01-31",
            "vintage": "month",
        },
        timezone="America/New_York",
    )
    assert runner.validate_config(runtime)["pairs"]["orders"]["sample"] == 0.1
    assert runtime["pairs"]["orders"]["audit"] is True
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


def test_editable_prompt_prefills_default_and_allows_clearing(monkeypatch):
    seen = {}

    def read(message, default):
        seen.update(message=message, default=default)
        return "", True

    monkeypatch.setattr(cli, "_read_prompt", read)

    assert cli.ask("Outlook recipients", "a@example.com", required=False) == ""
    assert seen == {
        "message": "Outlook recipients [a@example.com]: ",
        "default": "a@example.com",
    }


def test_prompt_session_undo_returns_to_previous_question(monkeypatch):
    answers = iter(("first", "second", "undo", "changed", "third"))
    monkeypatch.setattr("builtins.input", lambda _prompt: next(answers))
    session = cli.PromptSession()

    while True:
        session.begin()
        try:
            first = session.answer("First")
            second = session.answer("Second")
            third = session.answer("Third")
            break
        except cli.RestartPrompts:
            continue

    assert (first, second, third) == ("first", "changed", "third")
    assert session.answers == ["first", "changed", "third"]


def test_retry():
    store = MemoryStore()
    store.put_json(
        "row_check/result.json",
        {"partitions": {"all": {"result": "match"}}},
    )
    store.put_json("col_check/results/good.json", {"verdict": "match"})
    store.put_json("col_check/results/bad.json", {"verdict": "diff"})
    store.put_json("col_check/failures/error.json", {"error": "boom"})
    assert retry_failed(store) == 2
    assert store.exists("col_check/results/good.json")
    assert not store.exists("col_check/results/bad.json")
    assert not store.exists("col_check/failures/error.json")


def test_retry_keeps_left_counts_when_the_row_check_mismatched():
    store = MemoryStore()
    store.put_json("row_check/left.json", {"counts": {"2026-01-01": 2}})
    store.put_json(
        "row_check/result.json",
        {"partitions": {"2026-01-01": {"result": "count_diff"}}},
    )
    assert retry_failed(store) == 1
    assert store.exists("row_check/left.json")
    assert not store.exists("row_check/result.json")


def test_require_plan_windows_rejects_an_empty_plan():
    store = MemoryStore()
    pair_config = {"fromDate": "2023-09-01", "toDate": "2026-09-30"}
    runner.require_plan_windows(store, "orders", pair_config)
    store.put_json("plan.json", {"pair": "orders", "windows": [{"bucket": "2024-01-01"}]})
    runner.require_plan_windows(store, "orders", pair_config)
    store.put_json(
        "plan.json",
        {"pair": "orders", "windows": [], "excluded_partitions": ["2026-09-30"] * 36},
    )
    with pytest.raises(
        RuntimeError,
        match=r"plan for orders has no windows: .*2023-09-01 and 2026-09-30.*\(36 excluded\)",
    ):
        runner.require_plan_windows(store, "orders", pair_config)


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


def test_main_program_rejects_removed_sync_option():
    with pytest.raises(SystemExit):
        cli.main_program(["--sync"])


def test_progress():
    store = MemoryStore()
    write_run_status(
        store, {"state": "left_col_check", "run_id": "r", "timezone": "America/New_York"}
    )
    store.put_json("row_check/left.json", {"counts": {"all": 2}})
    store.put_json(plan_path("p"), {"pair": "p", "expected_units": 2, "windows": [], "columns": []})
    store.commit_units(
        1,
        [{"pair": "p", "window": "all", "unit_id": "u1", "left_statistics": {}}],
    )
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
    payload = json.loads(row["payload"])
    assert payload["row"]["left_rows"] == 2
    assert payload["run_status"]["state"] == "left_col_check"
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
    ("run_mode", "left_source", "right_source", "existing"),
    [
        ("full", "oracle", "databricks", True),
        ("full", "csv", "databricks", False),
        ("full", "oracle", "csv", False),
        ("full", "csv", "csv", False),
        ("left-only", "oracle", "databricks", False),
    ],
)
def test_prompt(monkeypatch, run_mode, left_source, right_source, existing, tmp_path, capsys):
    metadata = {
        "left": {
            "source": left_source,
            "table": "L",
            "path": str(tmp_path / "left_sample.csv"),
        },
        "right": {
            "source": right_source,
            "table": "R",
            "path": str(tmp_path / "right_sample.csv"),
        },
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
        "timezone": "America/New_York",
        "partition_mode": "all",
        "checks": "row_count,hash_sum",
        "sample": "all",
        "strict": False,
        "pair_workbook": str(tmp_path / "missing.xlsx"),
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
    assert action == ("resume" if existing else "new")
    assert not any("Run id" in prompt or "Existing run action" in prompt for prompt in prompts)
    assert not any(
        label in prompt
        for prompt in prompts
        for label in ("Left input", "Right input", "Left CSV path", "Right CSV path")
    )
    assert settings["run_mode"] == run_mode
    assert "left_input" not in settings and "right_input" not in settings
    remote_right = run_mode == "full" and right_source == "databricks"
    assert settings["sync_remote"] is remote_right
    assert settings["audit"] is False
    assert settings["left_workers"] == 2
    assert settings["right_workers"] == (8 if remote_right else 1)
    assert settings["work_dir"] == str(tmp_path / "artifacts")
    assert (tmp_path / "artifacts").is_dir()
    assert config["pairs"]["orders"]["statistics"] == ["hash_sum"]
    assert config["pairs"]["orders"]["audit"] is False
    assert config["pairs"]["orders"]["left"]["source"] == left_source
    assert "skipping workbook synchronization" in capsys.readouterr().out
    if left_source == "csv":
        assert config["pairs"]["orders"]["sample"] == 0.1
        assert config["pairs"]["orders"]["pre_sampled"] is True
    if run_mode == "left-only":
        assert config["pairs"]["orders"]["right"]["source"] == "databricks"
    if right_source == "csv":
        assert config["pairs"]["orders"]["right"]["source"] == "csv"
        assert config["pairs"]["orders"]["right_pre_sampled"] is True


def test_prompt_takes_dates_and_budgets_from_pair_run_defaults(monkeypatch, tmp_path):
    row = {
        "pair_name": "orders",
        "left_source": "oracle",
        "left_conn": "PCDS",
        "left_table": "L",
        "left_date_col": "ORDER_DT",
        "right_source": "databricks_table",
        "right_conn": "main.sales",
        "right_table": "R",
        "right_date_col": "order_dt",
        "run_defaults_json": json.dumps(
            {
                "from_date": "2026-03-01",
                "to_date": "2026-03-31",
                "vintage": "week",
                "excluded_partitions": ["2026-03-05"],
                "audit": True,
                "sync_remote": False,
                "left_workers": 3,
                "right_workers": 12,
            }
        ),
        "column_map_rows": [{"left_column": "A", "right_column": "a"}],
    }

    class Catalog:
        def __init__(self, *_args):
            pass

        def pairs(self):
            return [row]

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
        "pair": "other",
        "timezone": "America/New_York",
        "from_date": "2025-01-01",
        "to_date": "2025-01-31",
    }
    monkeypatch.setattr(cli, "Catalog", Catalog)
    monkeypatch.setattr(cli, "make_run_id", lambda *_args: "r-new")
    monkeypatch.setattr("builtins.input", lambda _prompt: "")
    monkeypatch.setenv("PAIR_RECON_OUTPUT_DIR", str(tmp_path / "artifacts"))

    settings, _config, _action, _client, _catalog = cli.prompts(
        defaults, workspace_factory=lambda _profile: SimpleNamespace(statement_execution=object())
    )

    assert settings["from_date"] == "2026-03-01"
    assert settings["to_date"] == "2026-03-31"
    assert settings["vintage"] == "week"
    assert settings["excluded_partitions"] == ["2026-03-05"]
    assert settings["audit"] is True
    assert settings["sync_remote"] is False
    assert settings["left_workers"] == 3
    assert settings["right_workers"] == 12


def test_prompt_records_and_applies_date_columns_and_types(monkeypatch, tmp_path):
    metadata = {
        "left": {
            "source": "oracle",
            "table": "L",
            "date_col": "ORDER_DT",
            "date_type": {"type": "string", "format": "compact"},
        },
        "right": {
            "source": "databricks",
            "table": "R",
            "date_col": "order_dt",
            "date_type": "date",
        },
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
        "run_mode": "full",
        "timezone": "America/New_York",
        "partition_mode": "date",
        "checks": "row_count,hash_sum",
        "sample": "all",
        "strict": False,
        "from_date": "2026-01-01",
        "to_date": "2026-01-31",
        "excluded_partitions": [
            "2026-01-02",
            "2026-01-03",
            "2026-01-04",
            "2026-01-05",
            "2026-01-10",
        ],
    }
    replies = {
        "Left date column": "BUSINESS_DT",
        "Left date type": '{"type":"sas","format":"datetime"}',
        "Right date column": "business_dt",
        "Right date type": '{"type":"string","format":"dash"}',
        "Excluded partitions": "2026-01-02->2026-01-05, 2026-01-10",
    }
    prompts = []

    def answer(prompt):
        prompts.append(prompt)
        return next((value for label, value in replies.items() if prompt.startswith(label)), "")

    monkeypatch.setattr(cli, "Catalog", Catalog)
    monkeypatch.setattr(cli, "make_run_id", lambda *_args: "r-new")
    monkeypatch.setattr("builtins.input", answer)
    monkeypatch.setenv("PAIR_RECON_OUTPUT_DIR", str(tmp_path / "artifacts"))

    settings, config, _action, _client, _catalog = cli.prompts(
        defaults, workspace_factory=lambda _profile: SimpleNamespace(statement_execution=object())
    )

    assert any(prompt.startswith("Left date column [ORDER_DT]") for prompt in prompts)
    assert any(
        prompt.startswith(
            'Left date type (date/string/number/sas or JSON) [{"type":"string","format":"compact"}]'
        )
        for prompt in prompts
    )
    assert any(
        prompt.startswith(
            "Excluded partitions (YYYY-MM-DD or start->end, comma-separated) "
            "[2026-01-02->2026-01-05, 2026-01-10]"
        )
        for prompt in prompts
    )
    assert settings["left_date_col"] == "BUSINESS_DT"
    assert settings["left_date_type"] == {"type": "sas", "format": "datetime"}
    assert settings["right_date_col"] == "business_dt"
    assert settings["right_date_type"] == {"type": "string", "format": "dash"}
    assert settings["excluded_partitions"] == [
        "2026-01-02",
        "2026-01-03",
        "2026-01-04",
        "2026-01-05",
        "2026-01-10",
    ]
    pair = config["pairs"]["orders"]
    assert pair["left"]["date_col"] == "BUSINESS_DT"
    assert pair["left"]["date_type"] == {"type": "sas", "format": "datetime"}
    assert pair["right"]["date_col"] == "business_dt"
    assert pair["right"]["date_type"] == {"type": "string", "format": "dash"}
    assert pair["excluded_partitions"] == settings["excluded_partitions"]


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


def test_synchronize_pair_workbook_skips_missing(monkeypatch, tmp_path):
    workbook = tmp_path / "missing.xlsx"

    class Catalog:
        def configuration_rows(self):
            pytest.fail("missing workbook must not query catalog rows for synchronization")

    monkeypatch.setattr(
        cli,
        "read_pair_workbook",
        lambda _path: pytest.fail("missing workbook must not be read"),
    )

    rows, result = cli.synchronize_pair_workbook(
        "sql", Catalog(), "main.recon.pair_recon_pair_meta", workbook
    )

    assert rows == []
    assert result == {
        "rows": 0,
        "pairs": [],
        "source": str(workbook),
        "skipped": "missing",
    }


def test_pair_workbook_path_uses_environment_even_when_missing(monkeypatch, tmp_path):
    workbook = tmp_path / "pairs.xlsx"
    monkeypatch.setenv("DBX_PAIR_WORKBOOK", str(workbook))

    assert cli.pair_workbook_path({"pair_workbook": "ignored.xlsx"}) == workbook


def test_prompt_can_use_databricks_metadata_without_syncing_local_workbook(
    monkeypatch, tmp_path, capsys
):
    defaults = {
        "owner": "me",
        "profile": "corp",
        "warehouse_id": "warehouse",
        "pair_table": "main.recon.pair_recon_pair_meta",
        "status_table": "main.recon.pair_recon_run_status",
        "error_table": "main.recon.pair_recon_run_error",
        "volume_root": "/Volumes/main/recon/artifacts/runs",
        "pair_workbook": str(tmp_path / "pairs.xlsx"),
    }

    class Catalog:
        def __init__(self, *_args):
            pass

        def pairs(self):
            return []

    (tmp_path / "pairs.xlsx").touch()
    monkeypatch.setenv("DBX_SYNC_PAIR_METADATA", "false")
    monkeypatch.setattr(cli, "Catalog", Catalog)
    monkeypatch.setattr(
        cli,
        "synchronize_pair_workbook",
        lambda *_args: pytest.fail("disabled metadata synchronization must not read the workbook"),
    )

    with pytest.raises(RuntimeError, match="have no enabled pair"):
        cli.prompts(
            defaults,
            workspace_factory=lambda _profile: SimpleNamespace(statement_execution=object()),
            interactive=False,
        )

    output = capsys.readouterr().out
    assert "DBX_SYNC_PAIR_METADATA=false" in output
    assert "using Databricks metadata as the source of truth" in output


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

    def left(*_args, release=None, **_kwargs):
        release()
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


def test_run_left_force_bypasses_cached_row_check(monkeypatch, tmp_path):
    events = []
    config = {"pairs": {"orders": {"checks": ["row_count"]}}}
    monkeypatch.setattr(runner, "validate_config", lambda value: value)
    monkeypatch.setattr(
        runner,
        "reuse_row_check",
        lambda *_args, **_kwargs: pytest.fail("force must not read row-check cache"),
    )
    monkeypatch.setattr(
        runner,
        "run_row_check",
        lambda *_args, **_kwargs: events.append("fresh-row-check"),
    )

    result = runner.run_left(
        config,
        MemoryStore(),
        "orders-me-20261022",
        "orders",
        tmp_path,
        force=True,
    )

    assert result == {"row": "ready", "units": 0}
    assert events == ["fresh-row-check"]


def test_run_left_fresh_row_check_invalidates_downstream_checkpoints(monkeypatch, tmp_path):
    store = MemoryStore()
    for key in (
        "row_check/result.json",
        "plan.json",
        "mapping.csv",
        "fixed_keys.json",
        "_status/row_check.closed.json",
        "_status/col_check.closed.json",
    ):
        store.put_json(key, {})
    config = {"pairs": {"orders": {"checks": ["row_count"]}}}
    monkeypatch.setattr(runner, "validate_config", lambda value: value)
    monkeypatch.setattr(runner, "reuse_row_check", lambda *_args, **_kwargs: None)

    def fresh_row_check(*_args, **_kwargs):
        assert not any(
            store.exists(key)
            for key in (
                "row_check/result.json",
                "plan.json",
                "mapping.csv",
                "fixed_keys.json",
                "_status/row_check.closed.json",
                "_status/col_check.closed.json",
            )
        )

    monkeypatch.setattr(runner, "run_row_check", fresh_row_check)

    assert runner.run_left(config, store, "run", "orders", tmp_path) == {
        "row": "ready",
        "units": 0,
    }


def test_remote_resume_starts_right_after_left_row_release(monkeypatch, tmp_path):
    store = MemoryStore()
    store.put_bytes("report/index.html", b"old")
    store.put_bytes("report/row_check.csv", b"old")
    store.put_bytes("_status/units.csv", b"old")
    store.put_bytes("audit/keep.sql", b"select 1")
    events = []

    class Root:
        def __init__(self, *_args, **_kwargs):
            pass

        def child(self, _run_id):
            return store

    class Remote:
        run_id = 91

    class Monitor:
        final_state = "success"
        error = None

        def __init__(self, *_args, **_kwargs):
            self.outcome = {"row_errors": 0, "column_errors": 0, "failures": 0}

        def start(self):
            events.append("monitor-start")

        def raise_if_failed(self):
            events.append("health-check")

        def result(self):
            events.append("monitor-result")

    def run_now(*_args, **_kwargs):
        events.append("right-start")
        return Remote()

    def left(*_args, release=None, abort_if=None, **_kwargs):
        events.append("left-row-ready")
        assert "right-start" not in events
        assert store.list_keys("report/") == []
        assert not store.exists("_status/units.csv")
        assert store.exists("audit/keep.sql")
        release()
        abort_if()
        return {"row": "ready", "units": 0}

    monkeypatch.setattr(runner, "VolumeStore", Root)
    monkeypatch.setattr(runner, "RightJobMonitor", Monitor)
    monkeypatch.setattr(runner, "configure_left_logging", lambda *_args: None)
    monkeypatch.setattr(runner, "run_left", left)
    settings = {
        "run_id": "resume-1",
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
    client = SimpleNamespace(
        files=object(),
        jobs=SimpleNamespace(run_now=run_now, cancel_run=lambda _run_id: None),
    )
    config = {"pairs": {"orders": {"right": {"source": "databricks"}}}}

    result = runner.launch(
        settings,
        config,
        action="resume",
        workspace_client=client,
        catalog=None,
    )

    assert result["state"] == "passed"
    assert events == [
        "left-row-ready",
        "right-start",
        "monitor-start",
        "health-check",
        "monitor-result",
    ]


def test_new_run_starts_right_only_after_left_row_release(monkeypatch, tmp_path):
    store = MemoryStore()
    events = []

    class Root:
        def __init__(self, *_args, **_kwargs):
            pass

        def child(self, _run_id):
            return store

    class Remote:
        run_id = 98

    class Monitor:
        final_state = "success"
        error = None

        def __init__(self, *_args, **_kwargs):
            self.outcome = {"row_errors": 0, "column_errors": 0, "failures": 0}

        def start(self):
            events.append("monitor-start")

        def raise_if_failed(self):
            events.append("health-check")

        def result(self):
            events.append("monitor-result")

    def run_now(*_args, **_kwargs):
        events.append("right-start")
        return Remote()

    def left(*_args, release=None, abort_if=None, **_kwargs):
        events.append("left-row-ready")
        assert "right-start" not in events
        assert store.exists("_status/heartbeat.json")
        release()
        abort_if()
        return {"row": "ready", "units": 0}

    monkeypatch.setattr(runner, "VolumeStore", Root)
    monkeypatch.setattr(runner, "RightJobMonitor", Monitor)
    monkeypatch.setattr(runner, "configure_left_logging", lambda *_args: None)
    monkeypatch.setattr(runner, "run_left", left)
    settings = {
        "run_id": "new-1",
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
    client = SimpleNamespace(
        files=object(),
        jobs=SimpleNamespace(run_now=run_now, cancel_run=lambda _run_id: None),
    )
    config = {"pairs": {"orders": {"right": {"source": "databricks"}}}}

    result = runner.launch(settings, config, action="new", workspace_client=client, catalog=None)

    assert result["state"] == "passed"
    assert events == [
        "left-row-ready",
        "right-start",
        "monitor-start",
        "health-check",
        "monitor-result",
    ]


def test_launch_injects_checked_vintages_from_window_history(monkeypatch, tmp_path):
    seen = {}

    class Root:
        def __init__(self, *_args, **_kwargs):
            pass

        def child(self, _run_id):
            return MemoryStore()

    class Sampler:
        def __init__(self, *_args, **_kwargs):
            pass

        def start(self, **_kwargs):
            return self

        def stop(self, *_args, **_kwargs):
            pass

    class Catalog:
        progress = staticmethod(lambda _row: None)

        def clear(self, _run_id):
            pass

        def status(self, **_values):
            pass

        def finish(self, _store, **_values):
            pass

        def checked_windows(self, pair, vintage, *, exclude_run_id):
            seen["query"] = (pair, vintage, exclude_run_id)
            return ["2026-01-01", "2026-03-01"]

    def left(config, *_args, release=None, **_kwargs):
        seen["checked"] = config["pairs"]["orders"].get("checked_vintages")
        release()
        return {"row": "ready", "units": 0}

    monkeypatch.setattr(runner, "VolumeStore", Root)
    monkeypatch.setattr(runner, "ProgressSampler", Sampler)
    monkeypatch.setattr(runner, "configure_left_logging", lambda *_args: None)
    monkeypatch.setattr(runner, "run_left", left)
    monkeypatch.setattr(runner, "run_right_row_check", lambda *_args: None)
    monkeypatch.setattr(
        runner,
        "run_right",
        lambda *_args, **_kwargs: {"row_errors": 0, "column_errors": 0, "failures": 0},
    )
    settings = {
        "run_id": "orders-me-20261024",
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
    pair = {
        "right": {"source": "csv"},
        "partition_mode": "date",
        "vintage": "month",
        "vintage_sample_size": 4,
    }

    runner.launch(
        settings,
        {"pairs": {"orders": dict(pair)}},
        action="new",
        workspace_client=client,
        catalog=Catalog(),
    )
    assert seen == {
        "query": ("orders", "month", "orders-me-20261024"),
        "checked": ["2026-01-01", "2026-03-01"],
    }

    seen.clear()
    runner.launch(
        {**settings, "recheck": True},
        {"pairs": {"orders": {**pair, "checked_vintages": ["stale"]}}},
        action="new",
        workspace_client=client,
        catalog=Catalog(),
    )
    assert seen == {"checked": None}


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

        def finish(self, _store, **values):
            states.append(values["state"])

        def fail(self, **values):
            states.append(values["state"])

    monkeypatch.setattr(runner, "VolumeStore", Root)
    monkeypatch.setattr(runner, "run_left", lambda *_args, release=None, **_kwargs: release())
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


def test_launch_reports_right_failure_while_left_is_running(monkeypatch, tmp_path):
    store = MemoryStore()
    states = []
    cancelled = []

    class Root:
        def __init__(self, *_args, **_kwargs):
            pass

        def child(self, _run_id):
            return store

    class Sampler:
        def __init__(self, *_args, **_kwargs):
            pass

        def start(self, **_kwargs):
            return self

        def stop(self, _state, **_kwargs):
            pass

    class Catalog:
        progress = staticmethod(lambda _row: None)

        def clear(self, _run_id):
            pass

        def status(self, **values):
            states.append(values["state"])

        def finish(self, _store, **values):
            states.append(f"finish:{values['state']}")

        def fail(self, **values):
            states.append(f"fail:{values['state']}")

    class Remote:
        run_id = 91

        def result(self, *, timeout):
            raise RuntimeError("health check exploded")

    def left(*_args, release=None, abort_if=None, **_kwargs):
        assert abort_if is not None
        release()
        for _ in range(100):
            abort_if()
            threading.Event().wait(0.01)
        pytest.fail("Left did not observe the Right failure")

    monkeypatch.setattr(runner, "VolumeStore", Root)
    monkeypatch.setattr(runner, "ProgressSampler", Sampler)
    monkeypatch.setattr(runner, "run_left", left)
    settings = {
        "run_id": "failed-right-1",
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
    client = SimpleNamespace(
        files=object(),
        jobs=SimpleNamespace(
            run_now=lambda *_args, **_kwargs: Remote(),
            cancel_run=lambda run_id: cancelled.append(run_id),
            get_run=lambda run_id: SimpleNamespace(
                run_id=run_id,
                run_page_url="https://example/runs/91",
                tasks=[SimpleNamespace(run_id=910)],
            ),
            get_run_output=lambda run_id: SimpleNamespace(
                error="ModuleNotFoundError: No module named 'pair_recon_right'",
                error_trace=f"Traceback for {run_id}\n  File notebook, line 6\n",
                notebook_output=None,
            ),
        ),
    )
    config = {"pairs": {"orders": {"right": {"source": "databricks"}}}}

    with pytest.raises(
        RuntimeError,
        match="Right Databricks job failed: run_id=91: health check exploded",
    ):
        runner.launch(
            settings,
            config,
            action="new",
            workspace_client=client,
            catalog=Catalog(),
        )

    content = (
        local_run_directory(tmp_path, "failed-right-1", settings["timezone"]) / "left.log"
    ).read_text(encoding="utf-8")
    assert "Right Databricks job failed: run_id=91: health check exploded" in content
    assert "run page: https://example/runs/91" in content
    assert "task run 910: ModuleNotFoundError" in content
    assert "Traceback for 910\n  File notebook, line 6" in content
    assert states == ["running", "fail:failed"]
    assert not store.exists("col_check/failures/right-job.json")
    assert cancelled == [91]


def test_right_job_monitor_reports_failed_task_traceback():
    class Remote:
        run_id = 93

        def result(self, *, timeout):
            return SimpleNamespace(
                run_id=93,
                run_page_url="https://example/runs/93",
                tasks=[SimpleNamespace(run_id=930)],
                state=SimpleNamespace(result_state="FAILED", state_message="Workload failed"),
            )

    jobs = SimpleNamespace(
        get_run=lambda _run_id: pytest.fail("completed run must be reused"),
        get_run_output=lambda run_id: SimpleNamespace(
            error="ValueError: boom",
            error_trace=f"Traceback {run_id}\n  File job.py, line 55\n",
        ),
    )
    monitor = runner.RightJobMonitor(Remote(), jobs=jobs).start()
    with pytest.raises(RuntimeError) as failure:
        monitor.result()
    message = str(failure.value)
    assert message.startswith(
        "Right Databricks job failed: run_id=93: Right job finished with FAILED: Workload failed"
    )
    assert "run page: https://example/runs/93" in message
    assert "task run 930: ValueError: boom" in message
    assert "Traceback 930\n  File job.py, line 55" in message


def test_right_job_monitor_forwards_cancelled_state():
    forwarded = []

    class Remote:
        run_id = 95

        def result(self, *, timeout):
            return SimpleNamespace(
                run_id=95,
                state=SimpleNamespace(result_state="CANCELED", state_message="Cancelled by user"),
            )

    monitor = runner.RightJobMonitor(Remote(), on_terminal=forwarded.append).start()

    with pytest.raises(RuntimeError, match="CANCELED: Cancelled by user"):
        monitor.result()
    assert monitor.final_state == "cancelled"
    assert forwarded == ["cancelled"]


def test_launch_forwards_cancelled_job_and_finalizes_catalog(monkeypatch, tmp_path):
    store = MemoryStore()
    states = []
    cancelled = []

    class Root:
        def __init__(self, *_args, **_kwargs):
            pass

        def child(self, _run_id):
            return store

    class Sampler:
        def __init__(self, *_args, **_kwargs):
            pass

        def start(self, **_kwargs):
            return self

        def stop(self, _state, **_kwargs):
            pass

    class Catalog:
        progress = staticmethod(lambda _row: None)

        def clear(self, _run_id):
            pass

        def status(self, **values):
            states.append(values["state"])

        def finish(self, _store, **values):
            states.append(f"finish:{values['state']}")

        def fail(self, **values):
            states.append(f"fail:{values['state']}")

    class Remote:
        run_id = 96

        def result(self, *, timeout):
            return SimpleNamespace(
                run_id=96,
                state=SimpleNamespace(result_state="CANCELED", state_message="Cancelled by user"),
            )

    def left(*_args, release=None, abort_if=None, **_kwargs):
        release()
        for _ in range(100):
            abort_if()
            threading.Event().wait(0.01)
        pytest.fail("Left did not observe cancellation")

    monkeypatch.setattr(runner, "VolumeStore", Root)
    monkeypatch.setattr(runner, "ProgressSampler", Sampler)
    monkeypatch.setattr(runner, "run_left", left)
    settings = {
        "run_id": "cancelled-right-1",
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
    jobs = SimpleNamespace(
        run_now=lambda *_args, **_kwargs: Remote(),
        cancel_run=lambda run_id: cancelled.append(run_id),
        get_run_output=lambda _run_id: SimpleNamespace(
            error=None,
            error_trace=None,
            notebook_output=None,
        ),
    )

    with pytest.raises(RuntimeError, match="CANCELED: Cancelled by user"):
        runner.launch(
            settings,
            {"pairs": {"orders": {"right": {"source": "databricks"}}}},
            action="new",
            workspace_client=SimpleNamespace(files=object(), jobs=jobs),
            catalog=Catalog(),
        )

    assert states == ["running", "fail:cancelled"]
    assert not store.exists("col_check/failures/right-job.json")
    assert cancelled == []


def test_right_job_monitor_outcome_is_empty_without_notebook_exit():
    class Remote:
        run_id = 94

        def result(self, *, timeout):
            return SimpleNamespace(run_id=94, state=SimpleNamespace(result_state="SUCCESS"))

    jobs = SimpleNamespace(
        get_run_output=lambda run_id: SimpleNamespace(
            notebook_output=SimpleNamespace(result="not json")
        )
    )
    monitor = runner.RightJobMonitor(Remote(), jobs=jobs).start()
    monitor.result()
    assert monitor.outcome == {}


def test_right_job_monitor_keeps_waiting_past_each_wait_chunk(caplog):
    attempts = []

    class Remote:
        run_id = 97

        def result(self, *, timeout):
            attempts.append(timeout)
            if len(attempts) < 3:
                raise TimeoutError("timed out after 1:00:00: RUNNING")
            return SimpleNamespace(run_id=97, state=SimpleNamespace(result_state="SUCCESS"))

    with caplog.at_level("INFO"):
        monitor = runner.RightJobMonitor(Remote()).start()
        monitor.result()
    assert monitor.final_state == "success" and len(attempts) == 3
    assert all(value == runner.RIGHT_WAIT_CHUNK for value in attempts)
    assert caplog.text.count("Right Databricks job still running: run_id=97") == 2


def test_launch_uses_background_right_result_for_success(monkeypatch, tmp_path):
    store = MemoryStore()
    calls = []

    class Root:
        def __init__(self, *_args, **_kwargs):
            pass

        def child(self, _run_id):
            return store

    class Sampler:
        def __init__(self, *_args, **_kwargs):
            pass

        def start(self, **_kwargs):
            return self

        def stop(self, state, **_kwargs):
            calls.append(f"stop:{state}:capture={_kwargs.get('capture')}")

    class Catalog:
        progress = staticmethod(lambda _row: None)

        def clear(self, _run_id):
            pass

        def status(self, **values):
            calls.append(f"status:{values['state']}")

        def finish(self, _store, **values):
            calls.append(f"finish:{values['state']}")

        def run(self, _run_id):
            pytest.fail("notebook outcome should avoid a Catalog read")

    class Remote:
        run_id = 92

        def result(self, *, timeout):
            calls.append(f"result:{int(timeout.total_seconds())}")
            return SimpleNamespace(
                run_id=92,
                tasks=[SimpleNamespace(run_id=920)],
                state=SimpleNamespace(result_state="SUCCESS"),
            )

    def start_right(_job_id, *, notebook_params):
        calls.append(f"workers:{notebook_params['right_workers']}")
        return Remote()

    def run_output(run_id):
        calls.append(f"output:{run_id}")
        return SimpleNamespace(
            notebook_output=SimpleNamespace(
                result=json.dumps(
                    {"row_errors": 0, "column_errors": 0, "failures": 0, "results": 3}
                )
            )
        )

    def left(*_args, release=None, **_kwargs):
        release()
        return {"row": "ready", "units": 3}

    monkeypatch.setattr(runner, "VolumeStore", Root)
    monkeypatch.setattr(runner, "ProgressSampler", Sampler)
    monkeypatch.setattr(runner, "run_left", left)
    settings = {
        "run_id": "success-right-1",
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
    client = SimpleNamespace(
        files=object(),
        jobs=SimpleNamespace(
            run_now=start_right,
            cancel_run=lambda *_args: pytest.fail("successful Right run must not be cancelled"),
            get_run_output=run_output,
        ),
    )
    config = {"pairs": {"orders": {"right": {"source": "databricks"}}}}

    result = runner.launch(
        settings,
        config,
        action="new",
        workspace_client=client,
        catalog=Catalog(),
    )

    assert result["state"] == "passed"
    assert result["right"] == {
        "run_id": 92,
        "row_errors": 0,
        "column_errors": 0,
        "failures": 0,
        "results": 3,
    }
    assert "workers:8" in calls
    assert calls.count("result:3600") == 1
    assert calls == [
        "status:running",
        "workers:8",
        "result:3600",
        "output:920",
        "stop:passed:capture=False",
    ]


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
    assert len(units) == 1
    assert len(units[0][1]["units"]) == 2
    assert all("left_statistics" in value for value in units[0][1]["units"])
    assert store.get_json("_state/left.json")["phase"] == "col_check_closed"
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


def _stub_column_checks(monkeypatch, store, seen):
    plan = {
        "pair": "orders",
        "version": 2,
        "windows": [{"bucket": "all", "partitions": ["all"]}],
        "excluded_partitions": [],
    }
    store.put_json("plan.json", plan)

    class Runtime:
        def __init__(self, *_args, **_kwargs):
            self.pull_window = None
            self.compute_stats = None

    class Pipeline:
        def __init__(self, *_args, **_kwargs):
            pass

        def run(self, _manifests):
            seen.append(store.exists("row_check/result.json"))
            return 3

    monkeypatch.setattr(runner, "validate_config", lambda value: value)
    monkeypatch.setattr(runner, "reuse_row_check", lambda *_args, **_kwargs: ("run", []))
    monkeypatch.setattr(runner, "wait_for_manifests", lambda *_args, **_kwargs: [plan])
    monkeypatch.setattr(runner, "publish_fixed_keys", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(runner, "LeftRuntime", Runtime)
    monkeypatch.setattr(runner, "ColCheckPipeline", Pipeline)
    return {"pairs": {"orders": {"checks": ["row_count", "hash_sum"], "version": 2}}}


def test_run_left_pulls_only_after_the_right_row_check_result(monkeypatch, tmp_path):
    store = MemoryStore()
    seen = []
    config = _stub_column_checks(monkeypatch, store, seen)

    def release():
        store.put_json(
            "row_check/result.json",
            {"pair": "orders", "partitions": {"all": {"result": "match"}}},
        )

    result = runner.run_left(config, store, "run", "orders", tmp_path, release=release)
    assert result == {"row": "ready", "units": 3} and seen == [True]
    polls = []

    def abort():
        polls.append(True)
        if len(polls) == 3:
            raise RuntimeError("Right failed")

    monkeypatch.setattr(left_col_check.time, "sleep", lambda _seconds: None)
    with pytest.raises(RuntimeError, match="Right failed"):
        runner.run_left(config, MemoryStore(), "run", "orders", tmp_path, abort_if=abort)
    assert len(polls) == 3 and seen == [True]


def test_run_left_reports_unexcluded_mismatches_before_pulling(monkeypatch, tmp_path):
    store = MemoryStore()
    seen = []
    config = _stub_column_checks(monkeypatch, store, seen)
    store.put_json(
        "row_check/result.json",
        {"pair": "orders", "partitions": {"2026-01-05": {"result": "count_diff"}}},
    )
    with pytest.raises(RuntimeError, match="unexcluded mismatched partitions") as info:
        runner.run_left(config, store, "run", "orders", tmp_path)
    assert mismatched_partitions(info.value, "orders") == ["2026-01-05"]
    assert seen == []


def test_run_left_fails_before_release_when_the_plan_has_no_windows(monkeypatch, tmp_path):
    store = MemoryStore()
    config = _stub_column_checks(monkeypatch, store, [])
    store.put_json("plan.json", {"pair": "orders", "windows": [], "excluded_partitions": ["x"]})
    released = []
    with pytest.raises(RuntimeError, match="plan for orders has no windows"):
        runner.run_left(
            config, store, "run", "orders", tmp_path, release=lambda: released.append(True)
        )
    assert released == []


def test_progress_sampler_survives_transient_snapshot_errors(monkeypatch):
    from pair_recon.core import progress

    calls = []
    published = []
    enough = threading.Event()

    def flaky(*_args, **_kwargs):
        calls.append(True)
        if len(calls) == 2:
            raise ConnectionError("IncompleteRead(0 bytes read, 481 more expected)")
        return {"state": "ok"}

    def publish(row):
        published.append(row)
        if len(published) >= 3:
            enough.set()

    monkeypatch.setattr(progress, "progress_snapshot", flaky)
    sampler = ProgressSampler(MemoryStore(), "r", "p", "America/New_York", publish, interval=0.01)
    sampler.start(required=True)
    assert enough.wait(2)
    sampler.stop()
    assert sampler.thread.is_alive() is False and len(calls) >= 4

    def broken(*_args, **_kwargs):
        raise ConnectionError("down")

    monkeypatch.setattr(progress, "progress_snapshot", broken)
    with pytest.raises(ConnectionError, match="down"):
        ProgressSampler(MemoryStore(), "r", "p", "America/New_York", publish).start(required=True)
