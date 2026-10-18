import logging
from types import SimpleNamespace

from support import MemoryStore

from pair_recon.app import cli
from pair_recon.app.runner import make_run_id, retry_failed
from pair_recon_left.common.runtime.run_logging import CONSOLE_LOG_FORMAT, ZonedFormatter


def test_runid():
    value = make_run_id("订单 pair", "测试 owner", "America/New_York")
    assert "/" not in value and "订单-pair-测试-owner" in value


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


def test_prompt(monkeypatch):
    metadata = {
        "left": {"source": "oracle", "table": "L"},
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
        "timezone": "America/New_York",
        "partition_mode": "all",
        "checks": "row_count,hash_sum",
        "sample": "all",
        "strict": False,
    }
    monkeypatch.setattr(cli, "Catalog", Catalog)
    monkeypatch.setattr(cli, "make_run_id", lambda *_args: "r-new")
    monkeypatch.setattr("builtins.input", lambda _prompt: "")
    settings, config, action, _client, _catalog = cli.prompts(
        defaults, workspace_factory=lambda _profile: SimpleNamespace(statement_execution=object())
    )
    assert settings["run_id"] == "r-new" and action == "new"
    assert config["pairs"]["orders"]["statistics"] == ["hash_sum"]
