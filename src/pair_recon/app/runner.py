import json
import secrets
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

from pair_recon_left.backends.store import VolumeStore, validate_run_id
from pair_recon_left.cli.config import validate_config
from pair_recon_left.common.checks.col_check import (
    ColCheckPipeline,
    LeftRuntime,
    publish_fixed_keys,
    wait_for_manifests,
)
from pair_recon_left.common.checks.row_check import reuse_row_check, run_row_check
from pair_recon_left.common.model.protocol import list_failures, list_results, list_row_results
from pair_recon_left.common.runtime.run_logging import configure_left_logging


class StatementSql:
    def __init__(self, workspace, warehouse_id: str):
        self.api = workspace.statement_execution
        self.warehouse_id = warehouse_id

    def execute(self, statement: str, parameters=None):
        values = [
            StatementParameterListItem(
                name=name,
                type="STRING",
                value=None if value is None else str(value),
            )
            for name, value in (parameters or {}).items()
        ]
        response = self.api.execute_statement(
            statement,
            self.warehouse_id,
            parameters=values,
            wait_timeout="50s",
        )
        state = getattr(getattr(response, "status", None), "state", None)
        while getattr(state, "value", state) in {"PENDING", "RUNNING"}:
            time.sleep(1)
            response = self.api.get_statement(response.statement_id)
            state = getattr(getattr(response, "status", None), "state", None)
        if getattr(state, "value", state) != "SUCCEEDED":
            error = getattr(getattr(response, "status", None), "error", None)
            raise RuntimeError(getattr(error, "message", None) or f"SQL finished with {state}")
        columns = (
            [
                column.name
                for column in getattr(getattr(response, "manifest", None), "schema", None).columns
            ]
            if getattr(getattr(response, "manifest", None), "schema", None)
            else []
        )
        rows = getattr(getattr(response, "result", None), "data_array", None) or []
        return [dict(zip(columns, row)) for row in rows]


def workspace(profile: str):
    return WorkspaceClient(profile=profile)


def make_run_id(pair: str, owner: str, timezone: str, now=None) -> str:
    stamp = (now or datetime.now(ZoneInfo(timezone))).strftime("%Y%m%dT%H%M%S")
    clean = "-".join(
        filter(
            None,
            (
                "".join(c if c.isalnum() else "-" for c in value).strip("-")
                for value in (pair, owner)
            ),
        )
    )
    return validate_run_id(f"{clean}-{stamp}-{secrets.token_hex(2)}")


def pair_config(row: dict) -> dict:
    value = row.get("config_json")
    if isinstance(value, str):
        value = json.loads(value)
    if not isinstance(value, dict):
        raise TypeError("pair metadata requires config_json")
    return value


def run_left(
    config: dict,
    store,
    run_id: str,
    pair: str,
    work_dir: str | Path,
    manifest_timeout: float = 1800,
):
    config = validate_config(config)
    reused = reuse_row_check(config, store, run_id=run_id, pair_names=[pair])
    if reused is None:
        run_row_check(config, store, run_id=run_id, pair_names=[pair], pair_workers=1)
    checks = config["pairs"][pair].get("checks") or []
    if not any(check != "row_count" for check in checks):
        return {"row": "ready", "units": 0}
    timeout = manifest_timeout if config["pairs"][pair].get("snapshot") else 0
    manifests = wait_for_manifests(store, timeout=timeout, poll_interval=2, pair_names=[pair])
    for manifest in manifests:
        publish_fixed_keys(store, manifest, work_dir)
    runtime = LeftRuntime(work_dir, store)
    units = ColCheckPipeline(
        store,
        pull_window=runtime.pull_window,
        compute_stats=runtime.compute_stats,
        pull_workers=1,
        stat_workers=1,
    ).run(manifests)
    return {"row": "ready", "units": units}


def retry_failed(store) -> int:
    keys = [key for key, _ in list_failures(store)]
    keys += [key for key, value in list_results(store) if value.get("verdict") != "match"]
    for key, value in list_row_results(store):
        if any(row.get("verdict") != "match" for row in (value.get("partitions") or {}).values()):
            keys.append(key)
            left = key.removesuffix("result.json") + "left.json"
            if store.exists(left):
                keys.append(left)
    return store.delete_keys(sorted(set(keys)))


def launch(settings: dict, config: dict, *, action: str, workspace_client=None, catalog=None):
    pair = next(iter(config["pairs"]))
    run_id = validate_run_id(settings["run_id"])
    client = workspace_client or workspace(settings["profile"])
    store = VolumeStore(settings["volume_root"], files=client.files).child(run_id)
    if action == "restart":
        store.clear()
        if catalog:
            catalog.clear(run_id)
    elif action == "resume":
        retry_failed(store)
    left_log = Path(settings["work_dir"]) / run_id / "left.log"
    configure_left_logging(left_log, settings["timezone"])
    if catalog:
        catalog.status(
            run_id=run_id,
            pair=pair,
            owner=settings["owner"],
            state="running",
            timezone=settings["timezone"],
        )
    params = {
        "run_root": settings["volume_root"],
        "run_id": run_id,
        "pair": pair,
        "owner": settings["owner"],
        "timezone": settings["timezone"],
        "pair_table": settings["pair_table"],
        "status_table": settings["status_table"],
        "error_table": settings["error_table"],
    }
    remote = client.jobs.run_now(int(settings["right_job_id"]), notebook_params=params)
    try:
        left = run_left(
            config,
            store,
            run_id,
            pair,
            Path(settings["work_dir"]) / run_id,
            float(settings.get("timeout", 120)) * 60,
        )
        completed = remote.result(timeout=timedelta(minutes=float(settings.get("timeout", 120))))
        result = getattr(getattr(completed, "state", None), "result_state", None)
        if getattr(result, "value", result) != "SUCCESS":
            raise RuntimeError(f"Right job finished with {getattr(result, 'value', result)}")
        status = catalog.run(run_id) if catalog else None
        return {
            "run_id": run_id,
            "pair": pair,
            "state": (status or {}).get("state", "finished"),
            "left": left,
            "left_log": str(left_log),
            "right_run_id": int(remote.run_id),
        }
    except BaseException:
        try:
            client.jobs.cancel_run(int(remote.run_id))
        finally:
            if catalog:
                catalog.status(
                    run_id=run_id,
                    pair=pair,
                    owner=settings["owner"],
                    state="failed",
                    timezone=settings["timezone"],
                )
        raise
