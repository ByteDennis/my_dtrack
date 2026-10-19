import json
import secrets
import threading
import time
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

from pair_recon.core.progress import ProgressSampler
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
from pair_recon_right.engine.backends.targets import CsvTarget
from pair_recon_right.engine.checks.row_check import run_row_check as run_right_row_check
from pair_recon_right.engine.job import run as run_right

_SENSITIVE_FIELDS = {"user", "password", "access_token", "dsn", "server_hostname", "http_path"}
_SENSITIVE_WORDS = ("password", "secret", "token", "credential")


class StatementSql:
    def __init__(self, workspace, warehouse_id: str):
        self.api = workspace.statement_execution
        self.warehouse_id = warehouse_id
        self.lock = threading.Lock()

    def execute(self, statement: str, parameters=None):
        with self.lock:
            return self._execute(statement, parameters)

    def _execute(self, statement: str, parameters=None):
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
    value = deepcopy(value)
    pair = row.get("pair_name") or "pair"

    mappings = row.get("column_map_rows") or []
    if mappings:
        col_map = {}
        overrides = {}
        column_types = {}
        for mapping in mappings:
            left = str(mapping.get("left_column") or "").strip()
            right = str(mapping.get("right_column") or "").strip()
            if not left or not right:
                raise ValueError(f"{pair}: column mapping names must be non-empty")
            col_map[left] = right
            data_type = str(mapping.get("data_type") or "").strip()
            if not data_type:
                continue
            if data_type in {"numeric", "categorical"}:
                overrides[left] = data_type
            elif data_type.startswith("{"):
                try:
                    column_types[left] = json.loads(data_type)
                except json.JSONDecodeError as exc:
                    raise ValueError(f"{pair}.{left}: invalid custom data_type JSON") from exc
            else:
                column_types[left] = data_type
        value["col_map"] = col_map
        value["col_type_overrides"] = overrides
        value["column_types"] = column_types

    query_row = row.get("pair_sql")
    if query_row is not None:
        for side, field in (("left", "left_sql"), ("right", "right_sql")):
            target = value.setdefault(side, {})
            sql = str(query_row.get(field) or "").strip().rstrip(";").rstrip()
            if sql:
                target["query"] = sql
            else:
                target.pop("query", None)
                target.pop("query_windows", None)
        sample_size = query_row.get("vintage_sample_size")
        if sample_size in (None, ""):
            value.pop("vintage_sample_size", None)
        else:
            try:
                value["vintage_sample_size"] = int(sample_size)
            except (TypeError, ValueError):
                raise ValueError(f"{pair}: vintage_sample_size must be an integer") from None
        vintage_values = query_row.get("vintage_values")
        if isinstance(vintage_values, str):
            try:
                vintage_values = json.loads(vintage_values)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{pair}: vintage_values must be an array") from exc
        if vintage_values in (None, []):
            value.pop("vintage_values", None)
        elif isinstance(vintage_values, list) and all(
            isinstance(item, str) for item in vintage_values
        ):
            value["vintage_values"] = vintage_values
        else:
            raise ValueError(f"{pair}: vintage_values must be an array of strings")
    return value


def _snapshot_value(value):
    if isinstance(value, dict):
        clean = {}
        for key, item in value.items():
            normalized = str(key).lower()
            if (
                normalized in _SENSITIVE_FIELDS
                or normalized.endswith("_env")
                or any(word in normalized for word in _SENSITIVE_WORDS)
            ):
                continue
            clean[key] = _snapshot_value(item)
        return clean
    if isinstance(value, list):
        return [_snapshot_value(item) for item in value]
    return value


def write_request_snapshot(settings: dict, config: dict) -> Path:
    catalog = deepcopy(settings.get("_catalog_row") or {})
    raw_config = catalog.get("config_json")
    if isinstance(raw_config, str):
        try:
            catalog["config_json"] = json.loads(raw_config)
        except json.JSONDecodeError:
            pass
    public_settings = {key: value for key, value in settings.items() if not key.startswith("_")}
    payload = {
        "format": "pair-recon-request-v1",
        "saved_at": datetime.now(ZoneInfo(settings["timezone"])).isoformat(timespec="seconds"),
        "settings": _snapshot_value(public_settings),
        "catalog": _snapshot_value(catalog),
        "effective_config": _snapshot_value(config),
    }
    target = Path(settings["work_dir"]) / settings["run_id"] / "request.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    part = target.with_name(target.name + ".part")
    part.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    part.replace(target)
    return target


def run_left(
    config: dict,
    store,
    run_id: str,
    pair: str,
    work_dir: str | Path,
    manifest_timeout: float = 1800,
    pull_workers: int = 2,
    release=None,
):
    config = validate_config(config)
    reused = reuse_row_check(config, store, run_id=run_id, pair_names=[pair])
    if reused is None:
        run_row_check(config, store, run_id=run_id, pair_names=[pair], pair_workers=1)
    if release:
        release()
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
        pull_workers=max(1, int(pull_workers)),
        stat_workers=max(1, int(pull_workers)),
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
    sampler = None
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
    remote = None
    final_state = "failed"
    local_right = config["pairs"][pair]["right"].get("source") == "csv"
    try:
        if catalog:
            sampler = ProgressSampler(
                store,
                run_id,
                pair,
                settings["timezone"],
                catalog.progress,
                interval=float(settings.get("progress_interval", 10)),
            ).start(required=True)
        target_factory = lambda values: CsvTarget(values)
        if not local_right:
            remote = client.jobs.run_now(int(settings["right_job_id"]), notebook_params=params)
        left = run_left(
            config,
            store,
            run_id,
            pair,
            Path(settings["work_dir"]) / run_id,
            float(settings.get("timeout", 120)) * 60,
            int(settings.get("pull_workers", 2)),
            (lambda: run_right_row_check(store, target_factory)) if local_right else None,
        )
        if local_right:
            right = run_right(
                None,
                target_factory,
                run_id=run_id,
                store=store,
                poll_interval=0.1,
                wait_timeout=float(settings.get("timeout", 120)) * 60,
            )
            final_state = (
                "different"
                if sum(right.get(name, 0) for name in ("row_errors", "column_errors", "failures"))
                else "passed"
            )
            if catalog:
                catalog.finish(
                    store,
                    run_id=run_id,
                    pair=pair,
                    owner=settings["owner"],
                    state=final_state,
                    timezone=settings["timezone"],
                )
        else:
            completed = remote.result(
                timeout=timedelta(minutes=float(settings.get("timeout", 120)))
            )
            result = getattr(getattr(completed, "state", None), "result_state", None)
            if getattr(result, "value", result) != "SUCCESS":
                raise RuntimeError(f"Right job finished with {getattr(result, 'value', result)}")
            status = catalog.run(run_id) if catalog else None
            final_state = (status or {}).get("state", "finished")
        if sampler:
            sampler.stop(final_state, required=True)
            sampler = None
        return {
            "run_id": run_id,
            "pair": pair,
            "state": final_state,
            "left": left,
            "left_log": str(left_log),
            "right": right if local_right else {"run_id": int(remote.run_id)},
        }
    except BaseException:
        try:
            if remote is not None:
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
    finally:
        if sampler:
            sampler.stop(final_state)
