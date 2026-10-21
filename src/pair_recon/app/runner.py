import json
import re
import threading
import time
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem

from pair_recon.core.metadata.configuration import build_pair_config
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
from pair_recon_left.common.model.protocol import (
    list_failures,
    list_results,
    list_row_results,
    write_run_status,
)
from pair_recon_left.common.runtime.run_logging import configure_left_logging, get_left_logger
from pair_recon_right.engine.backends.targets import CsvTarget
from pair_recon_right.engine.checks.row_check import run_row_check as run_right_row_check
from pair_recon_right.engine.job import run as run_right

_SENSITIVE_FIELDS = {"user", "password", "access_token", "dsn", "server_hostname", "http_path"}
_SENSITIVE_WORDS = ("password", "secret", "token", "credential")
_RUN_DATE = re.compile(r"-(\d{4})(\d{2})(\d{2})$")
log = get_left_logger("runner")


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
    stamp = (now or datetime.now(ZoneInfo(timezone))).strftime("%Y%m%d")
    clean = "-".join(
        filter(
            None,
            (
                "".join(c if c.isalnum() else "-" for c in value).strip("-")
                for value in (pair, owner)
            ),
        )
    )
    return validate_run_id(f"{clean}-{stamp}")


def local_run_directory(
    work_dir: str | Path,
    run_id: str,
    timezone: str,
    *,
    now: datetime | None = None,
) -> Path:
    value = validate_run_id(run_id)
    root = Path(work_dir)
    match = _RUN_DATE.search(value)
    day = (
        "-".join(match.groups())
        if match is not None
        else (now or datetime.now(ZoneInfo(timezone))).date().isoformat()
    )
    return root / day / value


def pair_config(row: dict) -> dict:
    return build_pair_config(row)


def resolve_job_id(client, reference: object) -> int:
    value = str(reference or "").strip()
    if not value:
        raise ValueError("Right job id or name is required")
    try:
        return int(value)
    except ValueError:
        pass
    matches = [
        job
        for job in client.jobs.list(name=value)
        if str(getattr(getattr(job, "settings", None), "name", "") or "").casefold()
        == value.casefold()
        and getattr(job, "job_id", None) is not None
    ]
    if not matches:
        raise ValueError(f"Right Databricks job not found by name: {value}")
    if len(matches) > 1:
        ids = ", ".join(str(job.job_id) for job in matches)
        raise ValueError(f"Right Databricks job name is not unique: {value} ({ids})")
    return int(matches[0].job_id)


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
    target = (
        local_run_directory(settings["work_dir"], settings["run_id"], settings["timezone"])
        / "request.json"
    )
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
    left_only: bool = False,
):
    config = validate_config(config)
    pair_config = config["pairs"][pair]
    if left_only and pair_config.get("snapshot"):
        raise ValueError("left-only mode does not support snapshot pairs that require Right")
    reused = reuse_row_check(config, store, run_id=run_id, pair_names=[pair])
    if reused is None:
        run_row_check(config, store, run_id=run_id, pair_names=[pair], pair_workers=1)
    if release:
        release()
    checks = pair_config.get("checks") or []
    if not any(check != "row_count" for check in checks):
        return {"row": "ready", "units": 0}
    timeout = manifest_timeout if pair_config.get("snapshot") else 0
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
        wait_for_right=not left_only,
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
    run_mode = str(settings.get("run_mode") or "full")
    if run_mode not in {"full", "left-only"}:
        raise ValueError("run_mode must be full or left-only")
    left_only = run_mode == "left-only"
    run_dir = local_run_directory(settings["work_dir"], run_id, settings["timezone"])
    run_dir.mkdir(parents=True, exist_ok=True)
    left_log = run_dir / "left.log"
    configure_left_logging(left_log, settings["timezone"])
    log.info(
        "run started: run_id=%s pair=%s mode=%s action=%s",
        run_id,
        pair,
        run_mode,
        action,
    )
    client = None
    store = None
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
    right = None
    final_state = "failed"
    local_right = not left_only and config["pairs"][pair]["right"].get("source") == "csv"
    try:
        client = workspace_client or workspace(settings["profile"])
        log.info(
            "Databricks target: profile=%s warehouse_id=%s volume_root=%s",
            settings["profile"],
            settings.get("warehouse_id"),
            settings["volume_root"],
        )
        store = VolumeStore(settings["volume_root"], files=client.files).child(run_id)
        if action in {"new", "restart"}:
            store.clear()
            if catalog:
                catalog.clear(run_id)
        elif action == "resume":
            retry_failed(store)
        if catalog:
            log.info("updating run status: table=%s state=running", settings["status_table"])
            catalog.status(
                run_id=run_id,
                pair=pair,
                owner=settings["owner"],
                state="running",
                timezone=settings["timezone"],
            )
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
        if not left_only and not local_right:
            job_reference = settings["right_job_id"]
            job_id = resolve_job_id(client, job_reference)
            log.info(
                "starting Right Databricks job: reference=%s job_id=%s",
                job_reference,
                job_id,
            )
            remote = client.jobs.run_now(job_id, notebook_params=params)
        log.info("starting Left checks")
        left = run_left(
            config,
            store,
            run_id,
            pair,
            run_dir,
            float(settings.get("timeout", 120)) * 60,
            int(settings.get("pull_workers", 2)),
            (lambda: run_right_row_check(store, target_factory)) if local_right else None,
            left_only=left_only,
        )
        log.info("Left checks completed: %s", left)
        if left_only:
            final_state = "left_complete"
            right = {"state": "not_run"}
            write_run_status(
                store,
                {"state": final_state, "run_id": run_id, "role": "left", "mode": run_mode},
                timezone_name=settings["timezone"],
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
        elif local_right:
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
            right = {"run_id": int(remote.run_id)}
        if sampler:
            sampler.stop(final_state, required=True)
            sampler = None
        log.info("run completed: state=%s", final_state)
        return {
            "run_id": run_id,
            "pair": pair,
            "mode": run_mode,
            "state": final_state,
            "left": left,
            "left_log": str(left_log),
            "right": right,
            "volume_run_root": store.root_uri,
        }
    except BaseException:
        final_state = "failed"
        log.exception("run failed")
        if remote is not None and client is not None:
            try:
                client.jobs.cancel_run(int(remote.run_id))
            except Exception:
                log.exception("could not cancel Right Databricks run: run_id=%s", remote.run_id)
        raise
    finally:
        try:
            if sampler:
                sampler.stop(final_state)
        finally:
            if catalog and final_state == "failed":
                try:
                    catalog.status(
                        run_id=run_id,
                        pair=pair,
                        owner=settings["owner"],
                        state=final_state,
                        timezone=settings["timezone"],
                    )
                except Exception:
                    log.exception("could not update final run status: state=%s", final_state)
