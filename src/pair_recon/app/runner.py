import csv
import gzip
import io
import json
import os
import re
import threading
import time
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.sql import StatementParameterListItem

from pair_recon.core.backends.store import volume_failure_streak
from pair_recon.core.metadata.configuration import build_pair_config
from pair_recon.core.progress import ProgressSampler
from pair_recon_left.backends.sources import FETCH_PROGRESS, source_from_config
from pair_recon_left.backends.store import VolumeStore, validate_run_id
from pair_recon_left.cli.config import validate_config
from pair_recon_left.common.checks.col_check import (
    UNEXCLUDED_PARTITIONS,
    ColCheckPipeline,
    LeftRuntime,
    publish_fixed_keys,
    wait_for_manifests,
    wait_for_row_check_result,
    wait_for_row_result,
)
from pair_recon_left.common.checks.row_check import reuse_row_check, run_row_check
from pair_recon_left.common.model.hashing import composite_key, key_families
from pair_recon_left.common.model.protocol import (
    REPORT_PREFIX,
    RIGHT_SAMPLE_PREFIX,
    RIGHT_STATS_PREFIX,
    UNITS_STATUS,
    active_unit_ids,
    current_plan_ids,
    diagnostic_prefix,
    difference_keys_path,
    empty_plan_state,
    fixed_keys_path,
    get_left_state,
    get_right_state,
    list_failures,
    list_plans,
    list_results,
    list_row_results,
    mapping_path,
    plan_path,
    refresh_units_status,
    right_stats_path,
    row_result_path,
    write_right_state,
    write_run_status,
)
from pair_recon_left.common.model.protocol import (
    failure_path as outcome_failure_path,
)
from pair_recon_left.common.runtime.run_logging import configure_left_logging, get_left_logger
from pair_recon_left.common.runtime.stop import StopSignal, beat
from pair_recon_right.catalog import run_state, status_run_values
from pair_recon_right.engine.backends.targets import CsvTarget
from pair_recon_right.engine.checks.col_check import ColCheck as RightColCheck
from pair_recon_right.engine.checks.row_check import run_row_check as run_right_row_check
from pair_recon_right.engine.job import publish_report as publish_right_report
from pair_recon_right.engine.job import run as run_right
from pair_recon_right.engine.job import summary as right_summary

_SENSITIVE_FIELDS = {"user", "password", "access_token", "dsn", "server_hostname", "http_path"}
_SENSITIVE_WORDS = ("password", "secret", "token", "credential")
_RUN_DATE = re.compile(r"-(\d{4})(\d{2})(\d{2})$")
RIGHT_WAIT_CHUNK = timedelta(hours=1)
WATCHDOG_INTERVAL = 30.0
log = get_left_logger("runner")


# >>> run ids whose notebook output to read: each task's, else the run's own <<< #
def task_run_ids(run) -> list[int]:
    ids = [getattr(task, "run_id", None) for task in getattr(run, "tasks", None) or []]
    return [value for value in ids if value is not None] or [run.run_id]


# >>> run page url plus every task's notebook error and traceback <<< #
def describe_run(jobs, run) -> str:
    lines = []
    if getattr(run, "run_page_url", None):
        lines.append(f"run page: {run.run_page_url}")
    for task_run_id in task_run_ids(run):
        try:
            output = jobs.get_run_output(task_run_id)
        except (DatabricksError, OSError) as exc:
            lines.append(f"task run {task_run_id}: output unavailable ({exc})")
            continue
        if getattr(output, "error", None):
            lines.append(f"task run {task_run_id}: {output.error}")
        if getattr(output, "error_trace", None):
            lines.append(output.error_trace.rstrip())
    return "\n".join(lines)


# >>> dict the notebook returned through dbutils.notebook.exit, else empty <<< #
def run_outcome(jobs, run) -> dict:
    for task_run_id in task_run_ids(run):
        try:
            output = jobs.get_run_output(task_run_id)
        except (DatabricksError, OSError) as exc:
            log.warning("Right run output unavailable: task run %s: %s", task_run_id, exc)
            continue
        text = getattr(getattr(output, "notebook_output", None), "result", None)
        if not text:
            continue
        try:
            value = json.loads(text)
        except ValueError:
            log.warning("Right run output is not JSON: task run %s", task_run_id)
            continue
        if isinstance(value, dict):
            return value
    return {}


def record_runtime_failure(store, pair: str, source: str, error: BaseException | str) -> None:
    message = str(error).strip() or type(error).__name__
    store.put_json(
        outcome_failure_path(pair, source),
        {
            "pair": pair,
            "source": source,
            "error": message,
        },
    )


def mismatched_partitions(error: BaseException | str, pair: str) -> list[str]:
    text = str(error)
    if UNEXCLUDED_PARTITIONS not in text:
        return []
    match = re.search(
        rf"{re.escape(pair)}:\s*((?:\d{{4}}-\d{{2}}-\d{{2}}(?:,\s*)?)+)",
        text,
    )
    return sorted(set(re.findall(r"\d{4}-\d{2}-\d{2}", match.group(1)))) if match else []


def right_job_params(settings: dict, pair: str, run_id: str, run_values: dict) -> dict:
    return {
        "run_root": settings["volume_root"],
        "run_id": run_id,
        "pair": pair,
        "owner": settings["owner"],
        "timezone": settings["timezone"],
        "pair_table": settings["pair_table"],
        "status_table": settings["status_table"],
        "error_table": settings["error_table"],
        "right_workers": str(max(1, int(settings.get("right_workers", 8)))),
        "local_reconciliation": ("true" if settings.get("local_reconciliation") else "false"),
        **{name: "" if value is None else str(value) for name, value in run_values.items()},
    }


def _env_seconds(name: str, default: float) -> float:
    return float(os.getenv(name) or default)


# >>> cancels both sides when a stage stops making progress; thresholds come from the environment, 0 disables one <<< #
class StageWatchdog:
    def __init__(self, store, stop, *, interval: float = WATCHDOG_INTERVAL):
        self.store = store
        self.stop = stop
        self.interval = interval
        self.fetch_stall = _env_seconds("FETCH_STALL_SECONDS", 1800)
        self.first_result = _env_seconds("RIGHT_FIRST_RESULT_SECONDS", 14400)
        self.state_stall = _env_seconds("RIGHT_STATE_STALL_SECONDS", 7200)
        self.volume_failures = int(_env_seconds("VOLUME_READ_FAILURES", 10))
        self.done = threading.Event()
        self.left_published_at = None
        self.right_snapshot = (None, None)
        self.thread = threading.Thread(target=self._watch, name="pair-recon-watchdog", daemon=True)

    def start(self):
        self.thread.start()
        return self

    def close(self):
        self.done.set()

    def _watch(self):
        while not self.done.wait(self.interval):
            try:
                reason = self.check()
            except Exception:
                log.debug("watchdog check skipped", exc_info=True)
                continue
            if reason:
                log.error(
                    "watchdog: %s; cancelling both sides, Volume checkpoints are kept", reason
                )
                self.stop.trigger(RuntimeError(f"watchdog: {reason}"))
                return

    def check(self, now: float | None = None) -> str | None:
        now = time.monotonic() if now is None else now
        if self.fetch_stall:
            for key, (rows, at) in list(FETCH_PROGRESS.items()):
                if now - at > self.fetch_stall:
                    return (
                        f"Oracle fetch {key} has had no new rows for {now - at:.0f}s (rows={rows})"
                    )
        streak = volume_failure_streak()
        if self.volume_failures and streak >= self.volume_failures:
            return f"{streak} consecutive Volume read failures"
        if not (self.first_result or self.state_stall):
            return None
        left = get_left_state(self.store)
        right = get_right_state(self.store)
        published = int(left.get("published_units") or 0)
        completed = int(right.get("completed_units") or 0)
        if self.left_published_at is None and published:
            self.left_published_at = now
        if self.first_result and self.left_published_at is not None and not completed:
            waited = now - self.left_published_at
            if waited > self.first_result:
                return f"Right produced no result {waited:.0f}s after the Left published its first unit"
        if self.state_stall:
            snapshot = json.dumps(
                {k: v for k, v in right.items() if k not in {"updated_at", "revision"}},
                sort_keys=True,
            )
            if snapshot != self.right_snapshot[0]:
                self.right_snapshot = (snapshot, now)
            elif (
                right and published > completed and now - self.right_snapshot[1] > self.state_stall
            ):
                return (
                    f"Right state unchanged for {now - self.right_snapshot[1]:.0f}s "
                    f"(phase={right.get('phase')}, completed_units={completed}/{published} ready)"
                )
        return None


# >>> watches the Right run with no deadline; a failure fires the stop signal so local statements get cancelled <<< #
class RightJobMonitor:
    def __init__(self, remote, jobs=None, on_terminal=None, stop=None):
        self.remote = remote
        self.jobs = jobs
        self.on_terminal = on_terminal
        self.stop = stop
        self.completed = None
        self.outcome: dict = {}
        self.error: Exception | None = None
        self.final_state: str | None = None
        self.done = threading.Event()
        self.cancel_requested = threading.Event()
        self.thread = threading.Thread(
            target=self._watch,
            name="pair-recon-right-monitor",
            daemon=True,
        )

    @property
    def run_id(self):
        return getattr(self.remote, "run_id", "unknown")

    def start(self):
        self.thread.start()
        return self

    def _describe(self, completed=None) -> str:
        if self.jobs is None:
            return ""
        try:
            return describe_run(self.jobs, completed or self.jobs.get_run(self.run_id))
        except (DatabricksError, OSError) as exc:
            return f"run details unavailable ({exc})"

    def _wait(self):
        started = time.monotonic()
        while True:
            try:
                return self.remote.result(timeout=RIGHT_WAIT_CHUNK)
            except TimeoutError:
                log.info(
                    "Right Databricks job still running: run_id=%s elapsed=%.0fs",
                    self.run_id,
                    time.monotonic() - started,
                )

    def _watch(self):
        completed = None
        try:
            completed = self._wait()
            result = getattr(getattr(completed, "state", None), "result_state", None)
            result_name = getattr(result, "value", result)
            if result_name != "SUCCESS":
                self.final_state = (
                    "cancelled"
                    if str(result_name or "").upper() in {"CANCELED", "CANCELLED"}
                    else "failed"
                )
                message = getattr(getattr(completed, "state", None), "state_message", None)
                detail = f": {message}" if message else ""
                raise RuntimeError(f"Right job finished with {result_name}{detail}")
            self.completed = completed
            self.final_state = "success"
            if self.jobs is not None:
                self.outcome = run_outcome(self.jobs, completed)
            log.info("Right Databricks job completed: run_id=%s", self.run_id)
        except Exception as exc:  # noqa: BLE001
            if completed is None and self.jobs is not None:
                try:
                    completed = self.jobs.get_run(self.run_id)
                except (DatabricksError, OSError):
                    completed = None
            result = getattr(getattr(completed, "state", None), "result_state", None)
            result_name = getattr(result, "value", result)
            if self.final_state is None:
                self.final_state = (
                    "cancelled"
                    if str(result_name or "").upper() in {"CANCELED", "CANCELLED"}
                    else "failed"
                )
            detail = self._describe(completed)
            self.error = RuntimeError(f"{exc}\n{detail}") if detail else exc
            if self.stop is not None and not self.cancel_requested.is_set():
                self.stop.trigger(self.failure())
            if not self.cancel_requested.is_set():
                if self.on_terminal is not None:
                    try:
                        self.on_terminal(self.final_state)
                    except Exception:
                        log.exception(
                            "could not forward Right terminal state: run_id=%s state=%s",
                            self.run_id,
                            self.final_state,
                        )
                log.error(
                    "Right Databricks job failed: run_id=%s: %s",
                    self.run_id,
                    exc,
                )
        finally:
            self.done.set()

    def request_cancel(self):
        self.cancel_requested.set()

    def failure(self) -> RuntimeError | None:
        if self.error is None:
            return None
        return RuntimeError(f"Right Databricks job failed: run_id={self.run_id}: {self.error}")

    def raise_if_failed(self):
        failure = self.failure()
        if failure is not None:
            raise failure from self.error

    def result(self):
        self.done.wait()
        self.raise_if_failed()
        return self.completed


class StatementSql:
    def __init__(self, workspace, warehouse_id: str, reporter=None):
        self.api = workspace.statement_execution
        self.warehouse_id = warehouse_id
        self.reporter = reporter
        self.lock = threading.Lock()

    def _report(self, message: str):
        if self.reporter:
            self.reporter(message)

    def execute(self, statement: str, parameters=None):
        with self.lock:
            return self._execute(statement, parameters)

    def _execute(self, statement: str, parameters=None):
        operation = statement.lstrip().split(None, 1)[0].upper()
        started = time.monotonic()
        self._report(
            f"Databricks SQL {operation}: submitted to warehouse {self.warehouse_id}; "
            "waiting for completion (a stopped warehouse may take a few minutes to start)."
        )
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
            wait_timeout="10s",
        )
        state = getattr(getattr(response, "status", None), "state", None)
        state_name = getattr(state, "value", state)
        last_reported = None
        last_reported_at = started
        while state_name in {"PENDING", "RUNNING"}:
            now = time.monotonic()
            if state_name != last_reported or now - last_reported_at >= 15:
                self._report(
                    f"Databricks SQL {operation}: {str(state_name).lower()} "
                    f"for {int(now - started)}s..."
                )
                last_reported = state_name
                last_reported_at = now
            time.sleep(1)
            response = self.api.get_statement(response.statement_id)
            state = getattr(getattr(response, "status", None), "state", None)
            state_name = getattr(state, "value", state)
        if state_name != "SUCCEEDED":
            error = getattr(getattr(response, "status", None), "error", None)
            message = getattr(error, "message", None) or f"SQL finished with {state}"
            self._report(
                f"Databricks SQL {operation}: failed after "
                f"{int(time.monotonic() - started)}s: {message}"
            )
            raise RuntimeError(message)
        columns = (
            [
                column.name
                for column in getattr(getattr(response, "manifest", None), "schema", None).columns
            ]
            if getattr(getattr(response, "manifest", None), "schema", None)
            else []
        )
        rows = getattr(getattr(response, "result", None), "data_array", None) or []
        self._report(
            f"Databricks SQL {operation}: completed in "
            f"{int(time.monotonic() - started)}s ({len(rows)} row(s))."
        )
        return [dict(zip(columns, row)) for row in rows]


# >>> browser login unless DATABRICKS_AUTH_TYPE says otherwise; other auth types fail on this setup <<< #
def workspace_auth_type() -> str:
    return os.environ.get("DATABRICKS_AUTH_TYPE") or "external-browser"


def workspace(profile: str):
    return WorkspaceClient(profile=profile, auth_type=workspace_auth_type())


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


def resolve_job_id(client, reference: object, *, required: bool = True) -> int | None:
    value = str(reference or "").strip()
    if not value:
        if required:
            raise ValueError("Right job id or name is required")
        return None
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
        if required:
            raise ValueError(f"Right Databricks job not found by name: {value}")
        return None
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


def plan_without_windows(store, pair: str) -> bool:
    return store.exists(plan_path(pair)) and not store.get_json(plan_path(pair)).get("windows")


# >>> a Left-built plan with no windows and rows on both sides means every partition was excluded <<< #
def require_plan_windows(store, pair: str, pair_config: dict) -> None:
    if not plan_without_windows(store, pair):
        return
    excluded = store.get_json(plan_path(pair)).get("excluded_partitions") or []
    raise RuntimeError(
        f"plan for {pair} has no windows: every partition between "
        f"{pair_config.get('fromDate') or '*'} and {pair_config.get('toDate') or '*'} is "
        f"excluded or empty ({len(excluded)} excluded); fix the date mapping or sampling "
        "instead of excluding more partitions"
    )


# >>> logs how many available vintages earlier runs already checked and what this run picks <<< #
def log_vintage_coverage(store, pair: str) -> None:
    if not store.exists(plan_path(pair)):
        return
    selection = store.get_json(plan_path(pair)).get("vintage_selection") or {}
    if selection.get("mode") != "random":
        return
    checked = len(selection.get("checked_before") or [])
    available = int(selection.get("available") or 0)
    selected = selection.get("selected") or []
    log.info(
        "vintage coverage: pair=%s checked_before=%d/%d selected=%d remaining_after=%d (%s)",
        pair,
        checked,
        available,
        len(selected),
        max(0, available - checked - len(selected)),
        ", ".join(selected),
    )


# >>> vintages earlier runs finished for this pair; the plan skips them unless --recheck <<< #
def checked_vintages(catalog, settings: dict, pair: str, pair_config: dict, run_id: str):
    if catalog is None or settings.get("recheck"):
        return []
    if pair_config.get("vintage_sample_size") in (None, ""):
        return []
    if str(pair_config.get("partition") or "day") == "all":
        return []
    vintage = str(pair_config.get("vintage") or "day")
    checked = catalog.checked_windows(pair, vintage, exclude_run_id=run_id)
    log.info("vintage history: pair=%s vintage=%s checked_before=%d", pair, vintage, len(checked))
    return checked


def run_left(
    config: dict,
    store,
    run_id: str,
    pair: str,
    work_dir: str | Path,
    left_workers: int = 2,
    release=None,
    left_only: bool = False,
    abort_if=None,
    force: bool = False,
    stop=None,
):
    abort_if = abort_if or (lambda: None)
    stop = stop or StopSignal()
    abort_if()
    config = validate_config(config)
    pair_config = config["pairs"][pair]
    if left_only and pair_config.get("snapshot"):
        raise ValueError("left-only mode does not support snapshot pairs that require Right")
    reused = None if force else reuse_row_check(config, store, run_id=run_id, pair_names=[pair])
    if reused is None:
        store.delete_keys(
            [
                row_result_path(pair),
                plan_path(pair),
                mapping_path(pair),
                fixed_keys_path(pair),
            ]
        )
        run_row_check(
            config,
            store,
            run_id=run_id,
            pair_names=[pair],
            source_factory=lambda values: source_from_config(values).set_stop(stop),
            pair_workers=1,
            abort_if=abort_if,
        )
    abort_if()
    empty = plan_without_windows(store, pair)
    if not empty:
        log_vintage_coverage(store, pair)
    if release:
        release()
    abort_if()
    if empty:
        if left_only:
            require_plan_windows(store, pair, pair_config)
        result = wait_for_row_result(store, pair, abort_if=abort_if)
        state = empty_plan_state(result)
        if state is None:
            require_plan_windows(store, pair, pair_config)
        log.info(
            "nothing to compare: pair=%s state=%s summary=%s", pair, state, result.get("summary")
        )
        return {"row": state, "units": 0}
    checks = pair_config.get("checks") or []
    if not any(check != "row_count" for check in checks):
        return {"row": "ready", "units": 0}
    manifests = wait_for_manifests(
        store,
        poll_interval=2,
        pair_names=[pair],
        abort_if=abort_if,
    )
    if not left_only:
        wait_for_row_check_result(store, pair, abort_if=abort_if)
    for manifest in manifests:
        abort_if()
        publish_fixed_keys(store, manifest, work_dir, stop=stop)
    abort_if()
    runtime = LeftRuntime(work_dir, store, force=force, stop=stop)
    units = ColCheckPipeline(
        store,
        pull_window=runtime.pull_window,
        compute_stats=runtime.compute_stats,
        pull_workers=max(1, int(left_workers)),
        wait_for_right=not left_only,
        abort_if=abort_if,
    ).run(manifests)
    return {"row": "ready", "units": units}


def retry_failed(store) -> int:
    keys = [key for key, _ in list_failures(store)]
    keys += [key for key, value in list_results(store) if value.get("verdict") != "match"]
    for key, value in list_row_results(store):
        if any(row.get("result") != "match" for row in (value.get("partitions") or {}).values()):
            keys.append(key)
    deleted = store.delete_keys(sorted(set(keys)))
    active = active_unit_ids(store)
    results = [value for _key, value in list_results(store) if value.get("unit_id") in active]
    failures = [value for _key, value in list_failures(store) if value.get("unit_id") in active]
    matched = sum(value.get("verdict") == "match" for value in results)
    write_right_state(
        store,
        {
            "phase": "retry_pending",
            "plan_ids": current_plan_ids(store),
            "expected_units": len(active),
            "completed_units": len(results) + len(failures),
            "matched_units": matched,
            "different_units": len(results) - matched,
            "failed_units": len(failures),
        },
    )
    return deleted


def clear_resume_outputs(store) -> int:
    keys = store.list_keys(REPORT_PREFIX)
    if store.exists(UNITS_STATUS):
        keys.append(UNITS_STATUS)
    return store.delete_keys(sorted(set(keys)))


class StoredRightStatistics:
    def __init__(self, store):
        self.store = store

    def window_statistics(self, manifest, window, progress=None):
        return self.store.get_json(right_stats_path(manifest["pair"], window["bucket"]))[
            "statistics"
        ]


def run_local_reconciliation(store, *, right_workers: int) -> dict:
    compared = RightColCheck(
        store,
        lambda _config: StoredRightStatistics(store),
        poll_interval=0.1,
        right_workers=max(1, int(right_workers)),
    ).run()
    result = right_summary(store)
    active = active_unit_ids(store)
    result["results"] = sum(value["unit_id"] in active for _key, value in list_results(store))
    result["new_results"] = len(compared)
    publish_right_report(store)
    write_run_status(store, {"state": "complete", "role": "local", **result})
    return result


def _gzip_csv_rows(body: bytes) -> list[dict[str, str]]:
    return list(
        csv.DictReader(io.StringIO(gzip.decompress(body).decode("utf-8-sig", errors="replace")))
    )


def _difference_sample_csv(
    *,
    left_path: Path,
    right_body: bytes,
    pairs: list[dict],
    manifest: dict,
    different: set[str],
    limit: int,
    key_tuples: list[list[object]],
) -> tuple[bytes, int, int, int, list[str]]:
    key_names = list(manifest.get("key_cols") or [])
    key_specs = key_families(manifest.get("key_families"), len(key_names))
    key_pairs = [row for row in pairs if row["left_column"] in key_names]
    attributes = [row for row in pairs if row["left_column"] in different]
    date_name = (manifest.get("left") or {}).get("date_col")
    partition = next(
        (
            row
            for row in pairs
            if date_name and row["left_column"] == date_name and row not in key_pairs
        ),
        None,
    )
    required_left = [row["left_column"] for row in [*key_pairs, *attributes]]
    if partition:
        required_left.append(partition["left_column"])

    def row_key(row: dict[str, str], side: str) -> str:
        names = [pair[f"{side}_column"] for pair in key_pairs]
        return composite_key([row.get(name) for name in names], key_specs, side)

    desired = [composite_key(values, key_specs, "left") for values in key_tuples]
    desired_set = set(desired)
    left_groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    with left_path.open("r", encoding="utf-8-sig", newline="", errors="replace") as handle:
        reader = csv.DictReader(handle)
        missing = [name for name in required_left if name not in (reader.fieldnames or [])]
        if missing:
            raise ValueError(
                f"Left diagnostic columns missing from {left_path}: {', '.join(missing)}"
            )
        for row in reader:
            key = row_key(row, "left") if key_pairs else str(len(left_groups))
            if desired_set and key not in desired_set:
                continue
            left_groups[key].append(row)
            if desired_set and desired_set <= left_groups.keys():
                break
            if not desired_set and sum(map(len, left_groups.values())) >= limit:
                break

    right_groups: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in _gzip_csv_rows(right_body):
        key = row_key(row, "right") if key_pairs else str(len(right_groups))
        right_groups[key].append(row)

    ordered = list(dict.fromkeys([*desired, *left_groups, *right_groups]))[:limit]
    columns = ["sampled_key", "partition", "attribute", "side", "value"]
    output = io.StringIO(newline="")
    writer = csv.writer(output, lineterminator="\n")
    writer.writerow(columns)
    rows_written = 0
    for key in ordered:
        left_rows = left_groups.get(key) or []
        right_rows = right_groups.get(key) or []
        for index in range(max(1, len(left_rows), len(right_rows))):
            left = left_rows[index] if index < len(left_rows) else {}
            right = right_rows[index] if index < len(right_rows) else {}
            for pair in attributes:
                writer.writerow(
                    [
                        key,
                        left.get(partition["left_column"], "") if partition else "",
                        pair["left_column"],
                        "left",
                        left.get(pair["left_column"], ""),
                    ]
                )
                writer.writerow(
                    [
                        key,
                        right.get(partition["right_column"], "") if partition else "",
                        pair["left_column"],
                        "right",
                        right.get(pair["right_column"], ""),
                    ]
                )
                rows_written += 2
    return (
        gzip.compress(output.getvalue().encode("utf-8")),
        sum(map(len, left_groups.values())),
        sum(map(len, right_groups.values())),
        rows_written,
        columns,
    )


def publish_difference_samples(store, run_dir: Path, pair: str) -> int:
    results = [
        value
        for _key, value in list_results(store)
        if value.get("pair") == pair and value.get("verdict") != "match"
    ]
    by_window: dict[str, set[str]] = {}
    for result in results:
        by_window.setdefault(str(result["vintage"]), set()).add(str(result["column"]))

    plans = {value["pair"]: value for _key, value in list_plans(store)}
    manifest = plans.get(pair)
    if manifest is None:
        return 0
    scratch_manifests = [
        key for key in store.list_keys(RIGHT_SAMPLE_PREFIX) if key.endswith("/manifest.json")
    ]
    published = 0
    for key in scratch_manifests:
        document = store.get_json(key)
        if document.get("pair") != pair:
            continue
        window = str(document["vintage"])
        different = by_window.get(window, set())
        prefix = key.removesuffix("manifest.json")
        if not different:
            store.delete_keys(store.list_keys(prefix))
            continue
        left_path = run_dir / pair / window / "left.csv"
        if not left_path.is_file():
            log.warning("Left difference sample unavailable: %s", left_path)
            continue
        keys_path = difference_keys_path(pair, window)
        key_tuples = (
            store.get_json(keys_path).get("key_tuples", []) if store.exists(keys_path) else []
        )
        body, left_rows, right_rows, sample_rows, columns = _difference_sample_csv(
            left_path=left_path,
            right_body=store.get_bytes(f"{prefix}{document['right_file']}"),
            pairs=document["pairs"],
            manifest=manifest,
            different=different,
            limit=int(document["limit"]),
            key_tuples=key_tuples,
        )
        target = diagnostic_prefix(pair, window)
        store.put_bytes(f"{target}samples.csv.gz", body, content_type="application/gzip")
        store.put_json(
            f"{target}manifest.json",
            {
                "pair": pair,
                "vintage": window,
                "different_columns": sorted(different),
                "columns": columns,
                "limit": int(document["limit"]),
                "left_rows": left_rows,
                "right_rows": right_rows,
                "sample_rows": sample_rows,
                "sample_file": "samples.csv.gz",
            },
        )
        store.delete_keys(store.list_keys(prefix))
        published += 1
    store.delete_keys(store.list_keys(RIGHT_STATS_PREFIX))
    return published


def launch(settings: dict, config: dict, *, action: str, workspace_client=None, catalog=None):
    pair = next(iter(config["pairs"]))
    run_id = validate_run_id(settings["run_id"])
    run_mode = str(settings.get("run_mode") or "full")
    if run_mode not in {"full", "left-only"}:
        raise ValueError("run_mode must be full or left-only")
    left_only = run_mode == "left-only"
    run_values = status_run_values(config["pairs"][pair])
    run_dir = local_run_directory(settings["work_dir"], run_id, settings["timezone"])
    run_dir.mkdir(parents=True, exist_ok=True)
    left_log = run_dir / "left.log"
    force = bool(settings.get("force", False))
    if force:
        left_log.unlink(missing_ok=True)
    configure_left_logging(left_log, settings["timezone"])
    log.info(
        "run started: run_id=%s pair=%s mode=%s action=%s",
        run_id,
        pair,
        run_mode,
        action,
    )
    if force:
        log.info("force enabled: PairRecon caches and existing run results will not be reused")
    client = None
    store = None
    sampler = None
    params = right_job_params(settings, pair, run_id, run_values)
    remote = None
    monitor = None
    right = None
    watchdog = None
    stop = StopSignal()
    final_state = "failed"
    failure_error = None
    failure_forwarded = False
    local_right = not left_only and config["pairs"][pair]["right"].get("source") == "csv"
    remote_right = not left_only and not local_right
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
            clear_resume_outputs(store)
            retry_failed(store)
        beat(store)
        watchdog = StageWatchdog(store, stop).start()
        if catalog:
            log.info("updating run status: table=%s state=running", settings["status_table"])
            catalog.status(
                run_id=run_id,
                pair=pair,
                owner=settings["owner"],
                state="running",
                timezone=settings["timezone"],
                **run_values,
            )
        if catalog:
            sampler = ProgressSampler(
                store,
                run_id,
                pair,
                settings["timezone"],
                catalog.progress,
                interval=float(settings.get("progress_interval", 30)),
            ).start(required=True)
        target_factory = lambda values: CsvTarget(values)
        remote_right_started = False

        def start_remote_right():
            nonlocal failure_forwarded, monitor, remote, remote_right_started
            if remote_right_started:
                return
            job_reference = settings["right_job_id"]
            job_id = resolve_job_id(client, job_reference)
            log.info(
                "starting Right Databricks job: reference=%s job_id=%s",
                job_reference,
                job_id,
            )
            remote = client.jobs.run_now(job_id, notebook_params=params)
            on_right_terminal = None
            if catalog:

                def on_right_terminal(state):
                    nonlocal failure_forwarded
                    log.info(
                        "forwarding Right terminal state: table=%s state=%s",
                        settings["status_table"],
                        state,
                    )
                    catalog.fail(
                        run_id=run_id,
                        pair=pair,
                        owner=settings["owner"],
                        state=state,
                        timezone=settings["timezone"],
                        **run_values,
                        detail=str(monitor.error or f"Right Databricks job {state}"),
                    )
                    failure_forwarded = True

            monitor = RightJobMonitor(
                remote,
                jobs=client.jobs,
                on_terminal=on_right_terminal,
                stop=stop,
            )
            monitor.start()
            remote_right_started = True

        release = None
        if local_right:
            release = lambda: run_right_row_check(store, target_factory)
        elif remote_right:
            release = start_remote_right
        checked = checked_vintages(catalog, settings, pair, config["pairs"][pair], run_id)
        if checked:
            config["pairs"][pair]["checked_vintages"] = checked
        else:
            config["pairs"][pair].pop("checked_vintages", None)
        log.info("starting Left checks")
        left = run_left(
            config,
            store,
            run_id,
            pair,
            run_dir,
            int(settings.get("left_workers", 2)),
            release=release,
            left_only=left_only,
            abort_if=lambda: (monitor.raise_if_failed() if monitor else None, stop.check()),
            force=force,
            stop=stop,
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
            refresh_units_status(store)
            if catalog:
                catalog.finish(
                    store,
                    run_id=run_id,
                    pair=pair,
                    owner=settings["owner"],
                    state=final_state,
                    timezone=settings["timezone"],
                    **run_values,
                )
        elif local_right:
            right = run_right(
                None,
                target_factory,
                run_id=run_id,
                store=store,
                poll_interval=0.1,
                right_workers=1,
            )
            final_state = run_state(right)
            if catalog:
                catalog.finish(
                    store,
                    run_id=run_id,
                    pair=pair,
                    owner=settings["owner"],
                    state=final_state,
                    timezone=settings["timezone"],
                    **run_values,
                )
        else:
            monitor.result()
            if settings.get("local_reconciliation"):
                log.info("starting local reconciliation from Databricks Right statistics")
                local_outcome = run_local_reconciliation(
                    store, right_workers=int(settings.get("right_workers", 8))
                )
                right = {"run_id": int(remote.run_id), **local_outcome}
                final_state = run_state(local_outcome)
                if catalog:
                    catalog.finish(
                        store,
                        run_id=run_id,
                        pair=pair,
                        owner=settings["owner"],
                        state=final_state,
                        timezone=settings["timezone"],
                        **run_values,
                    )
            else:
                right = {
                    "run_id": int(remote.run_id),
                    **monitor.outcome,
                }
                outcome_fields = ("row_errors", "column_errors", "failures")
                if all(name in monitor.outcome for name in outcome_fields):
                    final_state = run_state(monitor.outcome)
                else:
                    published = catalog.run(run_id) if catalog else None
                    final_state = (published or {}).get("state") or "finished"
        try:
            difference_samples = publish_difference_samples(store, run_dir, pair)
        except Exception:
            log.exception("could not publish Left/Right difference samples")
            difference_samples = 0
        if sampler:
            sampler.stop(final_state, required=not remote_right, capture=not remote_right)
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
            "difference_samples": difference_samples,
            "volume_run_root": store.root_uri,
        }
    except BaseException as exc:
        failure_error = exc
        stop.trigger(exc)
        final_state = "cancelled" if monitor and monitor.final_state == "cancelled" else "failed"
        log.exception("run %s", final_state)
        if store is not None and not remote_right:
            try:
                failure_source = (
                    "right-job" if monitor and monitor.error is not None else "launcher"
                )
                failure_key = outcome_failure_path(pair, failure_source)
                if not store.exists(failure_key):
                    record_runtime_failure(store, pair, failure_source, exc)
            except Exception:
                log.exception("could not publish runtime failure artifact")
            try:
                write_run_status(
                    store,
                    {"state": final_state, "run_id": run_id, "role": "launcher"},
                    timezone_name=settings["timezone"],
                )
            except Exception:
                log.exception("could not update Volume run status: state=%s", final_state)
        if (
            remote is not None
            and client is not None
            and (monitor is None or monitor.final_state not in {"success", "cancelled"})
        ):
            try:
                if monitor:
                    monitor.request_cancel()
                client.jobs.cancel_run(int(remote.run_id))
            except Exception:
                log.exception("could not cancel Right Databricks run: run_id=%s", remote.run_id)
        raise
    finally:
        if watchdog:
            watchdog.close()
        try:
            if sampler:
                sampler.stop(final_state, capture=not remote_right)
        finally:
            if catalog and final_state in {"failed", "cancelled"}:
                try:
                    values = {
                        "run_id": run_id,
                        "pair": pair,
                        "owner": settings["owner"],
                        "state": final_state,
                        "timezone": settings["timezone"],
                        **run_values,
                    }
                    if remote_right:
                        if not failure_forwarded:
                            catalog.fail(
                                **values,
                                detail=str(
                                    failure_error
                                    or (monitor.error if monitor else None)
                                    or f"Right Databricks job {final_state}"
                                ),
                            )
                    elif store is None:
                        catalog.status(**values)
                    else:
                        catalog.finish(store, **values)
                except Exception:
                    log.exception("could not update final run status: state=%s", final_state)
