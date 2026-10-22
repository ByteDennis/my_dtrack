from __future__ import annotations

import csv
import hashlib
import io
import os
import re
from contextlib import nullcontext
from datetime import UTC, datetime, timedelta, timezone
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .. import PROTOCOL_VERSION

RUN_STATUS = "_status/run.json"
HEARTBEAT = "_status/heartbeat.json"
UNITS_STATUS = "_status/units.csv"
LEFT_ROW_CHECK_CLOSED = "_status/left_row_check.closed.json"
ROW_CHECK_CLOSED = "_status/row_check.closed.json"
COL_CHECK_CLOSED = "_status/col_check.closed.json"
AUDIT_SUMMARY = "_status/audit.txt"
LEFT_STATE = "_state/left.json"
RIGHT_STATE = "_state/right.json"

UNIT_STATUS_FIELDS = (
    "pair",
    "stage",
    "window",
    "column",
    "unit_id",
    "state",
    "sequence",
    "left_ready_at",
    "right_ready_at",
    "completed_at",
    "verdict",
    "error",
)
UNIT_BATCH_FORMAT = "pair-recon-unit-batch-v1"
RESULT_BATCH_FORMAT = "pair-recon-result-batch-v1"
RESULT_BATCH_PREFIX = "col_check/result_batches/"


def left_row_path(_pair: str) -> str:
    return "row_check/left.json"


def row_result_path(_pair: str) -> str:
    return "row_check/result.json"


def plan_path(_pair: str) -> str:
    return "plan.json"


def mapping_path(_pair: str) -> str:
    return "mapping.csv"


def artifact_location(store, relative: str) -> str:
    root = str(getattr(store, "root_uri", "")).rstrip("/\\")
    return f"{root}/{relative}" if root else relative


def fixed_keys_path(_pair: str) -> str:
    return "fixed_keys.json"


def fixed_keys_document(store, pair: str) -> dict[str, Any] | None:
    key = fixed_keys_path(pair)
    return store.get_json(key) if store.exists(key) else None


def unit_prefix(_pair: str) -> str:
    return "units/"


def unit_batch_path(sequence: int, count: int) -> str:
    if int(sequence) < 1 or int(count) < 1:
        raise ValueError("unit batch sequence and count must be positive")
    return f"{unit_prefix('')}{int(sequence):08d}-{int(count):04d}.json"


def unit_batch_units(document: dict[str, Any]) -> list[dict[str, Any]]:
    if document.get("format") != UNIT_BATCH_FORMAT:
        raise ValueError("unsupported unit batch format")
    units = document.get("units")
    if (
        not isinstance(units, list)
        or not units
        or not all(isinstance(unit, dict) for unit in units)
    ):
        raise ValueError("unit batch must contain one or more unit objects")
    return units


def unit_batch_document(
    sequence: int,
    units: list[dict[str, Any]],
    *,
    left_ready_at: str,
) -> dict[str, Any]:
    if not units:
        raise ValueError("unit batch must contain one or more units")
    pairs = {unit.get("pair") for unit in units}
    windows = {unit.get("window") for unit in units}
    if len(pairs) != 1 or None in pairs or len(windows) != 1 or None in windows:
        raise ValueError("unit batch must belong to one pair and window")
    rows = [
        {
            **unit,
            "sequence": int(sequence) + offset,
            "left_ready_at": left_ready_at,
        }
        for offset, unit in enumerate(units)
    ]
    return {
        "format": UNIT_BATCH_FORMAT,
        "pair": rows[0]["pair"],
        "window": rows[0]["window"],
        "sequence_start": rows[0]["sequence"],
        "sequence_end": rows[-1]["sequence"],
        "units": rows,
    }


def result_batch_path(results: list[dict[str, Any]]) -> str:
    unit_ids = sorted(str(result.get("unit_id") or "") for result in results)
    if not unit_ids or any(not unit_id for unit_id in unit_ids):
        raise ValueError("result batch unit ids must be non-empty")
    digest = hashlib.sha256("\n".join(unit_ids).encode()).hexdigest()[:20]
    return f"{RESULT_BATCH_PREFIX}{digest}.json"


def result_batch_results(document: dict[str, Any]) -> list[dict[str, Any]]:
    if document.get("format") != RESULT_BATCH_FORMAT:
        raise ValueError("unsupported result batch format")
    results = document.get("results")
    if (
        not isinstance(results, list)
        or not results
        or not all(isinstance(result, dict) for result in results)
    ):
        raise ValueError("result batch must contain one or more result objects")
    if any(result.get("verdict") != "match" for result in results):
        raise ValueError("result batches may contain only matching results")
    unit_ids = [result.get("unit_id") for result in results]
    if any(not unit_id for unit_id in unit_ids) or len(set(unit_ids)) != len(unit_ids):
        raise ValueError("result batch unit ids must be non-empty and unique")
    return results


def result_batch_document(
    results: list[dict[str, Any]],
    *,
    committed_at: str,
) -> dict[str, Any]:
    if not results:
        raise ValueError("result batch must contain one or more results")
    rows = [
        {**result, "completed_at": result.get("completed_at") or committed_at} for result in results
    ]
    document = {
        "format": RESULT_BATCH_FORMAT,
        "protocol_version": PROTOCOL_VERSION,
        "batch_id": result_batch_path(rows).rsplit("/", 1)[-1].removesuffix(".json"),
        "committed_at": committed_at,
        "results": rows,
    }
    result_batch_results(document)
    return document


def result_path(_pair: str, unit_id: str) -> str:
    return f"col_check/results/{unit_id}.json"


def failure_path(_pair: str, unit_id: str) -> str:
    return f"col_check/failures/{unit_id}.json"


def right_ready_path(_pair: str, window: str) -> str:
    return f"col_check/right_ready/{quote(str(window), safe='')}.json"


def audit_prefix(pair: str | None) -> str:
    return "audit/" if pair else "_status/audit/"


def audit_error_prefix(pair: str | None) -> str:
    return "audit_errors/" if pair else "_status/audit_errors/"


def _list_json(store, prefix: str) -> list[tuple[str, dict[str, Any]]]:
    return [(key, store.get_json(key)) for key in store.list_keys(prefix) if key.endswith(".json")]


def _single_json(store, path: str) -> list[tuple[str, dict[str, Any]]]:
    return [(path, store.get_json(path))] if store.exists(path) else []


def list_left_documents(store) -> list[tuple[str, dict[str, Any]]]:
    return _single_json(store, left_row_path(""))


def list_row_results(store) -> list[tuple[str, dict[str, Any]]]:
    return _single_json(store, row_result_path(""))


def list_plans(store) -> list[tuple[str, dict[str, Any]]]:
    return _single_json(store, plan_path(""))


def list_units(store, pairs: list[str] | None = None) -> list[tuple[str, dict[str, Any]]]:
    if pairs is not None and not pairs:
        return []
    selected = set(pairs or [])
    return [
        (f"{key}#{int(unit['sequence']):08d}", unit)
        for key, document in _list_json(store, unit_prefix(""))
        for unit in unit_batch_units(document)
        if not selected or unit.get("pair") in selected
    ]


def list_result_batches(store) -> list[tuple[str, dict[str, Any]]]:
    return _list_json(store, RESULT_BATCH_PREFIX)


def _outcome_records(store) -> list[tuple[str, str, dict[str, Any]]]:
    records: dict[str, tuple[str, str, dict[str, Any]]] = {}

    def add(kind: str, key: str, value: dict[str, Any]) -> None:
        unit_id = value.get("unit_id") or key.rsplit("/", 1)[-1].removesuffix(".json")
        row = {"unit_id": unit_id, **value}
        existing = records.get(unit_id)
        if existing is not None:
            previous_kind, previous_key, previous = existing
            comparable = lambda item: {
                name: field
                for name, field in item.items()
                if name not in {"completed_at", "failed_at"}
            }
            if previous_kind != kind or comparable(previous) != comparable(row):
                raise RuntimeError(
                    f"conflicting outcomes for unit {unit_id}: {previous_key} and {key}"
                )
            return
        records[unit_id] = (kind, key, row)

    for kind, prefix in (("result", "col_check/results/"), ("failure", "col_check/failures/")):
        for key, value in _list_json(store, prefix):
            add(kind, key, value)
    for key, document in list_result_batches(store):
        for value in result_batch_results(document):
            add("result", key, value)
    return [records[unit_id] for unit_id in sorted(records)]


def list_results(store) -> list[tuple[str, dict[str, Any]]]:
    return [(key, value) for kind, key, value in _outcome_records(store) if kind == "result"]


def list_failures(store) -> list[tuple[str, dict[str, Any]]]:
    return [(key, value) for kind, key, value in _outcome_records(store) if kind == "failure"]


def list_right_ready(store) -> list[tuple[str, dict[str, Any]]]:
    return _list_json(store, "col_check/right_ready/")


def active_unit_ids(store) -> set[str]:
    from .manifest import planned_unit_ids

    return set().union(*(planned_unit_ids(plan) for _key, plan in list_plans(store)))


def current_plan_ids(store) -> dict[str, str | None]:
    return {plan["pair"]: plan.get("plan_id") for _key, plan in list_plans(store)}


def unit_statistics(store, unit: dict[str, Any]) -> dict[str, Any]:
    return unit["left_statistics"]


def get_run_status(store) -> dict[str, Any]:
    exists = getattr(store, "exists", None)
    return store.get_json(RUN_STATUS) if exists is not None and exists(RUN_STATUS) else {}


def _get_role_state(store, path: str) -> dict[str, Any]:
    return store.get_json(path) if store.exists(path) else {}


def get_left_state(store) -> dict[str, Any]:
    return _get_role_state(store, LEFT_STATE)


def get_right_state(store) -> dict[str, Any]:
    return _get_role_state(store, RIGHT_STATE)


def row_check_is_closed(store) -> bool:
    state = get_right_state(store)
    if "row_check_closed_at" in state:
        return bool(state.get("row_check_closed_at"))
    return store.exists(ROW_CHECK_CLOSED)


def left_row_check_close_document(store) -> dict[str, Any] | None:
    state = get_left_state(store)
    if "row_check_closed_at" in state:
        return state if state.get("row_check_closed_at") else None
    return store.get_json(LEFT_ROW_CHECK_CLOSED) if store.exists(LEFT_ROW_CHECK_CLOSED) else None


def col_check_close_document(store) -> dict[str, Any] | None:
    state = get_left_state(store)
    if "col_check_closed_at" in state:
        if not state.get("col_check_closed_at"):
            return None
        return {
            "pairs": state.get("pairs") or [],
            "expected_units": int(state.get("expected_units") or 0),
            "plan_ids": state.get("plan_ids") or {},
            "closed_at": state["col_check_closed_at"],
        }
    return store.get_json(COL_CHECK_CLOSED) if store.exists(COL_CHECK_CLOSED) else None


def _offset_name(value: datetime) -> str:
    offset = value.utcoffset() or timedelta(0)
    total = int(offset.total_seconds())
    sign = "+" if total >= 0 else "-"
    total = abs(total)
    hours, remainder = divmod(total, 3600)
    minutes = remainder // 60
    return f"{sign}{hours:02d}:{minutes:02d}"


def local_timezone_name() -> str:
    configured = os.environ.get("PAIR_RECON_TIMEZONE", "").strip()
    if configured:
        return configured
    value = datetime.now().astimezone()
    return str(getattr(value.tzinfo, "key", "") or _offset_name(value))


def timezone_from_name(name: str | None):
    name = str(name or local_timezone_name())
    match = re.fullmatch(r"([+-])(\d{2}):(\d{2})", name)
    if match:
        delta = timedelta(hours=int(match.group(2)), minutes=int(match.group(3)))
        return timezone(delta if match.group(1) == "+" else -delta)
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError:
        return UTC


def timestamp(store=None, *, timezone_name: str | None = None) -> str:
    if timezone_name is None and store is not None:
        timezone_name = get_run_status(store).get("timezone")
    return datetime.now(timezone_from_name(timezone_name)).isoformat(timespec="seconds")


def write_run_status(
    store,
    document: dict[str, Any],
    *,
    timezone_name: str | None = None,
) -> str:
    previous = get_run_status(store)
    zone = timezone_name or previous.get("timezone") or local_timezone_name()
    value = {
        **previous,
        **document,
        "protocol_version": PROTOCOL_VERSION,
        "timezone": zone,
        "updated_at": timestamp(timezone_name=zone),
    }
    return store.put_json(RUN_STATUS, value)


def _write_role_state(
    store,
    path: str,
    role: str,
    document: dict[str, Any],
    *,
    timezone_name: str | None = None,
) -> str:
    lock = getattr(store, "_state_lock", None)
    with lock if lock is not None else nullcontext():
        previous = _get_role_state(store, path)
        zone = timezone_name or get_run_status(store).get("timezone") or local_timezone_name()
        value = {
            **previous,
            **document,
            "role": role,
            "protocol_version": PROTOCOL_VERSION,
            "revision": int(previous.get("revision") or 0) + 1,
            "updated_at": timestamp(timezone_name=zone),
        }
        return store.put_json(path, value)


def write_left_state(store, document: dict[str, Any], *, timezone_name: str | None = None) -> str:
    return _write_role_state(store, LEFT_STATE, "left", document, timezone_name=timezone_name)


def write_right_state(store, document: dict[str, Any], *, timezone_name: str | None = None) -> str:
    return _write_role_state(store, RIGHT_STATE, "right", document, timezone_name=timezone_name)


def refresh_units_status(store) -> str:
    from .manifest import unit_id_for

    units = {value["unit_id"]: value for _key, value in list_units(store)}
    results = {value["unit_id"]: value for _key, value in list_results(store)}
    failures = {value["unit_id"]: value for _key, value in list_failures(store)}
    right_ready = {
        (value.get("pair"), value.get("window")): value for _key, value in list_right_ready(store)
    }
    rows = []
    for _key, plan in list_plans(store):
        pair = plan["pair"]
        for window in plan.get("windows") or []:
            bucket = window["bucket"]
            ready = right_ready.get((pair, bucket), {})
            if int(plan.get("version") or 1) == 2:
                from .manifest import window_id

                if ready.get("window_id") != window_id(plan, window):
                    ready = {}
            for column in plan.get("columns") or []:
                uid = unit_id_for(plan, window, column)
                unit = units.get(uid, {})
                result = results.get(uid, {})
                failure = failures.get(uid, {})
                if failure:
                    state = "failed"
                elif result:
                    state = "complete"
                elif ready:
                    state = "right_ready"
                elif unit:
                    state = "left_ready"
                else:
                    state = "waiting_left"
                rows.append(
                    {
                        "pair": pair,
                        "stage": "col_check",
                        "window": bucket,
                        "column": column,
                        "unit_id": uid,
                        "state": state,
                        "sequence": unit.get("sequence", ""),
                        "left_ready_at": unit.get("left_ready_at", ""),
                        "right_ready_at": ready.get("right_ready_at", ""),
                        "completed_at": result.get("completed_at") or failure.get("failed_at", ""),
                        "verdict": result.get("verdict", ""),
                        "error": failure.get("error", ""),
                    }
                )
    output = io.StringIO(newline="")
    writer = csv.DictWriter(output, fieldnames=UNIT_STATUS_FIELDS)
    writer.writeheader()
    writer.writerows(sorted(rows, key=lambda row: (row["pair"], row["window"], row["column"])))
    return store.put_bytes(
        UNITS_STATUS,
        output.getvalue().encode("utf-8"),
        content_type="text/csv; charset=utf-8",
    )
