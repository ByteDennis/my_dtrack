from __future__ import annotations

import csv
import io
import re
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
AUDIT_SUMMARY = "_status/audit.sql"

LEGACY_RUN_STATUS = "_STATUS.json"
LEGACY_HEARTBEAT = "_HEARTBEAT"
LEGACY_LEFT_ROW_CHECK_CLOSED = "row_check/_LEFT_CLOSED"
LEGACY_ROW_CHECK_CLOSED = "row_check/_CLOSED"
LEGACY_COL_CHECK_CLOSED = "manifest/_CLOSED"

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


def pair_segment(pair: str) -> str:
    return quote(str(pair), safe="")


def pair_root(pair: str) -> str:
    return f"pairs/{pair_segment(pair)}"


def left_row_path(pair: str) -> str:
    return f"{pair_root(pair)}/row_check/left.json"


def row_result_path(pair: str) -> str:
    return f"{pair_root(pair)}/row_check/result.json"


def plan_path(pair: str) -> str:
    return f"{pair_root(pair)}/plan.json"


def fixed_keys_path(pair: str) -> str:
    return f"{pair_root(pair)}/fixed_keys.json"


def fixed_keys_document(store, pair: str) -> dict[str, Any] | None:
    for key in (fixed_keys_path(pair), f"manifest/{pair}.keys.json"):
        if store.exists(key):
            return store.get_json(key)
    return None


def unit_prefix(pair: str) -> str:
    return f"{pair_root(pair)}/units/"


def unit_path(pair: str, sequence: int, unit_id: str) -> str:
    return f"{unit_prefix(pair)}{int(sequence):08d}-{unit_id}.json"


def result_path(pair: str, unit_id: str) -> str:
    return f"{pair_root(pair)}/col_check/results/{unit_id}.json"


def failure_path(pair: str, unit_id: str) -> str:
    return f"{pair_root(pair)}/col_check/failures/{unit_id}.json"


def right_ready_path(pair: str, window: str) -> str:
    return f"{pair_root(pair)}/col_check/right_ready/{quote(str(window), safe='')}.json"


def audit_prefix(pair: str | None) -> str:
    return f"{pair_root(pair)}/audit/" if pair else "_status/audit/"


def audit_error_prefix(pair: str | None) -> str:
    return f"{pair_root(pair)}/audit_errors/" if pair else "_status/audit_errors/"


def _list_pair_json(store, predicate) -> list[tuple[str, dict[str, Any]]]:
    return [
        (key, store.get_json(key))
        for key in store.list_keys("pairs/")
        if key.endswith(".json") and predicate(key)
    ]


def _list_v2(store, suffix: str) -> list[tuple[str, dict[str, Any]]]:
    return _list_pair_json(store, lambda key: key.endswith(suffix))


def _dedupe(
    legacy: list[tuple[str, dict[str, Any]]],
    current: list[tuple[str, dict[str, Any]]],
    identity: str,
) -> list[tuple[str, dict[str, Any]]]:
    rows: dict[str, tuple[str, dict[str, Any]]] = {}
    for key, value in [*legacy, *current]:
        name = value.get(identity)
        if isinstance(name, str) and name:
            rows[name] = (key, value)
    return sorted(rows.values(), key=lambda row: row[0])


def list_left_documents(store) -> list[tuple[str, dict[str, Any]]]:
    legacy = [
        (key, value) for key, value in store.list_json("row_check/") if key.endswith(".left.json")
    ]
    return _dedupe(legacy, _list_v2(store, "/row_check/left.json"), "pair")


def list_row_results(store) -> list[tuple[str, dict[str, Any]]]:
    legacy = [
        (key, value)
        for key, value in store.list_json("row_check/")
        if key.endswith(".json")
        and not key.endswith(".left.json")
        and not key.startswith("row_check/_")
    ]
    return _dedupe(legacy, _list_v2(store, "/row_check/result.json"), "pair")


def list_plans(store) -> list[tuple[str, dict[str, Any]]]:
    legacy = [
        (key, value)
        for key, value in store.list_json("manifest/")
        if key.endswith(".json")
        and key != LEGACY_COL_CHECK_CLOSED
        and not key.endswith(".keys.json")
    ]
    return _dedupe(legacy, _list_v2(store, "/plan.json"), "pair")


def list_units(store, pairs: list[str] | None = None) -> list[tuple[str, dict[str, Any]]]:
    wanted = None if pairs is None else set(pairs)
    legacy = [
        (key, value)
        for key, value in store.list_json("_INDEX/")
        if wanted is None or value.get("pair") in wanted
    ]
    if wanted is None:
        current = _list_pair_json(store, lambda key: "/units/" in key)
    else:
        current = [row for pair in sorted(wanted) for row in store.list_json(unit_prefix(pair))]
    return _dedupe(legacy, current, "unit_id")


def list_results(store) -> list[tuple[str, dict[str, Any]]]:
    legacy = [(key, value) for key, value in store.list_json("results/")]
    current = _list_pair_json(store, lambda key: "/col_check/results/" in key)
    return _dedupe(legacy, current, "unit_id")


def list_failures(store) -> list[tuple[str, dict[str, Any]]]:
    legacy = [(key, value) for key, value in store.list_json("dead/")]
    current = _list_pair_json(store, lambda key: "/col_check/failures/" in key)
    return _dedupe(legacy, current, "unit_id")


def list_right_ready(store) -> list[tuple[str, dict[str, Any]]]:
    return _list_pair_json(store, lambda key: "/col_check/right_ready/" in key)


def unit_statistics(store, unit: dict[str, Any]) -> dict[str, Any]:
    if isinstance(unit.get("left_statistics"), dict):
        return unit["left_statistics"]
    return store.get_json(unit["payload_key"])


def get_run_status(store) -> dict[str, Any]:
    exists = getattr(store, "exists", None)
    if exists is None:
        return {}
    for key in (RUN_STATUS, LEGACY_RUN_STATUS):
        if exists(key):
            return store.get_json(key)
    return {}


def row_check_is_closed(store) -> bool:
    return store.exists(ROW_CHECK_CLOSED) or store.exists(LEGACY_ROW_CHECK_CLOSED)


def left_row_check_close_document(store) -> dict[str, Any] | None:
    for key in (LEFT_ROW_CHECK_CLOSED, LEGACY_LEFT_ROW_CHECK_CLOSED):
        if store.exists(key):
            return store.get_json(key)
    return None


def col_check_close_document(store) -> dict[str, Any] | None:
    for key in (COL_CHECK_CLOSED, LEGACY_COL_CHECK_CLOSED):
        if store.exists(key):
            return store.get_json(key)
    return None


def _offset_name(value: datetime) -> str:
    offset = value.utcoffset() or timedelta(0)
    total = int(offset.total_seconds())
    sign = "+" if total >= 0 else "-"
    total = abs(total)
    hours, remainder = divmod(total, 3600)
    minutes = remainder // 60
    return f"{sign}{hours:02d}:{minutes:02d}"


def local_timezone_name() -> str:
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
    return datetime.now(timezone_from_name(timezone_name)).isoformat(timespec="microseconds")


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


def refresh_units_status(store) -> str:
    from .manifest import unit_id

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
            for column in plan.get("columns") or []:
                uid = unit_id(pair, bucket, column)
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
