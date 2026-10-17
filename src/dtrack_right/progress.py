from __future__ import annotations

import csv
import hashlib
import io
from datetime import UTC, datetime
from typing import Any

from .engine.backends.store import open_store
from .engine.common.model.protocol import (
    COL_CHECK_CLOSED,
    HEARTBEAT,
    LEFT_ROW_CHECK_CLOSED,
    ROW_CHECK_CLOSED,
    UNITS_STATUS,
    active_unit_ids,
    get_run_status,
    list_failures,
    list_left_documents,
    list_plans,
    list_results,
    list_row_results,
    list_units,
)

SNAPSHOT_COLUMNS = (
    "snapshot_ts",
    "run_id",
    "pair_name",
    "run_root",
    "run_mode",
    "run_state",
    "row_left_done_at",
    "row_done_at",
    "row_days_total",
    "row_days_match",
    "row_days_count_diff",
    "row_days_left_only",
    "row_days_right_only",
    "gate_policy",
    "gate_passed",
    "gate_excluded_ratio",
    "units_expected",
    "units_published",
    "units_compared",
    "units_dead",
    "units_diff",
    "first_unit_at",
    "last_result_at",
    "heartbeat_at",
    "report_published",
    "collector_error",
)
BLOCKER_COLUMNS = (
    "blocker_id",
    "pair_name",
    "run_id",
    "stage",
    "category",
    "severity",
    "title",
    "detail",
    "source",
    "owner",
    "opened_at",
    "resolved_at",
    "resolution",
    "updated_at",
)
STALE_HEARTBEAT_MIN = 30
NO_PROGRESS_HOURS = 2
DEAD_P1_RATIO = 0.05


def _iso(value: float | str | None) -> str | None:
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), UTC).isoformat(timespec="seconds")
    return str(value)


def _document(store, key: str) -> dict[str, Any]:
    return store.get_json(key) if store.exists(key) else {}


# >>> one row per unit from _status/units.csv when the Right side refreshed it, else from the unit/result/failure documents <<< #
def unit_rows(store) -> list[dict[str, str]]:
    if store.exists(UNITS_STATUS):
        text = store.get_bytes(UNITS_STATUS).decode("utf-8")
        return list(csv.DictReader(io.StringIO(text)))
    results = {value["unit_id"]: value for _key, value in list_results(store)}
    failures = {value["unit_id"]: value for _key, value in list_failures(store)}
    active = active_unit_ids(store)
    version2_pairs = {
        plan["pair"] for _key, plan in list_plans(store) if int(plan.get("version") or 1) == 2
    }
    rows = []
    for _key, unit in list_units(store):
        uid = unit["unit_id"]
        if unit.get("pair") in version2_pairs and uid not in active:
            continue
        result, failure = results.get(uid, {}), failures.get(uid, {})
        rows.append(
            {
                "pair": unit.get("pair", ""),
                "unit_id": uid,
                "state": "failed" if failure else "complete" if result else "left_ready",
                "left_ready_at": unit.get("left_ready_at", ""),
                "completed_at": result.get("completed_at") or failure.get("failed_at", ""),
                "verdict": result.get("verdict", ""),
            }
        )
    return rows


# >>> one snapshot row per pair of one run; every read is a document the pipeline already writes <<< #
def snapshot(
    store, run_id: str, *, run_root: str = "", now: datetime | None = None
) -> list[dict[str, Any]]:
    stamp = (now or datetime.now(UTC)).isoformat(timespec="seconds")
    status = get_run_status(store)
    heartbeat = _document(store, HEARTBEAT)
    left_closed = _document(store, LEFT_ROW_CHECK_CLOSED)
    row_closed = _document(store, ROW_CHECK_CLOSED)
    col_closed = _document(store, COL_CHECK_CLOSED)
    base = {
        "snapshot_ts": stamp,
        "run_id": run_id,
        "run_root": run_root or getattr(store, "root_uri", ""),
        "run_mode": "local" if status.get("role") == "local" else "distributed",
        "run_state": status.get("state"),
        "heartbeat_at": _iso(heartbeat.get("epoch_s")),
        "report_published": store.exists("report/index.html"),
        "collector_error": None,
    }
    try:
        lefts = {value["pair"]: value for _key, value in list_left_documents(store)}
        results = {value["pair"]: value for _key, value in list_row_results(store)}
        plans = {value["pair"]: value for _key, value in list_plans(store)}
        units = unit_rows(store)
    except Exception as exc:
        return [{**_empty(), **base, "pair_name": "*", "collector_error": str(exc)[:1000]}]
    rows = []
    for pair in sorted(set(lefts) | set(results) | set(plans)):
        row = {**_empty(), **base, "pair_name": pair}
        if pair in lefts:
            row["row_left_done_at"] = left_closed.get("closed_at")
        if pair in results:
            summary = results[pair].get("summary") or {}
            gate = results[pair].get("gate_decision") or {}
            row.update(
                row_done_at=row_closed.get("closed_at") or col_closed.get("closed_at"),
                row_days_total=sum(int(summary.get(k) or 0) for k in summary),
                row_days_match=int(summary.get("match") or 0),
                row_days_count_diff=int(summary.get("count_diff") or 0),
                row_days_left_only=int(summary.get("left_only") or 0),
                row_days_right_only=int(summary.get("right_only") or 0),
                gate_policy=gate.get("policy"),
                gate_passed=gate.get("passed"),
                gate_excluded_ratio=gate.get("excluded_ratio"),
            )
        if pair in plans:
            row["units_expected"] = int(plans[pair].get("expected_units") or 0)
        mine = [unit for unit in units if unit.get("pair") == pair]
        published = [u for u in mine if u.get("left_ready_at")]
        compared = [u for u in mine if u.get("state") == "complete"]
        dead = [u for u in mine if u.get("state") == "failed"]
        row.update(
            units_published=len(published),
            units_compared=len(compared),
            units_dead=len(dead),
            units_diff=sum(1 for u in compared if u.get("verdict") and u["verdict"] != "match"),
            first_unit_at=min((u["left_ready_at"] for u in published), default=None),
            last_result_at=max(
                (u["completed_at"] for u in compared + dead if u.get("completed_at")), default=None
            ),
        )
        rows.append(row)
    return rows


def _empty() -> dict[str, Any]:
    return dict.fromkeys(SNAPSHOT_COLUMNS)


# >>> every run below each root that is not complete for longer than `keep_complete_days`; one bad run never stops the sweep <<< #
def collect(
    roots: list[str], *, now: datetime | None = None, keep_complete_days: float = 1
) -> list[dict[str, Any]]:
    now = now or datetime.now(UTC)
    rows: list[dict[str, Any]] = []
    for root in roots:
        parent = open_store(root)
        for name in parent.list_prefixes():
            child = parent.child(name)
            try:
                status = get_run_status(child)
                finished = status.get("state") == "complete" and status.get("updated_at")
                if finished:
                    age = now - datetime.fromisoformat(str(status["updated_at"]))
                    if age.total_seconds() > keep_complete_days * 86400:
                        continue
                rows.extend(snapshot(child, name, run_root=f"{root.rstrip('/')}/{name}", now=now))
            except Exception as exc:
                rows.append(
                    {
                        **_empty(),
                        "snapshot_ts": now.isoformat(timespec="seconds"),
                        "run_id": name,
                        "pair_name": "*",
                        "run_root": f"{root.rstrip('/')}/{name}",
                        "collector_error": str(exc)[:1000],
                    }
                )
    return rows


def _blocker_id(pair: str, run_id: str, rule: str) -> str:
    return hashlib.sha1(f"{pair}|{run_id}|{rule}".encode()).hexdigest()


# >>> the AUTO rules of the design: open when the condition holds, resolved otherwise; ids are deterministic <<< #
def auto_blockers(
    rows: list[dict[str, Any]],
    *,
    now: datetime | None = None,
    last_progress_at: dict[tuple[str, str], str] | None = None,
) -> list[dict[str, Any]]:
    now = now or datetime.now(UTC)
    stamp = now.isoformat(timespec="seconds")
    progress = last_progress_at or {}
    out = []

    def emit(row, rule, condition, severity, stage, title, detail=""):
        out.append(
            {
                "blocker_id": _blocker_id(row["pair_name"], row["run_id"], rule),
                "pair_name": row["pair_name"],
                "run_id": row["run_id"],
                "stage": stage,
                "category": rule,
                "severity": severity,
                "title": title,
                "detail": detail,
                "source": "AUTO",
                "owner": None,
                "opened_at": stamp,
                "resolved_at": None if condition else stamp,
                "resolution": None if condition else "condition cleared",
                "updated_at": stamp,
            }
        )

    for row in rows:
        complete = row.get("run_state") == "complete"
        in_col = bool(row.get("units_expected")) and not complete
        stage = "ROW_CHECK" if row.get("row_done_at") is None else "COL_CHECK"
        emit(
            row,
            "GATE_FAILED",
            row.get("gate_passed") is False,
            "P1",
            "ROW_CHECK",
            "row-count gate failed",
            f"policy={row.get('gate_policy')} excluded={row.get('gate_excluded_ratio')}",
        )
        mismatched = (
            (row.get("row_days_count_diff") or 0)
            + (row.get("row_days_left_only") or 0)
            + (row.get("row_days_right_only") or 0)
        )
        emit(
            row,
            "ROW_COUNT_DIFF",
            row.get("gate_passed") is True and mismatched > 0,
            "P3",
            "ROW_CHECK",
            f"{mismatched} partition(s) differ in row count",
        )
        dead, expected = row.get("units_dead") or 0, row.get("units_expected") or 0
        emit(
            row,
            "DEAD_UNITS",
            dead > 0,
            "P1" if expected and dead / expected > DEAD_P1_RATIO else "P2",
            "COL_CHECK",
            f"{dead} dead unit(s)",
        )
        heartbeat = row.get("heartbeat_at")
        stale = False
        if heartbeat and not complete:
            stale = (
                now - datetime.fromisoformat(heartbeat).astimezone(UTC)
            ).total_seconds() > STALE_HEARTBEAT_MIN * 60
        emit(
            row,
            "STALE_HEARTBEAT",
            stale,
            "P2",
            stage,
            f"no Left heartbeat for over {STALE_HEARTBEAT_MIN} minutes",
        )
        last = progress.get((row["run_id"], row["pair_name"]))
        stuck = False
        if in_col and last:
            stuck = (
                now - datetime.fromisoformat(last).astimezone(UTC)
            ).total_seconds() > NO_PROGRESS_HOURS * 3600
        emit(
            row,
            "NO_PROGRESS",
            stuck,
            "P2",
            "COL_CHECK",
            f"no unit compared for over {NO_PROGRESS_HOURS} hours",
        )
        emit(
            row,
            "COLLECTOR_ERROR",
            bool(row.get("collector_error")),
            "P3",
            "OTHER",
            "collector could not read this run",
            row.get("collector_error") or "",
        )
    return out


def _literal(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("\\", "\\\\").replace("'", "\\'") + "'"


def _values(rows: list[dict[str, Any]], columns: tuple[str, ...], casts: dict[str, str]) -> str:
    def cell(column, value):
        text = _literal(value)
        return f"CAST({text} AS {casts[column]})" if column in casts and value is not None else text

    return ",\n".join("(" + ", ".join(cell(c, row.get(c)) for c in columns) + ")" for row in rows)


SNAPSHOT_CASTS = {
    "snapshot_ts": "TIMESTAMP",
    "row_left_done_at": "TIMESTAMP",
    "row_done_at": "TIMESTAMP",
    "first_unit_at": "TIMESTAMP",
    "last_result_at": "TIMESTAMP",
    "heartbeat_at": "TIMESTAMP",
}
BLOCKER_CASTS = {"opened_at": "TIMESTAMP", "resolved_at": "TIMESTAMP", "updated_at": "TIMESTAMP"}


# >>> one INSERT for a collection batch <<< #
def insert_snapshot_sql(schema: str, rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    return (
        f"INSERT INTO {schema}.dtrack_progress_snapshot ({', '.join(SNAPSHOT_COLUMNS)}) VALUES\n"
        + _values(rows, SNAPSHOT_COLUMNS, SNAPSHOT_CASTS)
    )


# >>> MERGE that never touches MANUAL rows: AUTO rows open, close, or appear <<< #
def merge_blockers_sql(schema: str, rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    columns = ", ".join(BLOCKER_COLUMNS)
    return f"""MERGE INTO {schema}.dtrack_pair_blocker AS t
USING (SELECT * FROM VALUES
{_values(rows, BLOCKER_COLUMNS, BLOCKER_CASTS)}
AS s({columns})) AS s
ON t.blocker_id = s.blocker_id AND t.source = 'AUTO'
WHEN MATCHED AND t.resolved_at IS NULL AND s.resolved_at IS NOT NULL THEN
  UPDATE SET resolved_at = s.resolved_at, resolution = s.resolution, updated_at = s.updated_at
WHEN MATCHED AND t.resolved_at IS NOT NULL AND s.resolved_at IS NULL THEN
  UPDATE SET resolved_at = NULL, resolution = NULL, opened_at = s.opened_at, severity = s.severity,
             title = s.title, detail = s.detail, updated_at = s.updated_at
WHEN MATCHED AND t.resolved_at IS NULL AND s.resolved_at IS NULL THEN
  UPDATE SET severity = s.severity, title = s.title, detail = s.detail, updated_at = s.updated_at
WHEN NOT MATCHED AND s.resolved_at IS NULL THEN INSERT ({columns}) VALUES ({", ".join("s." + c for c in BLOCKER_COLUMNS)})"""


# >>> hours since units_compared last grew, per (run, pair), from the snapshot history <<< #
def last_progress_sql(schema: str) -> str:
    return f"""WITH h AS (
  SELECT run_id, pair_name, snapshot_ts, units_compared + units_dead AS done,
         lag(units_compared + units_dead) OVER (PARTITION BY run_id, pair_name ORDER BY snapshot_ts) AS prev
  FROM {schema}.dtrack_progress_snapshot)
SELECT run_id, pair_name, max(snapshot_ts) AS last_progress_at FROM h
WHERE prev IS NULL OR done > prev GROUP BY run_id, pair_name"""


# >>> tables: plan (manual), snapshot (append-only), blocker (manual + auto) <<< #
def ddl(schema: str) -> list[str]:
    return [
        f"""CREATE TABLE IF NOT EXISTS {schema}.dtrack_pair_plan (
  pair_name STRING NOT NULL, display_name STRING, wave STRING, priority INT, owner STRING,
  planned_start_date DATE, target_done_date DATE, active BOOLEAN NOT NULL, notes STRING,
  updated_by STRING, updated_at TIMESTAMP)
TBLPROPERTIES (delta.enableChangeDataFeed = true)""",
        f"""CREATE TABLE IF NOT EXISTS {schema}.dtrack_progress_snapshot (
  snapshot_ts TIMESTAMP NOT NULL, run_id STRING NOT NULL, pair_name STRING NOT NULL,
  run_root STRING, run_mode STRING, run_state STRING,
  row_left_done_at TIMESTAMP, row_done_at TIMESTAMP, row_days_total INT, row_days_match INT,
  row_days_count_diff INT, row_days_left_only INT, row_days_right_only INT,
  gate_policy STRING, gate_passed BOOLEAN, gate_excluded_ratio DOUBLE,
  units_expected INT, units_published INT, units_compared INT, units_dead INT, units_diff INT,
  first_unit_at TIMESTAMP, last_result_at TIMESTAMP, heartbeat_at TIMESTAMP,
  report_published BOOLEAN, collector_error STRING)
CLUSTER BY (pair_name, snapshot_ts)""",
        f"""CREATE TABLE IF NOT EXISTS {schema}.dtrack_pair_blocker (
  blocker_id STRING NOT NULL, pair_name STRING NOT NULL, run_id STRING, stage STRING NOT NULL,
  category STRING, severity STRING NOT NULL, title STRING NOT NULL, detail STRING,
  source STRING NOT NULL, owner STRING, opened_at TIMESTAMP NOT NULL, resolved_at TIMESTAMP,
  resolution STRING, updated_at TIMESTAMP)""",
    ]


# >>> views: status / stage / progress / ETA rules live here only, so both dashboard pages agree <<< #
def views(schema: str) -> list[str]:
    status = f"""CREATE OR REPLACE VIEW {schema}.v_pair_status AS
WITH latest_snap AS (
  SELECT * FROM {schema}.dtrack_progress_snapshot
  QUALIFY row_number() OVER (PARTITION BY run_id, pair_name ORDER BY snapshot_ts DESC) = 1),
current_run AS (
  SELECT * FROM latest_snap
  QUALIFY row_number() OVER (PARTITION BY pair_name
    ORDER BY greatest(coalesce(heartbeat_at, to_timestamp(0)), coalesce(last_result_at, to_timestamp(0)),
                      coalesce(row_done_at, to_timestamp(0)), coalesce(row_left_done_at, to_timestamp(0)),
                      snapshot_ts) DESC) = 1),
rate AS (
  SELECT run_id, pair_name, count(*) AS rate_points,
         (max(units_compared + units_dead) - min(units_compared + units_dead))
           / nullif(timestampdiff(SECOND, min(snapshot_ts), max(snapshot_ts)) / 3600.0, 0) AS units_per_hour
  FROM {schema}.dtrack_progress_snapshot
  WHERE snapshot_ts >= current_timestamp() - INTERVAL 6 HOURS
  GROUP BY run_id, pair_name),
blockers AS (
  SELECT pair_name,
         count_if(resolved_at IS NULL) AS open_blockers,
         count_if(resolved_at IS NULL AND severity = 'P1') AS open_p1,
         max_by(title, struct(severity = 'P1', opened_at)) FILTER (WHERE resolved_at IS NULL) AS top_blocker
  FROM {schema}.dtrack_pair_blocker GROUP BY pair_name),
progress_age AS (
  SELECT run_id, pair_name, last_progress_at FROM ({last_progress_sql(schema).replace(schema + ".", schema + ".")})),
joined AS (
  SELECT p.pair_name, p.display_name, p.wave, p.priority, p.owner, p.planned_start_date, p.target_done_date,
         c.* EXCEPT (pair_name), r.units_per_hour, r.rate_points, b.open_blockers, b.open_p1, b.top_blocker,
         a.last_progress_at,
         timestampdiff(MINUTE, c.heartbeat_at, current_timestamp()) AS heartbeat_age_minutes,
         c.heartbeat_at + INTERVAL {STALE_HEARTBEAT_MIN} MINUTES AS heartbeat_deadline,
         timestampdiff(MINUTE, current_timestamp(), c.heartbeat_at + INTERVAL {STALE_HEARTBEAT_MIN} MINUTES) AS timeout_in_minutes,
         coalesce(c.row_left_done_at, c.first_unit_at) AS started_at,
         CASE WHEN c.run_state = 'complete' THEN c.last_result_at END AS done_at,
         CASE WHEN c.run_id IS NULL THEN 'NOT_STARTED'
              WHEN c.run_state = 'complete' THEN 'DONE'
              WHEN c.units_expected > 0 AND c.units_compared + c.units_dead >= c.units_expected THEN 'REPORTING'
              WHEN c.row_done_at IS NOT NULL THEN 'COL_CHECK'
              ELSE 'ROW_CHECK' END AS stage,
         (0.15 * CASE WHEN c.row_done_at IS NOT NULL THEN 1 ELSE 0 END
          + 0.80 * coalesce((c.units_compared + c.units_dead) / nullif(c.units_expected, 0), 0)
          + 0.05 * CASE WHEN c.report_published THEN 1 ELSE 0 END) AS progress
  FROM {schema}.dtrack_pair_plan p
  LEFT JOIN current_run c USING (pair_name)
  LEFT JOIN rate r USING (run_id, pair_name)
  LEFT JOIN blockers b USING (pair_name)
  LEFT JOIN progress_age a USING (run_id, pair_name)
  WHERE p.active),
scored AS (
  SELECT *,
         CASE WHEN run_state = 'complete' AND coalesce(units_dead, 0) = 0 THEN 'DONE'
              WHEN gate_passed = false OR (run_state = 'complete' AND units_dead > 0) THEN 'FAILED'
              WHEN coalesce(open_p1, 0) > 0 THEN 'BLOCKED'
              WHEN run_id IS NOT NULL AND run_state <> 'complete'
                   AND (timestampdiff(MINUTE, heartbeat_at, current_timestamp()) > {STALE_HEARTBEAT_MIN}
                        OR (stage = 'COL_CHECK' AND timestampdiff(HOUR, last_progress_at, current_timestamp()) > {NO_PROGRESS_HOURS}))
                   THEN 'STALLED'
              ELSE stage END AS status,
         CASE WHEN stage = 'COL_CHECK' AND units_per_hour > 0
              THEN current_timestamp() + make_interval(0, 0, 0, 0, 0, 0,
                   (units_expected - units_compared - units_dead) / units_per_hour * 3600) END AS eta_ts,
         CASE WHEN stage = 'COL_CHECK' AND units_per_hour > 0 AND rate_points >= 3 AND coalesce(open_blockers, 0) = 0 THEN 'HIGH'
              WHEN stage = 'COL_CHECK' AND units_per_hour > 0 THEN 'LOW' ELSE 'NONE' END AS eta_confidence
  FROM joined)
SELECT *,
       round(100 * progress, 1) AS progress_pct,
       timestampdiff(HOUR, started_at, coalesce(done_at, current_timestamp())) AS elapsed_hours,
       CASE WHEN target_done_date IS NULL THEN 'NO_TARGET'
            WHEN status = 'DONE' THEN 'ON_TRACK'
            WHEN current_date() > target_done_date THEN 'LATE'
            WHEN eta_ts IS NOT NULL AND date(eta_ts) > target_done_date THEN 'AT_RISK'
            ELSE 'ON_TRACK' END AS schedule_health
FROM scored"""
    daily = f"""CREATE OR REPLACE VIEW {schema}.v_pair_daily AS
SELECT pair_name, run_id, date(snapshot_ts) AS day,
       max_by(units_compared + units_dead, snapshot_ts) AS units_done,
       max_by(units_expected, snapshot_ts) AS units_expected,
       max_by(run_state, snapshot_ts) AS run_state
FROM {schema}.dtrack_progress_snapshot
GROUP BY ALL"""
    return [status, daily]


# >>> plan rows from a dtrack config: every non-skipped pair, active, no dates yet <<< #
def plan_rows_from_config(
    config: dict[str, Any], *, updated_by: str = "", now: datetime | None = None
) -> list[dict[str, Any]]:
    stamp = (now or datetime.now(UTC)).isoformat(timespec="seconds")
    return [
        {
            "pair_name": name,
            "display_name": name,
            "wave": None,
            "priority": None,
            "owner": None,
            "planned_start_date": None,
            "target_done_date": None,
            "active": True,
            "notes": None,
            "updated_by": updated_by,
            "updated_at": stamp,
        }
        for name, pair in (config.get("pairs") or {}).items()
        if not pair.get("skip")
    ]


PLAN_COLUMNS = (
    "pair_name",
    "display_name",
    "wave",
    "priority",
    "owner",
    "planned_start_date",
    "target_done_date",
    "active",
    "notes",
    "updated_by",
    "updated_at",
)


# >>> MERGE plan rows: insert new pairs, never overwrite what an owner typed <<< #
def merge_plan_sql(schema: str, rows: list[dict[str, Any]]) -> str:
    if not rows:
        return ""
    columns = ", ".join(PLAN_COLUMNS)
    return f"""MERGE INTO {schema}.dtrack_pair_plan AS t
USING (SELECT * FROM VALUES
{_values(rows, PLAN_COLUMNS, {"updated_at": "TIMESTAMP", "planned_start_date": "DATE", "target_done_date": "DATE"})}
AS s({columns})) AS s
ON t.pair_name = s.pair_name
WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({", ".join("s." + c for c in PLAN_COLUMNS)})"""
