import json
import logging
from collections import Counter
from datetime import date, datetime
from zoneinfo import ZoneInfo

from .engine.common.model.attribute_checks import ATTRIBUTE_CHECKS
from .engine.common.model.manifest import sample_plan, unit_id_for
from .engine.common.model.protocol import (
    active_unit_ids,
    list_failures,
    list_plans,
    list_results,
    list_row_results,
)
from .engine.common.sql.dates import bucket_date

log = logging.getLogger("pair_recon.right.catalog")
WINDOW_HISTORY_TABLE = "pair_recon_window_history"
WINDOW_COLUMNS = (
    "pair_name",
    "vintage",
    "window_value",
    "run_id",
    "owner",
    "result",
    "partitions",
    "excluded_partitions",
    "columns",
    "sample_rate",
    "units",
    "matches",
    "differences",
    "checked_at",
)
ERROR_COLUMNS = (
    "run_id",
    "check_name",
    "pair_name",
    "vintage",
    "column_name",
    "partition_value",
    "left_value",
    "right_value",
    "detail",
    "payload",
    "created_at",
)
STATUS_SUMMARY_COLUMNS = (
    "row_total",
    "row_matches",
    "row_differences",
    "column_total",
    "column_matches",
    "column_differences",
    "runtime_failures",
)
STATUS_RUN_COLUMNS = (
    "from_date",
    "to_date",
    "vintage",
    "sample_rate",
    "vintage_sample",
)
STATUS_COLUMNS = (
    "run_id",
    "pair_name",
    "owner",
    "state",
    "timezone",
    *STATUS_RUN_COLUMNS,
    "started_at",
    "updated_at",
    "ended_at",
    *STATUS_SUMMARY_COLUMNS,
)


# >>> sibling table in the same catalog.schema as the pair table <<< #
def companion_table(pair_table: str, name: str) -> str:
    return f"{pair_table.rsplit('.', 1)[0]}.{name}"


# >>> terminal state from a Right summary: any runtime failure means the check did not finish <<< #
def run_state(outcome: dict) -> str:
    if int(outcome.get("failures") or 0):
        return "incomplete"
    if int(outcome.get("row_errors") or 0) or int(outcome.get("column_errors") or 0):
        return "different"
    return "passed"


def sample_rate_value(sample: dict) -> float | None:
    if sample.get("mode") == "fraction":
        return float(sample["rate"])
    return 1.0 if sample.get("mode") == "all" else None


def status_run_values(pair: dict) -> dict:
    sample_rate = sample_rate_value(sample_plan(pair.get("sample")))
    vintage_values = pair.get("vintage_values") or []
    vintage_size = pair.get("vintage_sample_size")
    if vintage_values:
        vintage_sample = json.dumps(list(vintage_values), ensure_ascii=False, separators=(",", ":"))
    elif vintage_size not in (None, ""):
        vintage_sample = str(int(vintage_size))
    else:
        vintage_sample = "all"
    return {
        "from_date": pair.get("fromDate"),
        "to_date": pair.get("toDate"),
        "vintage": pair.get("vintage"),
        "sample_rate": sample_rate,
        "vintage_sample": vintage_sample,
    }


def result_summary(store) -> dict[str, int]:
    active = active_unit_ids(store)
    scoped = bool(list_plans(store))
    partitions = [
        value
        for _, document in list_row_results(store)
        for value in (document.get("partitions") or {}).values()
    ]
    columns = [
        value for _, value in list_results(store) if not scoped or value.get("unit_id") in active
    ]
    failures = [
        value
        for _, value in list_failures(store)
        if not scoped or value.get("unit_id") in active or value.get("source")
    ]
    row_matches = sum(value.get("result") == "match" for value in partitions)
    column_matches = sum(value.get("verdict") == "match" for value in columns)
    return {
        "row_total": len(partitions),
        "row_matches": row_matches,
        "row_differences": len(partitions) - row_matches,
        "column_total": len(columns),
        "column_matches": column_matches,
        "column_differences": len(columns) - column_matches,
        "runtime_failures": len(failures),
    }


def error_rows(store, run_id, pair, now):
    active = active_unit_ids(store)
    scoped = bool(list_plans(store))
    rows = []
    for _, result in list_row_results(store):
        for partition, value in (result.get("partitions") or {}).items():
            if value.get("result") == "match":
                continue
            rows.append(
                {
                    "run_id": run_id,
                    "check_name": "row_count",
                    "pair_name": pair,
                    "vintage": None,
                    "column_name": None,
                    "partition_value": partition,
                    "left_value": str(value.get("left")),
                    "right_value": str(value.get("right")),
                    "detail": value.get("result"),
                    "payload": __import__("json").dumps(value, ensure_ascii=False, sort_keys=True),
                    "created_at": now,
                }
            )
    for _, result in list_results(store):
        if scoped and result.get("unit_id") not in active:
            continue
        for name, check in ATTRIBUTE_CHECKS.items():
            if check.result_match(result) is not False:
                continue
            rows.append(
                {
                    "run_id": run_id,
                    "check_name": name,
                    "pair_name": pair,
                    "vintage": result.get("vintage"),
                    "column_name": result.get("column"),
                    "partition_value": None,
                    "left_value": str(result.get(f"{name}_left")),
                    "right_value": str(result.get(f"{name}_right")),
                    "detail": "different",
                    "payload": __import__("json").dumps(result, ensure_ascii=False, sort_keys=True),
                    "created_at": now,
                }
            )
    for _, failure in list_failures(store):
        if scoped and failure.get("unit_id") not in active and not failure.get("source"):
            continue
        rows.append(
            {
                "run_id": run_id,
                "check_name": "runtime",
                "pair_name": pair,
                "vintage": failure.get("window"),
                "column_name": failure.get("column"),
                "partition_value": None,
                "left_value": None,
                "right_value": None,
                "detail": failure.get("error") or "runtime failure",
                "payload": __import__("json").dumps(failure, ensure_ascii=False, sort_keys=True),
                "created_at": now,
            }
        )
    return rows


# >>> one row per plan window whose every column unit has a durable match/diff and whose row counts matched <<< #
def window_rows(store, run_id, pair, owner, now):
    results = {value["unit_id"]: value for _, value in list_results(store)}
    failures = {value["unit_id"] for _, value in list_failures(store)}
    row_results = {value.get("pair"): value for _, value in list_row_results(store)}
    rows = []
    for _, plan in list_plans(store):
        if plan.get("pair") != pair or plan.get("partition_mode", "date") != "date":
            continue
        reconciled = row_results.get(pair) or {}
        synonyms = reconciled.get("date_synonyms") or {}
        partitions = reconciled.get("partitions") or {}
        vintage = str(plan.get("vintage") or "day")
        excluded_by_bucket = Counter(
            bucket_date(value, vintage) for value in plan.get("excluded_partitions") or []
        )
        rate = sample_rate_value(plan.get("sample") or {})
        for window in plan.get("windows") or []:
            ids = [unit_id_for(plan, window, column) for column in plan["columns"]]
            if any(uid in failures or uid not in results for uid in ids):
                continue
            counted = all(
                (partitions.get(synonyms.get(value, value)) or {}).get("result") == "match"
                for value in window["partitions"]
            )
            if not counted:
                continue
            matched = sum(results[uid].get("verdict") == "match" for uid in ids)
            rows.append(
                {
                    "pair_name": pair,
                    "vintage": vintage,
                    "window_value": window["bucket"],
                    "run_id": run_id,
                    "owner": owner,
                    "result": "match" if matched == len(ids) else "different",
                    "partitions": len(window["partitions"]),
                    "excluded_partitions": excluded_by_bucket.get(window["bucket"], 0),
                    "columns": len(plan["columns"]),
                    "sample_rate": rate,
                    "units": len(ids),
                    "matches": matched,
                    "differences": len(ids) - matched,
                    "checked_at": now,
                }
            )
    return rows


def append_rows(spark, table, columns, rows) -> None:
    if not rows:
        return
    schema = spark.table(table).select(*columns).schema
    spark.createDataFrame(rows, schema=schema).write.mode("append").saveAsTable(table)


def publish(
    spark,
    store,
    *,
    run_id,
    pair,
    owner,
    timezone,
    pair_table,
    status_table,
    error_table,
    state,
    from_date=None,
    to_date=None,
    vintage=None,
    sample_rate=None,
    vintage_sample="all",
):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    spark.conf.set("spark.sql.session.timeZone", timezone)
    spark.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")
    now = datetime.now(ZoneInfo(timezone)).replace(tzinfo=None)
    summary = result_summary(store)
    status_row = {
        "run_id": run_id,
        "pair_name": pair,
        "owner": owner,
        "state": state,
        "timezone": timezone,
        "from_date": date.fromisoformat(from_date) if from_date else None,
        "to_date": date.fromisoformat(to_date) if to_date else None,
        "vintage": vintage or None,
        "sample_rate": float(sample_rate) if sample_rate not in (None, "") else None,
        "vintage_sample": vintage_sample or "all",
        "started_at": now,
        "updated_at": now,
        "ended_at": now,
        **summary,
    }
    status_schema = spark.table(status_table).select(*STATUS_COLUMNS).schema
    status = spark.createDataFrame(
        [
            status_row,
        ],
        schema=status_schema,
    )
    target = DeltaTable.forName(spark, status_table)
    (
        target.alias("t")
        .merge(status.alias("s"), "t.run_id = s.run_id")
        .whenMatchedUpdate(
            set={
                "pair_name": "s.pair_name",
                "owner": "s.owner",
                "state": "s.state",
                "timezone": "s.timezone",
                **{name: f"s.{name}" for name in STATUS_RUN_COLUMNS},
                "updated_at": "s.updated_at",
                "ended_at": "s.ended_at",
                **{name: f"s.{name}" for name in STATUS_SUMMARY_COLUMNS},
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    errors = DeltaTable.forName(spark, error_table)
    errors.delete(F.col("run_id") == F.lit(run_id))
    rows = error_rows(store, run_id, pair, now)
    append_rows(spark, error_table, ERROR_COLUMNS, rows)
    window_table = companion_table(pair_table, WINDOW_HISTORY_TABLE)
    windows = window_rows(store, run_id, pair, owner, now)
    try:
        DeltaTable.forName(spark, window_table).delete(F.col("run_id") == F.lit(run_id))
        append_rows(spark, window_table, WINDOW_COLUMNS, windows)
    except Exception:
        log.exception(
            "window history not written: table=%s rows=%d; create it with setup/schema.sql",
            window_table,
            len(windows),
        )
        windows = []
    DeltaTable.forName(spark, pair_table).update(
        condition=F.col("pair_name") == F.lit(pair),
        set={
            "latest_run_id": F.lit(run_id),
            "latest_status": F.lit(state),
            "updated_at": F.lit(now),
        },
    )
    return {"status": state, "errors": len(rows), "windows": len(windows)}
