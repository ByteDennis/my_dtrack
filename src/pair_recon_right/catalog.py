from datetime import datetime
from zoneinfo import ZoneInfo

from .engine.common.model.attribute_checks import ATTRIBUTE_CHECKS
from .engine.common.model.protocol import list_failures, list_results, list_row_results

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


def result_summary(store) -> dict[str, int]:
    partitions = [
        value
        for _, document in list_row_results(store)
        for value in (document.get("partitions") or {}).values()
    ]
    columns = [value for _, value in list_results(store)]
    failures = [value for _, value in list_failures(store)]
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


def append_error_rows(spark, error_table, rows) -> None:
    if not rows:
        return
    schema = spark.table(error_table).select(*ERROR_COLUMNS).schema
    spark.createDataFrame(rows, schema=schema).write.mode("append").saveAsTable(error_table)


def publish(
    spark, store, *, run_id, pair, owner, timezone, pair_table, status_table, error_table, state
):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    spark.conf.set("spark.sql.session.timeZone", timezone)
    spark.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")
    now = datetime.now(ZoneInfo(timezone)).replace(tzinfo=None)
    summary = result_summary(store)
    status = spark.createDataFrame(
        [
            {
                "run_id": run_id,
                "pair_name": pair,
                "owner": owner,
                "state": state,
                "timezone": timezone,
                "started_at": now,
                "updated_at": now,
                "ended_at": now,
                **summary,
            }
        ]
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
    append_error_rows(spark, error_table, rows)
    DeltaTable.forName(spark, pair_table).update(
        condition=F.col("pair_name") == F.lit(pair),
        set={
            "latest_run_id": F.lit(run_id),
            "latest_status": F.lit(state),
            "updated_at": F.lit(now),
        },
    )
    return {"status": state, "errors": len(rows)}
