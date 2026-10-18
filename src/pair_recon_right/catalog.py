from datetime import datetime
from zoneinfo import ZoneInfo

from .engine.common.model.protocol import list_failures, list_results, list_row_results


def error_rows(store, run_id, pair, now):
    rows = []
    for _, result in list_row_results(store):
        for partition, value in (result.get("partitions") or {}).items():
            if value.get("verdict") == "match":
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
                    "detail": value.get("verdict"),
                    "payload": __import__("json").dumps(value, ensure_ascii=False, sort_keys=True),
                    "created_at": now,
                }
            )
    for _, result in list_results(store):
        for check in ("hash_sum", "n_unique"):
            different = result.get(f"{check}_match") is False or result.get(
                f"{check}_diff"
            ) not in (None, 0)
            if not different:
                continue
            rows.append(
                {
                    "run_id": run_id,
                    "check_name": check,
                    "pair_name": pair,
                    "vintage": result.get("vintage"),
                    "column_name": result.get("column"),
                    "partition_value": None,
                    "left_value": str(result.get(f"{check}_left")),
                    "right_value": str(result.get(f"{check}_right")),
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


def publish(
    spark, store, *, run_id, pair, owner, timezone, pair_table, status_table, error_table, state
):
    from delta.tables import DeltaTable
    from pyspark.sql import functions as F

    spark.conf.set("spark.sql.session.timeZone", timezone)
    spark.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")
    now = datetime.now(ZoneInfo(timezone)).replace(tzinfo=None)
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
            }
        )
        .whenNotMatchedInsertAll()
        .execute()
    )
    errors = DeltaTable.forName(spark, error_table)
    errors.delete(F.col("run_id") == F.lit(run_id))
    rows = error_rows(store, run_id, pair, now)
    if rows:
        spark.createDataFrame(rows).write.mode("append").saveAsTable(error_table)
    DeltaTable.forName(spark, pair_table).update(
        condition=F.col("pair_name") == F.lit(pair),
        set={
            "latest_run_id": F.lit(run_id),
            "latest_status": F.lit(state),
            "updated_at": F.lit(now),
        },
    )
    return {"status": state, "errors": len(rows)}
