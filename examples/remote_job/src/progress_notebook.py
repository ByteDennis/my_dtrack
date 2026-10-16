# Databricks notebook source
# ruff: noqa: F821, I001
# Collects protocol state into Delta snapshots. Views read those tables directly, so the
# published dashboard sees a run after this notebook commits its final snapshot.

# COMMAND ----------

# MAGIC %pip install -q -r ./requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import os
import sys
from datetime import UTC, datetime

sys.path.insert(0, os.path.dirname(os.getcwd()))

from dtrack_right import progress


dbutils.widgets.text("run_roots", "/Volumes/main/dtrack/work/dtrack/v1/runs")
dbutils.widgets.text("schema", "main.dtrack")
dbutils.widgets.text("keep_complete_days", "1")

roots = [value.strip() for value in dbutils.widgets.get("run_roots").split(",") if value.strip()]
schema = dbutils.widgets.get("schema")
now = datetime.now(UTC)

for statement in [*progress.ddl(schema), *progress.views(schema)]:
    spark.sql(statement)

rows = progress.collect(
    roots,
    now=now,
    keep_complete_days=float(dbutils.widgets.get("keep_complete_days")),
)
if rows:
    spark.sql(progress.insert_snapshot_sql(schema, rows))
    previous = {
        (row.run_id, row.pair_name): row.last_progress_at.isoformat()
        for row in spark.sql(progress.last_progress_sql(schema)).collect()
    }
    blockers = progress.auto_blockers(rows, now=now, last_progress_at=previous)
    merge = progress.merge_blockers_sql(schema, blockers)
    if merge:
        spark.sql(merge)

print(
    {
        "runs": len({row["run_id"] for row in rows}),
        "rows": len(rows),
        "errors": sum(1 for row in rows if row["collector_error"]),
    }
)
