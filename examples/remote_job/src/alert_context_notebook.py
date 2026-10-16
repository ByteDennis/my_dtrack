# Databricks notebook source
# ruff: noqa: F821, I001
# Records the run selected by the serialized alert Job. The following Alert task reads this
# singleton row, so every notification is tied to the run_id supplied by the local launcher.

# COMMAND ----------

import re


SCHEMA_PATTERN = re.compile(r"[A-Za-z_]\w*\.[A-Za-z_]\w*")
RUN_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}")

dbutils.widgets.text("schema", "main.dtrack")
dbutils.widgets.text("run_id", "")

schema = dbutils.widgets.get("schema")
run_id = dbutils.widgets.get("run_id")
if not SCHEMA_PATTERN.fullmatch(schema):
    raise ValueError("schema must be catalog.schema")
if not RUN_PATTERN.fullmatch(run_id):
    raise ValueError("run_id contains unsupported characters")

spark.createDataFrame([(run_id,)], ["run_id"]).createOrReplaceTempView("_dtrack_alert_run")
spark.sql(
    f"""MERGE INTO {schema}.dtrack_alert_context AS target
USING (
  SELECT 'current' AS context_key, run_id, current_timestamp() AS updated_at
  FROM _dtrack_alert_run) AS source
ON target.context_key = source.context_key
WHEN MATCHED THEN UPDATE SET run_id = source.run_id, updated_at = source.updated_at
WHEN NOT MATCHED THEN INSERT (context_key, run_id, updated_at)
  VALUES (source.context_key, source.run_id, source.updated_at)"""
)

print({"run_id": run_id})
