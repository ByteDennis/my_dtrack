# Databricks notebook source
# MAGIC %md
# MAGIC # dtrack cloud job
# MAGIC Start this notebook by hand (or as a Job). It watches the work location for `stage1/*.left.json`,
# MAGIC counts the Right side with `spark.sql`, reconciles, drains the queue, and publishes the report.
# MAGIC The folder containing this notebook is on `sys.path` when it lives in a Repo or workspace folder.
# MAGIC A job cluster is a fresh environment: the first cell installs the two packages the runtime may lack
# MAGIC (`openpyxl` for the Excel report, `boto3` only when `work` is an `s3://` location). Alternatively
# MAGIC attach `databricks/requirements.txt` as a job library and delete that cell.

# COMMAND ----------

# MAGIC %pip install -q -r ./requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import logging
import os
import sys
import time

sys.path.insert(0, os.getcwd())
from dtrack_cloud.app import run
from dtrack_cloud.targets import SparkTarget

dbutils.widgets.text("work", "/Volumes/main/dtrack/work/dtrack/v1/runs")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("report", "/Volumes/main/dtrack/report")
dbutils.widgets.text("gate", "auto")
dbutils.widgets.text("idle_timeout", "900")
dbutils.widgets.text("wait_timeout", "1800")

logging.Formatter.converter = time.gmtime
logging.basicConfig(level=logging.INFO, format="%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")

# COMMAND ----------

outcome = run(
    dbutils.widgets.get("work"),
    lambda config: SparkTarget(spark, config),
    run_id=dbutils.widgets.get("run_id") or None,
    report=dbutils.widgets.get("report") or None,
    out_dir="/tmp/dtrack-report",
    gate=dbutils.widgets.get("gate"),
    idle_timeout=float(dbutils.widgets.get("idle_timeout")),
    wait_timeout=float(dbutils.widgets.get("wait_timeout")),
)
print(outcome["summary"])
