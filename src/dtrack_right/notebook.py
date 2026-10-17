# Databricks notebook source
# MAGIC %md
# MAGIC # dtrack right job
# MAGIC Start this notebook by hand (or as a Job). It watches the work location for `pairs/*/row_check/left.json`,
# MAGIC counts the Right side with `spark.sql`, reconciles, drains the queue, and publishes the report.
# MAGIC The folder containing this notebook is on `sys.path` when it lives in a Repo or workspace folder.
# MAGIC A job cluster is a fresh environment: the first cell installs the two packages the runtime may lack
# MAGIC (`openpyxl` for the Excel report, `boto3` only when `work` is an `s3://` location). Alternatively
# MAGIC attach `src/dtrack_right/requirements.txt` as a job library and delete that cell.
# MAGIC `run_s3` is the stable runs root on a Volume, or an `s3://` URL read with boto3. The report is
# MAGIC always published under the resolved run's `report/`. For S3 on compute without an
# MAGIC instance profile, `aws_secret_scope` names a secret scope holding the AWS key (see tests/live/sources/provision.py).

# COMMAND ----------

# MAGIC %pip install -q -r ./requirements.txt

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import logging
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.getcwd()))
from dtrack_right.engine.backends.targets import SparkTarget
from dtrack_right.engine.job import run

dbutils.widgets.text("run_s3", "/Volumes/main/dtrack/work/dtrack/v1/runs")
dbutils.widgets.text("run_id", "")
dbutils.widgets.text("gate", "auto")
dbutils.widgets.text("idle_timeout", "900")
dbutils.widgets.text("wait_timeout", "1800")
dbutils.widgets.text("aws_secret_scope", "")

scope = dbutils.widgets.get("aws_secret_scope")
if scope:
    for key in ("aws_access_key_id", "aws_secret_access_key", "aws_default_region"):
        os.environ[key.upper()] = dbutils.secrets.get(scope, key)

logging.Formatter.converter = time.gmtime
logging.basicConfig(level=logging.INFO, format="%(asctime)s.%(msecs)03dZ %(levelname)s %(name)s: %(message)s", datefmt="%Y-%m-%dT%H:%M:%S")

# COMMAND ----------

outcome = run(
    dbutils.widgets.get("run_s3"),
    lambda config: SparkTarget(spark, config),
    run_id=dbutils.widgets.get("run_id") or None,
    out_dir="/tmp/dtrack-report",
    gate=dbutils.widgets.get("gate"),
    idle_timeout=float(dbutils.widgets.get("idle_timeout")),
    wait_timeout=float(dbutils.widgets.get("wait_timeout")),
)
print(outcome["summary"])
