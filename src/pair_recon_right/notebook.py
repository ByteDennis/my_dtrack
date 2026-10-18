# Databricks notebook source
import logging
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.getcwd()))

from pair_recon_right.catalog import publish
from pair_recon_right.engine.backends.store import open_store
from pair_recon_right.engine.backends.targets import SparkTarget
from pair_recon_right.engine.job import run

for name in (
    "run_root",
    "run_id",
    "pair",
    "owner",
    "timezone",
    "pair_table",
    "status_table",
    "error_table",
):
    dbutils.widgets.text(name, "")

values = {name: dbutils.widgets.get(name) for name in (
    "run_root",
    "run_id",
    "pair",
    "owner",
    "timezone",
    "pair_table",
    "status_table",
    "error_table",
)}
logging.Formatter.converter = staticmethod(
    lambda timestamp: datetime.fromtimestamp(
        timestamp, ZoneInfo(values["timezone"])
    ).timetuple()
)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
store = open_store(values["run_root"]).child(values["run_id"])

try:
    outcome = run(
        values["run_root"],
        lambda config: SparkTarget(spark, config),
        run_id=values["run_id"],
    )
    state = "passed" if not sum(outcome.get(name, 0) for name in ("row_errors", "column_errors", "failures")) else "different"
    publish(spark, store, **values, state=state)
    print(outcome)
except BaseException:
    publish(spark, store, **values, state="failed")
    raise
