# Databricks notebook source
# MAGIC %pip install -r "__PAIR_RECON_RIGHT_REQUIREMENTS__"

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import json
import logging
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

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
    "right_workers",
    "from_date",
    "to_date",
    "vintage",
    "sample_rate",
    "vintage_sample",
):
    dbutils.widgets.text(name, "")

values = {
    name: dbutils.widgets.get(name)
    for name in (
        "run_root",
        "run_id",
        "pair",
        "owner",
        "timezone",
        "pair_table",
        "status_table",
        "error_table",
        "right_workers",
        "from_date",
        "to_date",
        "vintage",
        "sample_rate",
        "vintage_sample",
    )
}
publish_values = {
    name: values[name]
    for name in (
        "run_id",
        "pair",
        "owner",
        "timezone",
        "pair_table",
        "status_table",
        "error_table",
        "from_date",
        "to_date",
        "vintage",
        "sample_rate",
        "vintage_sample",
    )
}
logging.Formatter.converter = staticmethod(
    lambda timestamp: datetime.fromtimestamp(timestamp, ZoneInfo(values["timezone"])).timetuple()
)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
    force=True,
)
logging.getLogger("pair_recon").info("widgets: %s", values)
store = open_store(values["run_root"]).child(values["run_id"])

try:
    outcome = run(
        values["run_root"],
        lambda config: SparkTarget(spark, config),
        run_id=values["run_id"],
        right_workers=int(values["right_workers"] or 8),
    )
    state = (
        "passed"
        if not sum(outcome.get(name, 0) for name in ("row_errors", "column_errors", "failures"))
        else "different"
    )
    publish(spark, store, **publish_values, state=state)
    print(outcome)
except BaseException:
    logging.getLogger("pair_recon").exception("right job failed")
    publish(spark, store, **publish_values, state="failed")
    raise
dbutils.notebook.exit(json.dumps(outcome, default=str))
