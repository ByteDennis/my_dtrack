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

from pair_recon_right.catalog import publish, run_state
from pair_recon_right.engine.backends.store import open_store
from pair_recon_right.engine.backends.targets import SparkTarget
from pair_recon_right.engine.backends.volume_log import VolumeLogHandler
from pair_recon_right.engine.job import run, smoke

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
    "smoke",
    "local_reconciliation",
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
        "smoke",
        "local_reconciliation",
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
volume_log = VolumeLogHandler(store)
volume_log.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
logging.getLogger().addHandler(volume_log)
spark.sql("SET spark.sql.legacy.timeParserPolicy = CORRECTED")

try:
    if values["smoke"]:
        outcome = smoke(store, lambda config: SparkTarget(spark, config))
    else:
        outcome = run(
            values["run_root"],
            lambda config: SparkTarget(spark, config),
            run_id=values["run_id"],
            right_workers=int(values["right_workers"] or 8),
            local_reconciliation=values["local_reconciliation"].casefold() == "true",
        )
        if values["local_reconciliation"].casefold() != "true":
            publish(spark, store, **publish_values, state=run_state(outcome))
    print(outcome)
except BaseException:
    logging.getLogger("pair_recon").exception("right job failed")
    if not values["smoke"]:
        publish(spark, store, **publish_values, state="failed")
    raise
finally:
    volume_log.close()
dbutils.notebook.exit(json.dumps(outcome, default=str))
