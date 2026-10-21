# Databricks notebook source
import importlib
import logging
import os
import subprocess
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
if not notebook_path.startswith("/Workspace/"):
    notebook_path = "/Workspace" + notebook_path
package_dir = os.path.dirname(notebook_path)
sys.path.insert(0, os.path.dirname(package_dir))
package = os.path.basename(package_dir)

requirements = os.path.join(package_dir, "requirements.txt")
if os.path.isfile(requirements):
    with open(requirements, encoding="utf-8") as stream:
        needed = [line.strip() for line in stream if line.strip() and not line.lstrip().startswith("#")]
    if needed:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-q", "-r", requirements])
        importlib.invalidate_caches()

publish = importlib.import_module(f"{package}.catalog").publish
open_store = importlib.import_module(f"{package}.engine.backends.store").open_store
SparkTarget = importlib.import_module(f"{package}.engine.backends.targets").SparkTarget
run = importlib.import_module(f"{package}.engine.job").run

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
