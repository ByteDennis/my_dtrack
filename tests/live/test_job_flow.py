import json
import os
import threading
from datetime import timedelta
from pathlib import Path

import pytest
from support import oracle_config

from dtrack_left.backends import databricks
from dtrack_left.backends.store import S3Store
from dtrack_left.cli import main

pytestmark = [pytest.mark.live, pytest.mark.docker]
RIGHT_PACKAGE = Path(__file__).resolve().parents[2] / "src/dtrack_right"


# >>> copy src/dtrack_right into the workspace: notebook.py as a notebook, the rest as plain files <<< #
def upload_right_package(workspace, folder: str) -> str:
    target = f"{folder}/dtrack_right"
    outcome = databricks.upload_workspace(workspace, RIGHT_PACKAGE, target, overwrite=True)
    assert outcome["failed"] == [], outcome
    return f"{target}/notebook"


# >>> one-time run of the Right notebook; the same parameters a scheduled Job would pass <<< #
def submit_right_job(workspace, notebook: str, parameters: dict[str, str]):
    from databricks.sdk.service import jobs

    compute = os.environ.get("DTRACK_LIVE_COMPUTE", "serverless")
    task = jobs.SubmitTask(
        task_key="dtrack_right",
        notebook_task=jobs.NotebookTask(
            notebook_path=notebook, base_parameters=parameters, source=jobs.Source.WORKSPACE
        ),
        existing_cluster_id=None if compute == "serverless" else compute,
    )
    return workspace.jobs.submit(run_name=f"dtrack-live-{parameters['run_id']}", tasks=[task])


# >>> every setting the flow needs, checked before any container or cluster is started <<< #
@pytest.fixture(scope="module")
def settings(need):
    names = ["DTRACK_LIVE_S3_WORK", "DTRACK_LIVE_WORKSPACE_DIR", "DTRACK_LIVE_SCHEMA"]
    values = dict(zip(names, need(*names), strict=True))
    volume = os.environ.get("DTRACK_LIVE_VOLUME_WORK", "").rstrip("/")
    values["RIGHT_WORK"] = volume or values["DTRACK_LIVE_S3_WORK"]
    values["SCOPE"] = os.environ.get("DTRACK_LIVE_AWS_SECRET_SCOPE", "")
    if values["RIGHT_WORK"].startswith("s3://") and not values["SCOPE"]:
        pytest.skip("no work Volume and no DTRACK_LIVE_AWS_SECRET_SCOPE: the job cannot reach S3")
    return values


# >>> step 8: Oracle -> Left/S3 -> Databricks Right; report stays under the same run <<< #
def test_left_on_this_machine_and_right_as_a_databricks_job(
    settings, workspace, right_tables, oracle, tag, keep, tmp_path, capsys, monkeypatch
):
    s3_work, right_work = settings["DTRACK_LIVE_S3_WORK"], settings["RIGHT_WORK"]
    folder = f"{settings['DTRACK_LIVE_WORKSPACE_DIR']}/{tag}"
    notebook = upload_right_package(workspace, folder)
    parameters = {
        "run_s3": f"{right_work}/runs",
        "run_id": tag,
        "aws_secret_scope": settings["SCOPE"],
        "gate": "auto",
        "idle_timeout": "900",
        "wait_timeout": "1800",
    }
    run = submit_right_job(workspace, notebook, parameters)
    finished = {}
    waiter = threading.Thread(
        target=lambda: finished.update(run=run.result(timeout=timedelta(minutes=45))), daemon=True
    )
    waiter.start()

    for name, suffix in (("user", "USR"), ("password", "PWD"), ("dsn", "DSN")):
        monkeypatch.setenv(f"live_ora_{suffix}", oracle[name])
    document = oracle_config({}, right_tables)
    for pair in document["pairs"].values():
        pair["left"]["conn_macro"] = "live_ora"
    config = tmp_path / "config.json"
    config.write_text(json.dumps(document))
    runs = f"{s3_work}/runs"
    run_uri = f"{runs}/{tag}"
    assert (
        main(["row-check", "--config", str(config), "--run-s3", runs, "--run-id", tag]) == 0
    )
    col_check = [
        "col-check",
        "--run-s3",
        runs,
        "--run-id",
        tag,
        "--work-dir",
        str(tmp_path / "work"),
    ]
    assert main([*col_check, "--wait-timeout", "1800", "--poll-interval", "5"]) == 0
    waiter.join(timeout=45 * 60)
    assert finished["run"].state.result_state.value == "SUCCESS", finished["run"].state

    capsys.readouterr()
    run_store = S3Store(run_uri)
    summary = run_store.get_json("report/summary.json")
    assert summary["run_id"] == tag and summary["status"] == "DONE"
    assert summary["findings"] == {"row_count_diff": 1, "partition_missing": 2, "col_stat_diff": 2}
    assert run_store.get_json("_STATUS.json")["state"] == "complete"
    if not keep:
        workspace.workspace.delete(folder, recursive=True)
