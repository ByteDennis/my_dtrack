import json

import pytest
from support import assert_store_contract

from dtrack_left.backends.store import S3Store, fetch_report
from dtrack_left.cli import main

pytestmark = pytest.mark.live


# >>> step 1 of the setup: this machine can write, list, read and delete under the scratch prefix <<< #
def test_s3_store_contract_on_the_real_bucket(s3_scratch):
    store = S3Store(f"{s3_scratch}/contract")
    assert_store_contract(store)


# >>> step 2: fetch-report resolves a completed run below the stable runs root <<< #
def test_fetch_report_command_downloads_from_the_real_bucket(s3_scratch, tmp_path, capsys):
    runs = S3Store(f"{s3_scratch}/runs")
    run = runs.child("r1")
    run.put_bytes("report/index.html", b"<html>", content_type="text/html")
    run.put_json("_STATUS.json", {"state": "complete", "run_id": "r1"})
    assert main(["tools", "fetch-report", "--run-s3", runs.root_uri, "--out", str(tmp_path)]) == 0
    assert json.loads(capsys.readouterr().out)["run_id"] == "r1"
    assert fetch_report(runs, tmp_path / "again")["files"]["index.html"].read_bytes() == b"<html>"
