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


# >>> step 2: fetch-report reads a report set back through latest/pointer.json <<< #
def test_fetch_report_command_downloads_from_the_real_bucket(s3_scratch, tmp_path, capsys):
    report = S3Store(f"{s3_scratch}/report")
    report.put_bytes("history/r1/index.html", b"<html>", content_type="text/html")
    report.put_json("latest/pointer.json", {"run_id": "r1", "status": "DONE", "findings": 0})
    assert (
        main(["tools", "fetch-report", "--report-s3", report.root_uri, "--out", str(tmp_path)]) == 0
    )
    assert json.loads(capsys.readouterr().out)["run_id"] == "r1"
    assert fetch_report(report, tmp_path / "again")["files"]["index.html"].read_bytes() == b"<html>"
