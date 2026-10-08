import pytest
from support import assert_store_contract

from dtrack_left.backends.store import S3Store

pytestmark = pytest.mark.live


# >>> step 1 of the setup: this machine can write, list, read and delete under the scratch prefix <<< #
def test_s3_store_contract_on_the_real_bucket(s3_scratch):
    store = S3Store(f"{s3_scratch}/contract")
    assert_store_contract(store)


# >>> step 2: completed report artifacts remain readable from the exact run store <<< #
def test_completed_report_is_readable_from_the_real_bucket(s3_scratch):
    runs = S3Store(f"{s3_scratch}/runs")
    run = runs.child("r1")
    run.put_bytes("report/index.html", b"<html>", content_type="text/html")
    run.put_json("_STATUS.json", {"state": "complete", "run_id": "r1"})
    assert run.get_bytes("report/index.html") == b"<html>"
    assert run.get_json("_STATUS.json")["state"] == "complete"
