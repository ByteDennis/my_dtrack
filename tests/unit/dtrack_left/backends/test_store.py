import boto3
import pytest
from support import RecordingClient, assert_store_contract

from dtrack_left.backends.store import S3Location, S3Store, fetch_report, validate_run_id
from dtrack_right.backends.store import VolumeStore


def test_s3_location_parses_bucket_and_prefix():
    assert S3Location.parse("s3://b/runs/r1/") == S3Location("b", "runs/r1")
    assert S3Location.parse("s3://b") == S3Location("b", "")
    with pytest.raises(ValueError, match="expected s3://"):
        S3Location.parse("/Volumes/x")


@pytest.mark.parametrize("value", ["", ".", "..", "a/b", r"a\b", "a\0b"])
def test_run_id_is_exactly_one_safe_folder_name(value):
    with pytest.raises(ValueError, match="one non-empty folder"):
        validate_run_id(value)
    assert validate_run_id("20261007T150000Z-orders") == "20261007T150000Z-orders"


def test_s3_store_honours_the_store_contract_on_mocked_s3(s3_bucket):
    assert_store_contract(S3Store(f"s3://{s3_bucket}/runs/r1"))
    keys = [row["Key"] for row in boto3.client("s3").list_objects_v2(Bucket=s3_bucket)["Contents"]]
    assert all(key.startswith("runs/r1/") for key in keys)


def test_s3_store_at_bucket_root_uses_bare_keys(s3_bucket):
    store = S3Store(f"s3://{s3_bucket}")
    store.put_json("/a.json", {"x": 1})
    assert store.list_keys("") == ["a.json"] and store._key("a.json") == "a.json"


def test_writes_carry_the_key_and_content_type():
    client = RecordingClient()
    S3Store("s3://b/p", client=client).put_json("x.json", {})
    _, kwargs = client.calls[0]
    assert kwargs["Key"] == "p/x.json" and kwargs["ContentType"] == "application/json"
    assert "ServerSideEncryption" not in kwargs


def test_list_keys_follows_continuation_tokens_and_drops_start_after_on_later_pages():
    client = RecordingClient(
        pages=[
            {
                "Contents": [{"Key": "p/_INDEX/2"}],
                "IsTruncated": True,
                "NextContinuationToken": "t",
            },
            {"Contents": [{"Key": "p/_INDEX/3"}], "IsTruncated": False},
        ]
    )
    store = S3Store("s3://b/p", client=client)
    assert store.list_keys("_INDEX/", start_after="_INDEX/1") == ["_INDEX/2", "_INDEX/3"]
    first, second = (kwargs for _, kwargs in client.calls)
    assert first["StartAfter"] == "p/_INDEX/1" and "StartAfter" not in second
    assert second["ContinuationToken"] == "t"


def test_exists_is_false_only_for_not_found_errors():
    class Error(Exception):
        def __init__(self, code):
            super().__init__(code)
            self.response = {"Error": {"Code": code}}

    assert S3Store("s3://b/p", client=RecordingClient(head_error=Error("404"))).exists("x") is False
    with pytest.raises(Error):
        S3Store("s3://b/p", client=RecordingClient(head_error=Error("403"))).exists("x")


def test_endpoint_override_switches_to_path_style_addressing(monkeypatch):
    monkeypatch.setenv("DTRACK_S3_ENDPOINT_URL", "http://127.0.0.1:4566")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    client = S3Store("s3://b/p").client
    assert client.meta.endpoint_url == "http://127.0.0.1:4566"
    assert client.meta.config.s3["addressing_style"] == "path"


def test_fetch_report_downloads_report_from_the_run_store(tmp_path):
    runs = VolumeStore(tmp_path / "runs")
    old = runs.child("r1")
    old.put_json("_STATUS.json", {"state": "complete"})
    old.put_bytes("report/index.html", b"<old>")
    latest = runs.child("r2")
    latest.put_json("_STATUS.json", {"state": "complete"})
    latest.put_bytes("report/index.html", b"<html>")
    fetched = fetch_report(runs, tmp_path / "fetched")
    assert fetched["run_id"] == "r2"
    assert (tmp_path / "fetched/index.html").read_bytes() == b"<html>"
    assert fetch_report(runs, tmp_path / "old", run_id="r1")["run_id"] == "r1"


def test_fetch_report_fails_clearly_without_report_files(tmp_path):
    runs = VolumeStore(tmp_path / "runs")
    with pytest.raises(FileNotFoundError, match="no completed run"):
        fetch_report(runs, tmp_path)
    runs.child("r1").put_json("_STATUS.json", {"state": "complete"})
    with pytest.raises(FileNotFoundError, match="report/"):
        fetch_report(runs, tmp_path, run_id="r1")


def test_fetch_report_rejects_an_explicit_incomplete_run_with_stale_files(tmp_path):
    runs = VolumeStore(tmp_path / "runs")
    run = runs.child("r1")
    run.put_json("_STATUS.json", {"state": "left_col_check"})
    run.put_bytes("report/index.html", b"<stale>")
    with pytest.raises(FileNotFoundError, match="not complete"):
        fetch_report(runs, tmp_path / "out", run_id="r1")
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize("name", ["../escape.txt", r"..\escape.txt", "/absolute.txt"])
def test_fetch_report_rejects_report_keys_that_escape_the_output_directory(
    s3_bucket, tmp_path, name
):
    runs = S3Store(f"s3://{s3_bucket}/runs")
    run = runs.child("r1")
    run.put_json("_STATUS.json", {"state": "complete"})
    run.put_bytes(f"report/{name}", b"bad", content_type="text/plain")

    with pytest.raises(ValueError, match="unsafe report object key"):
        fetch_report(runs, tmp_path / "out", run_id="r1")
    assert not (tmp_path / "escape.txt").exists()


# >>> Databricks mkdir on an S3-backed Volume leaves 0-byte "folder/" markers <<< #
def test_list_json_skips_folder_marker_objects(s3_bucket):
    store = S3Store(f"s3://{s3_bucket}/r")
    boto3.client("s3").put_object(Bucket=s3_bucket, Key="r/manifest/", Body=b"")
    store.put_json("manifest/a.json", {"x": 1})
    assert store.list_json("manifest/") == [("manifest/a.json", {"x": 1})]
