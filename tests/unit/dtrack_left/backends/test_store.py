import boto3
import pytest
from support import MemoryStore, RecordingClient, assert_store_contract

from dtrack_left.backends.store import S3Location, S3Store, fetch_report


def test_s3_location_parses_bucket_and_prefix():
    assert S3Location.parse("s3://b/runs/r1/") == S3Location("b", "runs/r1")
    assert S3Location.parse("s3://b") == S3Location("b", "")
    with pytest.raises(ValueError, match="expected s3://"):
        S3Location.parse("/Volumes/x")


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


def test_fetch_report_follows_the_latest_pointer_or_an_explicit_run(tmp_path):
    store = MemoryStore()
    store.put_bytes("history/r1/index.html", b"<html>", content_type="text/html")
    store.put_bytes("history/r2/index.html", b"<html2>", content_type="text/html")
    store.put_json("latest/pointer.json", {"run_id": "r2", "status": "DONE", "findings": 0})
    latest = fetch_report(store, tmp_path / "latest")
    assert latest["run_id"] == "r2" and (tmp_path / "latest/index.html").read_bytes() == b"<html2>"
    assert fetch_report(store, tmp_path / "old", run_id="r1")["files"]["index.html"].exists()


def test_fetch_report_fails_clearly_without_a_pointer_or_a_run(tmp_path):
    with pytest.raises(FileNotFoundError, match="latest/pointer.json"):
        fetch_report(MemoryStore(), tmp_path)
    with pytest.raises(FileNotFoundError, match="history/nope/"):
        fetch_report(MemoryStore(), tmp_path, run_id="nope")


# >>> Databricks mkdir on an S3-backed Volume leaves 0-byte "folder/" markers <<< #
def test_list_json_skips_folder_marker_objects(s3_bucket):
    store = S3Store(f"s3://{s3_bucket}/r")
    boto3.client("s3").put_object(Bucket=s3_bucket, Key="r/manifest/", Body=b"")
    store.put_json("manifest/a.json", {"x": 1})
    assert store.list_json("manifest/") == [("manifest/a.json", {"x": 1})]
