import pytest
from support import RecordingClient, assert_store_contract

from dtrack_right.backends.store import (
    S3Store,
    VolumeStore,
    open_store,
    resolve_run,
    run_is_complete,
    validate_run_id,
)


def test_volume_store_honours_the_store_contract(tmp_path):
    store = VolumeStore(tmp_path / "runs" / "r1")
    assert_store_contract(store)
    assert not list((tmp_path / "runs" / "r1").rglob("*.part"))


def test_resolve_run_rejects_nested_or_parent_run_ids(tmp_path):
    for run_id in ("../other", "a/b", r"a\b"):
        with pytest.raises(ValueError, match="one non-empty folder"):
            resolve_run(str(tmp_path), run_id)
    assert validate_run_id("r1") == "r1"


def test_volume_store_hides_partial_writes_and_normalises_prefixes(tmp_path):
    store = VolumeStore(tmp_path)
    store.put_json("_INDEX/00000001-a.json", {})
    (tmp_path / "_INDEX" / "00000002-b.json.part").write_text("{")
    assert store.list_keys("/_INDEX/") == ["_INDEX/00000001-a.json"]
    assert store.list_keys("_INDEX/0000000") == ["_INDEX/00000001-a.json"]


def test_s3_store_honours_the_store_contract_on_mocked_s3(s3_bucket):
    assert_store_contract(S3Store(f"s3://{s3_bucket}/runs/r1"))


@pytest.fixture(params=["volume", "s3"])
def runs_root(request, tmp_path):
    if request.param == "volume":
        return str(tmp_path / "runs")
    return f"s3://{request.getfixturevalue('s3_bucket')}/runs"


def test_list_prefixes_and_child_address_run_folders(runs_root):
    root = open_store(runs_root)
    root.child("r2").put_json("a.json", {})
    root.child("r1").put_json("deep/b.json", {})
    assert root.list_prefixes() == ["r1", "r2"]
    assert root.child("r1").list_prefixes() == ["deep"]
    assert root.child("r1").list_prefixes("deep") == []
    assert root.list_prefixes("nothing") == []


def test_resolve_run_picks_newest_unfinished_run_under_a_root(runs_root):
    root = open_store(runs_root)
    root.child("20260901T000000Z-aaa").put_json("_STATUS.json", {"state": "complete"})
    root.child("20260902T000000Z-bbb").put_json("row_check/p.left.json", {"pair": "p"})
    root.child("20260903T000000Z-ccc").put_json("row_check/p.left.json", {"pair": "p"})
    store, run_id = resolve_run(runs_root)
    assert run_id == "20260903T000000Z-ccc" and store.exists("row_check/p.left.json")
    assert resolve_run(runs_root, "20260902T000000Z-bbb")[1] == "20260902T000000Z-bbb"
    assert run_is_complete(root.child("20260901T000000Z-aaa"))
    assert not run_is_complete(root.child("20260902T000000Z-bbb"))


def test_resolve_run_does_not_treat_a_run_folder_as_the_runs_root(runs_root):
    run = open_store(runs_root).child("r1")
    run.put_json("row_check/p.left.json", {"pair": "p"})
    assert resolve_run(run.root_uri) == (None, None)


def test_resolve_run_returns_nothing_when_every_run_is_complete(runs_root):
    root = open_store(runs_root)
    root.child("r1").put_json("_STATUS.json", {"state": "complete"})
    assert resolve_run(runs_root) == (None, None)


def test_open_store_picks_the_backend_from_the_location(tmp_path, s3_bucket):
    assert isinstance(open_store(str(tmp_path / "v")), VolumeStore)
    assert isinstance(open_store(f"s3://{s3_bucket}/x"), S3Store)


def test_s3_store_pages_prefix_listings_and_children_share_the_client():
    client = RecordingClient(
        pages=[
            {
                "CommonPrefixes": [{"Prefix": "p/r2/"}],
                "IsTruncated": True,
                "NextContinuationToken": "t",
            },
            {"CommonPrefixes": [{"Prefix": "p/r1/"}], "IsTruncated": False},
        ]
    )
    store = S3Store("s3://b/p", client=client)
    assert store.list_prefixes() == ["r1", "r2"]
    assert client.calls[1][1]["ContinuationToken"] == "t"
    assert store.child("r1").root_uri == "s3://b/p/r1" and store.child("r1").client is client


def test_s3_store_exists_raises_on_access_errors():
    class Error(Exception):
        def __init__(self, code):
            super().__init__(code)
            self.response = {"Error": {"Code": code}}

    with pytest.raises(Error):
        S3Store("s3://b/p", client=RecordingClient(head_error=Error("403"))).exists("x")


def test_volume_store_lists_nothing_under_a_missing_folder(tmp_path):
    assert VolumeStore(tmp_path).list_keys("nope/deeper/") == []


# >>> Databricks mkdir on an S3-backed Volume leaves 0-byte "folder/" markers <<< #
def test_s3_list_keys_skips_folder_marker_objects():
    client = RecordingClient(
        pages=[{"Contents": [{"Key": "p/m/"}, {"Key": "p/m/a.json"}], "IsTruncated": False}]
    )
    assert S3Store("s3://b/p", client=client).list_keys("m/") == ["m/a.json"]
