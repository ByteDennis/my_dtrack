import io

import pytest
import requests
from databricks.sdk.errors import NotFound

from pair_recon.core.backends import store as store_module
from pair_recon.core.backends.store import ApiVolumeStore


class Files:
    def __init__(self):
        self.values = {}
        self.downloads = []

    def create_directory(self, path):
        return path

    def upload(self, path, contents, *, overwrite=None):
        self.values[path] = contents.read()

    def download(self, path):
        self.downloads.append(path)
        return type("Download", (), {"contents": io.BytesIO(self.values[path])})()

    def get_metadata(self, path):
        if path not in self.values:
            raise FileNotFoundError(path)
        return object()

    def list_directory_contents(self, path):
        prefix = path.rstrip("/") + "/"
        return [
            type("Entry", (), {"path": key, "is_directory": False})()
            for key in sorted(self.values)
            if key.startswith(prefix)
        ]

    def delete(self, path):
        self.values.pop(path, None)


def test_store():
    files = Files()
    store = ApiVolumeStore("/Volumes/main/recon/artifacts/runs", files=files).child("run-1")
    store.put_json("pairs/订单/value.json", {"value": "✓"})
    assert store.exists("pairs/订单/value.json")
    assert store.get_json("pairs/订单/value.json") == {"value": "✓"}
    assert store.list_keys("pairs") == ["pairs/订单/value.json"]
    assert store.delete_keys(["pairs/订单/value.json"]) == 1
    assert not store.exists("pairs/订单/value.json")


def test_sdk_not_found_means_remote_object_is_missing():
    class MissingFiles(Files):
        def get_metadata(self, path):
            raise NotFound(path)

        def list_directory_contents(self, path):
            raise NotFound(path)

    store = ApiVolumeStore("/Volumes/main/recon/artifacts/runs", files=MissingFiles())

    assert not store.exists("new-run/_status/run.json")
    assert store.list_keys("new-run") == []
    assert store.list_prefixes("new-run") == []


def test_delete_keys_ignores_remote_objects_that_are_already_missing():
    class StrictFiles(Files):
        def delete(self, path):
            if path not in self.values:
                raise NotFound(path)
            super().delete(path)

    files = StrictFiles()
    store = ApiVolumeStore("/Volumes/main/recon/artifacts/runs/run-1", files=files)
    store.put_json("row_check/left.json", {"counts": {"all": 1}})

    assert store.delete_keys(["row_check/left.json", "_status/col_check.closed.json"]) == 1
    assert not store.exists("row_check/left.json")


def test_transient_network_errors_are_retried(monkeypatch):
    monkeypatch.setattr(store_module, "RETRY_BACKOFF_S", 0)
    broken = requests.exceptions.ChunkedEncodingError(
        "Connection broken: IncompleteRead(0 bytes read, 479 more expected)"
    )

    class FlakyFiles(Files):
        def __init__(self, failures):
            super().__init__()
            self.failures = failures
            self.attempts = 0

        def download(self, path):
            self.attempts += 1
            if self.failures:
                self.failures -= 1
                raise broken
            return super().download(path)

    files = FlakyFiles(2)
    store = ApiVolumeStore("/Volumes/main/recon/runs/run-1", files=files)
    store.put_json("_status/right.json", {"phase": "running"})
    assert store.get_json("_status/right.json") == {"phase": "running"}
    assert files.attempts == 3

    dead = FlakyFiles(99)
    dead.values[store._path("_status/right.json")] = b"{}"
    with pytest.raises(requests.exceptions.ChunkedEncodingError):
        ApiVolumeStore("/Volumes/main/recon/runs/run-1", files=dead).get_json("_status/right.json")
    assert dead.attempts == store_module.RETRY_ATTEMPTS

    class MissingFiles(Files):
        calls = 0

        def get_metadata(self, path):
            MissingFiles.calls += 1
            raise NotFound(path)

    assert not ApiVolumeStore("/Volumes/x", files=MissingFiles()).exists("a.json")
    assert MissingFiles.calls == 1


def test_immutable_json_is_downloaded_once_and_invalidated_by_write_or_delete():
    files = Files()
    path = "/Volumes/main/recon/runs/run-1/units/00000001-0001.json"
    files.values[path] = b'{"value":1}'
    store = ApiVolumeStore("/Volumes/main/recon/runs/run-1", files=files)

    assert store.get_json("units/00000001-0001.json") == {"value": 1}
    first = store.get_json("units/00000001-0001.json")
    first["value"] = 99
    assert store.get_json("units/00000001-0001.json") == {"value": 1}
    assert files.downloads == [path]

    store.put_json("units/00000001-0001.json", {"value": 2})
    assert store.get_json("units/00000001-0001.json") == {"value": 2}
    assert files.downloads == [path]
    store.delete_keys(["units/00000001-0001.json"])
    assert path not in store._json_cache
