import io

import pytest

from pair_recon_left.backends.store import VolumeStore, validate_run_id


class Files:
    def __init__(self):
        self.values = {}

    def create_directory(self, _path):
        return None

    def upload(self, path, contents, **_options):
        self.values[path] = contents.read()

    def download(self, path):
        return type("Download", (), {"contents": io.BytesIO(self.values[path])})()

    def get_metadata(self, path):
        if path not in self.values:
            raise FileNotFoundError(path)
        return object()

    def list_directory_contents(self, path):
        prefix = path.rstrip("/") + "/"
        return [
            type("Entry", (), {"path": key, "is_directory": False})()
            for key in self.values
            if key.startswith(prefix)
        ]

    def delete(self, path):
        self.values.pop(path, None)


@pytest.mark.parametrize("value", ["", ".", "..", "a/b", "a\\b"])
def test_runid(value):
    with pytest.raises(ValueError):
        validate_run_id(value)


def test_protocol():
    store = VolumeStore("/Volumes/c/s/v/runs", files=Files()).child("run-1")
    store.commit_units(
        1,
        [
            {
                "pair": "p",
                "window": "2026-01",
                "unit_id": "u1",
                "left_statistics": {"hash_sum": "1"},
            }
        ],
    )
    assert store.ready_unclaimed() == 1
    store.put_result("u1", {"pair": "p", "verdict": "match"})
    assert store.ready_unclaimed() == 0
