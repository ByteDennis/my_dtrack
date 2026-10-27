import errno

import pytest

from pair_recon_right.engine.backends import store as store_module
from pair_recon_right.engine.backends.store import VolumeStore, resolve_run, validate_run_id


@pytest.mark.parametrize("value", ["", ".", "..", "a/b", "a\\b"])
def test_runid(value):
    with pytest.raises(ValueError):
        validate_run_id(value)


def test_store(tmp_path):
    root = VolumeStore(tmp_path)
    run = root.child("r1")
    run.put_json("row_check/left.json", {"pair": "p"})
    assert root.list_prefixes() == ["r1"]
    selected, run_id = resolve_run(tmp_path)
    assert run_id == "r1" and selected.get_json("row_check/left.json") == {"pair": "p"}


def test_temporary_name_does_not_repeat_target_name(tmp_path, monkeypatch):
    replaced = []
    replace = store_module.os.replace

    def capture_replace(source, target):
        replaced.append((source, target))
        replace(source, target)

    monkeypatch.setattr(store_module.os, "replace", capture_replace)
    store = VolumeStore(tmp_path)
    key = "nested/audit-event.sql"
    store.put_bytes(key, b"payload")

    temporary, target = replaced[0]
    assert temporary.parent == target.parent
    assert temporary.name.startswith(".tmp-") and temporary.name.endswith(".part")
    assert len(temporary.name) == 42
    assert target.name not in temporary.name
    assert store.get_bytes(key) == b"payload"


def test_reads_retry_fuse_errors(tmp_path, monkeypatch):
    monkeypatch.setattr(store_module, "RETRY_BACKOFF_S", 0)
    store = VolumeStore(tmp_path)
    store.put_json("_state/left.json", {"phase": "col_check"})
    attempts = []
    read_bytes = store_module.Path.read_bytes

    def flaky(self):
        attempts.append(1)
        if len(attempts) < 3:
            raise OSError(errno.EINVAL, "Invalid argument")
        return read_bytes(self)

    monkeypatch.setattr(store_module.Path, "read_bytes", flaky)
    assert store.get_json("_state/left.json") == {"phase": "col_check"} and len(attempts) == 3

    def gone(self):
        raise OSError(errno.ENOENT, "No such file")

    monkeypatch.setattr(store_module.Path, "read_bytes", gone)
    with pytest.raises(OSError, match="No such file"):
        store.get_bytes("_state/left.json")
