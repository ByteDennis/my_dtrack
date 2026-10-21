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
