import pytest

from pair_recon_right.engine.backends.store import VolumeStore, resolve_run, validate_run_id


@pytest.mark.parametrize("value", ["", ".", "..", "a/b", "a\\b"])
def test_runid(value):
    with pytest.raises(ValueError):
        validate_run_id(value)


def test_store(tmp_path):
    root = VolumeStore(tmp_path)
    run = root.child("r1")
    run.put_json("pairs/p/row_check/left.json", {"pair": "p"})
    assert root.list_prefixes() == ["r1"]
    selected, run_id = resolve_run(tmp_path)
    assert run_id == "r1" and selected.get_json("pairs/p/row_check/left.json") == {"pair": "p"}
