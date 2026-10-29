import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from support import MemoryStore

from pair_recon.app.cli import batch
from pair_recon.app.cli import smoke as smoke_module
from pair_recon_right.engine.job import SMOKE_REQUEST

RESULT = '{\n  "settings": {"x": 1}\n}\nlog line\n{\n  "state": "%s",\n  "pair": "orders"\n}\n'


def test_result_state_reads_the_last_json_block():
    assert batch.result_state(RESULT % "different") == "different"
    assert batch.result_state("2026-10-27 INFO nothing here") is None
    assert batch.result_state("{\n  broken\n") is None


@pytest.mark.parametrize(
    ("returncode", "state", "outcome"),
    [
        (0, "passed", "executed"),
        (0, "different", "executed"),
        (0, "empty", "executed"),
        (0, "awaiting_sample", "executed"),
        (0, None, "failed"),
        (1, "different", "failed"),
        (512, None, "failed"),
        (130, None, "cancelled"),
    ],
)
def test_classify_trusts_the_return_code_first(returncode, state, outcome):
    assert batch.classify(returncode, state) == outcome


def test_run_pair_writes_a_temporary_defaults_file(tmp_path):
    seen = {}

    def runner(command):
        seen["command"] = command
        return 0, RESULT % "passed"

    row = batch.run_pair("orders", {"owner": "me", "pair": "old"}, ["--force"], runner=runner)
    command = seen["command"]
    assert command[1:3] == ["-m", "pair_recon"] and command[-1] == "--force"
    saved = yaml.safe_load(Path(command[4]).read_text(encoding="utf-8"))
    assert saved == {"owner": "me", "pair": "orders"}
    assert row["pair"] == "orders" and row["state"] == "passed" and row["outcome"] == "executed"


def test_main_runs_every_pair_and_keeps_going_after_failures(tmp_path, monkeypatch):
    answers = {"a": (0, RESULT % "different"), "b": (1, "Traceback"), "c": (130, "Cancelled")}

    def fake_run(command):
        pair = yaml.safe_load(Path(command[4]).read_text(encoding="utf-8"))["pair"]
        return answers[pair]

    monkeypatch.setattr(batch, "_run", fake_run)
    defaults = tmp_path / ".pair_recon.yaml"
    defaults.write_text("owner: me\n", encoding="utf-8")
    summary = tmp_path / "summary.json"
    code = batch.main(["a", "b", "c", "--defaults", str(defaults), "--summary", str(summary)])
    rows = json.loads(summary.read_text(encoding="utf-8"))
    assert code == 1
    assert [(row["pair"], row["outcome"]) for row in rows] == [
        ("a", "executed"),
        ("b", "failed"),
        ("c", "cancelled"),
    ]
    assert "| a | executed | different | 0 |" in batch.summary_table(rows)


def test_smoke_runs_the_local_csv_pair_end_to_end(tmp_path, monkeypatch):
    left = tmp_path / "left.csv"
    left.write_text("ID,VALUE,NOTE\n1,a,x\n2,b,y\n3,c,z\n", encoding="utf-8")
    right = tmp_path / "right.csv"
    right.write_text("id,value,note\n1,a,x\n2,b,y\n", encoding="utf-8")
    store = MemoryStore()

    class Store:
        def __init__(self, _root, files=None):
            pass

        def child(self, _name):
            return store

    monkeypatch.setattr(smoke_module, "VolumeStore", Store)
    pair = {
        "version": 2,
        "partition": "all",
        "left": {"source": "csv", "path": str(left)},
        "right": {"source": "csv", "path": str(right)},
        "key": "ID",
        "sample": 1.0,
        "checks": ["row_count", "hash_sum"],
        "col_map": {"ID": "id", "VALUE": "value", "NOTE": "note"},
    }
    config = {"version": 2, "timezone": "America/New_York", "pairs": {"orders": pair}}
    settings = {"run_id": "orders-me-20261027", "volume_root": "/Volumes/x", "profile": "corp"}
    client = SimpleNamespace(files=None)

    report = smoke_module.smoke(settings, config, workspace_client=client)

    assert report["run_id"] == "orders-me-20261027-smoke"
    assert report["left"]["rows"] == 3 and report["left"]["health"]["ok"]
    assert report["right"] == {
        "health": {"ok": True, "source": "csv", "columns": ["id", "value", "note"]},
        "matched": 2,
        "expected": 3,
    }
    assert report["verdict"] == "sample_mismatch" and "2/3" in report["hint"]
    request = store.get_json(SMOKE_REQUEST)
    assert request["key_columns"] == ["id"] and request["columns"] == ["value", "note"]
    assert sorted(request["key_tuples"]) == [["1"], ["2"], ["3"]]

    right.write_text("id,value,note\n1,a,x\n2,b,y\n3,c,z\n", encoding="utf-8")
    assert smoke_module.smoke(settings, config, workspace_client=client)["verdict"] == "ok"
