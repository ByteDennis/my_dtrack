import json
from pathlib import Path

import pytest

from pair_recon.app.config import (
    env_value,
    load_config,
    load_defaults,
    load_environment,
    local_output_root,
    run_config,
    save_defaults,
)


def test_load_environment_reads_only_pair_recon_file(monkeypatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("DBX_PROFILE", raising=False)
    monkeypatch.delenv("DBX_WAREHOUSE_ID", raising=False)
    (tmp_path / ".env").write_text("DBX_PROFILE=wrong\n", encoding="utf-8")
    source = tmp_path / ".pair_recon.env"
    source.write_text("DBX_PROFILE=team\nDBX_WAREHOUSE_ID=w9\n", encoding="utf-8")

    assert load_environment() == source
    assert env_value("DBX_PROFILE") == "team"
    assert env_value("DBX_WAREHOUSE_ID") == "w9"


def test_load_environment_does_not_override_process_environment(monkeypatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("DBX_PROFILE", "process")
    (tmp_path / ".pair_recon.env").write_text("DBX_PROFILE=file\n", encoding="utf-8")

    load_environment()

    assert env_value("DBX_PROFILE") == "process"


@pytest.mark.parametrize(
    ("mode", "answers", "expected"),
    [
        (
            "date",
            {
                "from_date": "2026-01-01",
                "to_date": "2026-01-31",
                "vintage": "month",
                "left_date_col": "BUSINESS_DT",
                "left_date_type": {"type": "string", "format": "compact"},
                "right_date_col": "business_dt",
                "right_date_type": "date",
                "excluded_partitions": ["2026-01-04", "2026-01-05"],
                "sample": 0.1,
                "strict": True,
                "checks": ["row_count", "hash_sum"],
            },
            {"vintage": "month", "statistics": ["hash_sum"]},
        ),
        (
            "all",
            {
                "sample": False,
                "strict": False,
                "checks": ["row_count", "n_unique"],
            },
            {"partition_mode": "all", "statistics": ["n_unique"]},
        ),
        (
            "all",
            {"sample": False, "strict": False, "checks": ["row_count"]},
            {"partition_mode": "all", "checks": ["row_count"]},
        ),
    ],
)
def test_config(mode, answers, expected):
    pair = {
        "left": {"source": "oracle", "query": "select * from l", "date_col": "DT"},
        "right": {"source": "databricks", "query": "select * from r", "date_col": "dt"},
        "partition_mode": mode,
        "key": "ID",
        "col_map": {"ID": "id", "VALUE": "value"},
    }
    value = run_config("订单", pair, answers, timezone="America/New_York")
    assert list(value["pairs"]) == ["订单"]
    assert value["version"] == 2 and value["timezone"] == "America/New_York"
    for key, item in expected.items():
        assert value["pairs"]["订单"][key] == item
    if mode == "all":
        assert "fromDate" not in value["pairs"]["订单"]
    else:
        assert value["pairs"]["订单"]["left"]["date_col"] == "BUSINESS_DT"
        assert value["pairs"]["订单"]["left"]["date_type"] == {
            "type": "string",
            "format": "compact",
        }
        assert value["pairs"]["订单"]["right"]["date_col"] == "business_dt"
        assert value["pairs"]["订单"]["right"]["date_type"] == "date"
        assert value["pairs"]["订单"]["excluded_partitions"] == [
            "2026-01-04",
            "2026-01-05",
        ]
    if answers["checks"] == ["row_count"]:
        assert "statistics" not in value["pairs"]["订单"]


def test_defaults(tmp_path: Path):
    path = tmp_path / ".pair_recon.yaml"
    saved = save_defaults(
        path,
        {
            "owner": "测试用户",
            "profile": "corp",
            "password": "hidden",
            "access_token": "hidden",
        },
    )
    assert saved == {"owner": "测试用户", "profile": "corp"}
    assert load_defaults(path) == saved
    assert "hidden" not in path.read_text(encoding="utf-8")


def test_load_config(tmp_path: Path):
    plain = tmp_path / "plain.json"
    plain.write_text(json.dumps({"owner": "测试用户", "pair": "orders"}), encoding="utf-8")
    assert load_config(plain) == {"owner": "测试用户", "pair": "orders"}
    snapshot = tmp_path / "request.json"
    snapshot.write_text(
        json.dumps({"format": "pair-recon-request-v1", "settings": {"pair": "orders"}}),
        encoding="utf-8",
    )
    assert load_config(snapshot) == {"pair": "orders"}
    bad = tmp_path / "bad.json"
    bad.write_text("[]", encoding="utf-8")
    with pytest.raises(TypeError):
        load_config(bad)
    with pytest.raises(FileNotFoundError):
        load_config(tmp_path / "missing.json")


def test_local_output_root_defaults_to_outputs_and_creates_it(monkeypatch, tmp_path: Path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("PAIR_RECON_OUTPUT_DIR", raising=False)

    assert local_output_root() == Path("outputs")
    assert (tmp_path / "outputs").is_dir()


def test_local_output_root_environment_overrides_saved_work_dir(monkeypatch, tmp_path: Path):
    target = tmp_path / "artifacts"
    monkeypatch.setenv("PAIR_RECON_OUTPUT_DIR", str(target))

    assert local_output_root({"work_dir": "legacy"}) == target
    assert target.is_dir()
