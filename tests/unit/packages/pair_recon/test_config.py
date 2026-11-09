import json
from pathlib import Path

import pytest

from pair_recon.app.config import (
    env_boolean,
    env_value,
    load_config,
    load_defaults,
    load_environment,
    local_output_root,
    read_run_options,
    resolve_run_defaults,
    run_config,
    save_defaults,
    select_run_options,
)


@pytest.mark.parametrize(
    ("value", "expected"),
    [("true", True), ("YES", True), ("1", True), ("false", False), ("off", False), ("0", False)],
)
def test_env_boolean(monkeypatch, value, expected):
    monkeypatch.setenv("PAIR_RECON_TEST_BOOLEAN", value)

    assert env_boolean("PAIR_RECON_TEST_BOOLEAN", default=not expected) is expected


def test_env_boolean_uses_default_and_rejects_invalid(monkeypatch):
    monkeypatch.delenv("PAIR_RECON_TEST_BOOLEAN", raising=False)
    assert env_boolean("PAIR_RECON_TEST_BOOLEAN", default=True) is True
    monkeypatch.setenv("PAIR_RECON_TEST_BOOLEAN", "sometimes")
    with pytest.raises(ValueError, match="PAIR_RECON_TEST_BOOLEAN must be true or false"):
        env_boolean("PAIR_RECON_TEST_BOOLEAN")


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
    ("partition", "answers", "expected"),
    [
        (
            "month",
            {
                "from_date": "2026-01-01",
                "to_date": "2026-01-31",
                "vintage": "month",
                "partition": "month",
                "left_date_col": "BUSINESS_DT",
                "left_date_type": {"type": "string", "format": "compact"},
                "right_date_col": "business_dt",
                "right_date_type": "date",
                "excluded_partitions": ["2026-01-04", "2026-01-05"],
                "sample": 0.1,
                "checks": ["row_count", "hash_sum"],
            },
            {
                "vintage": "month",
                "partition": "month",
                "statistics": ["hash_sum"],
            },
        ),
        (
            "all",
            {
                "sample": False,
                "checks": ["row_count", "n_unique"],
            },
            {"partition": "all", "statistics": ["n_unique"]},
        ),
        (
            "all",
            {"sample": False, "checks": ["row_count"]},
            {"partition": "all", "checks": ["row_count"]},
        ),
    ],
)
def test_config(partition, answers, expected):
    pair = {
        "left": {"source": "oracle", "query": "select * from l", "date_col": "DT"},
        "right": {"source": "databricks", "query": "select * from r", "date_col": "dt"},
        "partition": partition,
        "key": "ID",
        "col_map": {"ID": "id", "VALUE": "value"},
    }
    value = run_config("订单", pair, answers, timezone="America/New_York")
    assert list(value["pairs"]) == ["订单"]
    assert "version" not in value and value["timezone"] == "America/New_York"
    for key, item in expected.items():
        assert value["pairs"]["订单"][key] == item
    if partition == "all":
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


def test_run_config_applies_excluded_columns():
    pair = {
        "left": {"source": "oracle", "table": "L", "date_col": "DT"},
        "right": {"source": "databricks", "table": "r", "date_col": "dt"},
        "partition": "all",
        "key": "ID",
        "col_map": {"ID": "id", "VALUE": "value"},
    }
    answers = {"partition": "all", "checks": ["row_count"], "excluded_columns": ["VALUE"]}
    value = run_config("p", pair, answers, timezone="UTC")
    assert value["pairs"]["p"]["excluded_columns"] == ["VALUE"]


@pytest.mark.parametrize("catalog_authoritative", [True, False])
def test_resolve_run_defaults_preserves_precedence_and_explicit_empty_values(catalog_authoritative):
    metadata = {
        "partition": "month",
        "vintage": "quarter",
        "excluded_columns": [],
        "_run_defaults": {"sample": 0.01, "audit": False, "sample_columns": [], "left_workers": 3},
    }
    saved = {
        "partition": "day",
        "sample": 0.25,
        "audit": True,
        "sample_columns": ["A"],
        "use_modular": "OLD",
        "sample_partitions": ["2026-01-01"],
        "excluded_columns": ["B"],
        "left_workers": 1,
        "work_dir": "local",
    }
    actual = resolve_run_defaults(metadata, saved, catalog_authoritative=catalog_authoritative)
    assert actual["sample"] == (0.01 if catalog_authoritative else 0.25)
    assert actual["audit"] is (not catalog_authoritative)
    assert actual["sample_columns"] == ([] if catalog_authoritative else ["A"])
    assert actual["excluded_columns"] == ([] if catalog_authoritative else ["B"])
    assert actual["use_modular"] == (None if catalog_authoritative else "OLD")
    assert actual["sample_partitions"] == (None if catalog_authoritative else ["2026-01-01"])
    assert actual["work_dir"] == "local"
    assert actual.get("absent", "fallback") == "fallback"
    assert saved["use_modular"] == "OLD" and "use_modular" not in metadata


def test_resolve_run_defaults_explicit_catalog_selector_wins():
    metadata = {"use_modular": "FROM_METADATA", "_run_defaults": {"use_modular": "CURRENT"}}
    assert (
        resolve_run_defaults(metadata, {"use_modular": "OLD"}, catalog_authoritative=True)[
            "use_modular"
        ]
        == "CURRENT"
    )


def test_automatic_run_options_normalize_before_saving_and_building_config(tmp_path):
    defaults = {
        "sample_rows": "0",
        "sample_columns": " B, A, B ",
        "sample_partitions": ["2026-02-01", " 2026-01-01 ", "2026-02-01"],
    }
    answers = {
        **read_run_options(defaults),
        "sample": False,
        "audit": False,
        "checks": ["row_count"],
        "unrelated": "not a run option",
    }
    expected = {
        "sample_rows": 0,
        "sample_columns": ["B", "A"],
        "sample_partitions": ["2026-02-01", "2026-01-01"],
        "sample": False,
        "audit": False,
    }
    path = tmp_path / "defaults.yaml"
    save_defaults(path, select_run_options(answers))
    assert load_defaults(path) == expected
    configured = run_config("p", {"partition": "all"}, answers, timezone="UTC")["pairs"]["p"]
    assert {name: configured[name] for name in expected} == expected
    assert "unrelated" not in configured
    configured["sample_columns"].append("C")
    assert answers["sample_columns"] == ["B", "A"]
    assert defaults["sample_partitions"][1] == " 2026-01-01 "


def test_automatic_run_options_clear_old_selectors_and_use_fresh_defaults():
    resolved = resolve_run_defaults(
        {"_run_defaults": {"sample_columns": []}},
        {"sample_columns": ["OLD"], "sample_partitions": ["2025-01-01"]},
        catalog_authoritative=True,
    )
    assert resolved["sample_partitions"] is None
    assert read_run_options(resolved) == {
        "sample_rows": 50,
        "sample_columns": [],
        "sample_partitions": [],
    }
    first = read_run_options({})
    first["sample_columns"].append("A")
    assert read_run_options({})["sample_columns"] == []


def test_automatic_run_options_reject_negative_row_limits():
    with pytest.raises(ValueError, match="sample_rows"):
        read_run_options({"sample_rows": -1})
