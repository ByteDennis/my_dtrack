from pathlib import Path

import pytest

from pair_recon.app.config import load_defaults, run_config, save_defaults


@pytest.mark.parametrize(
    ("mode", "answers", "expected"),
    [
        (
            "date",
            {
                "from_date": "2026-01-01",
                "to_date": "2026-01-31",
                "vintage": "month",
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
