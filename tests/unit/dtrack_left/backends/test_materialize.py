import json

import pytest

from dtrack_left.backends.materialize import materialize_pair
from dtrack_left.backends.sources import source_from_config
from dtrack_left.cli.config import load_config


def _config(tmp_path, *, where=None):
    csv = tmp_path / "huge.csv"
    csv.write_text(
        "D,K,V\n"
        "2026-01-01,001,a\n"
        "2026-01-01,002,b\n"
        "2026-02-01,003,c\n"
        "2026-02-01,004,d\n"
    )
    left = {
        "source": "file",
        "path": str(csv),
        "date_col": "D",
        "date_type": "string",
    }
    if where:
        left["where"] = where
    document = {
        "pairs": {
            "p": {
                "left": left,
                "right": {
                    "source": "databricks",
                    "table": "p",
                    "date_col": "d",
                    "date_type": "date",
                },
                "col_map": {"V": "v"},
                "key": "K",
                "key_families": {"K": "numeric"},
                "sample": 0.01,
            }
        }
    }
    config = tmp_path / "config.json"
    config.write_text(json.dumps(document))
    return config, csv


def test_materialize_validates_updates_config_and_only_then_deletes_csv(tmp_path):
    config, csv = _config(tmp_path)
    output = tmp_path / "parquet"

    result = materialize_pair(
        config, "p", output=output, partition="day", delete_source=True
    )

    assert result["rows"] == 4 and result["source_deleted"] is True
    assert not csv.exists()
    assert sorted(path.name for path in output.glob("dtrack_partition=*")) == [
        "dtrack_partition=2026-01-01",
        "dtrack_partition=2026-02-01",
    ]
    manifest = json.loads((output / "_dtrack_materialization.json").read_text())
    assert manifest["source_deleted"] is True
    assert manifest["summary"]["2026-01-01"]["rows"] == 2

    pair = load_config(config)["pairs"]["p"]
    assert pair["left"]["format"] == "parquet"
    assert pair["left"]["date_type"] == "date"
    assert pair["left"]["sample_hash"] == {
        "column": "dtrack_hash",
        "version": 1,
        "keys": ["K"],
        "families": ["numeric"],
    }
    source = source_from_config(pair["left"])
    assert source.partition_counts() == {"2026-01-01": 2, "2026-02-01": 2}

    audited = []
    source.set_sql_audit(
        type("Audit", (), {"record": lambda self, sql, **_meta: audited.append(sql)})()
    )
    sampled = source.pull_window(
        ["2026-01-01", "2026-02-01"],
        ["V"],
        tmp_path / "sample.csv",
        key_columns=["K"],
        sample_plan={"mode": "fraction", "rate": 1.0, "threshold": 2**32},
        key_families=["numeric"],
    )
    pull_sql = next(sql for sql in audited if '"dtrack_hash" <' in sql)
    assert '"dtrack_hash" < 4294967296' in pull_sql and "md5(" not in pull_sql
    assert len(sampled.read_text().splitlines()) == 5


def test_delete_source_refuses_derived_views_before_writing_anything(tmp_path):
    config, csv = _config(tmp_path, where="K <> '002'")
    output = tmp_path / "parquet"
    with pytest.raises(ValueError, match="where/query"):
        materialize_pair(config, "p", output=output, delete_source=True)
    assert csv.exists() and not output.exists()
