import json
import os

import pytest

from dtrack.platforms.base import resolve_pair_date_bounds, apply_pair_date_bounds
from dtrack.platforms.hadoop import extract_hadoop
from dtrack.platforms.oracle import extract_oracle
from dtrack.platforms.databricks import extract_databricks
from dtrack.platforms.athena import extract_aws
from dtrack.platforms.csv_duckdb import extract_csv


# >>> CLI args win; otherwise pair-config fromDate/toDate; single-sided ok <<< #
def test_resolve_pair_date_bounds_precedence():
    cfg = {"pairs": {"p": {"fromDate": "2024-02-01", "toDate": "2024-02-28"}}}
    tbl = {"_pairs": ["p"]}
    assert resolve_pair_date_bounds(tbl, cfg, None, None) == ("2024-02-01", "2024-02-28")
    assert resolve_pair_date_bounds(tbl, cfg, "2024-05-01", None) == ("2024-05-01", "2024-02-28")
    assert resolve_pair_date_bounds(tbl, cfg, None, "2024-09-09") == ("2024-02-01", "2024-09-09")
    assert resolve_pair_date_bounds({"_pairs": ["p"]}, {"pairs": {"p": {}}}, None, None) == (None, None)


# >>> the shared driver applies bounds + records effective from/to per table <<< #
def test_apply_pair_date_bounds_single_sided():
    cfg = {"pairs": {"p": {"fromDate": "2024-02-01"}}}
    tables = [{"date_col": "rpt_dt", "_pairs": ["p"]}]
    apply_pair_date_bounds(tables, cfg, None, None,
                           lambda tbl, dc, f, t: " AND ".join(
                               ([f"{dc} >= '{f}'"] if f else []) +
                               ([f"{dc} <= '{t}'"] if t else [])))
    assert tables[0]["_date_bounds"] == "rpt_dt >= '2024-02-01'"
    assert tables[0]["_from_date"] == "2024-02-01"
    assert tables[0]["_to_date"] is None


_EXTRACTORS = {
    "hadoop": (extract_hadoop, "hadoop", "string_dash", ""),
    "oracle": (extract_oracle, "oracle", "date", "svc"),
    "databricks": (extract_databricks, "databricks", "string_dash", ""),
    "aws": (extract_aws, "aws", "string_dash", "db.sch"),
}


# >>> every platform's gen path honors config fromDate/toDate with NO CLI args <<< #
@pytest.mark.parametrize("plat", list(_EXTRACTORS))
def test_config_date_bounds_honored_per_platform(plat, tmp_path):
    extractor, source, date_type, conn = _EXTRACTORS[plat]
    side = {"name": "t1", "table": "t1", "source": source, "conn_macro": conn,
            "date_col": "rpt_dt", "date_type": date_type}
    right = dict(side, name="t2", table="t2")
    cfg = {"metadata": {}, "settings": {}, "pairs": {"p": {
        "left": side, "right": right,
        "fromDate": "2024-02-05", "toDate": "2024-08-09"}}}
    cfg_path = tmp_path / "pairs.json"
    cfg_path.write_text(json.dumps(cfg))
    out = tmp_path / "out"
    out.mkdir()

    extractor(str(cfg_path), str(out), types=["row"], gen_only=True)
    import glob
    files = glob.glob(os.path.join(str(out), "extract_row*.sql"))
    assert files, os.listdir(str(out))
    sql = open(files[0]).read()
    assert "2024-02-05" in sql, sql
    assert "2024-08-09" in sql, sql


# >>> csv-source platform honors config fromDate/toDate (real DuckDB path) <<< #
@pytest.mark.skipif(
    __import__("importlib").util.find_spec("duckdb") is None,
    reason="duckdb not installed")
def test_config_date_bounds_honored_csv(tmp_path):
    csv_path = tmp_path / "data.csv"
    csv_path.write_text("rpt_dt,amt\n2024-02-05,10\n2024-08-09,20\n")
    side = {"name": "t1", "table": "t1", "source": "csv",
            "conn_macro": str(csv_path), "date_col": "rpt_dt",
            "date_type": "string_dash", "columns": {"rpt_dt": "DATE", "amt": "BIGINT"}}
    right = dict(side, name="t2", table="t2")
    cfg = {"metadata": {}, "settings": {}, "pairs": {"p": {
        "left": side, "right": right,
        "fromDate": "2024-02-05", "toDate": "2024-08-09"}}}
    cfg_path = tmp_path / "pairs.json"
    cfg_path.write_text(json.dumps(cfg))
    out = tmp_path / "out"
    out.mkdir()

    extract_csv(str(cfg_path), str(out), types=["row"], gen_only=True)
    import glob
    files = glob.glob(os.path.join(str(out), "extract_row*.sql"))
    assert files, os.listdir(str(out))
    sql = open(files[0]).read()
    assert "2024-02-05" in sql and "2024-08-09" in sql, sql
