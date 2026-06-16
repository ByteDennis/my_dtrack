"""Shared wiring for the live integration matrix (tests/integration/).

Unlike tests/e2e/ (DTRACK_MOCK, fixture CSVs) and tests/api/ (HTTP plumbing,
tolerates 200/500), these tests actually connect to the real backends and run
dtrack's generated SQL against REAL tables.

Each entry in LIVE_PAIRS is one self-contained (left, right) pair -- mirroring
_E2E_PAIRS in tests/e2e/conftest.py -- carrying the real table on each side,
its date column, the explicit `columns` map col extraction needs, and the
left<->right `col_map`. Tests parametrize over these pairs; pick a subset by
the per-pair mark (the dict key), e.g.:

    pytest tests/integration --run-live -m oracle_vs_athena
    pytest tests/integration --run-live --run-databricks -m oracle_vs_databricks

Skipped by default -- opt in with CLI flags (no env vars):
    pytest tests/integration --run-live
    pytest tests/integration --run-live --run-databricks

BEFORE RUNNING: fill in LIVE_PAIRS below with tables + columns that exist on
YOUR servers.
"""

import json
import sqlite3
from argparse import Namespace
from pathlib import Path

import pytest

from dtrack import cli as cli_mod
from dtrack.db import init_database
from dtrack.platforms.athena import extract_aws
from dtrack.platforms.databricks import extract_databricks
from dtrack.platforms.hadoop import extract_hadoop
from dtrack.platforms.oracle import extract_oracle

_HERE = Path(__file__).resolve().parent

# >>> direct extract fn per platform; hadoop is direct ODBC (extract_hadoop) -- no gen-hadoop CLI command <<< #
_EXTRACT_FN = {
    "oracle": extract_oracle, "hadoop": extract_hadoop,
    "athena": extract_aws, "databricks": extract_databricks,
}

# >>> config `source` value (= row/col-CSV {source}_{name} prefix); athena's source key is 'aws' <<< #
_SOURCE = {
    "oracle": "oracle", "hadoop": "hadoop", "athena": "aws", "databricks": "databricks",
}


# >>> named live pairs; fill each side with a real table + its columns on YOUR servers <<< #
# Per-side keys: platform (oracle|hadoop|athena|databricks), name, table,
# conn_macro, date_col, date_type, columns {name: sql_type}. The `columns` map
# is what col extraction reads -- omit it and col extraction prints
# "No columns specified ..." and skips the table. `col_map` maps left->right
# column names so match-columns / compare-col line the two sides up.
LIVE_PAIRS = {
    "oracle_vs_athena": {
        "description": "live: oracle vs athena",
        "left": {
            "platform": "oracle",
            "name": "oracle_probe", "table": "SCHEMA.YOUR_ORACLE_TABLE",
            "conn_macro": "pb23", "date_col": "RPT_DT", "date_type": "date",
            "columns": {"id": "NUMBER", "amount": "NUMBER", "name": "VARCHAR2"},
        },
        "right": {
            "platform": "athena",
            "name": "athena_probe", "table": "your_athena_table",
            "conn_macro": "analytics_db", "date_col": "rpt_dt", "date_type": "string_dash",
            "columns": {"id": "bigint", "amount": "double", "name": "string"},
        },
        "col_map": {"id": "id", "amount": "amount", "name": "name"},
    },
    "oracle_vs_databricks": {
        "description": "live: oracle vs databricks",
        "left": {
            "platform": "oracle",
            "name": "oracle_probe", "table": "SCHEMA.YOUR_ORACLE_TABLE",
            "conn_macro": "pb23", "date_col": "RPT_DT", "date_type": "date",
            "columns": {"id": "NUMBER", "amount": "NUMBER", "name": "VARCHAR2"},
        },
        "right": {
            "platform": "databricks",
            "name": "databricks_probe", "table": "your_databricks_table",
            "conn_macro": "main.analytics", "date_col": "rpt_dt", "date_type": "date",
            "columns": {"id": "bigint", "amount": "double", "name": "string"},
        },
        "col_map": {"id": "id", "amount": "amount", "name": "name"},
    },
    "hadoop_vs_athena": {
        "description": "live: hadoop vs athena",
        "left": {
            "platform": "hadoop",
            "name": "hadoop_probe", "table": "db.your_hive_table",
            "conn_macro": "hive_prod", "date_col": "rpt_dt", "date_type": "string_dash",
            "columns": {"id": "bigint", "amount": "double", "name": "string"},
        },
        "right": {
            "platform": "athena",
            "name": "athena_probe", "table": "your_athena_table",
            "conn_macro": "analytics_db", "date_col": "rpt_dt", "date_type": "string_dash",
            "columns": {"id": "bigint", "amount": "double", "name": "string"},
        },
        "col_map": {"id": "id", "amount": "amount", "name": "name"},
    },
}


def pytest_addoption(parser):
    parser.addoption("--run-live", action="store_true", default=False,
                     help="run live integration tests (need real .env + LIVE_PAIRS filled in)")
    parser.addoption("--run-databricks", action="store_true", default=False,
                     help="include the slow databricks live pairs")


# >>> register one mark per LIVE_PAIRS key so `-m <pair>` selects it without warnings <<< #
def pytest_configure(config):
    for pair_name in LIVE_PAIRS:
        config.addinivalue_line("markers", f"{pair_name}: live pair '{pair_name}'")


# >>> platforms on either side of a named pair <<< #
def _pair_platforms(pair_name):
    pair = LIVE_PAIRS[pair_name]
    return (pair["left"]["platform"], pair["right"]["platform"])


# >>> True when this test's pair has databricks on either side <<< #
def _is_databricks(item):
    cs = getattr(item, "callspec", None)
    if not cs:
        return False
    pair_name = cs.params.get("pair_name")
    return bool(pair_name) and "databricks" in _pair_platforms(pair_name)


# >>> skip everything under tests/integration/ unless opted in; scoped by path since modifyitems is global <<< #
def pytest_collection_modifyitems(config, items):
    run_live = config.getoption("--run-live")
    run_db = config.getoption("--run-databricks")
    skip_live = pytest.mark.skip(reason="needs --run-live (real backends)")
    skip_db = pytest.mark.skip(reason="needs --run-databricks (slow warehouse cold-start)")
    for item in items:
        try:
            Path(item.path).resolve().relative_to(_HERE)
        except (ValueError, AttributeError):
            continue
        if not run_live:
            item.add_marker(skip_live)
        if not run_db and _is_databricks(item):
            item.add_marker(skip_db)


# >>> one param per named pair, each tagged with its own mark for `-m` selection <<< #
def _pair_params():
    return [pytest.param(name, id=name, marks=getattr(pytest.mark, name))
            for name in LIVE_PAIRS]


# >>> {source}_{name} qname dtrack uses for a side's stored rows / output CSV <<< #
def _qname(side):
    return f"{_SOURCE[side['platform']]}_{side['name']}"


# >>> turn a LIVE_PAIRS side into a config table dict (platform -> source) <<< #
def _side_cfg(side):
    return {
        "name": side["name"], "table": side["table"],
        "source": _SOURCE[side["platform"]], "conn_macro": side["conn_macro"],
        "date_col": side["date_col"], "date_type": side["date_type"],
        "columns": side.get("columns", {}),
    }


# >>> write a one-pair config wiring the named pair's real tables + col_map <<< #
def _build_config(path, pair_name):
    pair = LIVE_PAIRS[pair_name]
    cfg = {
        "metadata": {"title": "live-matrix"},
        "settings": {"sas_outdir": "./sas/", "aws_outdir": "./csv/",
                     "from_date": "", "to_date": ""},
        "pairs": {pair_name: {
            "description": pair.get("description", pair_name),
            "left": _side_cfg(pair["left"]), "right": _side_cfg(pair["right"]),
            "col_map": pair.get("col_map", {}),
            "mode": "incremental",
            "fromDate": pair.get("from_date", ""), "toDate": pair.get("to_date", ""),
        }},
    }
    Path(path).write_text(json.dumps(cfg, indent=2))


# >>> count rows a side loaded into the given table (_row_counts / _col_stats) <<< #
def _loaded_count(db_path, qname, table):
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE source_table=?", (qname,))
        return cur.fetchone()[0]
    finally:
        conn.close()


# >>> live-pipeline operations bound to one pair's sandbox -- the per-stage files drive this <<< #
class _Pipe:
    def __init__(self, wd):
        self.wd = wd
        self.sides = (wd["pair"]["left"], wd["pair"]["right"])

    def _cli(self, sub, **kw):
        getattr(cli_mod, f"cmd_{sub.replace('-', '_')}")(Namespace(**kw))

    def extract(self, kind):
        for side in self.sides:
            _EXTRACT_FN[side["platform"]](self.wd["config"], self.wd["csv"], types=[kind],
                                          max_workers=None, db_path=self.wd["db"], vintage=None,
                                          force=False, from_date=None, to_date=None,
                                          preset=None, stats_override=None)

    def assert_real_csv(self, kind):
        for side in self.sides:
            csv = Path(self.wd["csv"]) / f"{_qname(side)}_{kind}.csv"
            assert csv.exists(), f"{side['platform']}: extract produced no {csv.name} (table absent or SQL failed?)"
            lines = [ln for ln in csv.read_text().splitlines() if ln.strip()]
            assert len(lines) > 1, f"{side['platform']}: {csv.name} has no data rows (header only): {lines[:2]}"

    def load(self, kind):
        if kind == "row":
            self._cli("load-row", project_db=self.wd["db"], folder=self.wd["csv"],
                      config=self.wd["config"], mode="upsert", db=None, date_var=None)
        else:
            self._cli("load-col", project_db=self.wd["db"], folder=self.wd["csv"],
                      config=self.wd["config"], mode="upsert")

    def assert_loaded(self, kind):
        table = "_row_counts" if kind == "row" else "_col_stats"
        for side in self.sides:
            assert _loaded_count(self.wd["db"], _qname(side), table) > 0, \
                f"{side['platform']}: nothing loaded into {table} for {_qname(side)}"

    def match_columns(self):
        self._cli("match-columns", project_db=self.wd["db"],
                  config=self.wd["config"], mode="merge", yes=True)

    def compare(self, kind):
        html = Path(self.wd["out"]) / f"compare_{kind}.html"
        if kind == "row":
            self._cli("compare-row", project_db=self.wd["db"], config=self.wd["config"],
                      from_date=None, to_date=None, yes=True, html=str(html),
                      title=None, subtitle=None)
        else:
            self._cli("compare-col", project_db=self.wd["db"], config=self.wd["config"],
                      from_date=None, to_date=None, no_date_filter=True, vintage="day",
                      yes=True, html=str(html), title=None, subtitle=None)

    def assert_html(self, kind):
        html = Path(self.wd["out"]) / f"compare_{kind}.html"
        assert html.exists(), f"compare-{kind} produced no HTML report"


# >>> parametrize every test over the named pairs (mark-selectable via -m) <<< #
@pytest.fixture(params=_pair_params())
def pair_name(request):
    return request.param


# >>> per-pair isolated sandbox: temp db + one-pair config wiring the real tables <<< #
@pytest.fixture
def live_workdir(pair_name, tmp_path):
    csv_dir = tmp_path / "csv"; csv_dir.mkdir()
    out_dir = tmp_path / "output"; out_dir.mkdir()
    db = str(tmp_path / "dtrack.db")
    config = str(tmp_path / "dtrack.json")
    init_database(db)
    _build_config(config, pair_name)
    return {"pair_name": pair_name, "pair": LIVE_PAIRS[pair_name],
            "csv": str(csv_dir), "out": str(out_dir), "db": db, "config": config}


# >>> stage operations bound to this pair's sandbox <<< #
@pytest.fixture
def pipe(live_workdir):
    return _Pipe(live_workdir)
