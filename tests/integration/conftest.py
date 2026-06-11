"""Shared wiring for the live integration matrix (tests/integration/).

Unlike tests/e2e/ (DTRACK_MOCK, fixture CSVs) and tests/api/ (HTTP plumbing,
tolerates 200/500), these tests actually connect to the real backends and run
dtrack's generated SQL against REAL tables, across the matrix
left in {oracle, hadoop} x right in {athena, databricks}.

One pipeline stage per file (test_01_extract_row ... test_06_compare_col). The
`combo` fixture parametrizes every test over the matrix; `live_workdir` gives
each test an isolated sandbox; `pipe` bundles the stage operations bound to it.

Skipped by default -- opt in with CLI flags (no env vars):
    pytest tests/integration --run-live
    pytest tests/integration --run-live --run-databricks

BEFORE RUNNING: fill in LIVE_TABLES below with tables that exist on YOUR
servers (table name, conn_macro, date column + its type).
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


# >>> fill these in with tables that actually exist on YOUR servers <<< #
LIVE_TABLES = {
    "oracle": {
        "table": "SCHEMA.YOUR_ORACLE_TABLE", "conn_macro": "pb23",
        "date_col": "RPT_DT", "date_type": "date",
    },
    "hadoop": {
        "table": "db.your_hive_table", "conn_macro": "hive_prod",
        "date_col": "rpt_dt", "date_type": "string_dash",
    },
    "athena": {
        "table": "your_athena_table", "conn_macro": "analytics_db",
        "date_col": "rpt_dt", "date_type": "string_dash",
    },
    "databricks": {
        "table": "your_databricks_table", "conn_macro": "main.analytics",
        "date_col": "rpt_dt", "date_type": "date",
    },
}

# >>> direct extract fn per platform; hadoop is direct ODBC (extract_hadoop) -- no gen-hadoop CLI command <<< #
_EXTRACT_FN = {
    "oracle": extract_oracle, "hadoop": extract_hadoop,
    "athena": extract_aws, "databricks": extract_databricks,
}

# >>> config `source` value (= row/col-CSV {source}_{name} prefix); athena's source key is 'aws' <<< #
_SOURCE = {
    "oracle": "oracle", "hadoop": "hadoop", "athena": "aws", "databricks": "databricks",
}

LEFT = ["oracle", "hadoop"]
RIGHT = ["athena", "databricks"]


def pytest_addoption(parser):
    parser.addoption("--run-live", action="store_true", default=False,
                     help="run live integration tests (need real .env + LIVE_TABLES filled in)")
    parser.addoption("--run-databricks", action="store_true", default=False,
                     help="include the slow databricks live combos")


# >>> True when this test's combo has databricks on either side <<< #
def _is_databricks(item):
    cs = getattr(item, "callspec", None)
    return bool(cs) and "databricks" in cs.params.get("combo", ())


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


# >>> the (left, right) matrix; databricks combos are gated in modifyitems via the combo param <<< #
def _combo_params():
    return [pytest.param((left, right), id=f"{left}_x_{right}")
            for left in LEFT for right in RIGHT]


# >>> {source}_{name} qname dtrack uses for a side's stored rows / output CSV <<< #
def _qname(plat):
    return f"{_SOURCE[plat]}_{plat}_probe"


# >>> assemble a one-pair config wiring a real left table against a real right table <<< #
def _build_config(path, left, right):
    def _side(plat):
        spec = LIVE_TABLES[plat]
        return {
            "name": f"{plat}_probe", "table": spec["table"],
            "source": _SOURCE[plat], "conn_macro": spec["conn_macro"],
            "date_col": spec["date_col"], "date_type": spec["date_type"],
        }
    cfg = {
        "metadata": {"title": "live-matrix"},
        "settings": {"sas_outdir": "./sas/", "aws_outdir": "./csv/",
                     "from_date": "", "to_date": ""},
        "pairs": {f"{left}_vs_{right}": {
            "description": f"live: {left} vs {right}",
            "left": _side(left), "right": _side(right),
            "mode": "incremental", "fromDate": "", "toDate": "",
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


# >>> live-pipeline operations bound to one combo's sandbox -- the per-stage files drive this <<< #
class _Pipe:
    def __init__(self, wd):
        self.wd = wd
        self.sides = (wd["left"], wd["right"])

    def _cli(self, sub, **kw):
        getattr(cli_mod, f"cmd_{sub.replace('-', '_')}")(Namespace(**kw))

    def extract(self, kind):
        for plat in self.sides:
            _EXTRACT_FN[plat](self.wd["config"], self.wd["csv"], types=[kind],
                              max_workers=None, db_path=self.wd["db"], vintage=None,
                              force=False, from_date=None, to_date=None,
                              preset=None, stats_override=None)

    def assert_real_csv(self, kind):
        for plat in self.sides:
            csv = Path(self.wd["csv"]) / f"{_qname(plat)}_{kind}.csv"
            assert csv.exists(), f"{plat}: extract produced no {csv.name} (table absent or SQL failed?)"
            lines = [ln for ln in csv.read_text().splitlines() if ln.strip()]
            assert len(lines) > 1, f"{plat}: {csv.name} has no data rows (header only): {lines[:2]}"

    def load(self, kind):
        if kind == "row":
            self._cli("load-row", project_db=self.wd["db"], folder=self.wd["csv"],
                      config=self.wd["config"], mode="upsert", db=None, date_var=None)
        else:
            self._cli("load-col", project_db=self.wd["db"], folder=self.wd["csv"],
                      config=self.wd["config"], mode="upsert")

    def assert_loaded(self, kind):
        table = "_row_counts" if kind == "row" else "_col_stats"
        for plat in self.sides:
            assert _loaded_count(self.wd["db"], _qname(plat), table) > 0, \
                f"{plat}: nothing loaded into {table} for {_qname(plat)}"

    def match_columns(self):
        self._cli("match-columns", project_db=self.wd["db"],
                  config=self.wd["config"], mode="merge")

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


# >>> parametrize every test over the (left, right) matrix <<< #
@pytest.fixture(params=_combo_params())
def combo(request):
    return request.param


# >>> per-combo isolated sandbox: temp db + one-pair config wiring the real tables <<< #
@pytest.fixture
def live_workdir(combo, tmp_path):
    left, right = combo
    csv_dir = tmp_path / "csv"; csv_dir.mkdir()
    out_dir = tmp_path / "output"; out_dir.mkdir()
    db = str(tmp_path / "dtrack.db")
    config = str(tmp_path / "dtrack.json")
    init_database(db)
    _build_config(config, left, right)
    return {"left": left, "right": right, "csv": str(csv_dir),
            "out": str(out_dir), "db": db, "config": config}


# >>> stage operations bound to this combo's sandbox <<< #
@pytest.fixture
def pipe(live_workdir):
    return _Pipe(live_workdir)
