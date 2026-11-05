import json
import threading
import time
from types import SimpleNamespace

import pandas as pd
import pytest

from pair_recon_left.common.model.hashing import default_normalization_rules
from pair_recon_left.common.runtime import stats


def _series(*values):
    return pd.Series(list(values), dtype=object)


@pytest.mark.parametrize(
    ("counts", "requested", "comparison", "expected"),
    [
        ((3, 3), None, False, ("numeric", False)),
        ((3, 3, 3), None, False, ("categorical", True)),
        ((3, 3, 2), None, False, ("numeric", False)),
        ((3, 2, 2), None, False, ("categorical", False)),
        ((0, 0, 0), None, False, ("categorical", False)),
        ((3, 3, 3), "categorical", False, ("categorical", True)),
        ((3, 3, 3), "numeric", False, ("numeric", True)),
        ((3, 0, 0), "numeric", False, ("numeric", False)),
        ((3, 3, 3), None, True, ("categorical", True)),
    ],
)
def test_infer(counts, requested, comparison, expected):
    assert stats.infer_column_type(*counts, requested=requested, comparison=comparison) == expected


def test_numeric():
    compute = stats.column_statistics
    a = compute(_series("7.50", "2", None))
    b = compute(_series("7.5", "2.0", None))
    assert a["hash_sum"] == b["hash_sum"] and a["n_unique"] == b["n_unique"] == 2
    assert a["col_type"] == "numeric" and a["n_missing"] == 1 and a["n_total"] == 3
    assert a["min_val"] == "2.0" and a["max_val"] == "7.5"
    assert a["mean"] == pytest.approx(4.75) and a["std"] == pytest.approx(3.889087, rel=1e-6)
    assert a["length_avg"] is None and a["top_10"] == ""


@pytest.mark.parametrize("source", ["oracle", "hadoop", "csv", "databricks"])
def test_numeric_type_hashes_equivalent_decimal_text_identically(tmp_path, source):
    left = tmp_path / f"{source}-left.csv"
    right = tmp_path / f"{source}-right.csv"
    left.write_text(
        "AMOUNT\n0.19\n1.0\n01.20\n+4.00\n-0.00\n1e3\n1.23E-4\n5.6475e-05\n1.23e+03\n-.25e+2\ninvalid\n",
        encoding="utf-8",
    )
    right.write_text(
        "AMOUNT\n.19\n1e0\n1.2\n4\n0\n1000\n0.000123\n0.000056475\n1230\n-25\ninvalid\n",
        encoding="utf-8",
    )

    options = {
        "normalization_rules": default_normalization_rules(),
        "normalization_source": source,
        "col_types": {"AMOUNT": "numeric"},
        "statistics": ["n_unique", "hash_sum", "mean", "min", "max"],
    }
    left_stats = stats.compute_csv_statistics(left, ["AMOUNT"], **options)["AMOUNT"]
    right_stats = stats.compute_csv_statistics(right, ["AMOUNT"], **options)["AMOUNT"]

    assert left_stats == right_stats


def test_numeric_type_does_not_round_distinct_high_precision_values_into_one_hash(tmp_path):
    path = tmp_path / "precision.csv"
    path.write_text(
        "AMOUNT\n0.12345678901234567891\n0.12345678901234567892\n1.1e-18\n0.000000000000000001\n",
        encoding="utf-8",
    )

    result = stats.compute_csv_statistics(
        path,
        ["AMOUNT"],
        col_types={"AMOUNT": "numeric"},
        statistics=["n_unique", "hash_sum"],
    )["AMOUNT"]

    assert result["n_unique"] == 4


def test_integral():
    result = stats.column_statistics(_series("100003", "7", "42"))
    assert result["col_type"] == "categorical"
    assert (result["min_val"], result["max_val"]) == ("100003", "7")
    assert result["mean"] is None and result["top_10"] == ""


def test_single():
    assert stats.column_statistics(_series("5"))["std"] is None


def test_category():
    result = stats.column_statistics(_series("b", "aa", "b", None))
    assert result["col_type"] == "categorical"
    assert (result["min_val"], result["max_val"]) == ("aa", "b")
    assert result["length_avg"] == pytest.approx(4 / 3) and result["length_max"] == 2
    assert json.loads(result["top_10"]) == [{"value": "b", "count": 2}, {"value": "aa", "count": 1}]


def test_null():
    result = stats.column_statistics(_series(None, None))
    assert result["col_type"] == "categorical" and result["n_missing"] == 2
    assert result["min_val"] is None and result["length_max"] is None


def test_enabled_normalization_rules_apply_automatically():
    compute = stats.column_statistics
    upper = compute(_series("Gold", "SILVER"))
    lower = compute(_series("gold", "silver"))
    assert upper["hash_sum"] == lower["hash_sum"]


def test_strip_datetime_normalization():
    compute = stats.column_statistics
    timestamp = compute(_series("2026-01-01 00:00:00.000"))
    iso_timestamp = compute(_series("2026-01-01T18:30:00"))
    date = compute(_series("2026-01-01"))

    assert timestamp["hash_sum"] == date["hash_sum"]
    assert iso_timestamp["hash_sum"] == date["hash_sum"]


def test_only_enabled_normalization_rules_apply():
    trim = [{"rule_name": "trim", "source": "all"}]
    left = stats.column_statistics(
        _series(" Gold ", "1.00"),
        normalization_rules=trim,
        col_type="categorical",
    )
    right = stats.column_statistics(
        _series("gold", "1"),
        normalization_rules=trim,
        col_type="categorical",
    )
    assert left["hash_sum"] != right["hash_sum"]

    enabled = default_normalization_rules()
    left = stats.column_statistics(
        _series(" Gold ", "1.00"),
        normalization_rules=enabled,
        col_type="categorical",
    )
    right = stats.column_statistics(
        _series("gold", "1"),
        normalization_rules=enabled,
        col_type="categorical",
    )
    assert left["hash_sum"] == right["hash_sum"]


def test_source_synonyms_support_runtime_values():
    rules = [
        {
            "rule_name": "oracle_missing",
            "source": "oracle",
            "match_value": "9999-12-31",
            "canonical_value": "@missing",
        },
        {
            "rule_name": "oracle_today",
            "source": "oracle",
            "match_value": "3000-01-01",
            "canonical_value": "@run_date",
        },
    ]
    oracle = stats.column_statistics(
        _series("9999-12-31", "3000-01-01"),
        normalization_rules=rules,
        normalization_source="oracle",
        normalization_date="2026-10-28",
    )
    canonical = stats.column_statistics(_series(None, "2026-10-28"))
    unchanged = stats.column_statistics(
        _series("9999-12-31", "3000-01-01"),
        normalization_rules=rules,
        normalization_source="databricks",
        normalization_date="2026-10-28",
    )

    assert oracle["hash_sum"] == canonical["hash_sum"]
    assert oracle["n_missing"] == 1
    assert unchanged["hash_sum"] != canonical["hash_sum"]


def test_date_column_type_normalizes_midnight_timestamp_for_hash_sum(tmp_path):
    left = tmp_path / "left.csv"
    right = tmp_path / "right.csv"
    left.write_text("DT\n2025-10-10 00:00:00.000\n", encoding="utf-8")
    right.write_text("DT\n2025-10-10\n", encoding="utf-8")

    left_stats = stats.compute_csv_statistics(
        left,
        ["DT"],
        column_types={"DT": "date"},
        side="left",
        statistics=["hash_sum"],
    )
    right_stats = stats.compute_csv_statistics(
        right,
        ["DT"],
        column_types={"DT": "date"},
        side="right",
        statistics=["hash_sum"],
    )

    assert left_stats["DT"]["hash_sum"] == right_stats["DT"]["hash_sum"]


def test_csv(tmp_path):
    path = tmp_path / "w.csv"
    path.write_text("ID,AMOUNT\n007,1.10\n008,2\n")
    result = stats.compute_csv_statistics(path, ["ID", "AMOUNT"])
    assert result["ID"]["col_type"] == "categorical"
    assert (result["ID"]["min_val"], result["ID"]["max_val"]) == ("007", "008")
    assert result["ID"]["top_10"] == "" and result["AMOUNT"]["n_unique"] == 2
    selected = stats.compute_csv_statistics(
        path, ["ID", "AMOUNT"], statistics=["hash_sum", "n_unique"]
    )
    assert selected["ID"] == {
        "col_type": "categorical",
        "n_total": 2,
        "n_unique": 2,
        "hash_sum": result["ID"]["hash_sum"],
    }
    assert selected["AMOUNT"] == {
        "col_type": "numeric",
        "n_total": 2,
        "n_unique": 2,
        "hash_sum": result["AMOUNT"]["hash_sum"],
    }
    with pytest.raises(ValueError, match="missing columns: NOPE"):
        stats.compute_csv_statistics(path, ["NOPE"])


def test_type():
    compute = stats.column_statistics
    forced = compute(_series("7.50", "n/a", "2", None), col_type="numeric")
    assert forced["col_type"] == "numeric" and forced["mean"] == pytest.approx(4.75)
    assert (forced["min_val"], forced["max_val"]) == ("2.0", "7.5") and forced["top_10"] == ""
    text = compute(_series("007", "7"), col_type="categorical")
    assert text["col_type"] == "categorical" and text["n_unique"] == 2
    assert (text["min_val"], text["max_val"]) == ("007", "7")
    assert text["top_10"] == ""


def test_csv2(tmp_path):
    path = tmp_path / "w.csv"
    path.write_text("ID,AMOUNT\n007,1.10\n008,2\n")
    result = stats.compute_csv_statistics(path, ["ID", "AMOUNT"], col_types={"ID": "categorical"})
    assert result["ID"]["col_type"] == "categorical" and result["AMOUNT"]["col_type"] == "numeric"


def test_date(tmp_path):
    left = tmp_path / "left.csv"
    right = tmp_path / "right.csv"
    left.write_text("D\n01AUG2026:09:45:35\n02AUG2026:00:00:00\n")
    right.write_text("D\n2026-08-01\n2026-08-02\n")
    spec = {
        "type": "date",
        "left_date_type": {"type": "sas", "format": "datetime"},
        "right_date_type": "date",
    }
    left_stats = stats.compute_csv_statistics(left, ["D"], column_types={"D": spec}, side="left")
    right_stats = stats.compute_csv_statistics(right, ["D"], column_types={"D": spec}, side="right")
    assert left_stats == right_stats
    assert left_stats["D"]["min_val"] == "2026-08-01"


@pytest.mark.slow
def test_batched(tmp_path, monkeypatch):
    import random

    random.seed(7)
    width = 150
    columns = [f"c{i}" for i in range(width)]
    rows = []
    for row in range(40):
        values = []
        for i in range(width):
            if i % 3 == 0:
                values.append("" if row % 7 == 0 else str(random.randint(-50, 50)))
            elif i % 3 == 1:
                values.append(f"{random.random() * 100:.3f}0")
            else:
                values.append(random.choice(["a", "b", "c", "", "ä"]))
        rows.append(values)
    path = tmp_path / "wide.csv"
    path.write_text(
        "\n".join([",".join(columns), *(",".join(values) for values in rows)]) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("PAIR_RECON_STATS_BATCH", "1000")
    single = stats.compute_csv_statistics(path, columns)
    monkeypatch.setenv("PAIR_RECON_STATS_BATCH", "7")
    batched = stats.compute_csv_statistics(path, columns)
    assert batched == single
    assert single["c0"]["col_type"] == "categorical" and single["c0"]["top_10"] == ""
    assert single["c2"]["col_type"] == "categorical"


def test_top(tmp_path):
    path = tmp_path / "ties.csv"
    path.write_text("K\nz\ny\nx\ny\nz\nx\nw\n")
    result = stats.compute_csv_statistics(path, ["K"])
    assert [item["value"] for item in json.loads(result["K"]["top_10"])] == ["z", "y", "x", "w"]


def test_statistics(tmp_path):
    path = tmp_path / "progress.csv"
    path.write_text("N,K\n1,a\n2,b\n", encoding="utf-8")
    events = []

    result = stats.compute_csv_statistics(
        path,
        ["N", "K"],
        progress=lambda phase, completed, total: events.append((phase, completed, total)),
    )

    assert set(result) == {"N", "K"}
    assert ("load", 1, 1) in events
    assert ("infer", 2, 2) in events
    assert ("aggregate", 2, 2) in events
    assert ("finalize", 2, 2) in events


def test_parallel(tmp_path, monkeypatch):
    path = tmp_path / "shards.csv"
    path.write_text("A,B,C,D\n1,x,10,p\n2,y,20,q\n", encoding="utf-8")
    monkeypatch.setenv("PAIR_RECON_STATS_BATCH", "2")
    monkeypatch.setenv("STATS_WORKERS", "2")
    batches = []
    statuses = []
    uploads = threading.Barrier(2)

    def upload(rows):
        uploads.wait(timeout=5)
        batches.append(rows)

    result = stats.compute_csv_statistics(
        path,
        ["A", "B", "C", "D"],
        on_batch=upload,
        shard_status=lambda active, done, total: statuses.append((active, done, total)),
    )

    assert set(result) == {"A", "B", "C", "D"}
    assert len(batches) == 2
    assert {frozenset(batch) for batch in batches} == {
        frozenset({"A", "B"}),
        frozenset({"C", "D"}),
    }
    assert statuses[0] == (2, 0, 2) and statuses[-1] == (0, 2, 2)


def test_invalid(tmp_path):
    left = tmp_path / "l.csv"
    right = tmp_path / "r.csv"
    left.write_text("D\n01AUG2026:09:45:35\n02AUG2026:00:00:00\n")
    right.write_text('D\n2026-08-01\n" "\n')
    spec = "sas_datetime_date"
    left_stats = stats.compute_csv_statistics(left, ["D"], column_types={"D": spec}, side="left")
    right_stats = stats.compute_csv_statistics(right, ["D"], column_types={"D": spec}, side="right")
    assert right_stats["D"]["n_missing"] == 0 and right_stats["D"]["n_unique"] == 2
    assert left_stats["D"]["hash_sum"] != right_stats["D"]["hash_sum"]


class Connection:
    def __init__(self, values, delay=0.1):
        self.values = iter(values)
        self.delay = delay
        self.executed = []

    def query_progress(self):
        value = next(self.values, 99.0)
        if isinstance(value, Exception):
            raise value
        return value

    def execute(self, sql):
        self.executed.append(sql)
        time.sleep(self.delay)
        return self


def test_execute_reporting_reports_known_percentages_then_clears():
    connection = Connection([-1.0, 12.5, 80.0])
    reports = []

    result = stats.execute_reporting(connection, "SELECT 1", reports.append, interval=0.01)

    assert result is connection and connection.executed == ["SELECT 1"]
    assert reports[-1] is None and -1.0 not in reports
    assert reports[:2] == [12.5, 80.0]


def test_execute_reporting_accepts_query_progress_objects():
    connection = Connection([SimpleNamespace(percentage=40.0)])
    reports = []

    stats.execute_reporting(connection, "SELECT 1", reports.append, interval=0.01)

    assert reports[0] == 40.0 and reports[-1] is None


def test_execute_reporting_survives_query_progress_errors(caplog):
    connection = Connection([RuntimeError("no progress")])
    reports = []

    with caplog.at_level("WARNING", logger="pair_recon.left.stats"):
        stats.execute_reporting(connection, "SELECT 1", reports.append, interval=0.01)

    assert reports == [None] and "query_progress failed" in caplog.text


def test_execute_reporting_without_listener_runs_directly():
    connection = Connection([], delay=0)
    before = threading.active_count()

    assert stats.execute_reporting(connection, "SELECT 1") is connection
    assert connection.executed == ["SELECT 1"] and threading.active_count() == before


def test_query_status_is_cleared_after_every_statement(tmp_path, monkeypatch):
    path = tmp_path / "q.csv"
    path.write_text("A,B\n1,x\n2,y\n", encoding="utf-8")
    monkeypatch.setenv("STATS_WORKERS", "1")
    events = []

    stats.compute_csv_statistics(
        path,
        ["A", "B"],
        statistics=["n_unique"],
        query_status=lambda phase, shard, percent: events.append((phase, shard, percent)),
    )

    assert events.count(("load", 0, None)) == 1
    assert events.count(("infer", 0, None)) == 1
    assert events.count(("aggregate", 0, None)) == 1
    assert all(percent is None or percent >= 0 for _phase, _shard, percent in events)


def test_step_family_column_type_normalizes_statistics(tmp_path):
    path = tmp_path / "ids.csv"
    path.write_text("K\n007\n7\n0007\n", encoding="utf-8")

    plain = stats.compute_csv_statistics(path, ["K"], statistics=["n_unique", "hash_sum"])
    typed = stats.compute_csv_statistics(
        path, ["K"], statistics=["n_unique", "hash_sum"], column_types={"K": "numeric"}
    )

    assert plain["K"]["n_unique"] == 3 and typed["K"]["n_unique"] == 1
    assert typed["K"]["col_type"] == "categorical"
