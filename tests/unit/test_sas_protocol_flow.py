from __future__ import annotations

import csv
import json

import pytest
from support import oracle_config, pairs_config

from dtrack_left.backends.sources import FileSource, write_rows
from dtrack_left.checks.sas import export_sas_bundle, import_sas_bundle
from dtrack_left.cli.config import validate_config
from dtrack_left.common.manifest import build_manifest
from dtrack_local.csv_target import CsvAnchorTarget
from dtrack_right.backends.store import VolumeStore
from dtrack_right.job import run as run_right


def _all_partition_config():
    return validate_config(
        {
            "version": 2,
            "pairs": {
                "ledger": {
                    "left": {
                        "source": "oracle",
                        "table": "LEDGER",
                        "date_col": "BUSINESS_DATE",
                        "date_type": "date",
                    },
                    "right": {
                        "source": "databricks",
                        "table": "main.finance.ledger",
                        "date_col": "business_date",
                        "date_type": "date",
                    },
                    "vintage": "week",
                    "col_map": {"AMOUNT": "amount"},
                }
            },
        }
    )


def test_sas_export_defaults_to_every_counted_partition(tmp_path):
    bundle = tmp_path / "sas"
    result = export_sas_bundle(_all_partition_config(), bundle, run_id="all-partitions")
    script = (bundle / "left-run.sas").read_text()
    request = json.loads((bundle / "request.json").read_text())

    assert result["pairs"] == ["ledger"]
    assert request["pairs"]["ledger"]["config"]["excluded_partitions"] == []
    assert "FROM LEDGER WHERE 1=1 GROUP BY" in script
    assert "DATE '" not in script
    assert "then delete;" not in script
    assert "select distinct dtrack_bucket into :dtrack_bucket1-" in script
    assert "intnx('week.2', dtrack_date, 0, 'b')" in script


def test_sas_export_pushes_each_vintage_into_query_windows(tmp_path):
    config = _all_partition_config()
    config["pairs"]["ledger"]["left"].update(
        query=(
            "WITH driver AS (SELECT * FROM LEDGER d WHERE {{query_windows:driver}}) "
            "SELECT BUSINESS_DATE, AMOUNT FROM driver"
        ),
        query_windows={"driver": {"expression": "d.BUSINESS_DATE", "date_type": "date"}},
    )
    validate_config(config)
    bundle = tmp_path / "sas"
    export_sas_bundle(config, bundle, run_id="query-windows")
    script = (bundle / "left-run.sas").read_text()

    assert "{{query_windows:" not in script
    assert "d.BUSINESS_DATE" in script
    assert "IN (&dtrack_partitions)" in script


def test_sas_export_renders_the_configured_query_projection(tmp_path):
    config = _all_partition_config()
    config["pairs"]["ledger"]["left"].update(
        query=("SELECT {{dtrack_projection}} FROM LEDGER d WHERE {{query_windows:driver}}"),
        query_windows={"driver": {"expression": "d.BUSINESS_DATE", "date_type": "date"}},
        query_projection={
            "BUSINESS_DATE": "d.BUSINESS_DATE",
            "AMOUNT": "d.AMOUNT",
        },
    )
    validate_config(config)
    bundle = tmp_path / "sas"

    export_sas_bundle(config, bundle, run_id="query-projection")
    script = (bundle / "left-run.sas").read_text()

    assert "{{dtrack_projection}}" not in script
    assert "d.BUSINESS_DATE AS BUSINESS_DATE" in script
    assert "d.AMOUNT AS AMOUNT" in script


def test_sas_export_canonicalizes_only_the_final_sample_projection(tmp_path):
    config = _all_partition_config()
    pair = config["pairs"]["ledger"]
    pair["key"] = "ACCOUNT_ID"
    pair["sample"] = 1
    pair["col_map"] = {"ACCOUNT_ID": "account_id", "AMOUNT": "amount"}
    validate_config(config)

    bundle = tmp_path / "sas"
    export_sas_bundle(config, bundle, run_id="canonical-sample")
    script = (bundle / "left-run.sas").read_text()

    assert "REGEXP_SUBSTR(DUMP(BUSINESS_DATE, 10)" in script
    assert "REGEXP_SUBSTR(DUMP(ACCOUNT_ID, 10)" in script
    assert "REGEXP_SUBSTR(DUMP(AMOUNT, 10)" in script
    assert (
        "%let dtrack_sample_1=TO_CHAR(BUSINESS_DATE, 'YYYY-MM-DD HH24:MI:SS') AS \"BUSINESS_DATE\";"
    ) in script
    assert (
        "%let dtrack_sample_2=TO_CHAR(ACCOUNT_ID, 'TM9', "
        "'NLS_NUMERIC_CHARACTERS=''.,''') AS \"ACCOUNT_ID\";"
    ) in script
    assert (
        "%let dtrack_sample_projection=&dtrack_sample_1, &dtrack_sample_2, &dtrack_sample_3;"
    ) in script
    assert "SELECT &dtrack_sample_projection FROM LEDGER WHERE" in script
    assert not list(bundle.rglob("*schema*"))

    # Counts and fixed-key selection keep their existing expressions and data types.
    assert "AS dtrack_partition, COUNT(*) AS dtrack_count" in script
    assert 'SELECT ACCOUNT_ID AS "ACCOUNT_ID", dtrack_norm FROM' in script


def _simulate_sas_output(config, bundle, left_root):
    request = json.loads((bundle / "request.json").read_text())
    file_pairs = pairs_config(left_root)["pairs"]
    for name, row in request["pairs"].items():
        pair = config["pairs"][name]
        source = FileSource(file_pairs[name]["left"])
        counts = source.partition_counts(pair.get("fromDate"), pair.get("toDate"))
        pair_root = bundle / "output" / row["directory"]
        write_rows(
            pair_root / "counts.csv",
            ["dtrack_partition", "dtrack_count"],
            sorted(counts.items()),
        )
        baseline = {
            "gate_decision": {"passed": True},
            "partitions": {
                day: {"left": count, "right": None, "verdict": "left_only"}
                for day, count in counts.items()
            },
        }
        manifest = build_manifest(request["run_id"], name, pair, baseline)
        for window in manifest["windows"]:
            if manifest["sample"]["mode"] == "fixed":
                key_path = pair_root / "keys" / f"{window['bucket']}.csv"
                source.sample_keys(
                    window["partitions"],
                    manifest["key_cols"],
                    key_path,
                    count=manifest["sample"]["count"],
                    key_families=manifest["key_families"],
                )
                with key_path.open(encoding="utf-8", newline="") as handle:
                    rows = csv.DictReader(handle)
                    window["key_tuples"] = [
                        [row[column] or None for column in manifest["key_cols"]] for row in rows
                    ]
            source.pull_window(
                window["partitions"],
                manifest["columns"],
                pair_root / "windows" / f"{window['bucket']}.csv",
                key_columns=manifest["key_cols"],
                sample_plan=manifest["sample"],
                key_families=manifest["key_families"],
                side="left",
                key_tuples=window.get("key_tuples"),
            )
    marker = {
        "format": request["format"],
        "run_id": request["run_id"],
        "config_hash": request["config_hash"],
    }
    (bundle / "output" / "_SUCCESS.json").write_text(
        json.dumps(marker, sort_keys=True, separators=(",", ":")), encoding="utf-8"
    )


# >>> real files and protocol objects exercise SAS import through the complete Right job <<< #
@pytest.mark.slow
def test_simulated_sas_bundle_runs_through_right_comparison_and_report(synthetic, tmp_path):
    config = oracle_config({})
    config["version"] = 2
    config["pairs"]["orders"]["excluded_partitions"] = [
        "2026-01-15",
        "2026-01-31",
        "2026-02-10",
    ]
    validate_config(config)
    bundle = tmp_path / "sas"
    export_sas_bundle(config, bundle, run_id="sas-flow")
    _simulate_sas_output(config, bundle, synthetic.left)

    runs = tmp_path / "runs"
    store = VolumeStore(runs / "sas-flow")
    imported = import_sas_bundle(
        config,
        bundle,
        store,
        run_id="sas-flow",
        pull_workers=1,
        stat_workers=1,
    )
    assert imported == {
        "run_id": "sas-flow",
        "pairs": ["orders", "customers"],
        "published_units": 11,
    }
    assert store.get_json("pairs/orders/row_check/left.json")["preflight"] == {
        "ok": True,
        "source": "oracle",
        "executor": "sas",
    }

    def target_factory(spec):
        return CsvAnchorTarget(spec, synthetic.right, tmp_path / "right-work")

    outcome = run_right(
        str(runs),
        target_factory,
        run_id="sas-flow",
        out_dir=tmp_path / "report",
        poll_interval=0,
        idle_timeout=5,
        wait_timeout=5,
    )
    assert outcome["results"] == 11
    assert outcome["summary"]["status"] == "DONE"
    assert outcome["summary"]["findings"] == {
        "row_count_diff": 1,
        "partition_missing": 2,
        "col_stat_diff": 2,
    }
    assert store.exists("report/report.xlsx")
