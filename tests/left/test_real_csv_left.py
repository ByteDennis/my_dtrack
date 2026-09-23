import json

import pandas as pd
import pytest
from support import pairs_config, synthetic_data

from dtrack_left.col_check import ColCheckPipeline, LeftRuntime, wait_for_manifests
from dtrack_left.manifest import build_manifest
from dtrack_left.row_check import run_row_check
from dtrack_left.sampling import sample_csv
from dtrack_left.sources import CsvSource, source_from_config
from dtrack_left.statistics import compute_csv_statistics
from dtrack_right.store import VolumeStore

ORDERS_LEFT = {"source": "csv", "date_col": "BUSINESS_DATE"}


@pytest.fixture(scope="module")
def data(tmp_path_factory):
    root = tmp_path_factory.mktemp("synthetic")
    summary = synthetic_data(root)
    return root, summary


def test_csv_source_counts_every_real_partition_exactly(data):
    root, summary = data
    source = CsvSource({**ORDERS_LEFT, "path": str(root / "left/ORDERS.csv")})
    assert source.preflight()["ok"]
    counts = source.partition_counts("2026-01-01", "2026-02-28")
    assert len(counts) == 58 and sum(counts.values()) == summary["orders"]["left_rows"] == 1740
    assert "2026-01-31" not in counts and counts["2026-01-15"] == 30
    assert source.partition_counts("2026-02-01", "2026-02-28") == {
        k: v for k, v in counts.items() if k >= "2026-02-01"
    }


def test_pull_window_honours_holes_and_keeps_only_requested_columns(data, tmp_path):
    root, _ = data
    source = CsvSource({**ORDERS_LEFT, "path": str(root / "left/ORDERS.csv")})
    days = [f"2026-01-{d:02d}" for d in range(1, 31) if d not in (15, 31)]
    out = source.pull_window(
        days, ["AMOUNT", "STATUS"], tmp_path / "w.csv", key_columns=["ORDER_ID"]
    )
    frame = pd.read_csv(out, dtype=object)
    assert list(frame.columns) == ["BUSINESS_DATE", "ORDER_ID", "AMOUNT", "STATUS"]
    assert len(frame) == 29 * 30 and "2026-01-15" not in set(frame["BUSINESS_DATE"])


def test_fraction_sampling_is_deterministic_and_identical_across_sides(data, tmp_path):
    root, _ = data
    left = pd.read_csv(root / "left/ORDERS.csv", dtype=object)
    right = pd.read_csv(root / "right/orders.csv", dtype=object)
    common = sorted(set(left["BUSINESS_DATE"]) & set(right["business_date"]))
    left[left["BUSINESS_DATE"].isin(common)].to_csv(tmp_path / "l.csv", index=False)
    right[right["business_date"].isin(common)].rename(columns=str.upper).to_csv(
        tmp_path / "r.csv", index=False
    )
    kwargs = {
        "key_columns": ["ORDER_ID"],
        "mode": "fraction",
        "rate": 0.25,
        "families": ["numeric"],
    }
    a = pd.read_csv(sample_csv(tmp_path / "l.csv", **kwargs), dtype=object)
    b = pd.read_csv(sample_csv(tmp_path / "l.csv", **kwargs), dtype=object)
    c = pd.read_csv(sample_csv(tmp_path / "r.csv", **kwargs), dtype=object)
    assert a.equals(b)
    dropped = {"2026-01-15"}
    left_keys = set(a[~a["BUSINESS_DATE"].isin(dropped)]["ORDER_ID"])
    right_keys = set(c[~c["BUSINESS_DATE"].isin(dropped)]["ORDER_ID"])
    assert (
        left_keys == right_keys
        and 0.15 < len(a) / len(left[left["BUSINESS_DATE"].isin(common)]) < 0.35
    )


def test_statistics_see_the_injected_status_and_amount_drift(data, tmp_path):
    root, _ = data
    left = pd.read_csv(root / "left/ORDERS.csv", dtype=object)
    right = pd.read_csv(root / "right/orders.csv", dtype=object).rename(columns=str.upper)
    for name, frame in (("l", left), ("r", right)):
        frame[
            frame["BUSINESS_DATE"].str.startswith("2026-01")
            & ~frame["BUSINESS_DATE"].isin(["2026-01-15", "2026-01-31"])
        ].to_csv(tmp_path / f"{name}.csv", index=False)
    stats_l = compute_csv_statistics(tmp_path / "l.csv", ["STATUS", "REGION", "AMOUNT"])
    stats_r = compute_csv_statistics(tmp_path / "r.csv", ["STATUS", "REGION", "AMOUNT"])
    assert stats_l["STATUS"]["n_unique"] == 4 and stats_r["STATUS"]["n_unique"] == 8
    assert stats_l["REGION"] == stats_r["REGION"]
    assert stats_l["AMOUNT"] == stats_r["AMOUNT"]
    strict_r = compute_csv_statistics(tmp_path / "r.csv", ["STATUS"], strict=True)
    strict_l = compute_csv_statistics(tmp_path / "l.csv", ["STATUS"], strict=True)
    assert strict_l["STATUS"]["hash_sum"] == strict_r["STATUS"]["hash_sum"]


def test_row_check_then_col_check_on_real_files_and_a_real_directory_store(data, tmp_path):
    root, _ = data
    store = VolumeStore(tmp_path / "run")
    config = pairs_config(root / "left")
    run_id, _documents = run_row_check(config, store, run_id="real")
    orders = store.get_json("row_check/orders.left.json")
    assert (
        orders["counts"]["2026-01-15"] == 30
        and "path" in orders["left"]
        and "dsn" not in orders["left"]
    )
    assert store.exists("row_check/_LEFT_CLOSED")
    right_counts = CsvSource(
        {"source": "csv", "path": str(root / "right/orders.csv"), "date_col": "business_date"}
    ).partition_counts("2026-01-01", "2026-02-28")
    from dtrack_right.gates import evaluate_gate, reconcile_partitions, summarize_partitions

    partitions = reconcile_partitions(orders["counts"], right_counts)
    verdict_doc = {
        "partitions": partitions,
        "summary": summarize_partitions(partitions),
        "gate_decision": evaluate_gate(partitions, "auto"),
    }
    manifest = build_manifest(run_id, "orders", orders["pair_config"], verdict_doc)
    assert [len(w["partitions"]) for w in manifest["windows"]] == [29, 27]
    store.put_json("manifest/orders.json", manifest)
    store.put_json("row_check/_CLOSED", {"pairs": ["orders"]})
    manifests = wait_for_manifests(store, timeout=1, poll_interval=0)
    runtime = LeftRuntime(tmp_path / "work", store)
    published = ColCheckPipeline(
        store, pull_window=runtime.pull_window, compute_stats=runtime.compute_stats
    ).run(manifests)
    assert published == 8 and store.get_json("manifest/_CLOSED")["expected_units"] == 8
    pointers = [row for _, row in store.list_json("_INDEX/")]
    unit = store.get_json(pointers[0]["payload_key"])
    assert unit["n_total"] > 0 and set(unit) >= {"n_unique", "hash_sum", "min_val", "max_val"}
    assert sorted({row["column"] for row in pointers}) == ["AMOUNT", "ORDER_ID", "REGION", "STATUS"]
    assert json.loads((tmp_path / "run" / "_HEARTBEAT").read_text())["epoch_s"] > 0
    assert source_from_config(orders["left"]).preflight()["ok"]
