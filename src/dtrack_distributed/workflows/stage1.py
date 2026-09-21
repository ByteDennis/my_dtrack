from __future__ import annotations

import csv
import hashlib
import io
import json
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pandas as pd

from ..adapters.sources.factory import source_from_config
from ..adapters.targets.databricks import DatabricksTarget
from ..domain.gates import evaluate_gate, reconcile_partitions, summarize_partitions
from ..domain.manifest import build_manifest
from ..engine.sampling import sample_csv


def make_run_id(config: dict[str, Any], *, now: datetime | None = None) -> str:
    now = now or datetime.now(UTC)
    digest = hashlib.sha256(
        json.dumps(config, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()[:6]
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}-{digest}"


def stage1_csv_bytes(document: dict[str, Any]) -> bytes:
    output = io.StringIO(newline="")
    writer = csv.DictWriter(
        output, fieldnames=["pair", "partition", "left", "right", "delta", "verdict"]
    )
    writer.writeheader()
    for partition, row in sorted(document["partitions"].items()):
        writer.writerow(
            {
                "pair": document["pair"],
                "partition": partition,
                "left": row.get("left"),
                "right": row.get("right"),
                "delta": row.get("delta", ""),
                "verdict": row["verdict"],
            }
        )
    return output.getvalue().encode()


def _document(
    run_id: str,
    name: str,
    pair: dict[str, Any],
    health: dict[str, Any],
    left_counts: dict[str, int],
    right_counts: dict[str, int],
    gate: str,
) -> dict[str, Any]:
    partitions = reconcile_partitions(left_counts, right_counts, pair.get("date_synonyms"))
    return {
        "run_id": run_id,
        "pair": name,
        "left": pair["left"],
        "right": pair["right"],
        "bounds": {"from": pair.get("fromDate"), "to": pair.get("toDate")},
        "partitions": partitions,
        "summary": summarize_partitions(partitions),
        "gate_decision": evaluate_gate(partitions, gate),
        "preflight": health,
    }


def run_stage1(
    config: dict[str, Any],
    store,
    *,
    run_id: str | None = None,
    gate: str = "auto",
    pair_names: list[str] | None = None,
    source_factory: Callable[[dict[str, Any]], Any] = source_from_config,
    target_factory: Callable[[dict[str, Any]], Any] = DatabricksTarget,
    kickstarter=None,
    report_store=None,
    pair_workers: int = 4,
    work_dir: str | Path = ".dtrack-work/stage1",
) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
    run_id = run_id or make_run_id(config)
    pairs = {
        name: pair
        for name, pair in config["pairs"].items()
        if not pair.get("skip") and (not pair_names or name in pair_names)
    }
    if not pairs:
        raise ValueError("no active pairs selected")
    store.put_json("_STATUS.json", {"state": "preflight", "run_id": run_id})

    # All Right-side preflights must pass before any expensive count begins.
    with ThreadPoolExecutor(max_workers=max(1, pair_workers)) as pool:
        futures = {
            name: pool.submit(target_factory(pair["right"]).preflight)
            for name, pair in pairs.items()
        }
        health = {name: future.result() for name, future in futures.items()}
    store.put_json(
        "_PREFLIGHT.json", {"ok": all(row.get("ok") for row in health.values()), "pairs": health}
    )
    failed = [name for name, row in health.items() if not row.get("ok")]
    if failed:
        raise RuntimeError(f"Databricks preflight failed: {', '.join(failed)}")

    # Finish every Right-side exact count before starting any Left-side count.
    def count(target, pair):
        started = time.monotonic()
        values = target.partition_counts(pair.get("fromDate"), pair.get("toDate"))
        return values, round(time.monotonic() - started, 6)

    with ThreadPoolExecutor(max_workers=max(1, pair_workers)) as pool:
        right_futures = {
            name: pool.submit(count, target_factory(pair["right"]), pair)
            for name, pair in pairs.items()
        }
        right = {name: future.result() for name, future in right_futures.items()}
    with ThreadPoolExecutor(max_workers=max(1, pair_workers)) as pool:
        left_futures = {
            name: pool.submit(count, source_factory(pair["left"]), pair)
            for name, pair in pairs.items()
        }
        left = {name: future.result() for name, future in left_futures.items()}

    documents, manifests = [], []
    kicked = False
    for name, pair in pairs.items():
        doc = _document(run_id, name, pair, health[name], left[name][0], right[name][0], gate)
        doc["timing"] = {"left_count_s": left[name][1], "right_count_s": right[name][1]}
        store.put_json(f"stage1/{name}.json", doc)
        report = stage1_csv_bytes(doc)
        store.put_bytes(f"reports/{name}_compare_row.csv", report, content_type="text/csv")
        if report_store:
            report_store.put_bytes(f"{name}_compare_row.csv", report, content_type="text/csv")
        documents.append(doc)
        if doc["gate_decision"]["passed"]:
            manifest = build_manifest(run_id, name, pair, doc)
            if manifest["sample"]["mode"] == "fixed":
                source = source_factory(pair["left"])
                for window in manifest["windows"]:
                    output = Path(work_dir) / name / window["bucket"] / "keys.csv"
                    path = source.pull_window(
                        window["partitions"], [], output, key_columns=manifest["key_cols"]
                    )
                    sampled = sample_csv(
                        path,
                        key_columns=manifest["key_cols"],
                        mode="fixed",
                        count=manifest["sample"]["count"],
                        families=manifest["key_families"],
                    )
                    key_rows = pd.read_csv(sampled, dtype=object)[manifest["key_cols"]]
                    window["key_tuples"] = key_rows.drop_duplicates().values.tolist()
            store.put_json(f"manifest/{name}.json", manifest)
            manifests.append(manifest)
            if kickstarter and not kicked:
                kickstarter.start(run_id=run_id, s3_root=store.root_uri)
                kicked = True
    store.close_stage1(list(pairs))
    store.put_json(
        "_STATUS.json",
        {"state": "stage1_closed", "run_id": run_id, "released_pairs": len(manifests)},
    )
    return run_id, documents, manifests
