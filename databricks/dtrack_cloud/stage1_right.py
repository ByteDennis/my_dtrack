from __future__ import annotations

import csv
import io
import logging
import time
from typing import Any

from .gates import evaluate_gate, reconcile_partitions, summarize_partitions
from .manifest import build_manifest

log = logging.getLogger("dtrack.cloud.stage1")


# >>> Stage 1 rows as CSV: pair,partition,left,right,delta,verdict <<< #
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


# >>> pending left-side count documents that have no Right-side verdict yet <<< #
def pending_left_documents(store) -> list[dict[str, Any]]:
    pending = []
    for key, document in store.list_json("stage1/"):
        if not key.endswith(".left.json"):
            continue
        if store.exists(f"stage1/{document['pair']}.json"):
            continue
        pending.append(document)
    return pending


# >>> count the Right side, reconcile against the left counts, gate, write verdict + manifest <<< #
def reconcile_pair(
    store, left_doc: dict[str, Any], target, *, gate: str = "auto"
) -> dict[str, Any]:
    pair = left_doc["pair"]
    started = time.monotonic()
    health = target.preflight()
    preflight_s = round(time.monotonic() - started, 6)
    if not health.get("ok"):
        log.error("preflight %s failed: %s", pair, health.get("error"))
        raise RuntimeError(f"Right-side preflight failed for {pair}: {health.get('error')}")
    started = time.monotonic()
    right_counts = target.partition_counts(
        left_doc["bounds"].get("from"), left_doc["bounds"].get("to")
    )
    right_s = round(time.monotonic() - started, 6)
    log.info(
        "right count %s: %d partitions, %d rows (%.3fs)",
        pair,
        len(right_counts),
        sum(right_counts.values()),
        right_s,
    )
    partitions = reconcile_partitions(
        left_doc["counts"], right_counts, left_doc.get("date_synonyms")
    )
    document = {
        "run_id": left_doc["run_id"],
        "pair": pair,
        "left": left_doc["left"],
        "right": left_doc["right"],
        "bounds": left_doc["bounds"],
        "partitions": partitions,
        "summary": summarize_partitions(partitions),
        "gate_decision": evaluate_gate(partitions, gate),
        "preflight": health,
        "timing": {
            "preflight_s": preflight_s,
            "left_count_s": left_doc.get("timing", {}).get("left_count_s"),
            "right_count_s": right_s,
        },
    }
    log.info("reconcile %s: %s", pair, document["summary"])
    for partition, row in sorted(partitions.items()):
        if row["verdict"] != "match":
            log.info("  finding %s %s left=%s right=%s", pair, partition, row["left"], row["right"])
    decision = document["gate_decision"]
    log.info(
        "gate %s: policy=%s passed=%s excluded_ratio=%.4f",
        pair,
        decision["policy"],
        decision["passed"],
        decision["excluded_ratio"],
    )
    store.put_json(f"stage1/{pair}.json", document)
    store.put_bytes(
        f"reports/{pair}_compare_row.csv", stage1_csv_bytes(document), content_type="text/csv"
    )
    if decision["passed"]:
        manifest = build_manifest(left_doc["run_id"], pair, left_doc["pair_config"], document)
        store.put_json(f"manifest/{pair}.json", manifest)
        log.info(
            "manifest %s: %d windows x %d columns = %d units -> manifest/%s.json",
            pair,
            len(manifest["windows"]),
            len(manifest["columns"]),
            manifest["expected_units"],
            pair,
        )
    return document


# >>> process every pending left document; close stage1 once the left side has closed and nothing is pending <<< #
def run_stage1_right(store, target_factory, *, gate: str = "auto") -> int:
    processed = 0
    for left_doc in pending_left_documents(store):
        reconcile_pair(store, left_doc, target_factory(left_doc["right"]), gate=gate)
        processed += 1
    if store.exists("stage1/_LEFT_CLOSED") and not store.exists("stage1/_CLOSED"):
        pairs = store.get_json("stage1/_LEFT_CLOSED")["pairs"]
        if all(store.exists(f"stage1/{pair}.json") for pair in pairs):
            store.close_stage1(pairs)
            released = [pair for pair in pairs if store.exists(f"manifest/{pair}.json")]
            log.info("stage1 closed: %d/%d pairs released", len(released), len(pairs))
    return processed
