from __future__ import annotations

import csv
import io
import logging
import time
from collections import Counter
from typing import Any

from ..common.dates import apply_synonyms
from ..common.manifest import build_manifest


def reconcile_partitions(
    left_counts: dict[str, int],
    right_counts: dict[str, int],
    date_synonyms: dict[str, str] | None = None,
) -> dict[str, dict[str, Any]]:
    left = apply_synonyms(left_counts, date_synonyms)
    right = apply_synonyms(right_counts, date_synonyms)
    result: dict[str, dict[str, Any]] = {}
    for partition in sorted(set(left) | set(right)):
        left_value = left.get(partition)
        right_value = right.get(partition)
        if left_value is None:
            verdict = "right_only"
        elif right_value is None:
            verdict = "left_only"
        elif int(left_value) == int(right_value):
            verdict = "match"
        else:
            verdict = "count_diff"
        row: dict[str, Any] = {
            "left": left_value,
            "right": right_value,
            "verdict": verdict,
        }
        if verdict == "count_diff":
            row["delta"] = int(right_value) - int(left_value)
        result[partition] = row
    return result


def summarize_partitions(partitions: dict[str, dict[str, Any]]) -> dict[str, int]:
    counts = Counter(row["verdict"] for row in partitions.values())
    return {name: counts[name] for name in ("match", "count_diff", "left_only", "right_only")}


def evaluate_gate(partitions: dict[str, dict[str, Any]], policy: str = "auto") -> dict[str, Any]:
    policy = (policy or "auto").strip().lower()
    total = len(partitions)
    matched = sum(row.get("verdict") == "match" for row in partitions.values())
    excluded_ratio = 0.0 if total == 0 else (total - matched) / total
    result = {
        "policy": policy,
        "excluded_ratio": excluded_ratio,
        "matched_partitions": matched,
        "total_partitions": total,
    }
    if policy == "manual":
        return {**result, "passed": False, "reason": "manual_review"}
    if policy == "auto":
        return {**result, "passed": matched > 0, "reason": "auto"}
    if policy.startswith("ratio:"):
        threshold = float(policy.split(":", 1)[1])
        if not 0 <= threshold <= 1:
            raise ValueError("ratio gate must be between 0 and 1")
        return {
            **result,
            "threshold": threshold,
            "passed": matched > 0 and excluded_ratio <= threshold,
            "reason": "ratio",
        }
    raise ValueError("gate must be auto, manual, or ratio:<0..1>")


log = logging.getLogger("dtrack.right.row_check")


# >>> row-check rows as CSV: pair,partition,left,right,delta,verdict <<< #
def row_check_csv_bytes(document: dict[str, Any]) -> bytes:
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
    for key, document in store.list_json("row_check/"):
        if not key.endswith(".left.json"):
            continue
        if store.exists(f"row_check/{document['pair']}.json"):
            continue
        pending.append(document)
    return pending


# >>> count the Right side, reconcile, gate; CSV and manifest first, the verdict last as the commit marker <<< #
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
    store.put_bytes(
        f"reports/{pair}_compare_row.csv", row_check_csv_bytes(document), content_type="text/csv"
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
    store.put_json(f"row_check/{pair}.json", document)
    return document


# >>> process every pending left document; close the row check once the left side has closed and nothing is pending <<< #
def run_row_check(store, target_factory, *, gate: str = "auto") -> int:
    processed = 0
    for left_doc in pending_left_documents(store):
        reconcile_pair(store, left_doc, target_factory(left_doc["right"]), gate=gate)
        processed += 1
    if store.exists("row_check/_LEFT_CLOSED") and not store.exists("row_check/_CLOSED"):
        pairs = store.get_json("row_check/_LEFT_CLOSED")["pairs"]
        if all(store.exists(f"row_check/{pair}.json") for pair in pairs):
            store.close_row_check(pairs)
            released = [pair for pair in pairs if store.exists(f"manifest/{pair}.json")]
            log.info("row_check closed: %d/%d pairs released", len(released), len(pairs))
    return processed
